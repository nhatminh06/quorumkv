package raft

import (
	"context"
	"errors"
)

// ErrReadIndexUnavailable is returned by ReadIndex when quorum could not
// be confirmed (not enough peers acknowledged the current-term read
// probe) before ctx's deadline, or before every reachable peer's attempt
// was exhausted. The read is simply unsafe to serve right now; it is not
// evidence the cluster is down.
var ErrReadIndexUnavailable = errors.New("raft: could not confirm read quorum")

// pendingBarrier tracks (at most) one in-flight current-term commit
// barrier — a Raft-internal no-op entry appended solely so a ReadIndex
// call has a current-term committed index to point to (see "why
// current-term commit is required" in docs/read-index.md). Guarded by
// Node.mu; a second ensureCurrentTermCommitted call for the same term
// joins the same pendingBarrier instead of appending its own no-op.
type pendingBarrier struct {
	term  Term
	index LogIndex
	done  chan struct{}
	err   error // valid only after done is closed
}

// hasCurrentTermCommitLocked reports whether the highest committed index
// is known to carry the current term — the safety precondition a leader
// needs before any ReadIndex read, per Raft's ReadIndex protocol. This is
// snapshot-boundary aware: Log.Term already answers correctly for
// commitIndex == the log's compaction boundary (returning the snapshot's
// lastIncludedTerm) without requiring the physical entry to still exist.
// Must be called with n.mu held.
func (n *Node) hasCurrentTermCommitLocked() bool {
	if n.commitIndex == 0 {
		return false
	}
	term, ok := n.log.Term(n.commitIndex)
	return ok && term == n.persistent.CurrentTerm
}

// nextReadContextLocked returns a new ReadContext value, unique among
// this process's currently active ReadIndex probes and never the
// reserved 0. Must be called with n.mu held. Not persisted (see
// docs/read-index.md) — uniqueness only needs to hold for one process
// run's concurrently active reads.
func (n *Node) nextReadContextLocked() ReadContext {
	n.readContextCounter++
	if n.readContextCounter == 0 { // wrapped past math.MaxUint64
		n.readContextCounter = 1
	}
	return ReadContext(n.readContextCounter)
}

// ensureCurrentTermCommitted establishes the current-term commit barrier
// a ReadIndex read requires (see hasCurrentTermCommitLocked): if the
// current term already has a committed entry (an ordinary client write
// already committed this term, or a previous barrier already did), it
// returns immediately with no Raft traffic. Otherwise it appends a
// reserved-empty-command no-op, replicates it through the normal
// AppendEntries path, and waits for it to be committed and applied.
// Concurrent callers in the same term single-flight onto one such no-op
// via Node.pendingBarrier rather than each appending their own.
//
// The no-op's replication/commit wait itself runs bound to n.bgCtx (the
// Node's own lifetime), not ctx: a caller whose ctx expires while the
// barrier is still in flight simply stops waiting on it here, but the
// barrier keeps trying in the background so a differently-timed
// concurrent (or later) caller in the same term can still benefit from
// it, and eventual outcomes (superseded entry, node Close) still resolve
// it rather than leaking the goroutine — see docs/read-index.md.
func (n *Node) ensureCurrentTermCommitted(ctx context.Context) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.persistent.CurrentTerm
	if n.hasCurrentTermCommitLocked() {
		n.mu.Unlock()
		return nil
	}

	var pb *pendingBarrier
	if n.pendingBarrier != nil && n.pendingBarrier.term == term {
		pb = n.pendingBarrier
		n.mu.Unlock()
	} else {
		entry := LogEntry{Term: term, Kind: EntryNoop, Command: nil}
		if err := n.log.Append([]LogEntry{entry}); err != nil {
			n.mu.Unlock()
			return err
		}
		index := n.log.LastIndex()
		n.maybeAdvanceCommitIndexLocked() // handles the single-node-cluster case
		pb = &pendingBarrier{term: term, index: index, done: make(chan struct{})}
		n.pendingBarrier = pb
		n.mu.Unlock()

		n.bgWG.Add(1)
		go func() {
			defer n.bgWG.Done()
			n.replicateToAllPeers(n.bgCtx)
		}()
		n.bgWG.Add(1)
		go func() {
			defer n.bgWG.Done()
			err := n.WaitApplied(n.bgCtx, index, term)
			n.mu.Lock()
			if n.pendingBarrier == pb {
				n.pendingBarrier = nil
			}
			n.mu.Unlock()
			pb.err = err
			close(pb.done)
		}()
	}

	select {
	case <-pb.done:
		if pb.err != nil {
			return pb.err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.bgCtx.Done():
		return ErrNodeClosed
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.persistent.CurrentTerm != term {
		return ErrNotLeader
	}
	return nil
}

// readProbeResult is one peer's outcome for a specific ReadContext quorum
// probe, delivered over a channel private to one ReadIndex call.
type readProbeResult struct {
	id   NodeID
	resp AppendEntriesResponse
	err  error
}

// ReadIndex confirms this node still holds leadership over a quorum in
// its current term and returns a committed log index safe to read
// through: the caller must WaitApplied(ctx, readIndex, 0) before
// consulting application state (ReadIndex does not itself read or wait
// for application — see docs/read-index.md for the full read path and
// linearization point).
//
// ReadIndex fails with:
//   - ErrNotLeader if this node is not Leader, or stops being Leader (or
//     observes a higher term) before quorum is confirmed.
//   - ErrReadIndexUnavailable if quorum could not be confirmed from the
//     peers that did respond before they were exhausted.
//   - ctx's error if ctx is done before quorum is confirmed.
//   - ErrNodeClosed if Close is called while this call is in progress.
//   - whatever establishing the current-term commit barrier failed with
//     (see ensureCurrentTermCommitted), if one was needed.
func (n *Node) ReadIndex(ctx context.Context) (LogIndex, error) {
	n.mu.Lock()
	if n.transfer != nil && n.transfer.phase == transferHandoff {
		// Handoff freeze (see leadership_transfer.go): this leader is
		// intentionally giving up leadership: it must not serve — or even
		// start establishing quorum for — a new read once that's underway.
		// (ensureCurrentTermCommitted's own internal barrier proposal
		// would also be blocked by proposeLocked's identical check, but a
		// read that reuses an already-established barrier would otherwise
		// slip through that path entirely, so this is checked explicitly
		// here too.)
		n.mu.Unlock()
		return 0, ErrLeadershipTransferInProgress
	}
	n.mu.Unlock()

	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		return 0, err
	}

	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	term := n.persistent.CurrentTerm
	readCtx := n.nextReadContextLocked()
	leaderID := n.id
	prevIndex, prevTerm := n.lastLogInfo()
	leaderCommit := n.commitIndex
	peers := n.resolveTargetsLocked()
	membership := n.membership
	n.mu.Unlock()

	acked := map[NodeID]bool{leaderID: true} // self, counted immediately — no network I/O for a single-node cluster
	if membership.HasQuorum(acked) {
		return n.finalizeReadIndex(term)
	}

	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      nil, // a read probe never carries entries
		LeaderCommit: leaderCommit,
		ReadContext:  readCtx,
	}

	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel() // quorum reached early, or we're returning: stop chasing stragglers (item 32)

	resultCh := make(chan readProbeResult, len(peers))
	for id, addr := range peers {
		n.bgWG.Add(1)
		go func(id NodeID, addr string) {
			defer n.bgWG.Done()
			resp, err := n.sendAppend(probeCtx, addr, req)
			resultCh <- readProbeResult{id: id, resp: resp, err: err}
		}(id, addr)
	}

	seen := make(map[NodeID]bool, len(peers))
	for i := 0; i < len(peers); i++ {
		select {
		case r := <-resultCh:
			if r.err != nil || seen[r.id] {
				continue // unreachable peer, or a duplicate response for one we already counted
			}
			seen[r.id] = true

			n.mu.Lock()
			if r.resp.Term > n.persistent.CurrentTerm {
				_ = n.stepDownLocked(r.resp.Term) // best-effort; on failure state is unchanged
				n.mu.Unlock()
				return 0, ErrNotLeader
			}
			n.mu.Unlock()

			if r.resp.Term != term || r.resp.ReadContext != readCtx {
				continue // stale term, or a response to a different (older/newer) read
			}
			acked[r.id] = true
			if membership.HasQuorum(acked) {
				return n.finalizeReadIndex(term)
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-n.bgCtx.Done():
			return 0, ErrNodeClosed
		}
	}
	return 0, ErrReadIndexUnavailable
}

// finalizeReadIndex re-verifies this node is still Leader in the term the
// quorum was confirmed for, then returns its current commitIndex as the
// safe read boundary — which may be newer than it was when ReadIndex was
// called (safe: a later commit only means more state is now certifiably
// committed). It does not return an uncommitted lastLogIndex.
func (n *Node) finalizeReadIndex(term Term) (LogIndex, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.persistent.CurrentTerm != term {
		return 0, ErrNotLeader
	}
	return n.commitIndex, nil
}
