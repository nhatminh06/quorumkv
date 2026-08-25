package raft

import (
	"context"
)

// replicationWorker is the volatile runtime scheduling state for one
// replication target: a coalescing wake signal and the cancel func for
// its own goroutine's context (a child of the current leaderCtx — see
// becomeLeaderLocked/stepToFollowerLocked, which is why losing
// leadership alone is enough to stop every worker with no separate
// bookkeeping). Never persisted; reconciled fresh against
// n.membership.Targets on every rebuildMembershipLocked — see
// reconcileReplicationWorkersLocked.
type replicationWorker struct {
	// wakeCh is buffered 1: multiple wake reasons arriving before the
	// worker gets a chance to run collapse into a single "replicate
	// current state now" (item 19) rather than requiring one wakeup per
	// reason. See wakeAllLocked/wakePeerLocked.
	wakeCh chan struct{}
	cancel context.CancelFunc
}

// reconcileReplicationWorkersLocked starts a worker for every current
// replication target that doesn't already have one, and stops (cancels,
// removes) every worker for a peer no longer among them — the only
// place replication worker lifecycle is decided. Called from
// becomeLeaderLocked (initial set) and from the end of
// rebuildMembershipLocked (every membership change, including a
// just-appended not-yet-committed Joint entry — a new voter must start
// catching up immediately, not only once its entry commits). A no-op
// while not Leader. Must be called with n.mu held.
func (n *Node) reconcileReplicationWorkersLocked() {
	if n.role != Leader {
		return
	}
	targets := n.resolveTargetsLocked()
	for id := range targets {
		if _, ok := n.workers[id]; ok {
			continue
		}
		if _, ok := n.nextIndex[id]; !ok {
			n.nextIndex[id] = n.log.LastIndex() + 1
			n.matchIndex[id] = 0
		}
		n.replicationGeneration[id]++ // fresh epoch for a (re)created worker
		ctx, cancel := context.WithCancel(n.leaderCtx)
		w := &replicationWorker{wakeCh: make(chan struct{}, 1), cancel: cancel}
		n.workers[id] = w
		if n.spawnBackgroundLocked() {
			go n.replicationWorkerLoop(ctx, id, w.wakeCh)
		} else {
			cancel()
		}
		wakeNow(w.wakeCh) // don't wait for the next heartbeat tick to start catching up
	}
	for id, w := range n.workers {
		if _, ok := targets[id]; ok {
			continue
		}
		w.cancel()
		delete(n.workers, id)
		delete(n.nextIndex, id)
		delete(n.matchIndex, id)
		delete(n.replicationGeneration, id)
		delete(n.snapshotSending, id)
	}
}

// wakeNow performs the non-blocking, coalescing send every wake site
// uses: if the channel already has a pending wake, this is a no-op —
// the worker will observe current state when it next runs regardless of
// how many wake reasons fired in the meantime (item 19/20).
func wakeNow(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// wakeAllReplicationLocked wakes every current replication worker —
// used after any local log mutation (a proposal batch, a configuration
// entry, the ReadIndex no-op barrier) and by the heartbeat ticker. Must
// be called with n.mu held.
func (n *Node) wakeAllReplicationLocked() {
	for _, w := range n.workers {
		wakeNow(w.wakeCh)
	}
}

// replicationWorkerLoop is one peer's entire replication schedule for as
// long as ctx is alive: block for a wake reason, then repeatedly step
// (send one bounded batch, apply its response) for as long as each step
// reports more work remains, with no wait in between (item 21) — only
// once a step reports the peer is caught up (or ctx ends, or a
// send/step aborts) does it go back to waiting for the next wake. There
// is deliberately no other loop anywhere that keeps sending to this
// peer — heartbeatLoop only wakes workers, it does not send RPCs
// itself.
func (n *Node) replicationWorkerLoop(ctx context.Context, id NodeID, wakeCh chan struct{}) {
	defer n.bgWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wakeCh:
		}
		for {
			if ctx.Err() != nil {
				return
			}
			if !n.replicationStep(ctx, id) {
				break
			}
		}
	}
}

// replicationStep performs exactly one bounded unit of replication work
// for id — either one AppendEntries batch or one InstallSnapshot
// chunk-loop transfer — and reports whether the caller should
// immediately step again (more work is known to remain) or go back to
// waiting for a wake. It never sends network I/O while holding n.mu
// (item 24): request state is captured under lock, sent unlocked, and
// the response is validated/applied under lock again.
func (n *Node) replicationStep(ctx context.Context, id NodeID) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false
	}
	if _, ok := n.workers[id]; !ok {
		n.mu.Unlock()
		return false // removed since this loop iteration started
	}
	addr, ok := n.resolveTargetsLocked()[id]
	if !ok {
		n.mu.Unlock()
		return false
	}
	term := n.persistent.CurrentTerm
	generation := n.replicationGeneration[id]
	baseIndex := n.log.BaseIndex()
	next := n.nextIndex[id]
	if next < 1 {
		next = 1
	}
	needsSnapshot := baseIndex > 0 && next <= baseIndex
	if needsSnapshot {
		if n.snapshotSending[id] {
			// A transfer is already in flight for this peer from a
			// previous step (sendSnapshotToPeer runs its own full
			// chunk loop before returning) — should not normally
			// happen given the worker is single-flight per peer, but
			// defensively avoid starting a second one.
			n.mu.Unlock()
			return false
		}
		n.snapshotSending[id] = true
		n.replicationGeneration[id]++ // invalidate any in-flight AppendEntries assumptions before taking over
		n.mu.Unlock()

		ok := n.sendSnapshotToPeer(ctx, term, id, addr)
		n.mu.Lock()
		delete(n.snapshotSending, id)
		if ok {
			n.replicationGeneration[id]++ // item 45: a fresh epoch for the suffix catch-up that resumes after this
		}
		more := ok && n.role == Leader && n.persistent.CurrentTerm == term && n.nextIndex[id] <= n.log.LastIndex()
		n.mu.Unlock()
		return more
	}

	prevIndex := next - 1
	prevTerm, _ := n.log.Term(prevIndex)
	entries := n.log.EntriesRange(next, maxEntriesPerAppend, MaxAppendEntriesBytes)
	leaderCommit := n.commitIndex
	req := AppendEntriesRequest{
		Term: term, LeaderID: n.id,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: leaderCommit,
	}
	n.mu.Unlock()

	resp, err := n.sendAppend(ctx, addr, req)
	if err != nil {
		return false // transient failure; the next wake (heartbeat or new entry) retries
	}

	return n.applyReplicationResponse(id, term, generation, req, resp)
}

// applyReplicationResponse validates and applies one AppendEntries
// response, returning whether the worker should immediately step again.
// A response is only ever allowed to mutate nextIndex/matchIndex if it
// is still for the current role/term/generation for this peer — a
// higher term steps this node down (affecting every peer, handled once
// here); anything else stale (older term, older generation, peer no
// longer a target) is discarded with no effect, exactly like a response
// that never arrived. See docs/replication-performance.md's generation
// model.
func (n *Node) applyReplicationResponse(id NodeID, sentTerm Term, sentGeneration uint64, req AppendEntriesRequest, resp AppendEntriesResponse) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.persistent.CurrentTerm {
		_ = n.stepDownLocked(resp.Term) // best-effort; on failure state is unchanged
		return false
	}
	if n.role != Leader || n.persistent.CurrentTerm != sentTerm {
		return false
	}
	if _, ok := n.workers[id]; !ok {
		return false // peer removed since this request was sent
	}
	if n.replicationGeneration[id] != sentGeneration {
		return false // superseded by a conflict backtrack, a snapshot takeover, or a worker recreation
	}

	if resp.Success {
		newMatch := req.PrevLogIndex + LogIndex(len(req.Entries))
		if newMatch > n.matchIndex[id] {
			n.matchIndex[id] = newMatch
			n.nextIndex[id] = newMatch + 1
			// A leadership-transfer catch-up waiter may be watching
			// this peer's matchIndex specifically (see
			// leadership_transfer.go).
			n.pingTransferChanged()
		}
		n.maybeAdvanceCommitIndexLocked()
		return n.nextIndex[id] <= n.log.LastIndex()
	}

	// Failure: the assumed prefix was wrong. Back off by one (this
	// package's existing simple conflict-repair strategy — no second
	// algorithm invented here) and invalidate the generation so any
	// other still-in-flight speculative response for the old assumption
	// can never apply itself afterward.
	if n.nextIndex[id] > 1 {
		n.nextIndex[id]--
	}
	n.replicationGeneration[id]++
	return true
}
