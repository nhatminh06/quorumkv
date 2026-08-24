package raft

import (
	"context"
	"errors"
	"fmt"

	"quorumkv/internal/transport"
)

// ErrMembershipChangeInProgress is returned by CreateSnapshot while a
// joint-consensus membership transition is active. A snapshot must
// preserve a single Stable membership at its boundary (see Snapshot.
// Configuration); rather than support a joint-config snapshot, this
// milestone simply refuses to snapshot until the transition finishes —
// an acceptable limitation since membership changes are short and
// serialized one at a time (see docs/membership.md).
var ErrMembershipChangeInProgress = errors.New("raft: a membership change is already in progress")

// SnapshotFunc asks the application for a deterministic serialization of
// its entire current state, called by CreateSnapshot. It runs with
// applyMu held (not Node's own lock), so it never races with ApplyFunc —
// the state it captures always corresponds to exactly the lastApplied
// index CreateSnapshot labels the resulting snapshot with.
type SnapshotFunc func() ([]byte, error)

// RestoreFunc replaces the application's entire state with the state
// encoded in data — a full replacement, not commands layered on top of
// existing state. Like SnapshotFunc, it runs with applyMu held.
type RestoreFunc func(data []byte) error

// installSnapshotSender issues an InstallSnapshot RPC chunk to addr and
// returns the decoded response, mirroring sender/appendSender's real-
// transport/fake-network substitutability.
type installSnapshotSender func(ctx context.Context, addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)

func sendInstallSnapshotOverTransport(ctx context.Context, addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	payload, err := EncodeInstallSnapshot(req)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	msg := transport.NewMessage(transport.MessageInstallSnapshot, payload)
	resp, err := transport.Send(ctx, addr, msg)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	if resp.Type != transport.MessageInstallSnapshotResponse {
		return InstallSnapshotResponse{}, fmt.Errorf("raft: unexpected response message type %d", resp.Type)
	}
	return DecodeInstallSnapshotResponse(resp.Payload)
}

// incomingSnapshot tracks one in-progress InstallSnapshot transfer this
// node is receiving as a follower. A chunk stream must stay consistent
// across leaderID/term/lastIncludedIndex/lastIncludedTerm for its
// duration; any mismatch (or an unexpected offset) resets/rejects rather
// than concatenating bytes from two different snapshots.
type incomingSnapshot struct {
	leaderID          NodeID
	term              Term
	lastIncludedIndex LogIndex
	lastIncludedTerm  Term
	data              []byte
	configuration     Configuration
}

// CreateSnapshot serializes the application's current state at this
// node's lastApplied index (via SnapshotFunc), persists it durably, and
// only then compacts the covered Raft log prefix — never the reverse
// order. If persisting the snapshot fails, the log is left uncompacted.
//
// CreateSnapshot is this milestone's only snapshot trigger: it must be
// called explicitly (by a test or an operator-facing mechanism outside
// this package); there is no automatic threshold policy.
func (n *Node) CreateSnapshot() error {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	n.mu.Lock()
	if n.snapshotFn == nil {
		n.mu.Unlock()
		return errors.New("raft: no snapshot function configured")
	}
	if n.membership.Mode == ModeJoint {
		n.mu.Unlock()
		return ErrMembershipChangeInProgress
	}
	index := n.lastApplied
	if index == 0 {
		n.mu.Unlock()
		return errors.New("raft: nothing applied yet to snapshot")
	}
	if index <= n.log.BaseIndex() {
		n.mu.Unlock()
		return nil // already compacted through this point
	}
	term, ok := n.log.Term(index)
	if !ok {
		n.mu.Unlock()
		return fmt.Errorf("raft: cannot determine term at index %d", index)
	}
	cfg := n.membership.Stable
	fn := n.snapshotFn
	n.mu.Unlock()

	// Held under applyMu (above), not Node.mu: encoding potentially
	// megabytes of state must not block ordinary Raft bookkeeping
	// (elections, replication scheduling) while it runs. Because applyMu
	// is held, no concurrent ApplyFunc call can be in flight or start
	// until this returns, so `index` and the state fn() is about to
	// serialize are guaranteed to describe the same point.
	data, err := fn()
	if err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.membership.Mode == ModeJoint {
		// A membership change started while fn() ran unlocked; the stable
		// configuration captured above may already be superseded.
		return ErrMembershipChangeInProgress
	}
	if err := n.snapshotStore.Save(Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: data, Configuration: cfg}); err != nil {
		return err
	}
	if err := n.log.Compact(index, term); err != nil {
		return err
	}
	// The log's boundary just moved past index: any Configuration entries
	// at or before it are now gone from the log, so this snapshot's own
	// Configuration becomes the new base a future rebuild starts from.
	n.baseConfiguration = cfg
	n.hasBaseConfiguration = true
	return nil
}

// HandleInstallSnapshot implements the Raft InstallSnapshot RPC handler.
// Term handling mirrors AppendEntries exactly (stale rejected; higher
// term steps down first; valid same/higher-term contact resets the
// election timer and is tracked as the current leader). Chunks
// accumulate in an in-memory transfer session until the final (Done)
// chunk; only then is anything durably installed, in this order: persist
// the canonical snapshot, reconcile the local log's boundary (retaining
// any verified-matching suffix, discarding it otherwise), advance
// durable commit metadata if needed, replace application state via
// RestoreFunc, then update lastApplied — never acknowledging success
// before the persistence steps that precede it in this order have
// completed.
func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	n.mu.Lock()

	if req.Term < n.persistent.CurrentTerm {
		n.mu.Unlock()
		return InstallSnapshotResponse{Term: n.persistent.CurrentTerm, Success: false}, nil
	}
	if req.Term > n.persistent.CurrentTerm {
		if err := n.stepDownLocked(req.Term); err != nil {
			n.mu.Unlock()
			return InstallSnapshotResponse{}, err
		}
	} else if n.role != Follower {
		n.stepToFollowerLocked()
	}
	n.resetTimer()
	leader := req.LeaderID
	n.leaderID = &leader

	// Session identity: a fresh session only starts at offset 0; an
	// offset on a session we don't recognize (wrong leader/term/boundary,
	// or none in progress) means the leader must restart from scratch.
	if n.incoming == nil || n.incoming.leaderID != req.LeaderID || n.incoming.term != req.Term ||
		n.incoming.lastIncludedIndex != req.LastIncludedIndex || n.incoming.lastIncludedTerm != req.LastIncludedTerm {
		if req.Offset != 0 {
			n.mu.Unlock()
			return InstallSnapshotResponse{Term: n.persistent.CurrentTerm, Success: false, NextOffset: 0}, nil
		}
		n.incoming = &incomingSnapshot{
			leaderID: req.LeaderID, term: req.Term,
			lastIncludedIndex: req.LastIncludedIndex, lastIncludedTerm: req.LastIncludedTerm,
		}
	}

	if req.Offset != uint64(len(n.incoming.data)) {
		next := uint64(len(n.incoming.data))
		n.mu.Unlock()
		return InstallSnapshotResponse{Term: n.persistent.CurrentTerm, Success: false, NextOffset: next}, nil
	}
	if uint64(len(n.incoming.data))+uint64(len(req.Data)) > maxSnapshotPayloadSize {
		n.incoming = nil
		n.mu.Unlock()
		return InstallSnapshotResponse{}, fmt.Errorf("raft: incoming snapshot exceeds max size %d", maxSnapshotPayloadSize)
	}
	n.incoming.data = append(n.incoming.data, req.Data...)
	n.incoming.configuration = req.Configuration

	if !req.Done {
		next := uint64(len(n.incoming.data))
		n.mu.Unlock()
		return InstallSnapshotResponse{Term: n.persistent.CurrentTerm, Success: true, NextOffset: next}, nil
	}

	snap := Snapshot{
		LastIncludedIndex: n.incoming.lastIncludedIndex, LastIncludedTerm: n.incoming.lastIncludedTerm,
		Data: n.incoming.data, Configuration: n.incoming.configuration,
	}
	n.incoming = nil // transfer session is over either way

	// Stale or already-applied snapshot: never regress. Acknowledge
	// success without reinstalling — this makes a repeated/superseded
	// snapshot idempotent rather than an error.
	if snap.LastIncludedIndex <= n.lastApplied {
		n.mu.Unlock()
		return InstallSnapshotResponse{Term: n.persistent.CurrentTerm, Success: true, NextOffset: uint64(len(snap.Data))}, nil
	}
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	if err := n.installSnapshot(snap); err != nil {
		return InstallSnapshotResponse{}, err
	}
	return InstallSnapshotResponse{Term: term, Success: true, NextOffset: uint64(len(snap.Data))}, nil
}

// installSnapshot performs the actual crash-safe installation described
// on HandleInstallSnapshot, serializing application-state access against
// ApplyFunc/CreateSnapshot via applyMu the same way normal application
// does.
func (n *Node) installSnapshot(snap Snapshot) error {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	if err := n.snapshotStore.Save(snap); err != nil {
		return err
	}
	if err := n.log.InstallSnapshotBoundary(snap.LastIncludedIndex, snap.LastIncludedTerm); err != nil {
		return err
	}

	n.mu.Lock()
	if n.commitIndex < snap.LastIncludedIndex {
		if err := n.commitStore.Save(snap.LastIncludedIndex); err != nil {
			n.mu.Unlock()
			return err
		}
		n.commitIndex = snap.LastIncludedIndex
	}
	// The log's boundary just moved: any Configuration entries before it
	// are gone, so this snapshot's own Configuration becomes the new base
	// effective membership must be rebuilt from.
	n.baseConfiguration = snap.Configuration
	n.hasBaseConfiguration = true
	n.rebuildMembershipLocked()
	restoreFn := n.restoreFn
	n.mu.Unlock()

	if err := restoreFn(snap.Data); err != nil {
		return err
	}

	n.mu.Lock()
	if n.lastApplied < snap.LastIncludedIndex {
		n.lastApplied = snap.LastIncludedIndex
	}
	n.notifyWaitersLocked()
	n.kickApplyLocked() // continue applying any retained suffix beyond the snapshot boundary
	n.mu.Unlock()
	return nil
}

// sendSnapshotToPeer transfers the current canonical snapshot to one
// peer as a tight sequence of chunks — not paced by the heartbeat
// interval — used when that peer's nextIndex has fallen behind this
// leader's compacted log prefix. The caller must have already recorded
// this transfer as in-progress (n.snapshotSending[id] = true) before
// starting the goroutine that calls this; it always clears that flag on
// return.
func (n *Node) sendSnapshotToPeer(ctx context.Context, term Term, id NodeID, addr string) {
	defer func() {
		n.mu.Lock()
		delete(n.snapshotSending, id)
		n.mu.Unlock()
	}()

	n.mu.Lock()
	if n.role != Leader || n.persistent.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	leaderID := n.id
	fallbackCfg := n.membership.Stable
	n.mu.Unlock()

	snap, err := n.snapshotStore.Load()
	if err != nil || snap == nil {
		return
	}
	if !snap.ConfigurationPresent {
		// A legacy (pre-Milestone-10) snapshot has no stored membership —
		// fall back to this leader's own bootstrap/configured membership
		// as the historical stable config (see docs/membership.md).
		snap.Configuration = fallbackCfg
	}

	var offset uint64
	for {
		end := offset + maxSnapshotChunkSize
		if end > uint64(len(snap.Data)) {
			end = uint64(len(snap.Data))
		}
		done := end == uint64(len(snap.Data))
		req := InstallSnapshotRequest{
			Term: term, LeaderID: leaderID,
			LastIncludedIndex: snap.LastIncludedIndex, LastIncludedTerm: snap.LastIncludedTerm,
			Offset: offset, Data: snap.Data[offset:end], Done: done,
			Configuration: snap.Configuration,
		}
		resp, err := n.sendInstallSnapshot(ctx, addr, req)
		if err != nil {
			return // transient failure; a later replication round retries from scratch
		}

		n.mu.Lock()
		if resp.Term > n.persistent.CurrentTerm {
			_ = n.stepDownLocked(resp.Term)
			n.mu.Unlock()
			return
		}
		if n.role != Leader || n.persistent.CurrentTerm != term {
			n.mu.Unlock()
			return
		}
		if !resp.Success {
			nextOffset := resp.NextOffset
			n.mu.Unlock()
			if nextOffset >= end {
				return // nonsensical reply; give up this round rather than loop
			}
			offset = nextOffset
			continue
		}
		if done {
			if snap.LastIncludedIndex > n.matchIndex[id] {
				n.matchIndex[id] = snap.LastIncludedIndex
			}
			n.nextIndex[id] = snap.LastIncludedIndex + 1
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		offset = end
	}
}
