package raft

import (
	"context"
	"errors"
	"fmt"
)

// ApplyFunc applies one committed log entry's command to the caller's
// application state machine. Node calls it with entries strictly in log
// order, exactly once each per running process, and never while holding
// Node's internal state lock (though it is serialized against
// SnapshotFunc/RestoreFunc via an internal lock — see CreateSnapshot).
// Command is opaque to Raft — ApplyFunc (and whatever it decodes Command
// with) is the only place that gives it meaning.
type ApplyFunc func(index LogIndex, command []byte) error

// ErrNodeClosed is returned by WaitApplied when Node.Close is called
// while a caller is still waiting.
var ErrNodeClosed = errors.New("raft: node closed")

// ErrEntryLost is returned by WaitApplied when the log entry at the
// waited-on index was superseded (its term changed) before ever being
// applied — the caller's proposal was overwritten by conflict repair
// before it committed, most likely because this node lost leadership.
// This does not mean the original command definitely never committed
// anywhere; it means this node can no longer confirm that outcome.
var ErrEntryLost = errors.New("raft: proposed entry was superseded before being applied")

type applyWaiter struct {
	index LogIndex
	term  Term // 0 disables the superseded-entry check (see checkEntryLocked)
	ch    chan error
}

// checkEntryLocked reports whether the entry at index is still the one
// this waiter is waiting on: if term is 0 no check is performed (used for
// "wait until caught up to at least this already-committed index", where
// supersession cannot happen — a committed entry is permanently fixed by
// Raft's safety property). Must be called with n.mu held.
func (n *Node) checkEntryLocked(index LogIndex, term Term) error {
	if term == 0 {
		return nil
	}
	e, ok := n.log.Entry(index)
	if !ok || e.Term != term {
		return ErrEntryLost
	}
	return nil
}

// kickApplyLocked starts the apply loop if it isn't already running and
// there is committed-but-unapplied work, unless application has already
// permanently failed. Must be called with n.mu held.
func (n *Node) kickApplyLocked() {
	if n.applying || n.applyErr != nil {
		return
	}
	if n.lastApplied >= n.commitIndex {
		return
	}
	n.applying = true
	if n.spawnBackgroundLocked() {
		go n.applyLoop()
	} else {
		n.applying = false
	}
}

// applyLoop applies committed-but-unapplied entries one at a time, in
// strict order, never holding n.mu while ApplyFunc runs. If ApplyFunc (or
// decoding what it needs) fails, application halts permanently for this
// process run: lastApplied stops advancing, applyErr is recorded, and
// every current and future waiter for an index beyond lastApplied
// receives that error. A committed entry is never skipped.
func (n *Node) applyLoop() {
	defer n.bgWG.Done()
	for {
		n.mu.Lock()
		if n.applyErr != nil || n.lastApplied >= n.commitIndex {
			n.applying = false
			n.mu.Unlock()
			return
		}
		nextIndex := n.lastApplied + 1
		entry, ok := n.log.Entry(nextIndex)
		fn := n.applyFunc
		n.mu.Unlock()

		if !ok {
			// commitIndex must never exceed lastLogIndex; reaching this
			// means that invariant was violated somewhere upstream.
			n.mu.Lock()
			n.applyErr = fmt.Errorf("raft: committed entry missing from log at index %d", nextIndex)
			n.applying = false
			n.notifyWaitersLocked()
			n.mu.Unlock()
			return
		}

		// EntryNoop (see Propose/ensureCurrentTermCommitted in
		// read_index.go) exists only to advance the log/commitIndex past a
		// current-term barrier; EntryConfiguration carries membership
		// changes, which Node derives from its local log directly (see
		// membership.go), not through ApplyFunc. Neither carries
		// application meaning, so ApplyFunc must never see them — advance
		// lastApplied directly instead.
		if entry.Kind != EntryApplication {
			n.mu.Lock()
			n.lastApplied = nextIndex
			n.notifyWaitersLocked()
			n.mu.Unlock()
			continue
		}

		// applyMu (not n.mu, the Raft state lock) serializes this call
		// against CreateSnapshot's own application-state access, so a
		// snapshot always captures state as of exactly the lastApplied
		// index it claims — never mid-apply, never one command ahead.
		n.applyMu.Lock()
		err := fn(nextIndex, entry.Command)
		n.applyMu.Unlock()

		n.mu.Lock()
		if err != nil {
			n.applyErr = fmt.Errorf("raft: apply failed at index %d: %w", nextIndex, err)
			n.applying = false
			n.notifyWaitersLocked()
			n.mu.Unlock()
			return
		}
		n.lastApplied = nextIndex
		n.notifyWaitersLocked()
		n.mu.Unlock()
	}
}

// notifyWaitersLocked wakes every waiter whose condition is now resolved:
// its entry was superseded (error), application has permanently failed
// (error), or lastApplied has reached its index (success). Must be called
// with n.mu held.
func (n *Node) notifyWaitersLocked() {
	// lastApplied may have just crossed membershipEntryIndex — wake every
	// AddVoter/RemoveVoter caller waiting on that (see config_change.go
	// and notifyMembershipChangedLocked).
	n.notifyMembershipChangedLocked()
	remaining := n.waiters[:0]
	for _, w := range n.waiters {
		if err := n.checkEntryLocked(w.index, w.term); err != nil {
			w.ch <- err
			continue
		}
		switch {
		case n.applyErr != nil:
			w.ch <- n.applyErr
		case n.lastApplied >= w.index:
			w.ch <- nil
		default:
			remaining = append(remaining, w)
			continue
		}
	}
	n.waiters = remaining
}

// WaitApplied blocks until lastApplied >= index, returning nil once that
// holds. Pass term as the term the entry at index was created with (e.g.
// from Propose) to detect if that specific entry gets superseded by
// conflict repair before ever being applied (ErrEntryLost); pass 0 to
// wait for an index known to already be committed, where supersession
// cannot happen.
//
// WaitApplied returns early with an error if: ctx is done, Close is
// called (ErrNodeClosed), application has permanently failed
// (the stored apply error), or the entry is superseded (ErrEntryLost).
func (n *Node) WaitApplied(ctx context.Context, index LogIndex, term Term) error {
	n.mu.Lock()
	if err := n.checkEntryLocked(index, term); err != nil {
		n.mu.Unlock()
		return err
	}
	if n.applyErr != nil && n.lastApplied < index {
		err := n.applyErr
		n.mu.Unlock()
		return err
	}
	if n.lastApplied >= index {
		n.mu.Unlock()
		return nil
	}
	w := &applyWaiter{index: index, term: term, ch: make(chan error, 1)}
	n.waiters = append(n.waiters, w)
	n.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		n.removeWaiter(w)
		return ctx.Err()
	case <-n.bgCtx.Done():
		n.removeWaiter(w)
		return ErrNodeClosed
	}
}

func (n *Node) removeWaiter(target *applyWaiter) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, w := range n.waiters {
		if w == target {
			n.waiters = append(n.waiters[:i], n.waiters[i+1:]...)
			return
		}
	}
}

// LastApplied returns the highest log index applied so far in this
// process run.
func (n *Node) LastApplied() LogIndex {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// ApplyError returns the error that permanently halted application, if
// any has occurred in this process run.
func (n *Node) ApplyError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applyErr
}
