package raft

import (
	"context"
	"errors"
	"fmt"
)

// ErrAlreadyVoter is returned by AddVoter when id is already a voter in
// the current stable configuration — including reusing an existing
// NodeID with a different address: this milestone never reinterprets an
// Add as an address change for an existing member, it always errors.
var ErrAlreadyVoter = errors.New("raft: node is already a voter")

// ErrNotAVoter is returned by RemoveVoter when id is not a voter in the
// current stable configuration.
var ErrNotAVoter = errors.New("raft: node is not a voter")

// AddVoter starts a joint-consensus transition adding id (at addr) as a
// new voter, and blocks until that transition fully completes: the final
// Stable(C_new) entry has committed and been applied on this node — not
// merely appended, and not merely committed. See docs/membership.md for
// the full protocol.
//
// AddVoter fails with:
//   - ErrNotLeader if this node is not currently Leader.
//   - ErrMembershipChangeInProgress if a transition is already active
//     (only one add/remove runs at a time).
//   - ErrAlreadyVoter if id is already a voter (including reusing an
//     existing NodeID with a different address — always an error, never
//     reinterpreted as an address change).
//   - ErrInvalidConfiguration if addr is empty/oversized or the resulting
//     configuration would be otherwise invalid.
//   - ctx's error if ctx is done before the transition completes. The
//     transition itself is not aborted by this — it may still commit
//     later; the caller must inspect current membership (MembershipStatus)
//     before retrying rather than assuming failure.
//   - ErrNodeClosed if Close is called while this call is waiting.
func (n *Node) AddVoter(ctx context.Context, id NodeID, addr string) error {
	return n.changeMembership(ctx, func(old Configuration) (Configuration, error) {
		if old.Has(id) {
			return Configuration{}, ErrAlreadyVoter
		}
		voters := make(map[NodeID]string, len(old.Voters)+1)
		for vid, vaddr := range old.Voters {
			voters[vid] = vaddr
		}
		voters[id] = addr
		return NewConfiguration(voters)
	})
}

// RemoveVoter starts a joint-consensus transition removing id as a voter
// (id may be this node itself — see docs/membership.md for self-removal
// behavior), and blocks until that transition fully completes, with the
// same completion/error semantics as AddVoter.
//
// RemoveVoter fails with ErrNotAVoter if id is not currently a voter, in
// addition to every error AddVoter can return except ErrAlreadyVoter.
func (n *Node) RemoveVoter(ctx context.Context, id NodeID) error {
	return n.changeMembership(ctx, func(old Configuration) (Configuration, error) {
		if !old.Has(id) {
			return Configuration{}, ErrNotAVoter
		}
		voters := make(map[NodeID]string, len(old.Voters)-1)
		for vid, vaddr := range old.Voters {
			if vid != id {
				voters[vid] = vaddr
			}
		}
		return NewConfiguration(voters)
	})
}

// changeMembership implements the shared AddVoter/RemoveVoter flow:
// validate and build the new configuration, append a Joint entry
// transitioning from the current stable configuration to it (activating
// immediately, before commit — see rebuildMembershipLocked), then wait
// for the whole transition (through the automatically appended final
// Stable entry) to commit and apply.
func (n *Node) changeMembership(ctx context.Context, mutate func(old Configuration) (Configuration, error)) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.membership.Mode == ModeJoint {
		n.mu.Unlock()
		return ErrMembershipChangeInProgress
	}
	oldC := n.membership.Stable
	newC, err := mutate(oldC)
	if err != nil {
		n.mu.Unlock()
		return err
	}

	joint := JointMembership(oldC, newC)
	b, err := EncodeMembership(joint)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("raft: encoding joint configuration: %w", err)
	}
	term := n.persistent.CurrentTerm
	if err := n.log.Append([]LogEntry{{Term: term, Kind: EntryConfiguration, Command: b}}); err != nil {
		n.mu.Unlock()
		return err
	}
	n.rebuildMembershipLocked() // activates the Joint immediately, before commit
	n.maybeAdvanceCommitIndexLocked()
	n.mu.Unlock()

	n.bgWG.Add(1)
	go func() {
		defer n.bgWG.Done()
		n.replicateToAllPeers(n.bgCtx)
	}()

	return n.waitForStableConfiguration(ctx, newC)
}

// waitForStableConfiguration blocks until n.membership is Stable(target)
// and that has been applied (n.lastApplied covers the entry that produced
// it) — see membershipEntryIndex/membershipChanged. It does not poll:
// every place that can change this outcome pings membershipChanged.
func (n *Node) waitForStableConfiguration(ctx context.Context, target Configuration) error {
	for {
		n.mu.Lock()
		if n.membership.Mode == ModeStable && n.membership.Stable.Equal(target) && n.lastApplied >= n.membershipEntryIndex {
			n.mu.Unlock()
			return nil
		}
		n.mu.Unlock()

		select {
		case <-n.membershipChanged:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-n.bgCtx.Done():
			return ErrNodeClosed
		}
	}
}

// maybeCompleteMembershipTransitionLocked appends the final Stable(C_new)
// entry once the preceding Joint entry has committed, if this node is
// Leader and no such completing entry has been appended yet
// (pendingStableIndex == 0). This is what makes a leader-crash mid
// transition self-healing: whichever node eventually becomes (or already
// is) Leader once the Joint entry is committed will find
// membership.Mode == ModeJoint with no pending Stable entry and finish
// the transition automatically — see docs/membership.md. Must be called
// with n.mu held.
func (n *Node) maybeCompleteMembershipTransitionLocked() {
	if n.role != Leader {
		return
	}
	if n.membership.Mode != ModeJoint {
		return
	}
	if n.pendingStableIndex != 0 {
		return // already appended (by this leader or a predecessor); waiting for it to commit
	}
	if n.membershipEntryIndex == 0 || n.membershipEntryIndex > n.commitIndex {
		return // the Joint entry itself hasn't committed yet
	}

	stable := StableMembership(n.membership.New)
	b, err := EncodeMembership(stable)
	if err != nil {
		return // unreachable: New was already validated when the Joint entry was built
	}
	if err := n.log.Append([]LogEntry{{Term: n.persistent.CurrentTerm, Kind: EntryConfiguration, Command: b}}); err != nil {
		return // best-effort; the next commit-index advance or leadership change retries
	}
	n.rebuildMembershipLocked()
	n.maybeAdvanceCommitIndexLocked()
}

// MembershipStatus is a read-only snapshot of a Node's effective
// membership: safe to inspect freely, and never lets a caller mutate Raft
// state through it (every Configuration it carries is a defensive copy).
type MembershipStatus struct {
	Mode   MembershipMode
	Stable Configuration // valid only when Mode == ModeStable
	Old    Configuration // valid only when Mode == ModeJoint
	New    Configuration // valid only when Mode == ModeJoint
}

// MembershipStatus returns a copy of this node's current effective
// membership.
func (n *Node) MembershipStatus() MembershipStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	m := n.membership.clone()
	return MembershipStatus{Mode: m.Mode, Stable: m.Stable, Old: m.Old, New: m.New}
}
