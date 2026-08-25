package raft

import (
	"context"
	"errors"
)

// ErrLeadershipTransferInProgress is returned by TransferLeadership when
// another transfer is already active, and by AddVoter/RemoveVoter/
// Propose (its internal write-admission freeze — see proposeLocked) when
// one is active or in its final handoff phase respectively. Only one
// major administrative transition — a membership change or a leadership
// transfer — runs at a time.
var ErrLeadershipTransferInProgress = errors.New("raft: a leadership transfer is already in progress")

// ErrCannotTransferToSelf is returned by TransferLeadership when target
// is this node's own ID.
var ErrCannotTransferToSelf = errors.New("raft: cannot transfer leadership to self")

// ErrLeadershipTransferRejected is returned by TransferLeadership when
// the target's TimeoutNow response explicitly declines (as opposed to a
// network/context error reaching it at all).
var ErrLeadershipTransferRejected = errors.New("raft: transfer target rejected TimeoutNow")

// ErrUnknownTargetAddress is returned by TransferLeadership when target
// is a valid voter but this node has no address to reach it at.
var ErrUnknownTargetAddress = errors.New("raft: no known address for transfer target")

// transferPhase is a leadership transfer's current stage.
type transferPhase uint8

const (
	// transferCatchingUp: the target is being brought up to LastIndex via
	// ordinary replication (see waitForTransferCatchUp). Client writes
	// remain allowed — freezing them here would be needless disruption
	// for what may take several replication rounds.
	transferCatchingUp transferPhase = iota + 1
	// transferHandoff: the target was caught up and TimeoutNow is about
	// to be (or has been) sent. New Propose calls, membership changes,
	// and new ReadIndex calls are all rejected with
	// ErrLeadershipTransferInProgress from this point on — see
	// proposeLocked, changeMembership, ReadIndex.
	transferHandoff
)

// transferState is one Node's in-progress leadership transfer. Never
// persisted — see the Node.transfer field doc.
type transferState struct {
	target       NodeID
	originalTerm Term
	phase        transferPhase
}

// TransferLeadership hands leadership to target through Raft's normal
// election mechanics — it never simply declares target the leader.
// The flow: ensure target is fully caught up (ordinary replication,
// diverting through InstallSnapshot if target is behind the compacted
// log prefix, exactly like any other catch-up — see replicateToAllPeers),
// freeze new write/membership/read admission, send target an authorized
// TimeoutNow, and wait for real evidence (valid AppendEntries contact
// from target at a higher term) that it actually won. See
// docs/leadership-transfer.md for the full protocol and every documented
// failure mode.
//
// TransferLeadership fails with:
//   - ErrNotLeader if this node is not currently Leader (checked both
//     initially and again if lost during catch-up).
//   - ErrCannotTransferToSelf if target == this node's own ID.
//   - ErrMembershipChangeInProgress if membership is currently Joint —
//     this milestone deliberately does not mix the two administrative
//     transitions; retry once the Joint transition finishes.
//   - ErrLeadershipTransferInProgress if another transfer is already
//     active.
//   - ErrNotAVoter if target is not a voter in the current stable
//     configuration (includes a removed node, and a not-yet-added one).
//   - ErrUnknownTargetAddress if target is a voter but has no known
//     address.
//   - ErrLeadershipTransferRejected if target's TimeoutNow response
//     explicitly declined.
//   - ctx's error if ctx is done before the transfer completes. Before
//     TimeoutNow is sent, this simply aborts the attempt: this node
//     remains leader, the freeze (if any) is released, normal operation
//     resumes — see item 121/123 in the milestone notes. After TimeoutNow
//     is accepted, cancellation cannot undo it: the target may still
//     become leader regardless of what this call returns, an
//     intentionally ambiguous administrative outcome (an operator should
//     inspect cluster state rather than assume failure).
//   - ErrNodeClosed if Close is called while this call is waiting.
func (n *Node) TransferLeadership(ctx context.Context, target NodeID) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if target == n.id {
		n.mu.Unlock()
		return ErrCannotTransferToSelf
	}
	if n.membership.Mode == ModeJoint {
		n.mu.Unlock()
		return ErrMembershipChangeInProgress
	}
	if n.transfer != nil {
		n.mu.Unlock()
		return ErrLeadershipTransferInProgress
	}
	if !n.membership.IsVoter(target) {
		n.mu.Unlock()
		return ErrNotAVoter
	}
	originalTerm := n.persistent.CurrentTerm
	n.transfer = &transferState{target: target, originalTerm: originalTerm, phase: transferCatchingUp}
	n.mu.Unlock()

	defer n.clearTransfer(target, originalTerm)

	for {
		if err := n.waitForTransferCatchUp(ctx, target); err != nil {
			return err
		}
		sent, err := n.attemptHandoff(ctx, target)
		if err != nil {
			return err
		}
		if sent {
			break
		}
		// Target regressed between catch-up and the final caught-up
		// check (item 125: another write landed in between) — loop back
		// and catch it up again before trying the handoff once more.
	}

	return n.waitForTransferCompletion(ctx, target, originalTerm)
}

// clearTransfer removes n.transfer if it still identifies the exact
// transfer this call started (target+originalTerm) — guarding against
// clearing a different, newer transfer in the unlikely case this one's
// own cleanup runs late.
func (n *Node) clearTransfer(target NodeID, originalTerm Term) {
	n.mu.Lock()
	if n.transfer != nil && n.transfer.target == target && n.transfer.originalTerm == originalTerm {
		n.transfer = nil
	}
	n.mu.Unlock()
}

// waitForTransferCatchUp blocks until matchIndex[target] has reached
// this node's own LastIndex — i.e. target holds every entry this leader
// currently has. It does not itself drive replication: the existing
// heartbeat loop already replicates to every voter, target included,
// diverting to InstallSnapshot automatically if target has fallen behind
// the compacted log prefix (see replicateToAllPeers) — there is no
// separate catch-up mechanism to build or poll (item 101/102/103): this
// simply waits on transferChanged, pinged whenever matchIndex or
// LastIndex moves, rather than sleeping in a loop.
func (n *Node) waitForTransferCatchUp(ctx context.Context, target NodeID) error {
	for {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return ErrNotLeader
		}
		caughtUp := n.matchIndex[target] >= n.log.LastIndex()
		n.mu.Unlock()
		if caughtUp {
			return nil
		}
		select {
		case <-n.transferChanged:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-n.bgCtx.Done():
			return ErrNodeClosed
		}
	}
}

// attemptHandoff re-verifies (under lock, against a fresh read of
// matchIndex/LastIndex — item 84: never act on a stale snapshot of
// them) that target is still caught up, enters the Handoff freeze phase,
// and sends TimeoutNow. Returns (true, nil) once TimeoutNow has been
// accepted; (false, nil) if target turned out to no longer be caught up
// (the caller should catch it up again and retry); (false, err) for any
// definitive failure.
func (n *Node) attemptHandoff(ctx context.Context, target NodeID) (bool, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false, ErrNotLeader
	}
	if n.matchIndex[target] < n.log.LastIndex() {
		n.mu.Unlock()
		return false, nil
	}
	if n.transfer == nil || n.transfer.target != target {
		n.mu.Unlock()
		return false, ErrLeadershipTransferInProgress
	}
	n.transfer.phase = transferHandoff
	term := n.persistent.CurrentTerm
	leaderID := n.id
	addr, ok := n.resolveTargetsLocked()[target]
	n.mu.Unlock()
	if !ok || addr == "" {
		return false, ErrUnknownTargetAddress
	}

	resp, err := n.sendTimeoutNow(ctx, addr, TimeoutNowRequest{Term: term, LeaderID: leaderID})
	if err != nil {
		return false, err
	}
	if !resp.Accepted {
		return false, ErrLeadershipTransferRejected
	}
	return true, nil
}

// waitForTransferCompletion blocks until this node observes real
// evidence that target actually became leader: valid AppendEntries
// contact from target at a term higher than originalTerm (see
// HandleAppendEntries, which is also where the old leader itself learns
// of and steps down for that higher term — through the exact same
// ordinary higher-term mechanics every RPC handler uses, no special
// leadership-transfer case in that path). Accepting TimeoutNow is
// deliberately not treated as success on its own (item 55/67).
func (n *Node) waitForTransferCompletion(ctx context.Context, target NodeID, originalTerm Term) error {
	for {
		n.mu.Lock()
		done := n.leaderID != nil && *n.leaderID == target && n.persistent.CurrentTerm > originalTerm
		n.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-n.transferChanged:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-n.bgCtx.Done():
			return ErrNodeClosed
		}
	}
}
