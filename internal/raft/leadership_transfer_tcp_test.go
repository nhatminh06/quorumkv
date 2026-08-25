package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"quorumkv/internal/transport"
)

// TestLeadershipTransferOverRealTCP is the mandatory real-socket
// leadership-transfer scenario (item 117): A leads a real three-node TCP
// cluster; TransferLeadership(B) catches B up, freezes new admission,
// sends an authorized TimeoutNow, and B wins a real (PreVote-bypassing)
// election — all through genuine Raft RPCs, no manual role assignment.
// It also folds in item 118 (client state across the handoff): a write
// committed before the transfer is still readable through the new
// leader via ReadIndex, and a further write against the new leader
// succeeds.
func TestLeadershipTransferOverRealTCP(t *testing.T) {
	c := newSnapshottingTCPCluster(t, []NodeID{1, 2, 3})
	defer c.closeAll()
	a, b := c.nodes[1], c.nodes[2]

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	preIndex := proposeAsLeaderAndWaitApplied(t, a.Node, "before-transfer")
	originalTerm := a.CurrentTerm()

	if err := a.TransferLeadership(ctx, 2); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if a.Role() != Follower {
		t.Fatalf("A.Role() = %v, want Follower after transferring leadership away", a.Role())
	}
	if b.Role() != Leader {
		t.Fatalf("B.Role() = %v, want Leader", b.Role())
	}
	if b.CurrentTerm() <= originalTerm {
		t.Fatalf("B.CurrentTerm() = %d, want > %d (a real election must have advanced the term)", b.CurrentTerm(), originalTerm)
	}

	// A pre-transfer write must still be readable through B.
	readIndex, err := b.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if err := b.WaitApplied(ctx, readIndex, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if got := b.state.snapshotOf(); len(got) == 0 || got[0] != "before-transfer" {
		t.Fatalf("B's applied state = %v, want to include before-transfer", got)
	}
	if readIndex < preIndex {
		t.Fatalf("readIndex = %d, want >= %d", readIndex, preIndex)
	}

	// A write against the new leader must succeed normally.
	postIndex := proposeAsLeaderAndWaitApplied(t, b.Node, "after-transfer")
	if postIndex <= preIndex {
		t.Fatalf("postIndex = %d, want > preIndex %d", postIndex, preIndex)
	}
}

// TestLeadershipTransferToFarBehindSnapshotTarget is the mandatory
// scenario proving Milestones 7, 10, and 11 compose (items 45/80/120): a
// brand-new node D, added to the cluster and already behind the leader's
// compacted log prefix, must be caught up (via real InstallSnapshot, the
// same replication machinery any far-behind voter uses — no special
// transfer-specific data path) before TimeoutNow is ever sent.
func TestLeadershipTransferToFarBehindSnapshotTarget(t *testing.T) {
	c := newSnapshottingTCPCluster(t, []NodeID{1, 2, 3})
	defer c.closeAll()
	a := c.nodes[1]

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	var last LogIndex
	for _, cmd := range []string{"one", "two", "three"} {
		last = proposeAsLeaderAndWaitApplied(t, a.Node, cmd)
	}
	if err := a.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if a.log.BaseIndex() != last {
		t.Fatalf("a.log.BaseIndex() = %d, want %d (snapshot must have compacted the log)", a.log.BaseIndex(), last)
	}

	dDir := t.TempDir()
	smD := newFakeStateMachine()
	d := openSnapshottingNode(t, dDir, 4, nil, smD)
	dtr, err := transport.Listen("127.0.0.1:0", d.Handler())
	if err != nil {
		t.Fatalf("Listen(D): %v", err)
	}
	defer func() { d.Close(); dtr.Close() }()
	go d.Run(ctx)

	if err := a.AddVoter(ctx, 4, dtr.Addr()); err != nil {
		t.Fatalf("AddVoter(D): %v", err)
	}
	if !waitFor(5*time.Second, func() bool { return d.LastApplied() >= last }) {
		t.Fatalf("D did not catch up via InstallSnapshot before AddVoter returned: LastApplied()=%d, want >= %d", d.LastApplied(), last)
	}

	if err := a.TransferLeadership(ctx, 4); err != nil {
		t.Fatalf("TransferLeadership(D): %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return d.Role() == Leader }) {
		t.Fatalf("D never became leader: role=%v", d.Role())
	}
	if a.Role() != Follower {
		t.Fatalf("A.Role() = %v, want Follower", a.Role())
	}
}

// TestLeadershipTransferTargetPartitionedFails is the mandatory
// target-unreachable scenario (item 121): the target never becomes
// reachable, so TransferLeadership must fail via ctx — the leader
// remains leader throughout, with no term change and no disruption, and
// can serve normally once the caller gives up.
func TestLeadershipTransferTargetPartitionedFails(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	a := c.node(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	originalTerm := a.CurrentTerm()

	c.net.partition(1, 2) // B (the intended target) is unreachable
	// Give B something to be behind on — an empty log trivially satisfies
	// matchIndex >= LastIndex, which would let the transfer race straight
	// to TimeoutNow (and fail there instead, for a different, less
	// interesting reason) rather than genuinely blocking in catch-up.
	if _, _, err := a.Propose([]byte("PUT unreachable 1")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	transferCtx, transferCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer transferCancel()
	err := a.TransferLeadership(transferCtx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("TransferLeadership: err = %v, want context.DeadlineExceeded", err)
	}
	if a.Role() != Leader || a.CurrentTerm() != originalTerm {
		t.Fatalf("A after failed transfer: role=%v term=%d, want unchanged Leader/%d", a.Role(), a.CurrentTerm(), originalTerm)
	}

	// Normal operation resumes: the freeze (never actually reached,
	// since catch-up itself never completed) is not in effect.
	index := proposeAndWait(t, a, "PUT z 9")
	if a.CommitIndex() < index {
		t.Fatalf("write after failed transfer did not commit: commitIndex=%d, want >= %d", a.CommitIndex(), index)
	}
}

// TestRepeatedLeadershipTransferCycle is item 146: A -> B -> C -> A, with
// a write between each transfer, proving the cycle is stable — the
// committed KV state, term monotonicity, and single-leader-per-term
// invariant all hold throughout, with no deadlocks.
func TestRepeatedLeadershipTransferCycle(t *testing.T) {
	c := newSnapshottingTCPCluster(t, []NodeID{1, 2, 3})
	defer c.closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.nodes[1].StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	var lastTerm Term
	cycle := []struct{ from, to NodeID }{{1, 2}, {2, 3}, {3, 1}}
	for i, step := range cycle {
		leader := c.nodes[step.from]
		target := c.nodes[step.to]
		if leader.Role() != Leader {
			t.Fatalf("step %d: node %d.Role() = %v, want Leader before transferring", i, step.from, leader.Role())
		}
		_ = proposeAsLeaderAndWaitApplied(t, leader.Node, "cycle-write")

		if err := leader.TransferLeadership(ctx, step.to); err != nil {
			t.Fatalf("step %d: TransferLeadership(%d -> %d): %v", i, step.from, step.to, err)
		}
		if target.Role() != Leader {
			t.Fatalf("step %d: node %d.Role() = %v, want Leader after the transfer", i, step.to, target.Role())
		}
		newTerm := target.CurrentTerm()
		if newTerm <= lastTerm {
			t.Fatalf("step %d: term did not advance: %d <= %d", i, newTerm, lastTerm)
		}
		lastTerm = newTerm
	}

	// The final leader's own commitIndex cannot retroactively cover the
	// earlier terms' entries until it commits something in its own term
	// (Raft's commit rule — see maybeAdvanceCommitIndexLocked: an
	// older-term entry only becomes committed as a side effect of
	// committing a later current-term one). One more write forces that.
	final := c.nodes[1]
	_ = proposeAsLeaderAndWaitApplied(t, final.Node, "final-write")

	if !waitFor(2*time.Second, func() bool { return len(final.state.snapshotOf()) >= len(cycle)+1 }) {
		t.Fatalf("final node never caught up on all cycle writes: got %v", final.state.snapshotOf())
	}
	if got := final.state.snapshotOf(); len(got) != len(cycle)+1 {
		t.Fatalf("final applied command count = %d, want %d (one per cycle step, plus the final write)", len(got), len(cycle)+1)
	}
}
