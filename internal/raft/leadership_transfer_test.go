package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTransferLeadershipRejectsNonLeader(t *testing.T) {
	n := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.TransferLeadership(ctx, 2); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("TransferLeadership on a Follower: err = %v, want ErrNotLeader", err)
	}
}

func TestTransferLeadershipRejectsSelf(t *testing.T) {
	n := singleNodeLeader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.TransferLeadership(ctx, n.id); !errors.Is(err, ErrCannotTransferToSelf) {
		t.Fatalf("TransferLeadership(self): err = %v, want ErrCannotTransferToSelf", err)
	}
}

func TestTransferLeadershipRejectsUnknownTarget(t *testing.T) {
	n := singleNodeLeader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.TransferLeadership(ctx, 99); !errors.Is(err, ErrNotAVoter) {
		t.Fatalf("TransferLeadership(unknown target): err = %v, want ErrNotAVoter", err)
	}
}

// TestTransferLeadershipRejectsNonVoterTarget proves a target that is
// not (yet, or no longer) an effective voter is rejected the same way an
// entirely unknown NodeID is.
func TestTransferLeadershipRejectsNonVoterTarget(t *testing.T) {
	n := singleNodeLeader(t)
	// Node 5 is known operationally (n.peers has a real address for it —
	// e.g. a removed former voter this node hasn't forgotten how to dial)
	// but is not in the effective Stable configuration — distinct from an
	// entirely unknown NodeID, proving the check consults effective
	// membership, not merely n.peers.
	n.SetPeers(map[NodeID]string{5: "old-address"})
	n.mu.Lock()
	n.membership = StableMembership(cfg(1)) // 5 not in the effective configuration
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.TransferLeadership(ctx, 5); !errors.Is(err, ErrNotAVoter) {
		t.Fatalf("TransferLeadership(non-voter target): err = %v, want ErrNotAVoter", err)
	}
}

func TestTransferLeadershipRejectsDuringJointMembership(t *testing.T) {
	n := singleNodeLeader(t)
	n.mu.Lock()
	n.membership = JointMembership(cfg(1), cfg(1, 2))
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.TransferLeadership(ctx, 2); !errors.Is(err, ErrMembershipChangeInProgress) {
		t.Fatalf("TransferLeadership during Joint: err = %v, want ErrMembershipChangeInProgress", err)
	}
}

// TestConcurrentTransferRequestsOnlyOneSucceeds is the mandatory
// concurrency scenario: two goroutines call TransferLeadership
// concurrently; exactly one proceeds, the other gets
// ErrLeadershipTransferInProgress.
func TestConcurrentTransferRequestsOnlyOneSucceeds(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c := newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*Node{a, b, c} {
		n.send, n.sendAppend, n.sendPreVote, n.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	// Block replication to both targets BEFORE proposing anything: A's
	// replication workers (see replication_worker.go) are real,
	// persistent, and already idling-ready the instant A becomes
	// leader — blocking after Propose returns leaves a real window where
	// an already-woken worker completes a full (in-process, near-instant)
	// round trip and advances matchIndex before the block ever takes
	// effect, especially now that there is no per-round goroutine-spawn
	// delay to rely on for timing separation.
	net.setBlocked("B", true)
	net.setBlocked("C", true)
	// Something to catch up on: without a proposed entry, an empty log
	// trivially satisfies matchIndex >= LastIndex and both calls would
	// race straight to TimeoutNow instead of contending on the same
	// guard for the duration of the test.
	if _, _, err := a.Propose([]byte("x")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer shortCancel()

	results := make(chan error, 2)
	go func() { results <- a.TransferLeadership(shortCtx, 2) }()
	go func() { results <- a.TransferLeadership(shortCtx, 3) }()

	r1, r2 := <-results, <-results
	successes, inProgress := 0, 0
	for _, err := range []error{r1, r2} {
		switch {
		case errors.Is(err, ErrLeadershipTransferInProgress):
			inProgress++
		case errors.Is(err, context.DeadlineExceeded):
			successes++ // the one that actually started, then timed out catching up
		default:
			t.Fatalf("unexpected result: %v", err)
		}
	}
	if successes != 1 || inProgress != 1 {
		t.Fatalf("results = [%v, %v], want exactly one ErrLeadershipTransferInProgress and one deadline-exceeded (the one that started)", r1, r2)
	}
}

// TestTransferLeadershipTargetAlreadyCaughtUp proves the catch-up phase
// is skipped entirely (no wait) when the target already matches
// LastIndex at the moment TransferLeadership is called.
func TestTransferLeadershipTargetAlreadyCaughtUp(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A"})
	a.send, a.sendAppend, a.sendPreVote, a.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	b.send, b.sendAppend, b.sendPreVote, b.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	net.register("A", a)
	net.register("B", b)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if _, _, err := a.Propose([]byte("x")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !waitFor(time.Second, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.matchIndex[2] >= a.log.LastIndex()
	}) {
		t.Fatalf("B never caught up naturally via heartbeat replication")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.TransferLeadership(ctx, 2); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if !waitFor(time.Second, func() bool { return b.Role() == Leader }) {
		t.Fatalf("B never became leader: role=%v", b.Role())
	}
	if a.Role() != Follower {
		t.Fatalf("A.Role() = %v, want Follower after a successful transfer", a.Role())
	}
}

// TestTransferLeadershipTargetBehindWaitsForCatchUp proves the transfer
// waits for (and completes only after) an initially-behind target
// actually catches up — it does not send TimeoutNow prematurely.
func TestTransferLeadershipTargetBehindWaitsForCatchUp(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A"})
	a.send, a.sendAppend, a.sendPreVote, a.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	b.send, b.sendAppend, b.sendPreVote, b.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	net.register("A", a)
	net.register("B", b)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	// B falls behind: block it, propose several entries, then unblock.
	net.setBlocked("B", true)
	for _, cmd := range []string{"x", "y", "z"} {
		if _, _, err := a.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
	}
	lastIndex := a.LastLogIndex()

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- a.TransferLeadership(ctx, 2)
	}()

	// While B is still unreachable, the transfer must not have completed.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("TransferLeadership returned early (err=%v) before B ever caught up", err)
	default:
	}

	net.setBlocked("B", false)
	if err := <-done; err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if !waitFor(time.Second, func() bool { return b.Role() == Leader }) {
		t.Fatalf("B never became leader: role=%v", b.Role())
	}
	if b.LastLogIndex() < lastIndex {
		t.Fatalf("B.LastLogIndex() = %d, want >= %d — B must have actually caught up before winning", b.LastLogIndex(), lastIndex)
	}
}

// TestTransferLeadershipContextCancellationDuringCatchUp is the
// mandatory pre-handoff cancellation scenario: if ctx is canceled while
// still catching up (before TimeoutNow is ever sent), the leader remains
// leader and normal operation resumes.
func TestTransferLeadershipContextCancellationDuringCatchUp(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A"})
	a.send, a.sendAppend, a.sendPreVote, a.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	b.send, b.sendAppend, b.sendPreVote, b.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	net.register("A", a)
	net.register("B", b)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("B", true) // B can never catch up
	if _, _, err := a.Propose([]byte("x")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := a.TransferLeadership(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("TransferLeadership: err = %v, want context.DeadlineExceeded", err)
	}
	if a.Role() != Leader {
		t.Fatalf("A.Role() = %v, want unchanged Leader after a failed catch-up", a.Role())
	}
	// Normal operation resumes: the freeze (if any was even set — it
	// shouldn't have been, catch-up never reached Handoff) is released.
	if _, _, err := a.Propose([]byte("y")); err != nil {
		t.Fatalf("Propose after failed transfer: %v", err)
	}
}

// TestTransferLeadershipReturnsBoundedErrorOnNodeClose proves Close
// unblocks a pending TransferLeadership call rather than leaking it.
func TestTransferLeadershipReturnsBoundedErrorOnNodeClose(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A"})
	a.send, a.sendAppend, a.sendPreVote, a.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	b.send, b.sendAppend, b.sendPreVote, b.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	net.register("A", a)
	net.register("B", b)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("B", true)
	if _, _, err := a.Propose([]byte("x")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- a.TransferLeadership(context.Background(), 2)
	}()
	time.Sleep(20 * time.Millisecond) // ensure the call has registered its wait
	a.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrNodeClosed) {
			t.Fatalf("err = %v, want ErrNodeClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("TransferLeadership did not return after Close — leaked")
	}
}

// TestMembershipChangeRejectedDuringTransfer is the mandatory pairing to
// TestTransferLeadershipRejectsDuringJointMembership: while a leadership
// transfer is active (catching up or already in handoff), AddVoter/
// RemoveVoter must be rejected — only one major administrative
// transition runs at a time, in either direction.
func TestMembershipChangeRejectedDuringTransfer(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c := newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*Node{a, b, c} {
		n.send, n.sendAppend, n.sendPreVote, n.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	// Block B (the transfer target) before proposing anything — see the
	// identical fix in TestConcurrentTransferRequestsOnlyOneSucceeds: A's
	// replication worker for B is real and already idling-ready the
	// instant A becomes leader, so blocking only after Propose returns
	// leaves a window where B's worker races ahead and catches up before
	// the block takes effect. If that happens here, TransferLeadership
	// finds catch-up already satisfied, its TimeoutNow to now-blocked B
	// fails instantly, and the whole call can return before this test's
	// own polling loop below ever gets a chance to observe a.transfer
	// non-nil.
	net.setBlocked("B", true)
	if _, _, err := a.Propose([]byte("x")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	transferCtx, transferCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer transferCancel()
	transferDone := make(chan error, 1)
	go func() { transferDone <- a.TransferLeadership(transferCtx, 2) }()

	// Give the transfer a moment to register itself as active before
	// racing a membership change against it.
	if !waitFor(time.Second, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.transfer != nil
	}) {
		t.Fatalf("transfer never registered as active")
	}

	if err := a.AddVoter(context.Background(), 4, "D"); !errors.Is(err, ErrLeadershipTransferInProgress) {
		t.Fatalf("AddVoter during active transfer: err = %v, want ErrLeadershipTransferInProgress", err)
	}

	transferCancel()
	<-transferDone // let the transfer's own goroutine finish before test cleanup
}

// TestSelfRemovalDoesNotRequireLeadershipTransfer is the mandatory
// regression check (items 82/132): Milestone 10's leader self-removal
// remains independently valid — it never requires a prior
// TransferLeadership call.
func TestSelfRemovalDoesNotRequireLeadershipTransfer(t *testing.T) {
	a, _, _, _, _, _, _ := threeNodeFakeClusterWithApply(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.RemoveVoter(ctx, 1); err != nil {
		t.Fatalf("RemoveVoter(self): %v", err)
	}
	if a.Role() == Leader {
		t.Fatalf("Role() = Leader after self-removal completed, want a passive Follower")
	}
	if a.MembershipStatus().Stable.Has(1) {
		t.Fatalf("final Stable configuration still has self-removed voter 1")
	}
}
