package raft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newFakeNodeWithApply is newFakeNode plus a real ApplyFunc/state
// machine, so a membership-change test can prove a node actually
// replicated and applied entries (not merely that its log grew).
func newFakeNodeWithApply(t *testing.T, id NodeID, peers map[NodeID]string) (*Node, *fakeStateMachine) {
	t.Helper()
	sm := newFakeStateMachine()
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "state"))
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	n, err := NewNode(id, store, log, commitStore, NewSnapshotStore(filepath.Join(dir, "snapshot")), peers, sm.apply, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(n.Close)
	return n, sm
}

// threeNodeFakeClusterWithApply wires A/B/C together via a fakeNetwork
// (like setupThreeNodeFakeCluster) but with real ApplyFunc/state
// machines, and elects A leader.
func threeNodeFakeClusterWithApply(t *testing.T) (a, b, c *Node, smA, smB, smC *fakeStateMachine, net *fakeNetwork) {
	t.Helper()
	net = newFakeNetwork()
	a, smA = newFakeNodeWithApply(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b, smB = newFakeNodeWithApply(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c, smC = newFakeNodeWithApply(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*Node{a, b, c} {
		n.send = net.send
		n.sendAppend = net.sendAppend
		n.sendInstallSnapshot = net.sendInstallSnapshot
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	return a, b, c, smA, smB, smC, net
}

func TestAddVoterRejectsNonLeader(t *testing.T) {
	n := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.AddVoter(ctx, 3, "C"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("AddVoter on a Follower: err = %v, want ErrNotLeader", err)
	}
}

func TestRemoveVoterRejectsNonLeader(t *testing.T) {
	n := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.RemoveVoter(ctx, 2); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("RemoveVoter on a Follower: err = %v, want ErrNotLeader", err)
	}
}

// singleNodeLeader starts a one-node cluster and elects it leader (a
// single-node cluster wins its own election with no network traffic),
// letting AddVoter/RemoveVoter validation be tested without needing a
// real quorum of peers to also be reachable.
func singleNodeLeader(t *testing.T) *Node {
	t.Helper()
	n := newFakeNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader", n.Role())
	}
	return n
}

func TestAddVoterRejectsExistingID(t *testing.T) {
	n := singleNodeLeader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.AddVoter(ctx, 1, "self-addr"); !errors.Is(err, ErrAlreadyVoter) {
		t.Fatalf("AddVoter(existing self ID): err = %v, want ErrAlreadyVoter", err)
	}
}

func TestRemoveVoterRejectsUnknownID(t *testing.T) {
	n := singleNodeLeader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.RemoveVoter(ctx, 99); !errors.Is(err, ErrNotAVoter) {
		t.Fatalf("RemoveVoter(unknown ID): err = %v, want ErrNotAVoter", err)
	}
}

// TestRemoveVoterRejectsRemovingLastVoter proves NewConfiguration's
// empty-configuration rejection surfaces through RemoveVoter rather than
// silently producing a zero-voter cluster.
func TestRemoveVoterRejectsRemovingLastVoter(t *testing.T) {
	n := singleNodeLeader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.RemoveVoter(ctx, 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("RemoveVoter(last voter): err = %v, want ErrInvalidConfiguration", err)
	}
}

// TestConcurrentMembershipChangesOnlyOneSucceeds is the mandatory
// concurrency scenario (item ~112): two goroutines call AddVoter/
// RemoveVoter concurrently on the same leader; exactly one transition
// starts, the other observes ErrMembershipChangeInProgress, and there is
// no race (run under -race).
func TestConcurrentMembershipChangesOnlyOneSucceeds(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c := newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*Node{a, b, c} {
		n.send = net.send
		n.sendAppend = net.sendAppend
		n.sendInstallSnapshot = net.sendInstallSnapshot
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0] = a.AddVoter(ctx, 4, "D")
	}()
	go func() {
		defer wg.Done()
		results[1] = a.RemoveVoter(ctx, 2)
	}()
	wg.Wait()

	successes, inProgress := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrMembershipChangeInProgress):
			inProgress++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || inProgress != 1 {
		t.Fatalf("results = %v, want exactly one success and one ErrMembershipChangeInProgress", results)
	}
}

// TestMembershipStatusIsDefensiveCopy proves mutating a returned
// MembershipStatus's Configuration maps never reaches back into Node
// state.
func TestMembershipStatusIsDefensiveCopy(t *testing.T) {
	n := singleNodeLeader(t)
	status := n.MembershipStatus()
	if status.Mode != ModeStable {
		t.Fatalf("Mode = %v, want ModeStable", status.Mode)
	}
	for id := range status.Stable.Voters {
		status.Stable.Voters[id] = "corrupted"
	}
	status2 := n.MembershipStatus()
	for id, addr := range status2.Stable.Voters {
		if addr == "corrupted" {
			t.Fatalf("mutating a returned MembershipStatus affected Node state: voter %d = %q", id, addr)
		}
	}
}

// TestAddVoterEndToEndNewNodeCatchesUpAndBecomesVoter is the mandatory
// end-to-end add scenario: a fresh node (no prior knowledge of the
// cluster) is added via AddVoter, replicates and applies the cluster's
// existing history plus the transition's own entries, and ends up a full
// Stable voter — all before AddVoter returns (it only returns once the
// final Stable entry has committed and applied, not merely appended).
func TestAddVoterEndToEndNewNodeCatchesUpAndBecomesVoter(t *testing.T) {
	a, _, _, smA, _, _, net := threeNodeFakeClusterWithApply(t)
	preIndex := proposeAsLeaderAndWaitApplied(t, a, "before-add")

	d, smD := newFakeNodeWithApply(t, 4, nil) // D starts knowing nothing about the cluster
	d.send = net.send
	d.sendAppend = net.sendAppend
	d.sendInstallSnapshot = net.sendInstallSnapshot
	net.register("D", d)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.AddVoter(ctx, 4, "D"); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}

	status := a.MembershipStatus()
	if status.Mode != ModeStable {
		t.Fatalf("Mode = %v, want ModeStable once the transition completes", status.Mode)
	}
	for _, id := range []NodeID{1, 2, 3, 4} {
		if !status.Stable.Has(id) {
			t.Fatalf("final Stable configuration missing voter %d: %+v", id, status.Stable)
		}
	}

	// D must have actually replicated+applied the pre-existing entry, not
	// just the transition's own Configuration entries.
	if !waitFor(2*time.Second, func() bool { return d.LastApplied() >= preIndex }) {
		t.Fatalf("D did not catch up: LastApplied()=%d, want >= %d", d.LastApplied(), preIndex)
	}
	if got := smD.snapshotOf(); len(got) != 1 || got[0] != "before-add" {
		t.Fatalf("D's applied commands = %v, want [before-add]", got)
	}

	// A post-add write must still work and reach every voter including D.
	postIndex := proposeAsLeaderAndWaitApplied(t, a, "after-add")
	if !waitFor(2*time.Second, func() bool { return d.LastApplied() >= postIndex }) {
		t.Fatalf("D did not receive the post-add write: LastApplied()=%d, want >= %d", d.LastApplied(), postIndex)
	}
	_ = smA // keep for readability/symmetry with the multi-return helper
}

// TestRemoveVoterEndToEndDropsVoterFromTargets is the mandatory end-to-end
// remove scenario: RemoveVoter completes, the removed node is no longer a
// replication/quorum target, and it is absent from the final Stable
// configuration.
func TestRemoveVoterEndToEndDropsVoterFromTargets(t *testing.T) {
	a, _, c, _, _, _, _ := threeNodeFakeClusterWithApply(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.RemoveVoter(ctx, 2); err != nil {
		t.Fatalf("RemoveVoter: %v", err)
	}

	status := a.MembershipStatus()
	if status.Mode != ModeStable {
		t.Fatalf("Mode = %v, want ModeStable", status.Mode)
	}
	if status.Stable.Has(2) {
		t.Fatalf("final Stable configuration still has removed voter 2: %+v", status.Stable)
	}
	if !status.Stable.Has(1) || !status.Stable.Has(3) {
		t.Fatalf("final Stable configuration missing a retained voter: %+v", status.Stable)
	}

	// A write must still commit through the surviving two-node majority
	// (A+C, majority(2)=2) without node B's participation.
	index := proposeAsLeaderAndWaitApplied(t, a, "after-remove")
	if !waitFor(2*time.Second, func() bool { return c.LastApplied() >= index }) {
		t.Fatalf("C did not receive the post-remove write: LastApplied()=%d, want >= %d", c.LastApplied(), index)
	}
}

// TestSelfRemovalLeaderStepsDownAfterCompletion is the mandatory
// self-removal scenario: a leader can remove itself; the transition
// proceeds to completion (the leader keeps leading long enough to finish
// it); once the final Stable entry — which excludes it — commits and
// applies, it stops acting as leader, with no higher term required.
func TestSelfRemovalLeaderStepsDownAfterCompletion(t *testing.T) {
	a, _, _, _, _, _, _ := threeNodeFakeClusterWithApply(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.RemoveVoter(ctx, 1); err != nil {
		t.Fatalf("RemoveVoter(self): %v", err)
	}

	if a.Role() == Leader {
		t.Fatalf("Role() = Leader after self-removal completed, want a passive Follower")
	}
	status := a.MembershipStatus()
	if status.Stable.Has(1) {
		t.Fatalf("final Stable configuration still has self-removed voter 1: %+v", status.Stable)
	}
}
