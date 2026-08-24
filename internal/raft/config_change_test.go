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

// TestCreateSnapshotBlockedDuringJointThenAllowedAfter is the mandatory
// scenario: CreateSnapshot must refuse with ErrMembershipChangeInProgress
// while a joint transition is active, and succeed once it completes — a
// snapshot can only ever describe a single Stable membership.
func TestCreateSnapshotBlockedDuringJointThenAllowedAfter(t *testing.T) {
	net := newFakeNetwork()
	smA := newFakeStateMachine()
	a := openSnapshottingNode(t, t.TempDir(), 1, map[NodeID]string{2: "B", 3: "C"}, smA)
	smB := newFakeStateMachine()
	b := openSnapshottingNode(t, t.TempDir(), 2, map[NodeID]string{1: "A", 3: "C"}, smB)
	smC := newFakeStateMachine()
	c := openSnapshottingNode(t, t.TempDir(), 3, map[NodeID]string{1: "A", 2: "B"}, smC)
	for _, n := range []*Node{a, b, c} {
		n.send = net.send
		n.sendAppend = net.sendAppend
		n.sendInstallSnapshot = net.sendInstallSnapshot
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)
	electCtx, electCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer electCancel()
	if err := a.StartElection(electCtx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	_ = proposeAsLeaderAndWaitApplied(t, a, "before")

	// Block every other node so the Joint entry AddVoter appends can
	// never reach the majority(old=ABC)=2 it needs to commit, keeping the
	// transition stuck in ModeJoint until healed below.
	net.setBlocked("B", true)
	net.setBlocked("C", true)

	changeCtx, changeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer changeCancel()
	changeDone := make(chan error, 1)
	go func() { changeDone <- a.AddVoter(changeCtx, 4, "D") }()

	if !waitFor(time.Second, func() bool { return a.MembershipStatus().Mode == ModeJoint }) {
		t.Fatalf("Joint entry never locally activated")
	}
	if err := a.CreateSnapshot(); !errors.Is(err, ErrMembershipChangeInProgress) {
		t.Fatalf("CreateSnapshot during Joint: err = %v, want ErrMembershipChangeInProgress", err)
	}

	net.setBlocked("B", false)
	net.setBlocked("C", false)
	if err := <-changeDone; err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if a.MembershipStatus().Mode != ModeStable {
		t.Fatalf("Mode = %v after transition completed, want ModeStable", a.MembershipStatus().Mode)
	}
	if err := a.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot after transition completed: %v", err)
	}
}

// TestJointWriteCommitRequiresBothMajorities is the mandatory partition
// proof for write commitment during a Joint transition: reaching a
// majority of the OLD configuration alone is not sufficient — a write
// only commits once a majority of NEW is also reachable. This is a
// real end-to-end commit proof (real Propose/replication/CommitIndex),
// complementing the pure Membership.HasQuorum math in membership_test.go.
func TestJointWriteCommitRequiresBothMajorities(t *testing.T) {
	a, _, _, _, _, _, net := threeNodeFakeClusterWithApply(t)
	d, _ := newFakeNodeWithApply(t, 4, nil)
	d.send, d.sendAppend, d.sendInstallSnapshot = net.send, net.sendAppend, net.sendInstallSnapshot
	net.register("D", d)
	net.setBlocked("D", true) // D is registered but never reachable in this test
	// Block C before the Joint entry ever exists, so the transition can
	// never auto-complete out from under this test (new=ABCD needs 3 of
	// 4; with C and D both blocked, only A+B are ever mutually reachable
	// until this test opens C up below).
	net.setBlocked("C", true)

	activateJointDirectly(t, a, cfg(1, 2, 3), cfg(1, 2, 3, 4))

	// Case 1: only B reachable besides self. old={A,B}=2/2 (majority(ABC)
	// = 2) is satisfied, but new={A,B}=2/4 (majority(ABCD)=3) is not —
	// the write must NOT commit.
	index1, _, err := a.Propose([]byte("case1"))
	if err != nil {
		t.Fatalf("Propose(case1): %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if a.CommitIndex() >= index1 {
		t.Fatalf("CommitIndex() = %d, want < %d — old-majority-alone must not be sufficient during Joint", a.CommitIndex(), index1)
	}

	// Case 2: also unblock C. old={A,B,C}=3/3 and new={A,B,C}=3/4 are
	// both satisfied — the write (and everything still pending) must now
	// commit, without D ever having been reachable.
	net.setBlocked("C", false)
	if !waitFor(2*time.Second, func() bool { return a.CommitIndex() >= index1 }) {
		t.Fatalf("CommitIndex() = %d, want >= %d once both majorities are reachable", a.CommitIndex(), index1)
	}
}

// TestJointReadIndexRequiresBothMajorities is the mandatory partition
// proof for ReadIndex during a Joint transition: reaching a majority of
// OLD alone must not be sufficient to confirm a read quorum.
func TestJointReadIndexRequiresBothMajorities(t *testing.T) {
	a, _, _, _, _, _, net := threeNodeFakeClusterWithApply(t)
	d, _ := newFakeNodeWithApply(t, 4, nil)
	d.send, d.sendAppend, d.sendInstallSnapshot = net.send, net.sendAppend, net.sendInstallSnapshot
	net.register("D", d)
	net.setBlocked("D", true)
	// Block C before the Joint entry ever exists — see the identical
	// comment in TestJointWriteCommitRequiresBothMajorities for why.
	net.setBlocked("C", true)

	activateJointDirectly(t, a, cfg(1, 2, 3), cfg(1, 2, 3, 4))

	// old={A,B}=2/2 satisfied, new={A,B}=2/4 not — ReadIndex must fail.
	roCtx, roCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err := a.ReadIndex(roCtx)
	roCancel()
	if err == nil {
		t.Fatalf("ReadIndex succeeded with only old's majority reachable during Joint, want failure")
	}

	// old={A,B,C}=3/3, new={A,B,C}=3/4 — both satisfied, ReadIndex must
	// now succeed.
	net.setBlocked("C", false)
	roCtx2, roCancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer roCancel2()
	if _, err := a.ReadIndex(roCtx2); err != nil {
		t.Fatalf("ReadIndex failed once both majorities are reachable: %v", err)
	}
}

// activateJointDirectly appends and locally activates Joint(oldC, newC)
// on n's own log, bypassing AddVoter/RemoveVoter (which block until the
// whole transition completes) so a test can control quorum reachability
// precisely while Mode stays Joint. n must currently be Leader.
func activateJointDirectly(t *testing.T, n *Node, oldC, newC Configuration) {
	t.Helper()
	joint := JointMembership(oldC, newC)
	b, err := EncodeMembership(joint)
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}
	n.mu.Lock()
	term := n.persistent.CurrentTerm
	if err := n.log.Append([]LogEntry{{Term: term, Kind: EntryConfiguration, Command: b}}); err != nil {
		n.mu.Unlock()
		t.Fatalf("Append: %v", err)
	}
	n.rebuildMembershipLocked()
	if n.membership.Mode != ModeJoint {
		n.mu.Unlock()
		t.Fatalf("test bug: Joint did not activate")
	}
	n.mu.Unlock()
}

// TestJointElectionRequiresBothMajorities is the mandatory partition
// proof for leader election during a Joint transition: a candidate must
// not win with only a majority of OLD — it needs a majority of NEW too.
// C and D are kept blocked from before the Joint entry is even appended,
// so the transition can never auto-complete (new=ABCD needs 3 of 4, and
// only A+B are ever mutually reachable until this test opens C up) —
// otherwise the leader-crash self-healing logic (by design) would race
// ahead and finish the transition before this test can observe it mid-way.
func TestJointElectionRequiresBothMajorities(t *testing.T) {
	a, b, _, _, _, _, net := threeNodeFakeClusterWithApply(t)
	d, _ := newFakeNodeWithApply(t, 4, nil)
	d.send, d.sendAppend, d.sendInstallSnapshot = net.send, net.sendAppend, net.sendInstallSnapshot
	net.register("D", d)
	net.setBlocked("C", true)
	net.setBlocked("D", true)

	activateJointDirectly(t, a, cfg(1, 2, 3), cfg(1, 2, 3, 4))
	if !waitFor(time.Second, func() bool { return b.MembershipStatus().Mode == ModeJoint }) {
		t.Fatalf("B never received/activated the replicated Joint entry")
	}

	// Case 1: only A reachable besides self (C, D still blocked). B's
	// candidacy gets A's vote: old={A,B}=2/2 (majority(ABC)=2) satisfied,
	// but new={A,B}=2/4 (majority(ABCD)=3) is not — B must not win.
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	if err := b.StartElection(ctx1); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	cancel1()
	if b.Role() == Leader {
		t.Fatalf("B won with only old's majority (A+B) during a Joint add transition")
	}

	// Case 2: also unblock C (D stays blocked). B's candidacy gets A's
	// and C's votes: old={A,B,C}=3/3 and new={A,B,C}=3/4 — both
	// satisfied, B must win, with D never having been reachable.
	net.setBlocked("C", false)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := b.StartElection(ctx2); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("B did not win with both majorities satisfied (A+B+C) during a Joint add transition")
	}
}
