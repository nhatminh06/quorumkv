package raft

import (
	"context"
	"testing"
	"time"
)

// clearLeaderContact resets lastLeaderContact on every still-running node
// in c, simulating enough real time having passed for PreVote's
// leader-contact safeguard to no longer apply — see electAndWaitLeader in
// fault_recovery_test.go for the identical technique/rationale.
func clearLeaderContact(c *faultCluster) {
	for _, n := range c.nodes {
		n.mu.Lock()
		n.lastLeaderContact = time.Time{}
		n.mu.Unlock()
	}
}

// seedJointEntry directly appends and commits Joint(oldC, newC) on n's
// own log — mimicking what changeMembership does, but without calling
// maybeCompleteMembershipTransitionLocked, so the caller controls
// precisely whether/when the completing Stable entry ever gets appended.
// Used to construct an exact "the leader crashed here" state rather than
// racing a live system into it (see TestRestartFinishesInterruptedCompaction
// for the established pattern this follows).
func seedJointEntry(t *testing.T, n *Node, term Term, oldC, newC Configuration) LogIndex {
	t.Helper()
	joint := JointMembership(oldC, newC)
	b, err := EncodeMembership(joint)
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.log.Append([]LogEntry{{Term: term, Kind: EntryConfiguration, Command: b}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	idx := n.log.LastIndex()
	n.rebuildMembershipLocked()
	return idx
}

// markCommittedLocked directly marks index as committed on n, without
// running the normal maybeAdvanceCommitIndexLocked quorum-counting path
// (which would also trigger maybeCompleteMembershipTransitionLocked) —
// simulating "this index is already known-committed" as of the crash
// point being constructed.
func markCommitted(t *testing.T, n *Node, index LogIndex) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.commitStore.Save(index); err != nil {
		t.Fatalf("commitStore.Save: %v", err)
	}
	n.commitIndex = index
	n.rebuildMembershipLocked()
}

// TestLeaderCrashAfterJointCommitBeforeStableAppended is the mandatory
// "strongest scenario," first crash point: the Joint entry has committed
// on a majority (here, all three nodes, to keep setup simple) but the
// leader crashes before ever appending the completing Stable entry. A
// newly elected leader must notice and finish the transition on its own —
// the cluster must never get permanently stuck in Joint.
func TestLeaderCrashAfterJointCommitBeforeStableAppended(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a := c.node(1)
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	term := a.CurrentTerm()

	// A removes itself: old=ABC (majority 2), new=BC (majority 2) — chosen
	// deliberately so the crashing leader (A) is not itself needed to
	// reach either majority afterward.
	oldC, newC := cfg(1, 2, 3), cfg(2, 3)
	idxA := seedJointEntry(t, a, term, oldC, newC)
	idxB := seedJointEntry(t, c.node(2), term, oldC, newC)
	idxC := seedJointEntry(t, c.node(3), term, oldC, newC)
	if idxA != idxB || idxA != idxC {
		t.Fatalf("test bug: seeded entries at mismatched indices %d/%d/%d", idxA, idxB, idxC)
	}
	// Commit it everywhere — a real majority has it — but critically
	// never call maybeCompleteMembershipTransitionLocked on A: this is
	// exactly "the leader crashed right after the Joint entry committed,
	// before it could append the Stable entry that would finish the
	// transition."
	markCommitted(t, a, idxA)
	markCommitted(t, c.node(2), idxA)
	markCommitted(t, c.node(3), idxA)
	if a.MembershipStatus().Mode != ModeJoint {
		t.Fatalf("test bug: A is not in ModeJoint before the simulated crash")
	}

	c.stop(1) // "A crashes" — Stable was never appended anywhere

	b := c.node(2)
	clearLeaderContact(c)
	if err := b.StartElection(ctx); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("B did not become leader (C is still reachable: old={B,C}=2/2 of ABC, new={B,C}=2/2 of BC)")
	}

	if !waitFor(2*time.Second, func() bool { return b.MembershipStatus().Mode == ModeStable }) {
		t.Fatalf("B never auto-completed the interrupted transition: %+v", b.MembershipStatus())
	}
	status := b.MembershipStatus()
	if status.Stable.Has(1) || !status.Stable.Has(2) || !status.Stable.Has(3) {
		t.Fatalf("final Stable configuration = %+v, want exactly {2,3}", status.Stable)
	}
	if !waitFor(2*time.Second, func() bool { return c.node(3).MembershipStatus().Mode == ModeStable }) {
		t.Fatalf("C never observed the auto-completed transition")
	}
}

// TestLeaderCrashAfterStableAppendedBeforeItCommits is the mandatory
// "strongest scenario," second crash point: the leader appended the
// completing Stable entry to its own log but crashed before it ever
// replicated/committed anywhere else. A newly elected leader (whose log
// never had that uncommitted, never-replicated entry) must still finish
// the transition — starting over with its own completing entry, not get
// stuck waiting for one that will never exist anywhere else.
func TestLeaderCrashAfterStableAppendedBeforeItCommits(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a := c.node(1)
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	term := a.CurrentTerm()

	oldC, newC := cfg(1, 2, 3), cfg(2, 3)
	idxA := seedJointEntry(t, a, term, oldC, newC)
	seedJointEntry(t, c.node(2), term, oldC, newC)
	seedJointEntry(t, c.node(3), term, oldC, newC)
	markCommitted(t, a, idxA)
	markCommitted(t, c.node(2), idxA)
	markCommitted(t, c.node(3), idxA)

	// A appends the completing Stable entry to ITS OWN log only —
	// simulating maybeCompleteMembershipTransitionLocked having run, but
	// the leader crashing before this entry ever reached a single peer.
	stableBytes, err := EncodeMembership(StableMembership(newC))
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}
	a.mu.Lock()
	if err := a.log.Append([]LogEntry{{Term: term, Kind: EntryConfiguration, Command: stableBytes}}); err != nil {
		a.mu.Unlock()
		t.Fatalf("Append: %v", err)
	}
	a.mu.Unlock()

	c.stop(1) // "A crashes" — its local-only Stable entry never reached anyone

	b := c.node(2)
	if b.LastLogIndex() != idxA {
		t.Fatalf("B's log = index %d, want exactly %d (A's Stable entry must never have replicated)", b.LastLogIndex(), idxA)
	}
	clearLeaderContact(c)
	if err := b.StartElection(ctx); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("B did not become leader")
	}

	if !waitFor(2*time.Second, func() bool { return b.MembershipStatus().Mode == ModeStable }) {
		t.Fatalf("B never completed the transition with its own Stable entry: %+v", b.MembershipStatus())
	}
	status := b.MembershipStatus()
	if status.Stable.Has(1) || !status.Stable.Has(2) || !status.Stable.Has(3) {
		t.Fatalf("final Stable configuration = %+v, want exactly {2,3}", status.Stable)
	}
}

// TestConflictRepairRevertsUncommittedJointEntry is the mandatory config
// conflict-repair scenario: a leader appends a Joint entry (activating it
// locally, before commit) but is partitioned away before it ever
// replicates; a new leader elected without that entry replicates its own
// (different) history over it once the partition heals. The old leader's
// conflict-repair truncation must revert its effective membership back
// to Stable — no stale Joint state left over from the entry it never
// actually got to keep.
func TestConflictRepairRevertsUncommittedJointEntry(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a := c.node(1)
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	c.net.partition(1, 2)
	c.net.partition(1, 3)

	// A appends a Joint entry locally — activating it immediately, per
	// the local-log-derived rule — but the partition means it can never
	// replicate or commit.
	seedJointEntry(t, a, a.CurrentTerm(), cfg(1, 2, 3), cfg(1, 2, 3, 4))
	if a.MembershipStatus().Mode != ModeJoint {
		t.Fatalf("test bug: A's Joint entry did not activate locally")
	}

	// B and C can still reach each other; B wins a new, higher-term
	// election without ever having seen A's Joint entry.
	b, cc := c.node(2), c.node(3)
	clearLeaderContact(c)
	if err := b.StartElection(ctx); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("B did not become leader")
	}
	if _, _, err := b.Propose([]byte("after-partition")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return cc.CommitIndex() >= b.CommitIndex() && b.CommitIndex() > 0 }) {
		t.Fatalf("B's write never committed to C")
	}

	// Heal the partition: A receives B's higher-term AppendEntries,
	// steps down, and conflict-repairs its log — discarding its own
	// never-replicated Joint entry.
	c.net.heal(1, 2)
	c.net.heal(1, 3)

	if !waitFor(2*time.Second, func() bool { return a.MembershipStatus().Mode == ModeStable }) {
		t.Fatalf("A's membership never reverted to Stable after conflict repair: %+v", a.MembershipStatus())
	}
	status := a.MembershipStatus()
	if status.Stable.Has(4) {
		t.Fatalf("A's membership still reflects the discarded Joint entry's New side: %+v", status.Stable)
	}
}
