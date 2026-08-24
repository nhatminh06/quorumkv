package raft

import "testing"

// appendConfigEntry appends a raw EntryConfiguration log entry encoding m,
// at the current term, without going through any membership-change API
// (which doesn't exist yet at the point these mechanics are exercised) —
// a direct, white-box way to test rebuildMembershipLocked's walk.
func appendConfigEntry(t *testing.T, n *Node, term Term, m Membership) LogIndex {
	t.Helper()
	b, err := EncodeMembership(m)
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}
	n.mu.Lock()
	if err := n.log.Append([]LogEntry{{Term: term, Kind: EntryConfiguration, Command: b}}); err != nil {
		n.mu.Unlock()
		t.Fatalf("Append: %v", err)
	}
	idx := n.log.LastIndex()
	n.rebuildMembershipLocked()
	n.mu.Unlock()
	return idx
}

// TestRebuildActivatesJointBeforeCommit proves a Joint configuration
// entry takes effect as soon as it is locally appended — before it ever
// commits — per the spec's "derive from local log" rule (item 29).
func TestRebuildActivatesJointBeforeCommit(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, map[NodeID]string{2: "B", 3: "C"})
	joint := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	idx := appendConfigEntry(t, n, 1, joint)

	n.mu.Lock()
	got := n.membership
	commitIndex := n.commitIndex
	n.mu.Unlock()

	if commitIndex >= idx {
		t.Fatalf("test bug: entry must not be committed yet")
	}
	if !got.Equal(joint) {
		t.Fatalf("membership = %+v, want the Joint entry activated pre-commit: %+v", got, joint)
	}
}

// TestRebuildKeepsJointQuorumUntilFinalStableCommits proves the
// deliberately conservative rule for the transition's final Stable
// entry: even though it is locally appended, effective membership stays
// at the preceding Joint state until the Stable entry itself commits.
func TestRebuildKeepsJointQuorumUntilFinalStableCommits(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, map[NodeID]string{2: "B", 3: "C"})
	oldC, newC := cfg(1, 2, 3), cfg(1, 2, 3, 4)
	joint := JointMembership(oldC, newC)
	appendConfigEntry(t, n, 1, joint)

	stable := StableMembership(newC)
	stableIdx := appendConfigEntry(t, n, 1, stable)

	n.mu.Lock()
	got := n.membership
	commitIndex := n.commitIndex
	n.mu.Unlock()
	if commitIndex >= stableIdx {
		t.Fatalf("test bug: final Stable entry must not be committed yet")
	}
	if !got.Equal(joint) {
		t.Fatalf("membership = %+v, want still Joint (uncommitted final Stable must not activate): %+v", got, joint)
	}

	// Now commit through stableIdx and rebuild: the final Stable entry
	// must activate.
	n.mu.Lock()
	n.commitIndex = stableIdx
	n.rebuildMembershipLocked()
	got2 := n.membership
	n.mu.Unlock()
	if !got2.Equal(stable) {
		t.Fatalf("membership = %+v, want Stable now that the final entry committed: %+v", got2, stable)
	}
}

// TestRebuildRevertsOnTruncation proves an uncommitted Joint entry that
// gets overwritten by conflict repair (log truncation) is no longer
// reflected in effective membership — it reverts to whatever preceded it.
func TestRebuildRevertsOnTruncation(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, map[NodeID]string{2: "B", 3: "C"})
	joint := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	idx := appendConfigEntry(t, n, 1, joint)

	n.mu.Lock()
	bootstrap := n.bootstrapConfigurationLocked()
	if err := n.log.TruncateAndAppend(idx, []LogEntry{{Term: 1, Command: []byte("x")}}); err != nil {
		n.mu.Unlock()
		t.Fatalf("TruncateAndAppend: %v", err)
	}
	n.rebuildMembershipLocked()
	got := n.membership
	n.mu.Unlock()

	want := StableMembership(bootstrap)
	if !got.Equal(want) {
		t.Fatalf("membership after truncating away the Joint entry = %+v, want reverted to %+v", got, want)
	}
}
