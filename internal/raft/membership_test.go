package raft

import "testing"

func cfg(voters ...NodeID) Configuration {
	m := make(map[NodeID]string, len(voters))
	for _, id := range voters {
		m[id] = addrOf(id)
	}
	c, err := NewConfiguration(m)
	if err != nil {
		panic(err)
	}
	return c
}

func addrOf(id NodeID) string {
	switch id {
	case 1:
		return "A"
	case 2:
		return "B"
	case 3:
		return "C"
	case 4:
		return "D"
	default:
		return "X"
	}
}

func acked(ids ...NodeID) map[NodeID]bool {
	m := make(map[NodeID]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// --- Stable quorum (item 17) ---

func TestStableQuorumBasic(t *testing.T) {
	abc := StableMembership(cfg(1, 2, 3))
	if !abc.HasQuorum(acked(1, 2)) {
		t.Fatalf("A+B should be quorum for stable ABC (majority=2)")
	}
	if abc.HasQuorum(acked(1)) {
		t.Fatalf("A alone should not be quorum for stable ABC")
	}
}

// --- Joint add quorum: old=ABC(maj 2), new=ABCD(maj 3) (item 17) ---
//
// Derivation: majority(3)=2, majority(4)=3.
//
//	A+B:   old={A,B}=2/2 yes; new={A,B}=2/4 no  -> NOT quorum
//	A+B+C: old={A,B,C}=3/2 yes; new={A,B,C}=3/4 yes -> quorum
//	A+B+D: old={A,B}=2/2 yes; new={A,B,D}=3/4 yes -> quorum
func TestJointAddQuorum(t *testing.T) {
	m := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	cases := []struct {
		name string
		ids  []NodeID
		want bool
	}{
		{"A+B", []NodeID{1, 2}, false},
		{"A+B+C", []NodeID{1, 2, 3}, true},
		{"A+B+D", []NodeID{1, 2, 4}, true},
	}
	for _, c := range cases {
		if got := m.HasQuorum(acked(c.ids...)); got != c.want {
			t.Fatalf("%s: HasQuorum = %v, want %v", c.name, got, c.want)
		}
	}
}

// --- Joint remove quorum: old=ABCD(maj 3), new=ABC(maj 2) (item 18) ---
//
// Derivation: majority(4)=3, majority(3)=2.
//
//	A+B:   old={A,B}=2/3 no  -> NOT quorum (regardless of new side)
//	A+B+C: old={A,B,C}=3/3 yes; new={A,B,C}=3/2 yes -> quorum
//	A+B+D: old={A,B,D}=3/3 yes; new={A,B}=2/2 yes -> quorum
//
// (The milestone prompt's own worked example for A+B+D was corrected
// here by deriving the majorities directly rather than copying a
// mistaken example — see item 18's explicit warning.)
func TestJointRemoveQuorum(t *testing.T) {
	m := JointMembership(cfg(1, 2, 3, 4), cfg(1, 2, 3))
	cases := []struct {
		name string
		ids  []NodeID
		want bool
	}{
		{"A+B", []NodeID{1, 2}, false},
		{"A+B+C", []NodeID{1, 2, 3}, true},
		{"A+B+D", []NodeID{1, 2, 4}, true},
	}
	for _, c := range cases {
		if got := m.HasQuorum(acked(c.ids...)); got != c.want {
			t.Fatalf("%s: HasQuorum = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestNoUnionMajorityBug is item 145: a set can be a majority of
// union(old,new) while still failing one side's own majority — the
// implementation must not accidentally compute quorum from the union.
// old=ABC (maj 2), new=ABCD (maj 3): {C,D} is 2 of union{A,B,C,D}=4 (not
// even a union-majority, but more pointedly) satisfies NEITHER old
// (C alone =1/2) NOR new (C+D=2/4) — confirms no union-based shortcut is
// silently granting quorum here. A sharper case: {A,D} is old={A}=1/2 no.
// The decisive case: old=ABC, new=ABCDE (5 members, maj 3); {A,D,E} is a
// majority of union (3 of 5 total members = ABCDE) and even 3/5 of new,
// but old={A}=1/2 — must still be rejected.
func TestNoUnionMajorityBug(t *testing.T) {
	m := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	// {A,D}: old={A}=1/2 no; new={A,D}=2/4 no.
	if m.HasQuorum(acked(1, 4)) {
		t.Fatalf("A+D must not be quorum: old has only A (1/2), new has only A+D (2/4)")
	}
	// Union(old,new) = {A,B,C,D}, size 4, union-majority = 3. {B,C,D} is a
	// union-majority (3/4) but old={B,C}=2/3... wait derive precisely:
	// old=ABC maj=2, {B,C,D}∩old={B,C}=2/2 yes; new=ABCD maj=3,
	// {B,C,D}∩new={B,C,D}=3/4 yes -> this one IS correctly quorum under
	// the real (AND) rule, so assert true here to keep the derivation
	// honest rather than asserting an incorrect expectation.
	if !m.HasQuorum(acked(2, 3, 4)) {
		t.Fatalf("B+C+D should be quorum: old={B,C}=2/2, new={B,C,D}=3/4")
	}
	// A case that IS a strict union-majority but fails the real rule:
	// old=AB C (3 members), new=ABCDE (5 members). Union size 5,
	// union-majority=3. {C,D,E}: old={C}=1/2 no; new={C,D,E}=3/5 yes ->
	// union-majority(3/5 of union since union==new here) but old fails.
	m2 := JointMembership(cfg(1, 2, 3), func() Configuration {
		v := map[NodeID]string{1: "A", 2: "B", 3: "C", 4: "D", 5: "E"}
		c, err := NewConfiguration(v)
		if err != nil {
			t.Fatalf("NewConfiguration: %v", err)
		}
		return c
	}())
	if m2.HasQuorum(acked(3, 4, 5)) {
		t.Fatalf("C+D+E must not be quorum: old has only C (1/3 of majority 2)")
	}
}

// --- IsVoter / Targets (items 47, 52, 114) ---

func TestIsVoterStableAndJoint(t *testing.T) {
	stable := StableMembership(cfg(1, 2, 3))
	if !stable.IsVoter(1) || stable.IsVoter(4) {
		t.Fatalf("stable IsVoter incorrect")
	}
	joint := JointMembership(cfg(1, 2, 3), cfg(2, 3, 4))
	if !joint.IsVoter(1) { // old-only
		t.Fatalf("joint IsVoter(1) [old-only] = false, want true")
	}
	if !joint.IsVoter(4) { // new-only
		t.Fatalf("joint IsVoter(4) [new-only] = false, want true")
	}
	if !joint.IsVoter(2) { // both
		t.Fatalf("joint IsVoter(2) [both] = false, want true")
	}
	if joint.IsVoter(5) {
		t.Fatalf("joint IsVoter(5) [neither] = true, want false")
	}
}

func TestTargetsExcludesSelfAndUnionsJoint(t *testing.T) {
	joint := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	targets := joint.Targets(1)
	if _, ok := targets[1]; ok {
		t.Fatalf("Targets(1) must exclude self")
	}
	for _, id := range []NodeID{2, 3, 4} {
		if _, ok := targets[id]; !ok {
			t.Fatalf("Targets(1) missing %d", id)
		}
	}
	if len(targets) != 3 {
		t.Fatalf("Targets(1) = %v, want exactly {2,3,4}", targets)
	}
}

// --- Configuration validation (item 9) ---

func TestNewConfigurationRejectsEmpty(t *testing.T) {
	if _, err := NewConfiguration(map[NodeID]string{}); err == nil {
		t.Fatalf("NewConfiguration(empty) succeeded, want error")
	}
}

func TestNewConfigurationRejectsZeroNodeID(t *testing.T) {
	if _, err := NewConfiguration(map[NodeID]string{0: "x"}); err == nil {
		t.Fatalf("NewConfiguration(zero NodeID) succeeded, want error")
	}
}

func TestNewConfigurationRejectsEmptyAddress(t *testing.T) {
	if _, err := NewConfiguration(map[NodeID]string{1: ""}); err == nil {
		t.Fatalf("NewConfiguration(empty address) succeeded, want error")
	}
}

func TestNewConfigurationRejectsOversizedAddress(t *testing.T) {
	big := make([]byte, MaxPeerAddrLen+1)
	if _, err := NewConfiguration(map[NodeID]string{1: string(big)}); err == nil {
		t.Fatalf("NewConfiguration(oversized address) succeeded, want error")
	}
}

func TestNewConfigurationRejectsTooManyVoters(t *testing.T) {
	v := make(map[NodeID]string, MaxVoters+1)
	for i := 1; i <= MaxVoters+1; i++ {
		v[NodeID(i)] = "addr"
	}
	if _, err := NewConfiguration(v); err == nil {
		t.Fatalf("NewConfiguration(%d voters) succeeded, want error", MaxVoters+1)
	}
}

// --- Equality (item 13) ---

func TestConfigurationEqualityIgnoresMapOrder(t *testing.T) {
	a := cfg(1, 2, 3)
	b, err := NewConfiguration(map[NodeID]string{3: "C", 1: "A", 2: "B"})
	if err != nil {
		t.Fatalf("NewConfiguration: %v", err)
	}
	if !a.Equal(b) {
		t.Fatalf("configurations with the same voters built in different insertion order are not Equal")
	}
}

func TestConfigurationEqualityConsidersAddress(t *testing.T) {
	a := cfg(1, 2, 3)
	b, err := NewConfiguration(map[NodeID]string{1: "A", 2: "B", 3: "DIFFERENT"})
	if err != nil {
		t.Fatalf("NewConfiguration: %v", err)
	}
	if a.Equal(b) {
		t.Fatalf("configurations with a different address for the same NodeID must not be Equal")
	}
}

func TestMembershipEqualityJoint(t *testing.T) {
	a := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	b := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	if !a.Equal(b) {
		t.Fatalf("identical joint memberships must be Equal")
	}
	c := JointMembership(cfg(1, 2, 3), cfg(1, 2, 4)) // different new
	if a.Equal(c) {
		t.Fatalf("joint memberships with different New must not be Equal")
	}
}
