package kv

import "testing"

func idA() (id [16]byte) { id[0] = 0xAA; return id }
func idB() (id [16]byte) { id[0] = 0xBB; return id }

// TestFirstIdentifiedRequestApplies is item 26: a brand-new ClientID's
// first request (sequence 1) applies and records state.
func TestFirstIdentifiedRequestApplies(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	outcome := m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1")))
	if outcome != AppliedNew {
		t.Fatalf("Apply() = %v, want AppliedNew", outcome)
	}
	v, ok := m.Get([]byte("x"))
	if !ok || string(v) != "1" {
		t.Fatalf("Get(x) = %q, %v; want 1, true", v, ok)
	}
}

// TestExactDuplicateDoesNotMutate is item 27: the central dedup
// behavior — same ClientID, same sequence, same fingerprint must not
// mutate state a second time, and reports AppliedDuplicate.
func TestExactDuplicateDoesNotMutate(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	cmd := NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1"))
	if outcome := m.Apply(cmd); outcome != AppliedNew {
		t.Fatalf("first Apply() = %v, want AppliedNew", outcome)
	}
	m.Put([]byte("x"), []byte("mutated-by-something-else")) // prove a real second mutation would be observable

	outcome := m.Apply(cmd) // exact same command again
	if outcome != AppliedDuplicate {
		t.Fatalf("second Apply() = %v, want AppliedDuplicate", outcome)
	}
	v, _ := m.Get([]byte("x"))
	if string(v) != "mutated-by-something-else" {
		t.Fatalf("duplicate Apply mutated state: Get(x) = %q", v)
	}
}

// TestSameIDDifferentPayloadIsConflict is item 28: same (ClientID,
// Sequence) but a different fingerprint must not return cached OK — it
// is a RequestConflict, and must not mutate state.
func TestSameIDDifferentPayloadIsConflict(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	if outcome := m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1"))); outcome != AppliedNew {
		t.Fatalf("first Apply() = %v, want AppliedNew", outcome)
	}

	outcome := m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("2"))) // same seq, different value
	if outcome != RequestConflict {
		t.Fatalf("Apply() with mismatched fingerprint = %v, want RequestConflict", outcome)
	}
	v, _ := m.Get([]byte("x"))
	if string(v) != "1" {
		t.Fatalf("conflicting Apply mutated state: Get(x) = %q, want unchanged 1", v)
	}
}

// TestOlderSequenceDoesNotMutate is item 29/30: a sequence behind the
// last applied one must not mutate state, and is reported StaleRequest.
func TestOlderSequenceDoesNotMutate(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1")))
	m.Apply(NewIdentifiedPutCommand(id, 2, []byte("x"), []byte("2")))

	outcome := m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1"))) // old sequence
	if outcome != StaleRequest {
		t.Fatalf("Apply() with old sequence = %v, want StaleRequest", outcome)
	}
	v, _ := m.Get([]byte("x"))
	if string(v) != "2" {
		t.Fatalf("stale Apply mutated state: Get(x) = %q, want unchanged 2", v)
	}
}

// TestSequenceGapIsRejected is items 31/69: with the exact-next-sequence
// policy, a gap ahead of the expected sequence is rejected without
// mutation, and a later request that correctly fills the gap succeeds.
func TestSequenceGapIsRejected(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1")))

	outcome := m.Apply(NewIdentifiedPutCommand(id, 3, []byte("x"), []byte("3"))) // skips 2
	if outcome != StaleRequest {
		t.Fatalf("Apply() with a sequence gap = %v, want StaleRequest", outcome)
	}
	v, _ := m.Get([]byte("x"))
	if string(v) != "1" {
		t.Fatalf("gapped Apply mutated state: Get(x) = %q, want unchanged 1", v)
	}

	// Filling the gap (sequence 2) must still succeed normally.
	if outcome := m.Apply(NewIdentifiedPutCommand(id, 2, []byte("x"), []byte("2"))); outcome != AppliedNew {
		t.Fatalf("Apply() for the correct next sequence = %v, want AppliedNew", outcome)
	}
	// The earlier-rejected sequence 3 must now be re-appliable as a new,
	// later entry (not retroactively — this call represents that new
	// entry), and must succeed.
	if outcome := m.Apply(NewIdentifiedPutCommand(id, 3, []byte("x"), []byte("3"))); outcome != AppliedNew {
		t.Fatalf("Apply() for sequence 3 after the gap was filled = %v, want AppliedNew", outcome)
	}
}

// TestUnidentifiedCommandBypassesDedup proves a zero-ClientID command
// (every pre-Milestone-9 command) is applied unconditionally, exactly as
// before, and never touches the dedup table.
func TestUnidentifiedCommandBypassesDedup(t *testing.T) {
	m := NewStateMachine()
	cmd := NewPutCommand([]byte("x"), []byte("1"))
	if outcome := m.Apply(cmd); outcome != AppliedNew {
		t.Fatalf("first Apply() = %v, want AppliedNew", outcome)
	}
	if outcome := m.Apply(cmd); outcome != AppliedNew {
		t.Fatalf("repeated unidentified Apply() = %v, want AppliedNew (no dedup for unidentified commands)", outcome)
	}
}

// TestIndependentClientsDoNotCollide is item 103: two different
// ClientIDs using the same sequence number are entirely independent.
func TestIndependentClientsDoNotCollide(t *testing.T) {
	m := NewStateMachine()
	a, b := idA(), idB()
	if outcome := m.Apply(NewIdentifiedPutCommand(a, 1, []byte("x"), []byte("from-a"))); outcome != AppliedNew {
		t.Fatalf("A's Apply() = %v, want AppliedNew", outcome)
	}
	if outcome := m.Apply(NewIdentifiedPutCommand(b, 1, []byte("y"), []byte("from-b"))); outcome != AppliedNew {
		t.Fatalf("B's Apply() = %v, want AppliedNew", outcome)
	}
	if v, ok := m.Get([]byte("x")); !ok || string(v) != "from-a" {
		t.Fatalf("Get(x) = %q, %v; want from-a, true", v, ok)
	}
	if v, ok := m.Get([]byte("y")); !ok || string(v) != "from-b" {
		t.Fatalf("Get(y) = %q, %v; want from-b, true", v, ok)
	}

	// B retrying its own sequence 1 must dedup against B's own record,
	// unaffected by A ever having used sequence 1 too.
	if outcome := m.Apply(NewIdentifiedPutCommand(b, 1, []byte("y"), []byte("from-b"))); outcome != AppliedDuplicate {
		t.Fatalf("B's retried Apply() = %v, want AppliedDuplicate", outcome)
	}
}

// TestLookupRequestMatchesApplyClassification proves LookupRequest (the
// service-layer optimization) agrees with what Apply would actually do,
// without mutating anything itself.
func TestLookupRequestMatchesApplyClassification(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	cmd1 := NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1"))

	if got := m.LookupRequest(id, 1, Fingerprint(cmd1)); got != AppliedNew {
		t.Fatalf("LookupRequest before any Apply = %v, want AppliedNew", got)
	}
	m.Apply(cmd1)

	if got := m.LookupRequest(id, 1, Fingerprint(cmd1)); got != AppliedDuplicate {
		t.Fatalf("LookupRequest for the just-applied request = %v, want AppliedDuplicate", got)
	}
	conflictCmd := NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("2"))
	if got := m.LookupRequest(id, 1, Fingerprint(conflictCmd)); got != RequestConflict {
		t.Fatalf("LookupRequest for a conflicting fingerprint = %v, want RequestConflict", got)
	}
	if got := m.LookupRequest(id, 5, Fingerprint(cmd1)); got != StaleRequest {
		t.Fatalf("LookupRequest for a gapped-ahead sequence = %v, want StaleRequest", got)
	}
	cmd2 := NewIdentifiedPutCommand(id, 2, []byte("x"), []byte("2"))
	if got := m.LookupRequest(id, 2, Fingerprint(cmd2)); got != AppliedNew {
		t.Fatalf("LookupRequest for the correct next sequence = %v, want AppliedNew", got)
	}

	// LookupRequest itself must never mutate anything.
	if v, _ := m.Get([]byte("x")); string(v) != "1" {
		t.Fatalf("LookupRequest mutated state: Get(x) = %q, want unchanged 1", v)
	}
}

// TestRequestConflictDoesNotWedgeSubsequentApplies is item 117: a
// RequestConflict outcome must not prevent later, unrelated Apply calls
// (for the same or other ClientIDs) from proceeding normally.
func TestRequestConflictDoesNotWedgeSubsequentApplies(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1")))
	if outcome := m.Apply(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("CONFLICT"))); outcome != RequestConflict {
		t.Fatalf("Apply() = %v, want RequestConflict", outcome)
	}
	// The same client's next legitimate sequence must still apply.
	if outcome := m.Apply(NewIdentifiedPutCommand(id, 2, []byte("y"), []byte("2"))); outcome != AppliedNew {
		t.Fatalf("Apply() after a conflict = %v, want AppliedNew", outcome)
	}
	if v, ok := m.Get([]byte("y")); !ok || string(v) != "2" {
		t.Fatalf("Get(y) = %q, %v; want 2, true", v, ok)
	}
}
