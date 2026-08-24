package kv

import (
	"bytes"
	"errors"
	"testing"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Delete([]byte("a"))

	data, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := NewStateMachine()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := restored.Get([]byte("a")); ok {
		t.Fatalf("a should be absent")
	}
	if v, ok := restored.Get([]byte("b")); !ok || string(v) != "2" {
		t.Fatalf("b = %q, %v; want 2, true", v, ok)
	}
}

// TestSnapshotIsDeterministicRegardlessOfInsertionOrder proves the same
// logical state produces byte-identical snapshots no matter what order
// keys were inserted in — map iteration order must not leak through.
func TestSnapshotIsDeterministicRegardlessOfInsertionOrder(t *testing.T) {
	m1 := NewStateMachine()
	m1.Put([]byte("a"), []byte("1"))
	m1.Put([]byte("b"), []byte("2"))

	m2 := NewStateMachine()
	m2.Put([]byte("b"), []byte("2"))
	m2.Put([]byte("a"), []byte("1"))

	s1, err := m1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s2, err := m2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !bytes.Equal(s1, s2) {
		t.Fatalf("snapshots differ by insertion order:\n s1 % x\n s2 % x", s1, s2)
	}
}

// TestSnapshotKnownByteVector independently derives the expected bytes
// for state {a:1, b:2} with no client dedup records: version 2, the same
// KV section Milestone 7 had, followed by an empty (clientCount=0)
// client section — Snapshot always produces version 2 now, even when
// the dedup table is empty.
func TestSnapshotKnownByteVector(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("b"), []byte("2")) // inserted out of sorted order on purpose
	m.Put([]byte("a"), []byte("1"))

	got, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	want := []byte{
		0x02,       // version
		0, 0, 0, 2, // kvEntryCount = 2
		0, 0, 0, 1, 0, 0, 0, 1, 'a', '1', // key="a" value="1"
		0, 0, 0, 1, 0, 0, 0, 1, 'b', '2', // key="b" value="2"
		0, 0, 0, 0, // clientCount = 0
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestSnapshotWithClientRecordKnownByteVector independently derives the
// expected bytes for state {a:1} plus an identified PUT x=1 (which both
// adds "x" to the KV section and records a client dedup record):
// ClientID 00..0f, LastSequence=1 (a client's first request),
// LastFingerprint = SHA-256("\x01" || 0,0,0,1 || "x" || 0,0,0,1 || "1")
// (the fingerprint of PUT x=1), LastResult=OK(1). Per item 76, this
// exercises the client-record section directly rather than only
// round-tripping.
func TestSnapshotWithClientRecordKnownByteVector(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	id := sampleClientID()
	cmd := NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1"))
	if outcome := m.Apply(cmd); outcome != AppliedNew {
		t.Fatalf("Apply: %v, want AppliedNew", outcome)
	}
	fp := Fingerprint(cmd)

	got, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	want := []byte{0x02, 0, 0, 0, 2}
	want = append(want, 0, 0, 0, 1, 0, 0, 0, 1, 'a', '1') // key="a" value="1"
	want = append(want, 0, 0, 0, 1, 0, 0, 0, 1, 'x', '1') // key="x" value="1" (from the identified PUT)
	want = append(want, 0, 0, 0, 1)                       // clientCount = 1
	want = append(want, id[:]...)                         // clientID
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 1)           // lastSequence = 1
	want = append(want, fp[:]...)                         // fingerprint
	want = append(want, 1)                                // result = OK
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestSnapshotV1LegacyStillDecodes(t *testing.T) {
	legacy := []byte{
		0x01,       // version = 1
		0, 0, 0, 1, // entryCount = 1
		0, 0, 0, 1, 0, 0, 0, 1, 'a', '1',
	}
	m := NewStateMachine()
	if err := m.Restore(legacy); err != nil {
		t.Fatalf("Restore(legacy v1): %v", err)
	}
	if v, ok := m.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", v, ok)
	}
	id := sampleClientID()
	fp := Fingerprint(NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1")))
	if got := m.LookupRequest(id, 1, fp); got != AppliedNew {
		t.Fatalf("LookupRequest after restoring a v1 snapshot = %v, want AppliedNew (empty dedup table)", got)
	}
}

// TestSnapshotClientRecordsSortedByClientID proves determinism for the
// client section too: identical dedup state built via different ClientID
// insertion order produces byte-identical snapshots.
func TestSnapshotClientRecordsSortedByClientID(t *testing.T) {
	a, b := idA(), idB()

	m1 := NewStateMachine()
	m1.Apply(NewIdentifiedPutCommand(a, 1, []byte("x"), []byte("1")))
	m1.Apply(NewIdentifiedPutCommand(b, 1, []byte("y"), []byte("2")))

	m2 := NewStateMachine()
	m2.Apply(NewIdentifiedPutCommand(b, 1, []byte("y"), []byte("2")))
	m2.Apply(NewIdentifiedPutCommand(a, 1, []byte("x"), []byte("1")))

	s1, err := m1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s2, err := m2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !bytes.Equal(s1, s2) {
		t.Fatalf("snapshots differ by client insertion order:\n s1 % x\n s2 % x", s1, s2)
	}
}

// TestSnapshotRestoreRoundTripWithDedupState proves a snapshot with both
// KV entries and client dedup records restores both correctly, and that
// the restored dedup table actually recognizes a retry.
func TestSnapshotRestoreRoundTripWithDedupState(t *testing.T) {
	m := NewStateMachine()
	id := idA()
	cmd := NewIdentifiedPutCommand(id, 1, []byte("x"), []byte("1"))
	if outcome := m.Apply(cmd); outcome != AppliedNew {
		t.Fatalf("Apply: %v, want AppliedNew", outcome)
	}

	data, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if v, ok := restored.Get([]byte("x")); !ok || string(v) != "1" {
		t.Fatalf("Get(x) = %q, %v; want 1, true", v, ok)
	}
	// The restored dedup table must recognize a retry of the exact same
	// request as a duplicate, not mutate state again.
	if outcome := restored.Apply(cmd); outcome != AppliedDuplicate {
		t.Fatalf("Apply(retry) after restore = %v, want AppliedDuplicate", outcome)
	}
}

func TestRestoreIsAtomicOnMalformedInput(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("existing"), []byte("value"))

	err := m.Restore([]byte{0x01, 0, 0, 0, 5}) // claims 5 entries, has none
	if err == nil {
		t.Fatalf("Restore succeeded on malformed input, want error")
	}
	v, ok := m.Get([]byte("existing"))
	if !ok || string(v) != "value" {
		t.Fatalf("existing state was mutated by a failed Restore: got %q, %v", v, ok)
	}
}

func TestRestoreDoesNotAliasInputBytes(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	data, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := NewStateMachine()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	data[len(data)-1] = 'X' // mutate the source buffer after Restore
	v, _ := restored.Get([]byte("a"))
	if string(v) != "1" {
		t.Fatalf("restored state changed after mutating the source snapshot bytes: got %q", v)
	}
}

func TestSnapshotEmptyStateMachine(t *testing.T) {
	m := NewStateMachine()
	data, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := NewStateMachine()
	restored.Put([]byte("stale"), []byte("x"))
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := restored.Get([]byte("stale")); ok {
		t.Fatalf("Restore of an empty snapshot should clear prior state")
	}
}

func TestDecodeSnapshotTooShort(t *testing.T) {
	m := NewStateMachine()
	err := m.Restore([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
}

func TestDecodeSnapshotUnsupportedVersion(t *testing.T) {
	m := NewStateMachine()
	data, _ := m.Snapshot()
	data[0] = 99
	err := m.Restore(data)
	if !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
}

func TestDecodeSnapshotOversizedEntryCountRejected(t *testing.T) {
	m := NewStateMachine()
	data := []byte{0x01, 0xFF, 0xFF, 0xFF, 0xFF} // huge declared entry count, no allocation should follow
	err := m.Restore(data)
	if !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
}

func TestDecodeSnapshotTruncatedEntry(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("hello"), []byte("world"))
	data, _ := m.Snapshot()
	err := m.Restore(data[:len(data)-2])
	if !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
}

func TestDecodeSnapshotTrailingBytes(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	data, _ := m.Snapshot()
	data = append(data, 0xFF)
	err := m.Restore(data)
	if !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
}

func TestDecodeSnapshotOversizedKeyLengthRejected(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	data, _ := m.Snapshot()
	// key length field of the first entry, right after the fixed header.
	off := snapshotFixedHeaderSize
	data[off], data[off+1], data[off+2], data[off+3] = 0xFF, 0xFF, 0xFF, 0xFF
	err := m.Restore(data)
	if !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
}
