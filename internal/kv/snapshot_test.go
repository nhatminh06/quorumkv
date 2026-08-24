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
// for state {a:1, b:2}.
func TestSnapshotKnownByteVector(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("b"), []byte("2")) // inserted out of sorted order on purpose
	m.Put([]byte("a"), []byte("1"))

	got, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	want := []byte{
		0x01,       // version
		0, 0, 0, 2, // entryCount = 2
		0, 0, 0, 1, 0, 0, 0, 1, 'a', '1', // key="a" value="1"
		0, 0, 0, 1, 0, 0, 0, 1, 'b', '2', // key="b" value="2"
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
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
