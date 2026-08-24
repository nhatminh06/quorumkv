package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func tempStatePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "raft-state")
}

func TestMissingFileInitializesZeroState(t *testing.T) {
	s := NewStore(tempStatePath(t))
	state, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.CurrentTerm != 0 || state.VotedFor != nil {
		t.Fatalf("state = %+v, want zero state", state)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	path := tempStatePath(t)
	voter := NodeID(3)

	s1 := NewStore(path)
	if err := s1.Save(PersistentState{CurrentTerm: 7, VotedFor: &voter}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2 := NewStore(path)
	got, err := s2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.CurrentTerm != 7 {
		t.Fatalf("CurrentTerm = %d, want 7", got.CurrentTerm)
	}
	if got.VotedFor == nil || *got.VotedFor != 3 {
		t.Fatalf("VotedFor = %v, want 3", got.VotedFor)
	}
}

func TestClearedVoteSurvivesReopen(t *testing.T) {
	path := tempStatePath(t)
	voter := NodeID(3)

	s1 := NewStore(path)
	if err := s1.Save(PersistentState{CurrentTerm: 7, VotedFor: &voter}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s1.Save(PersistentState{CurrentTerm: 8, VotedFor: nil}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s2 := NewStore(path)
	got, err := s2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.CurrentTerm != 8 || got.VotedFor != nil {
		t.Fatalf("state = %+v, want term 8, no vote", got)
	}
}

// TestKnownStateByteVector independently derives the expected on-disk
// bytes for term=7, votedFor=node 3, rather than round-tripping
// Save->Load, so a shared bug can't pass this test. The checksum was
// computed with the standard hash/crc32 Castagnoli table outside this
// package.
func TestKnownStateByteVector(t *testing.T) {
	path := tempStatePath(t)
	voter := NodeID(3)
	s := NewStore(path)
	if err := s.Save(PersistentState{CurrentTerm: 7, VotedFor: &voter}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	want := []byte{
		'R', 'F', 'T', '1', // magic
		0x01,                   // version
		0, 0, 0, 0, 0, 0, 0, 7, // currentTerm = 7
		0x01,                   // hasVotedFor = true
		0, 0, 0, 0, 0, 0, 0, 3, // votedFor = 3
		0x5a, 0x59, 0x65, 0x2a, // CRC32C(version|term|hasVotedFor|votedFor)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("state bytes:\n got  % x\n want % x", got, want)
	}
}

func writeRawState(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func TestBadMagicRejected(t *testing.T) {
	path := tempStatePath(t)
	NewStore(path).Save(PersistentState{CurrentTerm: 1})
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[0] ^= 0xFF
	writeRawState(t, path, corrupt)

	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("Load error = %v, want ErrCorruptState", err)
	}
}

func TestBadVersionRejected(t *testing.T) {
	path := tempStatePath(t)
	NewStore(path).Save(PersistentState{CurrentTerm: 1})
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[4] = 99
	writeRawState(t, path, corrupt)

	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("Load error = %v, want ErrCorruptState", err)
	}
}

func TestBadChecksumRejected(t *testing.T) {
	path := tempStatePath(t)
	NewStore(path).Save(PersistentState{CurrentTerm: 1})
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[len(corrupt)-1] ^= 0xFF
	writeRawState(t, path, corrupt)

	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("Load error = %v, want ErrCorruptState", err)
	}
}

func TestTruncatedStateRejected(t *testing.T) {
	path := tempStatePath(t)
	NewStore(path).Save(PersistentState{CurrentTerm: 1})
	full, _ := os.ReadFile(path)
	writeRawState(t, path, full[:len(full)-3])

	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("Load error = %v, want ErrCorruptState", err)
	}
}

func TestInvalidHasVotedForEncodingRejected(t *testing.T) {
	path := tempStatePath(t)
	NewStore(path).Save(PersistentState{CurrentTerm: 1})
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[13] = 7 // hasVotedFor byte; only 0/1 are valid, and this also
	// breaks the checksum — either way Load must reject the file.
	writeRawState(t, path, corrupt)

	_, err := NewStore(path).Load()
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("Load error = %v, want ErrCorruptState", err)
	}
}
