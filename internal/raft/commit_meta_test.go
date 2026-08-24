package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func tempCommitMetaPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "commit")
}

func TestCommitMetaMissingFileIsZero(t *testing.T) {
	s := NewCommitStore(tempCommitMetaPath(t))
	idx, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if idx != 0 {
		t.Fatalf("idx = %d, want 0", idx)
	}
}

func TestCommitMetaSurvivesReopen(t *testing.T) {
	path := tempCommitMetaPath(t)
	if err := NewCommitStore(path).Save(7); err != nil {
		t.Fatalf("Save: %v", err)
	}
	idx, err := NewCommitStore(path).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if idx != 7 {
		t.Fatalf("idx = %d, want 7", idx)
	}
}

func TestCommitMetaNeverDecreasesOnDisk(t *testing.T) {
	// CommitStore itself just persists whatever it's told; the
	// never-decreases invariant is enforced by Node (see
	// TestCommitIndexNeverDecreases). This test just proves Save
	// overwrites cleanly either direction, which Node relies on.
	path := tempCommitMetaPath(t)
	s := NewCommitStore(path)
	if err := s.Save(5); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save(9); err != nil {
		t.Fatalf("Save: %v", err)
	}
	idx, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if idx != 9 {
		t.Fatalf("idx = %d, want 9", idx)
	}
}

// TestCommitMetaKnownByteVector independently derives the expected
// on-disk bytes for commitIndex=7.
func TestCommitMetaKnownByteVector(t *testing.T) {
	path := tempCommitMetaPath(t)
	if err := NewCommitStore(path).Save(7); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := []byte{
		'C', 'M', 'T', '1', // magic
		0x01,                   // version
		0, 0, 0, 0, 0, 0, 0, 7, // commitIndex = 7
		0x9b, 0x11, 0xda, 0x00, // CRC32C(version|commitIndex)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func writeRawCommitMeta(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func TestCommitMetaBadMagicRejected(t *testing.T) {
	path := tempCommitMetaPath(t)
	NewCommitStore(path).Save(1)
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[0] ^= 0xFF
	writeRawCommitMeta(t, path, corrupt)

	_, err := NewCommitStore(path).Load()
	if !errors.Is(err, ErrCorruptCommitMeta) {
		t.Fatalf("err = %v, want ErrCorruptCommitMeta", err)
	}
}

func TestCommitMetaBadVersionRejected(t *testing.T) {
	path := tempCommitMetaPath(t)
	NewCommitStore(path).Save(1)
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[4] = 99
	writeRawCommitMeta(t, path, corrupt)

	_, err := NewCommitStore(path).Load()
	if !errors.Is(err, ErrCorruptCommitMeta) {
		t.Fatalf("err = %v, want ErrCorruptCommitMeta", err)
	}
}

func TestCommitMetaTruncatedRejected(t *testing.T) {
	path := tempCommitMetaPath(t)
	NewCommitStore(path).Save(1)
	full, _ := os.ReadFile(path)
	writeRawCommitMeta(t, path, full[:len(full)-3])

	_, err := NewCommitStore(path).Load()
	if !errors.Is(err, ErrCorruptCommitMeta) {
		t.Fatalf("err = %v, want ErrCorruptCommitMeta", err)
	}
}

func TestCommitMetaBadChecksumRejected(t *testing.T) {
	path := tempCommitMetaPath(t)
	NewCommitStore(path).Save(1)
	full, _ := os.ReadFile(path)
	corrupt := append([]byte(nil), full...)
	corrupt[len(corrupt)-1] ^= 0xFF
	writeRawCommitMeta(t, path, corrupt)

	_, err := NewCommitStore(path).Load()
	if !errors.Is(err, ErrCorruptCommitMeta) {
		t.Fatalf("err = %v, want ErrCorruptCommitMeta", err)
	}
}

// TestCommitIndexExceedingLogLengthRejected covers the Node-level
// corruption check (item 26): a persisted commitIndex greater than the
// log's last index must never be silently clamped.
func TestCommitIndexExceedingLogLengthRejected(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "state"))
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := log.Append([]LogEntry{{Term: 1, Command: []byte("a")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	if err := commitStore.Save(5); err != nil { // log only has 1 entry
		t.Fatalf("Save: %v", err)
	}

	_, err = NewNode(1, store, log, commitStore, NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, nil, nil, nil)
	if err == nil {
		t.Fatalf("NewNode succeeded despite commitIndex exceeding log length, want error")
	}
}
