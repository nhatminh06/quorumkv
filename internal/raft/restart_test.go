package raft

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestRestartRebuildAppliesOnlyCommittedPrefix constructs a log with a
// committed prefix and an uncommitted suffix, destroys the in-memory
// Node, and opens a fresh one from the same files — proving startup
// replays only entries 1..commitIndex, exactly matching Milestone 5's
// "restart replays only committed prefix" requirement.
func TestRestartRebuildAppliesOnlyCommittedPrefix(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "log")
	commitPath := filepath.Join(dir, "commit")

	// Build the on-disk fixture directly (no network): log has 4 entries,
	// only the first 2 are marked committed.
	log, err := OpenLog(logPath)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := log.Append([]LogEntry{
		{Term: 1, Command: []byte("one")},
		{Term: 1, Command: []byte("two")},
		{Term: 1, Command: []byte("three")},
		{Term: 1, Command: []byte("four")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := NewCommitStore(commitPath).Save(2); err != nil {
		t.Fatalf("Save commit meta: %v", err)
	}
	if err := NewStore(statePath).Save(PersistentState{CurrentTerm: 1}); err != nil {
		t.Fatalf("Save state: %v", err)
	}

	rec := newApplyRecorder()
	store := NewStore(statePath)
	log2, err := OpenLog(logPath)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := NewCommitStore(commitPath)
	n, err := NewNode(1, store, log2, commitStore, nil, rec.fn)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer n.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, 2, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	if n.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2", n.LastApplied())
	}
	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2", n.CommitIndex())
	}
	if got := rec.indexes(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("applied indexes = %v, want [1 2]", got)
	}
	// The uncommitted suffix must still be on disk, just not applied.
	if n.LastLogIndex() != 4 {
		t.Fatalf("LastLogIndex() = %d, want 4 (uncommitted suffix preserved in the log)", n.LastLogIndex())
	}

	// A restarted node must not remember Leader role.
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower", n.Role())
	}
}

// TestRestartRebuildKVStateViaOrderedApply proves that replaying a
// committed prefix through a real ordered apply sequence (PUT x=1, PUT
// y=2, DELETE x) yields the correct final state, mirroring item 28's
// exact scenario without depending on the service/kv packages: the
// recorder here just replays commands in order, which is the property
// this package is responsible for guaranteeing.
func TestRestartRebuildKVStateViaOrderedApply(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := log.Append([]LogEntry{
		{Term: 1, Command: []byte("PUT x=1")},
		{Term: 1, Command: []byte("PUT y=2")},
		{Term: 1, Command: []byte("DELETE x")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	if err := commitStore.Save(3); err != nil {
		t.Fatalf("Save: %v", err)
	}

	state := map[string]bool{}
	applied := []string{}
	apply := func(index LogIndex, cmd []byte) error {
		s := string(cmd)
		applied = append(applied, s)
		switch {
		case s == "PUT x=1":
			state["x"] = true
		case s == "PUT y=2":
			state["y"] = true
		case s == "DELETE x":
			delete(state, "x")
		}
		return nil
	}

	store := NewStore(filepath.Join(dir, "state"))
	n, err := NewNode(1, store, log, commitStore, nil, apply)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer n.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, 3, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	if state["x"] {
		t.Fatalf("x should have been deleted")
	}
	if !state["y"] {
		t.Fatalf("y should be present")
	}
	want := []string{"PUT x=1", "PUT y=2", "DELETE x"}
	if len(applied) != len(want) {
		t.Fatalf("applied = %v, want %v", applied, want)
	}
	for i := range want {
		if applied[i] != want[i] {
			t.Fatalf("applied = %v, want %v", applied, want)
		}
	}
	if n.LastApplied() != 3 || n.CommitIndex() != 3 {
		t.Fatalf("lastApplied=%d commitIndex=%d, want both 3", n.LastApplied(), n.CommitIndex())
	}
}

// TestRestartApplyRecoveryFailsExplicitlyOnCorruptCommittedCommand proves
// item 73: a malformed committed command must fail recovery/application
// explicitly rather than being silently skipped.
func TestRestartApplyRecoveryFailsExplicitlyOnCorruptCommittedCommand(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := log.Append([]LogEntry{
		{Term: 1, Command: []byte("good")},
		{Term: 1, Command: []byte("bad")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	if err := commitStore.Save(2); err != nil {
		t.Fatalf("Save: %v", err)
	}

	apply := func(index LogIndex, cmd []byte) error {
		if string(cmd) == "bad" {
			return errMalformedForTest
		}
		return nil
	}

	store := NewStore(filepath.Join(dir, "state"))
	n, err := NewNode(1, store, log, commitStore, nil, apply)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer n.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = n.WaitApplied(ctx, 2, 0)
	if err == nil {
		t.Fatalf("WaitApplied succeeded despite corrupt committed command, want error")
	}
	if n.LastApplied() != 1 {
		t.Fatalf("LastApplied() = %d, want 1 (the corrupt index must not count as applied)", n.LastApplied())
	}
}

var errMalformedForTest = errors.New("malformed test command")
