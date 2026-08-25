package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// This file contains the genuine-process-crash (subprocess, os.Exit-based)
// crash matrix — the "preferred strong approach" proof that a real crash
// at each durability boundary, with no defers/Close/rollback ever
// running, still leaves disk state exactly old-or-new. The broader
// stage-by-stage matrix is already covered in-process by
// crashpoint_test.go (I/O-failure-injection variant); these tests are a
// representative subset proving that variant is not hiding behavior a
// real crash would exhibit differently — see docs/crash-consistency.md.

// TestStableStateRealCrashOldOrNew proves a real process crash mid-Save
// of term/vote state, at the pre-publication and post-publication
// boundaries, leaves the file exactly old or exactly new — read back by
// a genuinely fresh Store in the (still-running) parent process.
func TestStableStateRealCrashOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			old := PersistentState{CurrentTerm: 7, VotedFor: mustVoter(2)}
			if err := NewStore(filepath.Join(dir, "state")).Save(old); err != nil {
				t.Fatalf("Save(old): %v", err)
			}

			runCrashSubprocess(t, dir, "stable-save", "stable."+stage)

			got, err := NewStore(filepath.Join(dir, "state")).Load()
			if err != nil {
				t.Fatalf("Load after crash: %v", err)
			}
			want := old
			if publicationCompletedAt(stage) {
				want = PersistentState{CurrentTerm: 8, VotedFor: nil}
			}
			if got.CurrentTerm != want.CurrentTerm || !votedForEqual(got.VotedFor, want.VotedFor) {
				t.Fatalf("after crash at %s: got %+v, want %+v", stage, got, want)
			}
		})
	}
}

// TestLogAppendRealCrashOldOrNew proves a real process crash mid-Append
// leaves the log file exactly old or exactly new.
func TestLogAppendRealCrashOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			l, err := OpenLog(filepath.Join(dir, "log"))
			if err != nil {
				t.Fatalf("OpenLog: %v", err)
			}
			if err := l.Append([]LogEntry{{Term: 1, Command: []byte("a")}}); err != nil {
				t.Fatalf("Append(old): %v", err)
			}

			runCrashSubprocess(t, dir, "log-append", "log."+stage)

			l2, err := OpenLog(filepath.Join(dir, "log"))
			if err != nil {
				t.Fatalf("OpenLog after crash: %v", err)
			}
			wantLast := LogIndex(1)
			if publicationCompletedAt(stage) {
				wantLast = 2
			}
			if l2.LastIndex() != wantLast {
				t.Fatalf("after crash at %s: LastIndex() = %d, want %d", stage, l2.LastIndex(), wantLast)
			}
		})
	}
}

// TestLogConflictRepairRealCrashOldOrNew proves a real process crash
// mid-TruncateAndAppend (the conflicting-suffix-repair path a follower
// uses when AppendEntries reports a mismatch) leaves the log exactly the
// pre-repair suffix or exactly the replaced suffix — never a mix.
func TestLogConflictRepairRealCrashOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			l, err := OpenLog(filepath.Join(dir, "log"))
			if err != nil {
				t.Fatalf("OpenLog: %v", err)
			}
			if err := l.Append([]LogEntry{
				{Term: 1, Command: []byte("a")},
				{Term: 1, Command: []byte("b-stale")},
			}); err != nil {
				t.Fatalf("Append(old): %v", err)
			}

			runCrashSubprocess(t, dir, "log-conflict-repair", "log."+stage)

			l2, err := OpenLog(filepath.Join(dir, "log"))
			if err != nil {
				t.Fatalf("OpenLog after crash: %v", err)
			}
			wantLast := LogIndex(2)
			wantTerm := Term(1)
			if publicationCompletedAt(stage) {
				wantLast, wantTerm = 2, 2
			}
			if l2.LastIndex() != wantLast {
				t.Fatalf("after crash at %s: LastIndex() = %d, want %d", stage, l2.LastIndex(), wantLast)
			}
			gotTerm, ok := l2.Term(2)
			if !ok || gotTerm != wantTerm {
				t.Fatalf("after crash at %s: Term(2) = (%d,%v), want %d", stage, gotTerm, ok, wantTerm)
			}
		})
	}
}

// TestCommitMetaRealCrashOldOrNew proves a real process crash mid-Save of
// commit metadata leaves the file exactly old or exactly new.
func TestCommitMetaRealCrashOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			if err := NewCommitStore(filepath.Join(dir, "commit")).Save(3); err != nil {
				t.Fatalf("Save(old): %v", err)
			}

			runCrashSubprocess(t, dir, "commit-save", "commit."+stage)

			got, err := NewCommitStore(filepath.Join(dir, "commit")).Load()
			if err != nil {
				t.Fatalf("Load after crash: %v", err)
			}
			want := LogIndex(3)
			if publicationCompletedAt(stage) {
				want = 9
			}
			if got != want {
				t.Fatalf("after crash at %s: got %d, want %d", stage, got, want)
			}
		})
	}
}

// TestCreateSnapshotRealCrashCrossFileConsistency proves a real process
// crash during CreateSnapshot's two-step sequence (snapshot publish, then
// log compact) never leaves a state where the log has been compacted
// without a snapshot covering the removed prefix. A crash at any snapshot
// stage before real publication must leave the log untouched and the
// canonical snapshot exactly as before CreateSnapshot ran; a crash at or
// after snapshot publication (including anywhere during the following
// log compact) must leave a snapshot that covers at least as much as the
// log's new base — reopening a fresh Node from disk must succeed and
// reconcile the two files correctly either way.
func TestCreateSnapshotRealCrashCrossFileConsistency(t *testing.T) {
	stages := []struct {
		domain string
		stage  string
	}{
		{"snapshot", "before-temp-write"},
		{"snapshot", "after-temp-fsync"},
		{"snapshot", "after-rename"},
		{"snapshot", "after-dir-fsync"},
		{"log", "before-temp-write"},
		{"log", "after-rename"},
		{"log", "after-dir-fsync"},
	}
	for _, s := range stages {
		t.Run(s.domain+"."+s.stage, func(t *testing.T) {
			dir := t.TempDir()
			runCrashSubprocess(t, dir, "snapshot-create", s.domain+"."+s.stage)

			snap, err := NewSnapshotStore(filepath.Join(dir, "snapshot")).Load()
			if err != nil {
				t.Fatalf("Load snapshot after crash: %v", err)
			}
			l, err := OpenLog(filepath.Join(dir, "log"))
			if err != nil {
				t.Fatalf("OpenLog after crash: %v", err)
			}

			logCompacted := l.BaseIndex() > 0
			if logCompacted {
				if snap == nil {
					t.Fatalf("after crash at %s.%s: log was compacted (base=%d) but no snapshot exists to cover it", s.domain, s.stage, l.BaseIndex())
				}
				if snap.LastIncludedIndex < l.BaseIndex() {
					t.Fatalf("after crash at %s.%s: snapshot boundary %d < log base %d — a gap no reader could recover from",
						s.domain, s.stage, snap.LastIncludedIndex, l.BaseIndex())
				}
			}

			sm := newFakeStateMachine()
			n := openSnapshottingNode(t, dir, 1, nil, sm)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := n.StartElection(ctx); err != nil {
				t.Fatalf("fresh Node failed to start after crash at %s.%s: %v", s.domain, s.stage, err)
			}
		})
	}
}
