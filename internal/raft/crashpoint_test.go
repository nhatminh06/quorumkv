package raft

import (
	"errors"
	"path/filepath"
	"testing"
)

// atomicFileStages is every durability stage atomicWriteFile checks a
// failpoint at, in the order they occur.
var atomicFileStages = []string{
	"before-temp-write",
	"after-temp-write",
	"after-temp-fsync",
	"after-rename",
	"after-dir-fsync",
}

// publicationCompletedAt reports whether stage occurs at-or-after the
// real os.Rename has already happened — the actual moment a reader would
// see the new file on POSIX filesystems. The trailing directory-fsync
// stage only concerns whether that rename's own durability is confirmed
// (surviving a hypothetical crash), not whether the new content is
// already visible to an ordinary read right now — which it is, from
// after-rename onward.
func publicationCompletedAt(stage string) bool {
	switch stage {
	case "after-rename", "after-dir-fsync":
		return true
	default:
		return false
	}
}

// errInjected is the sentinel error every failpoint in this file
// injects, distinguishing "the failpoint fired as intended" from some
// unrelated real failure that would otherwise also make Save return an
// error.
var errInjected = errors.New("injected failure")

// failAt returns a failpointFunc that returns errInjected exactly once,
// the first time it is called for target, and nil for every other name
// (including subsequent calls for target) — so a single Save call that
// checks multiple stages only ever fails at the one being tested, and a
// stale failpoint left installed across an unrelated later Save (there
// shouldn't be one, but this is defensive) does not silently keep
// injecting.
func failAt(target string) failpointFunc {
	fired := false
	return func(name string) error {
		if !fired && name == target {
			fired = true
			return errInjected
		}
		return nil
	}
}

// TestStableStateFailpointOldOrNew is the mandatory term/vote crash
// matrix (I/O-failure-injection variant): for every atomicWriteFile
// stage, an injected failure during Save(new) must leave the file
// exactly as old (if the failure preceded real publication) or exactly
// as new (if it occurred at-or-after the real rename — see
// publicationCompletedAt) — never a malformed or hybrid state, proven
// by reading the file back into a genuinely fresh Store.
func TestStableStateFailpointOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state")
			old := PersistentState{CurrentTerm: 7, VotedFor: mustVoter(2)}
			if err := NewStore(path).Save(old); err != nil {
				t.Fatalf("Save(old): %v", err)
			}
			newState := PersistentState{CurrentTerm: 8, VotedFor: nil}

			restore := setFailpoint(failAt("stable." + stage))
			err := NewStore(path).Save(newState)
			restore()
			if !errors.Is(err, errInjected) {
				t.Fatalf("Save(new): err = %v, want errInjected (failpoint never fired)", err)
			}

			got, err := NewStore(path).Load()
			if err != nil {
				t.Fatalf("Load after failed Save: %v", err)
			}
			want := old
			if publicationCompletedAt(stage) {
				want = newState
			}
			if got.CurrentTerm != want.CurrentTerm || !votedForEqual(got.VotedFor, want.VotedFor) {
				t.Fatalf("after failed Save at %s: got %+v, want %+v (old=%+v new=%+v)", stage, got, want, old, newState)
			}
		})
	}
}

func votedForEqual(a, b *NodeID) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// TestLogFailpointOldOrNew is the mandatory Raft log crash matrix
// (I/O-failure-injection variant). The log is rewritten atomically as a
// whole on every mutation (see docs/crash-consistency.md) — there is no
// separate append-only/torn-tail case to cover here, since the same
// old-or-new rule atomicWriteFile already provides applies directly.
func TestLogFailpointOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "log")
			l, err := OpenLog(path)
			if err != nil {
				t.Fatalf("OpenLog: %v", err)
			}
			if err := l.Append([]LogEntry{{Term: 1, Command: []byte("a")}}); err != nil {
				t.Fatalf("Append(old): %v", err)
			}

			restore := setFailpoint(failAt("log." + stage))
			err = l.Append([]LogEntry{{Term: 1, Command: []byte("b")}})
			restore()
			if !errors.Is(err, errInjected) {
				t.Fatalf("Append(new): err = %v, want errInjected", err)
			}

			l2, err := OpenLog(path)
			if err != nil {
				t.Fatalf("OpenLog after failed Append: %v", err)
			}
			wantLast := LogIndex(1)
			if publicationCompletedAt(stage) {
				wantLast = 2
			}
			if l2.LastIndex() != wantLast {
				t.Fatalf("after failed Append at %s: LastIndex() = %d, want %d", stage, l2.LastIndex(), wantLast)
			}
		})
	}
}

// TestCommitMetaFailpointOldOrNew is the mandatory commit-metadata crash
// matrix (I/O-failure-injection variant).
func TestCommitMetaFailpointOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "commit")
			if err := NewCommitStore(path).Save(3); err != nil {
				t.Fatalf("Save(old): %v", err)
			}

			restore := setFailpoint(failAt("commit." + stage))
			err := NewCommitStore(path).Save(9)
			restore()
			if !errors.Is(err, errInjected) {
				t.Fatalf("Save(new): err = %v, want errInjected", err)
			}

			got, err := NewCommitStore(path).Load()
			if err != nil {
				t.Fatalf("Load after failed Save: %v", err)
			}
			want := LogIndex(3)
			if publicationCompletedAt(stage) {
				want = 9
			}
			if got != want {
				t.Fatalf("after failed Save at %s: got %d, want %d", stage, got, want)
			}
		})
	}
}

// TestSnapshotFailpointOldOrNew is the mandatory snapshot-publication
// crash matrix (I/O-failure-injection variant).
func TestSnapshotFailpointOldOrNew(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "snapshot")
			old := Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("old"), Configuration: sampleSnapshotConfiguration(t)}
			if err := NewSnapshotStore(path).Save(old); err != nil {
				t.Fatalf("Save(old): %v", err)
			}
			newSnap := Snapshot{LastIncludedIndex: 9, LastIncludedTerm: 2, Data: []byte("new"), Configuration: sampleSnapshotConfiguration(t)}

			restore := setFailpoint(failAt("snapshot." + stage))
			err := NewSnapshotStore(path).Save(newSnap)
			restore()
			if !errors.Is(err, errInjected) {
				t.Fatalf("Save(new): err = %v, want errInjected", err)
			}

			got, err := NewSnapshotStore(path).Load()
			if err != nil {
				t.Fatalf("Load after failed Save: %v", err)
			}
			if got == nil {
				t.Fatalf("Load after failed Save returned nil — canonical snapshot must always remain readable")
			}
			want := old
			if publicationCompletedAt(stage) {
				want = newSnap
			}
			if got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm || string(got.Data) != string(want.Data) {
				t.Fatalf("after failed Save at %s: got {%d,%d,%q}, want {%d,%d,%q}",
					stage, got.LastIncludedIndex, got.LastIncludedTerm, got.Data, want.LastIncludedIndex, want.LastIncludedTerm, want.Data)
			}
		})
	}
}

// TestFailpointStagesAllReached is item 75/76: a table-driven scan
// proving every declared failpoint stage is actually exercised by the
// tests above, for every domain — a failpoint added but never tested
// would otherwise go unnoticed.
func TestFailpointStagesAllReached(t *testing.T) {
	for _, domain := range []string{"stable", "log", "commit", "snapshot"} {
		for _, stage := range atomicFileStages {
			var fired bool
			restore := setFailpoint(func(name string) error {
				if name == domain+"."+stage {
					fired = true
				}
				return nil
			})
			switch domain {
			case "stable":
				_ = NewStore(filepath.Join(t.TempDir(), "s")).Save(PersistentState{CurrentTerm: 1})
			case "log":
				l, _ := OpenLog(filepath.Join(t.TempDir(), "l"))
				_ = l.Append([]LogEntry{{Term: 1, Command: []byte("x")}})
			case "commit":
				_ = NewCommitStore(filepath.Join(t.TempDir(), "c")).Save(1)
			case "snapshot":
				_ = NewSnapshotStore(filepath.Join(t.TempDir(), "sn")).Save(Snapshot{Configuration: sampleSnapshotConfiguration(t)})
			}
			restore()
			if !fired {
				t.Fatalf("failpoint %s.%s was never reached by a normal (non-failing) operation", domain, stage)
			}
		}
	}
}
