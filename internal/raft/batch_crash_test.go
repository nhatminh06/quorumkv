package raft

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestBatchAppendFailpointAllOrNothing is item 106: a multi-entry
// Log.Append (what a proposal batch turns into — see proposal.go) is
// exactly one atomicWriteFile publication, so it inherits the same
// old-or-new guarantee crashpoint_test.go already proves for single
// entries — this test proves it specifically for a batch, checking that
// a failure never leaves the log with only part of the batch appended.
func TestBatchAppendFailpointAllOrNothing(t *testing.T) {
	for _, stage := range atomicFileStages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "log")
			l, err := OpenLog(path)
			if err != nil {
				t.Fatalf("OpenLog: %v", err)
			}
			if err := l.Append([]LogEntry{{Term: 1, Command: []byte("pre")}}); err != nil {
				t.Fatalf("Append(pre): %v", err)
			}

			batch := []LogEntry{
				{Term: 1, Command: []byte("b1")},
				{Term: 1, Command: []byte("b2")},
				{Term: 1, Command: []byte("b3")},
				{Term: 1, Command: []byte("b4")},
			}
			restore := setFailpoint(failAt("log." + stage))
			err = l.Append(batch)
			restore()
			if !errors.Is(err, errInjected) {
				t.Fatalf("Append(batch): err = %v, want errInjected", err)
			}

			l2, err := OpenLog(path)
			if err != nil {
				t.Fatalf("OpenLog after failed batch Append: %v", err)
			}
			wantLast := LogIndex(1)
			if publicationCompletedAt(stage) {
				wantLast = 5 // pre + all 4 batch entries — never a partial 2 or 3
			}
			if l2.LastIndex() != wantLast {
				t.Fatalf("after failed batch Append at %s: LastIndex() = %d, want %d (never a partial batch)", stage, l2.LastIndex(), wantLast)
			}
			if wantLast == 5 {
				for i, want := range []string{"pre", "b1", "b2", "b3", "b4"} {
					e, ok := l2.Entry(LogIndex(i + 1))
					if !ok || string(e.Command) != want {
						t.Fatalf("entry %d = (%q,%v), want %q", i+1, e.Command, ok, want)
					}
				}
			}
		})
	}
}

// TestBatchAppendRealCrashAllOrNothing is the genuine-process-crash
// variant of the above, reusing the raft package's subprocess crash
// helper (crash_subprocess_test.go) via its existing "log-append" op —
// that op already appends a single entry on top of a pre-existing one;
// this proves the same real-crash old-or-new guarantee generalizes to a
// multi-entry batch by appending a batch instead and checking no partial
// prefix survives.
func TestBatchAppendRealCrashAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 1, Command: []byte("pre")}}); err != nil {
		t.Fatalf("Append(pre): %v", err)
	}

	runCrashSubprocess(t, dir, "batch-log-append", "log.after-rename")

	l2, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog after crash: %v", err)
	}
	// after-rename: the real publication already happened for real
	// before the process died, so the whole batch must be present.
	if l2.LastIndex() != 5 {
		t.Fatalf("LastIndex() after crash = %d, want 5 (pre + a complete 4-entry batch, never partial)", l2.LastIndex())
	}
}
