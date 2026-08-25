package raft

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// crashExitCode is the exit code the subprocess crash helper deliberately
// terminates with at its target failpoint, distinct from Go test's own
// exit codes (0 pass, 1 fail) so the parent can prove the crash was
// actually reached rather than the subprocess merely exiting for some
// unrelated reason (see runCrashSubprocess).
const crashExitCode = 91

// crashMarkerPrefix is written to stderr by the subprocess immediately
// before os.Exit — the exit code alone is not proof of which failpoint
// fired, or that one fired at all rather than the process crashing for
// some other reason.
const crashMarkerPrefix = "QKV_CRASH_REACHED:"

// runCrashSubprocess re-executes this test binary in helper mode
// (TestCrashHelperSubprocess), instructing it to perform op against dir
// and terminate via os.Exit at failpoint — a genuine process death with
// no defers, no Close, no in-memory rollback, unlike an injected error
// return. It fails the test unless the subprocess both exited with
// crashExitCode AND printed the marker for exactly this failpoint: a
// subprocess that exits for any other reason (op finished without
// hitting the failpoint, a bug, a panic) must never be silently treated
// as a successful crash injection.
func runCrashSubprocess(t *testing.T, dir, op, failpoint string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashHelperSubprocess$", "-test.v")
	cmd.Env = append(os.Environ(),
		"QKV_CRASHTEST_MODE=1",
		"QKV_CRASHTEST_DIR="+dir,
		"QKV_CRASHTEST_OP="+op,
		"QKV_CRASHTEST_FAILPOINT="+failpoint,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	marker := crashMarkerPrefix + failpoint
	if !strings.Contains(stderr.String(), marker) {
		t.Fatalf("subprocess (op=%s) never reached failpoint %s\nstdout:\n%s\nstderr:\n%s", op, failpoint, stdout.String(), stderr.String())
	}
	exitErr, ok := runErr.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != crashExitCode {
		t.Fatalf("subprocess (op=%s) exit = %v, want exit code %d\nstdout:\n%s\nstderr:\n%s", op, runErr, crashExitCode, stdout.String(), stderr.String())
	}
}

// TestCrashHelperSubprocess is not a real test: it is only ever invoked
// by runCrashSubprocess as a re-exec'd subprocess, gated on
// QKV_CRASHTEST_MODE so ordinary `go test` runs skip it immediately. It
// performs exactly one durable operation against QKV_CRASHTEST_DIR and
// installs a failpoint that terminates the process (os.Exit, no cleanup)
// the first time QKV_CRASHTEST_FAILPOINT is reached.
func TestCrashHelperSubprocess(t *testing.T) {
	if os.Getenv("QKV_CRASHTEST_MODE") != "1" {
		t.Skip("only runs as a subprocess crash helper; see runCrashSubprocess")
	}
	dir := os.Getenv("QKV_CRASHTEST_DIR")
	op := os.Getenv("QKV_CRASHTEST_OP")
	fp := os.Getenv("QKV_CRASHTEST_FAILPOINT")

	setFailpoint(func(name string) error {
		if name == fp {
			fmt.Fprintln(os.Stderr, crashMarkerPrefix+fp)
			os.Exit(crashExitCode)
		}
		return nil
	})

	switch op {
	case "stable-save":
		if err := NewStore(filepath.Join(dir, "state")).Save(PersistentState{CurrentTerm: 8, VotedFor: nil}); err != nil {
			fmt.Fprintln(os.Stderr, "Save:", err)
		}
	case "log-append":
		l, err := OpenLog(filepath.Join(dir, "log"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "OpenLog:", err)
			os.Exit(1)
		}
		if err := l.Append([]LogEntry{{Term: 1, Command: []byte("b")}}); err != nil {
			fmt.Fprintln(os.Stderr, "Append:", err)
		}
	case "batch-log-append":
		l, err := OpenLog(filepath.Join(dir, "log"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "OpenLog:", err)
			os.Exit(1)
		}
		batch := []LogEntry{
			{Term: 1, Command: []byte("b1")},
			{Term: 1, Command: []byte("b2")},
			{Term: 1, Command: []byte("b3")},
			{Term: 1, Command: []byte("b4")},
		}
		if err := l.Append(batch); err != nil {
			fmt.Fprintln(os.Stderr, "Append(batch):", err)
		}
	case "log-conflict-repair":
		l, err := OpenLog(filepath.Join(dir, "log"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "OpenLog:", err)
			os.Exit(1)
		}
		if err := l.TruncateAndAppend(2, []LogEntry{{Term: 2, Command: []byte("conflict")}}); err != nil {
			fmt.Fprintln(os.Stderr, "TruncateAndAppend:", err)
		}
	case "commit-save":
		if err := NewCommitStore(filepath.Join(dir, "commit")).Save(9); err != nil {
			fmt.Fprintln(os.Stderr, "Save:", err)
		}
	case "snapshot-create":
		sm := newFakeStateMachine()
		n := openSnapshottingNode(t, dir, 1, nil, sm)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.StartElection(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "StartElection:", err)
			os.Exit(1)
		}
		proposeAsLeaderAndWaitApplied(t, n, "one")
		proposeAsLeaderAndWaitApplied(t, n, "two")
		if err := n.CreateSnapshot(); err != nil {
			fmt.Fprintln(os.Stderr, "CreateSnapshot:", err)
		}
	case "append-entries-ack":
		// item 92 (mandatory): a follower must never report Success=true
		// for an entry that is not yet durable. This op performs a real
		// AppendEntries call and, only if it actually returned
		// Success=true, crashes unconditionally right after — no
		// failpoint, no mid-operation injection — proving the ack itself
		// already implied durability rather than relying on whatever
		// cleanup a graceful path might otherwise perform.
		l, err := OpenLog(filepath.Join(dir, "log"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "OpenLog:", err)
			os.Exit(1)
		}
		n, err := NewNode(2, NewStore(filepath.Join(dir, "state")), l, NewCommitStore(filepath.Join(dir, "commit")), NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, nil, nil, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "NewNode:", err)
			os.Exit(1)
		}
		resp, err := n.HandleAppendEntries(AppendEntriesRequest{
			Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0,
			Entries: []LogEntry{{Term: 1, Command: []byte("acked")}},
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "HandleAppendEntries:", err)
			os.Exit(1)
		}
		if !resp.Success {
			fmt.Fprintln(os.Stderr, "HandleAppendEntries did not succeed:", resp)
			os.Exit(1)
		}
		// Crash unconditionally right after a successful ack — fp (set by
		// the caller to "append.after-ack") never matches an actual
		// atomicWriteFile failpoint name, so the append above ran to
		// completion normally; this is a deliberate post-operation crash,
		// not a mid-operation injection.
		fmt.Fprintln(os.Stderr, crashMarkerPrefix+fp)
		os.Exit(crashExitCode)
	case "install-snapshot":
		sm := newFakeStateMachine()
		n := openSnapshottingNode(t, dir, 2, nil, sm)
		payloadSM := newFakeStateMachine()
		if err := payloadSM.apply(1, []byte("installed")); err != nil {
			fmt.Fprintln(os.Stderr, "apply:", err)
			os.Exit(1)
		}
		payload, err := payloadSM.snapshot()
		if err != nil {
			fmt.Fprintln(os.Stderr, "snapshot:", err)
			os.Exit(1)
		}
		req := InstallSnapshotRequest{
			Term:              1,
			LeaderID:          1,
			LastIncludedIndex: 5,
			LastIncludedTerm:  1,
			Offset:            0,
			Data:              payload,
			Done:              true,
			Configuration:     sampleSnapshotConfiguration(t),
		}
		if _, err := n.HandleInstallSnapshot(req); err != nil {
			fmt.Fprintln(os.Stderr, "HandleInstallSnapshot:", err)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown op:", op)
		os.Exit(1)
	}

	// The failpoint never fired: this is a test-setup bug (wrong
	// failpoint name for this op), not a crash. Exit normally so the
	// parent's marker/exit-code check fails loudly instead of the test
	// silently reporting a crash that never actually happened.
	os.Exit(0)
}
