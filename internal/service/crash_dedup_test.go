package service

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

	"quorumkv/internal/client"
	"quorumkv/internal/clientproto"
	"quorumkv/internal/kv"
	"quorumkv/internal/raft"
	"quorumkv/internal/reqid"
	"quorumkv/internal/transport"
)

// dedupCrashExitCode/dedupCrashMarker mirror internal/raft's subprocess
// crash-helper pattern (see crash_subprocess_test.go there): a distinct
// exit code plus a stderr marker so the parent can prove the subprocess
// actually reached the point it crashed at, not merely that it exited.
const dedupCrashExitCode = 92

const dedupCrashMarker = "QKV_DEDUP_CRASH_REACHED"

// fixedCrashClientID is a known, non-zero, non-random ClientID shared
// between the crashing subprocess and the parent process — the parent
// needs it to construct the exact retry request after the subprocess is
// gone, and a real random ID would not survive the process boundary.
var fixedCrashClientID = reqid.ClientID{0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42}

func newSingleNodeService(dir string) (*Service, *raft.Node, *transport.Transport, error) {
	store := raft.NewStore(filepath.Join(dir, "state"))
	log, err := raft.OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		return nil, nil, nil, err
	}
	commitStore := raft.NewCommitStore(filepath.Join(dir, "commit"))
	svc := New(nil)
	node, err := raft.NewNode(1, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, svc.Apply, svc.Snapshot, svc.Restore)
	if err != nil {
		return nil, nil, nil, err
	}
	svc.Attach(node)
	tr, err := transport.Listen("127.0.0.1:0", svc.Handler())
	if err != nil {
		node.Close()
		return nil, nil, nil, err
	}
	return svc, node, tr, nil
}

// TestDedupCrashHelperSubprocess is not a real test: it only runs as a
// re-exec'd subprocess of TestSnapshotDedupSurvivesRealCrash, gated on
// QKV_CRASHTEST_MODE. It Puts one identified request, takes a snapshot
// (durably publishing KV + dedup table together — see kv.StateMachine's
// Snapshot/Restore), then crashes the process outright: no Close, no
// graceful shutdown, proving dedup survival does not depend on any
// cleanup path a real crash would skip.
func TestDedupCrashHelperSubprocess(t *testing.T) {
	if os.Getenv("QKV_CRASHTEST_MODE") != "1" {
		t.Skip("only runs as a subprocess crash helper; see runDedupCrashSubprocess")
	}
	dir := os.Getenv("QKV_CRASHTEST_DIR")

	_, node, tr, err := newSingleNodeService(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "newSingleNodeService:", err)
		os.Exit(1)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := node.StartElection(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "StartElection:", err)
		os.Exit(1)
	}

	c := client.NewWithID(fixedCrashClientID, tr.Addr())
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		fmt.Fprintln(os.Stderr, "Put:", err)
		os.Exit(1)
	}
	if err := node.CreateSnapshot(); err != nil {
		fmt.Fprintln(os.Stderr, "CreateSnapshot:", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, dedupCrashMarker)
	os.Exit(dedupCrashExitCode)
}

// runDedupCrashSubprocess re-executes this test binary as
// TestDedupCrashHelperSubprocess against dir, and fails the test unless
// it actually reached the crash marker with the expected exit code.
func runDedupCrashSubprocess(t *testing.T, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDedupCrashHelperSubprocess$")
	cmd.Env = append(os.Environ(), "QKV_CRASHTEST_MODE=1", "QKV_CRASHTEST_DIR="+dir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if !strings.Contains(stderr.String(), dedupCrashMarker) {
		t.Fatalf("subprocess never reached the crash marker\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	exitErr, ok := runErr.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != dedupCrashExitCode {
		t.Fatalf("subprocess exit = %v, want exit code %d\nstdout:\n%s\nstderr:\n%s", runErr, dedupCrashExitCode, stdout.String(), stderr.String())
	}
}

// TestSnapshotDedupSurvivesRealCrash is the crash-injection strengthening
// of TestSnapshotCompactionDedupSurvives (dedup_test.go's graceful
// variant, item 78): a real process performs an identified Put, snapshots
// it (durably publishing KV state and the dedup table together as one
// opaque blob — internal/raft never sees dedup, only bytes), and is then
// killed outright via os.Exit with no cleanup. A genuinely fresh Service
// and Node opened from the same directory in this (parent) process must
// still recognize a retry of that exact request as a duplicate rather
// than re-applying it, and must still answer Get with the original
// value — proving dedup recovery does not implicitly depend on any
// in-memory or Close()-path state that a real crash would never run.
func TestSnapshotDedupSurvivesRealCrash(t *testing.T) {
	dir := t.TempDir()
	runDedupCrashSubprocess(t, dir)

	svc, node, tr, err := newSingleNodeService(dir)
	if err != nil {
		t.Fatalf("newSingleNodeService (fresh, post-crash): %v", err)
	}
	defer node.Close()
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := node.StartElection(ctx); err != nil {
		t.Fatalf("StartElection (fresh): %v", err)
	}

	retryReq, err := clientproto.EncodeRequest(clientproto.Request{
		Operation: clientproto.OpPut, ClientID: fixedCrashClientID, Sequence: 1, Key: []byte("x"), Value: []byte("1"),
	})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	respMsg, err := transport.Send(ctx, tr.Addr(), transport.NewMessage(transport.MessageClientRequest, retryReq))
	if err != nil {
		t.Fatalf("Send retry: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil || resp.Status != clientproto.StatusOK {
		t.Fatalf("retry after crash: status=%v err=%v, want OK", resp.Status, err)
	}

	c := client.NewWithID(fixedCrashClientID, tr.Addr())
	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(x) after crash+retry = (%q,%v,%v), want (\"1\",true,nil)", v, ok, err)
	}

	cmd := kv.NewIdentifiedPutCommand(fixedCrashClientID, 1, []byte("x"), []byte("1"))
	svc.mu.Lock()
	outcome := svc.sm.LookupRequest(fixedCrashClientID, 1, kv.Fingerprint(cmd))
	svc.mu.Unlock()
	if outcome != kv.AppliedDuplicate {
		t.Fatalf("LookupRequest after retry = %v, want AppliedDuplicate — dedup table did not survive the crash", outcome)
	}
}
