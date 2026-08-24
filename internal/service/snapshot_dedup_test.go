package service

import (
	"context"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/clientproto"
	"quorumkv/internal/transport"
)

// TestSnapshotCompactionDedupSurvives is item 78 (mandatory): a client
// write is applied, snapshotted, and compacted out of the log entirely;
// after restart from that snapshot, retrying the exact same request must
// still be recognized as a duplicate — dedup state must not depend on
// the physical log entry still existing.
func TestSnapshotCompactionDedupSurvives(t *testing.T) {
	nodes := startDedupCluster(t, 1) // single node: simplest way to force compaction deterministically
	electDedupLeader(t, nodes, 0)
	n := nodes[0]

	c := client.New(n.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := n.svc.node.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if n.svc.node.LastLogIndex() != n.svc.node.LastApplied() {
		t.Fatalf("precondition: expected the log to be fully compacted through lastApplied")
	}
	// The physical log entry for this request is now gone — only the
	// snapshot (KV + dedup table) remains.
	if _, ok := n.svc.node.LogEntry(1); ok {
		t.Fatalf("precondition: entry 1 should have been compacted away")
	}

	// Retry the exact SAME logical request (ClientID, sequence 1) — not
	// c.Put again, which would allocate a new sequence and be a
	// different logical write. A raw request reproduces a genuine retry
	// of the already-completed one.
	retryReq, err := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpPut, ClientID: c.ID(), Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	respMsg, err := transport.Send(ctx, n.addr(), transport.NewMessage(transport.MessageClientRequest, retryReq))
	if err != nil {
		t.Fatalf("Send retry: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil || resp.Status != clientproto.StatusOK {
		t.Fatalf("retry after compaction: status=%v err=%v, want OK", resp.Status, err)
	}

	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(x) = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
	if got := n.applied.Load(); got != 1 {
		t.Fatalf("state mutated %d times, want exactly 1 — dedup must survive log compaction", got)
	}
}

// TestRestartFromSnapshotDedupSurvives is items 15-17: a node fully
// restarted (fresh process, snapshot on disk) must restore its dedup
// table from the snapshot and recognize a retry with no committed log
// entry for it at all in the fresh process's memory.
func TestRestartFromSnapshotDedupSurvives(t *testing.T) {
	nodes := startDedupCluster(t, 1)
	electDedupLeader(t, nodes, 0)
	n := nodes[0]

	c := client.New(n.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := n.svc.node.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Restart: close this node/transport and reconstruct fresh Raft +
	// Service state from the same on-disk directory, exactly like the
	// existing TestRestartRebuildsKVStateFromCommittedPrefix pattern —
	// here specifically to prove dedup state (not just KV) survives.
	n.tr.Close()
	n.svc.node.Close()

	restarted := reopenDedupNode(t, n)
	restartedCtx, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := restarted.svc.node.StartElection(restartedCtx); err != nil {
		t.Fatalf("StartElection after restart: %v", err)
	}
	if err := restarted.svc.node.WaitApplied(restartedCtx, restarted.svc.node.CommitIndex(), 0); err != nil {
		t.Fatalf("WaitApplied after restart: %v", err)
	}

	c2 := client.NewWithID(c.ID(), restarted.addr())
	if err := c2.Put(restartedCtx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	if got := restarted.applied.Load(); got != 0 {
		t.Fatalf("restarted node mutated state %d times for a recognized retry, want 0 (it should never have been unseen)", got)
	}
}
