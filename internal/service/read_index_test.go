package service

import (
	"context"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/clientproto"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// rawGet sends a GET directly to addr and returns the decoded response,
// bypassing the client's redirect-following so the exact status code from
// one specific node can be inspected.
func rawGet(t *testing.T, addr string, key string) clientproto.Response {
	t.Helper()
	req, err := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpGet, Key: []byte(key)})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	respMsg, err := transport.Send(ctx, addr, transport.NewMessage(transport.MessageClientRequest, req))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	return resp
}

// TestIsolatedOldLeaderCannotServeStaleGet is item 53 — the core Milestone
// 8 proof, run over real TCP (also satisfying item 94/95's real-network
// requirement): a leader isolated from the majority commits a write, is
// then partitioned away while the majority elects a new leader and
// commits a different write, and a GET sent directly to the still-running
// isolated old leader — which may still believe Role==Leader — must NOT
// return the stale value. TIMEOUT, NOT_LEADER, or a context error are all
// acceptable; a stale StatusOK is not.
func TestIsolatedOldLeaderCannotServeStaleGet(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)
	a, b, c := nodes[0], nodes[1], nodes[2]

	ca := client.New(a.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ca.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, nodes, a.svc.node.LastLogIndex())

	// Isolate A from both followers. A may not yet know it lost
	// leadership.
	f.partition(a.id, b.id)
	f.partition(a.id, c.id)

	// B/C elect a new leader and commit a different write while A is cut
	// off.
	electLeaderAmong(t, []*testNode{c}, nodes, 1)
	cb := client.New(b.addr())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := cb.Put(ctx2, []byte("x"), []byte("2")); err != nil {
		t.Fatalf("Put to new leader: %v", err)
	}

	// The core assertion: GET x sent directly to the isolated old leader
	// must never return the stale "1".
	resp := rawGet(t, a.addr(), "x")
	if resp.Status == clientproto.StatusOK {
		t.Fatalf("isolated old leader returned StatusOK value %q — stale successful GET, safety violation", resp.Value)
	}
	switch resp.Status {
	case clientproto.StatusTimeout, clientproto.StatusNotLeader:
		// acceptable
	default:
		t.Fatalf("isolated old leader GET status = %v, want Timeout or NotLeader", resp.Status)
	}
}

// TestNewLeaderReadServesQuorumConfirmedValue is item 54: in the same
// isolated-leader scenario, GET against the new majority leader must
// succeed and return the new value through ReadIndex.
func TestNewLeaderReadServesQuorumConfirmedValue(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)
	a, b, c := nodes[0], nodes[1], nodes[2]

	ca := client.New(a.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ca.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, nodes, a.svc.node.LastLogIndex())

	f.partition(a.id, b.id)
	f.partition(a.id, c.id)
	electLeaderAmong(t, []*testNode{c}, nodes, 1)

	cb := client.New(b.addr())
	if err := cb.Put(ctx, []byte("x"), []byte("2")); err != nil {
		t.Fatalf("Put to new leader: %v", err)
	}
	v, ok, err := cb.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "2" {
		t.Fatalf("Get(x) on new leader = %q, %v, %v; want 2, true, nil", v, ok, err)
	}
}

// TestHealedOldLeaderReturnsNotLeaderNoStaleRead is item 55: once the
// partition heals and the old leader learns the higher term, it must
// answer GET with NOT_LEADER (with a hint once it has one) — never a
// stale value.
func TestHealedOldLeaderReturnsNotLeaderNoStaleRead(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)
	a, b, c := nodes[0], nodes[1], nodes[2]

	ca := client.New(a.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ca.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, nodes, a.svc.node.LastLogIndex())

	f.partition(a.id, b.id)
	f.partition(a.id, c.id)
	electLeaderAmong(t, []*testNode{c}, nodes, 1)
	cb := client.New(b.addr())
	if err := cb.Put(ctx, []byte("x"), []byte("2")); err != nil {
		t.Fatalf("Put to new leader: %v", err)
	}

	f.heal(a.id, b.id)
	f.heal(a.id, c.id)
	eventually(t, 2*time.Second, func() bool {
		return a.svc.node.Role() == raft.Follower && a.svc.node.CurrentTerm() == b.svc.node.CurrentTerm()
	}, nil)

	resp := rawGet(t, a.addr(), "x")
	if resp.Status != clientproto.StatusNotLeader {
		t.Fatalf("healed old leader GET status = %v, want NotLeader", resp.Status)
	}
	if resp.Status == clientproto.StatusOK {
		t.Fatalf("healed old leader returned a stale value")
	}
}

// TestOneFollowerPartitionedReadStillSucceeds is item 56: with only one
// follower partitioned away, the leader + the remaining follower still
// form a quorum, so reads succeed normally.
func TestOneFollowerPartitionedReadStillSucceeds(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)
	a, b, c := nodes[0], nodes[1], nodes[2]

	ca := client.New(a.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ca.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, []*testNode{a, b}, a.svc.node.LastLogIndex())

	f.partition(a.id, c.id) // only C is cut off; A+B still form quorum

	v, ok, err := ca.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(x) with only one follower partitioned = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
}

// TestMajorityPartitionReadBehavior is item 57: the isolated minority
// leader cannot serve reads, while the connected majority's leader can —
// directly proving partition-aware read behavior on both sides at once.
func TestMajorityPartitionReadBehavior(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)
	a, b, c := nodes[0], nodes[1], nodes[2]

	ca := client.New(a.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ca.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, nodes, a.svc.node.LastLogIndex())

	f.partition(a.id, b.id)
	f.partition(a.id, c.id)
	electLeaderAmong(t, []*testNode{c}, nodes, 1)

	// A (isolated minority) must not succeed.
	respA := rawGet(t, a.addr(), "x")
	if respA.Status == clientproto.StatusOK {
		t.Fatalf("isolated minority leader GET succeeded, want no successful read")
	}

	// B (connected majority leader) must succeed.
	respB := rawGet(t, b.addr(), "x")
	if respB.Status != clientproto.StatusOK || string(respB.Value) != "1" {
		t.Fatalf("majority leader GET = %v %q, want OK 1", respB.Status, respB.Value)
	}
}

// TestReadAfterCompletedWriteRealTimeOrder is item 58 (mandatory): once a
// client has received OK for a PUT, a subsequent GET must observe that
// exact value — proven twice, for two different values, to rule out a
// GET that happens to read stale-but-matching state.
func TestReadAfterCompletedWriteRealTimeOrder(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := c.Put(ctx, []byte("x"), []byte("123")); err != nil {
		t.Fatalf("Put x=123: %v", err)
	}
	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "123" {
		t.Fatalf("Get(x) after Put x=123 = %q, %v, %v; want 123, true, nil", v, ok, err)
	}

	if err := c.Put(ctx, []byte("x"), []byte("456")); err != nil {
		t.Fatalf("Put x=456: %v", err)
	}
	v, ok, err = c.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "456" {
		t.Fatalf("Get(x) after Put x=456 = %q, %v, %v; want 456, true, nil", v, ok, err)
	}
}

// TestFailoverReadAfterCommittedWrite is item 59: a committed write
// survives leader failure and is visible via ReadIndex on the newly
// elected leader.
func TestFailoverReadAfterCommittedWrite(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, nodes, nodes[0].svc.node.LastLogIndex())

	nodes[0].tr.Close()
	nodes[0].svc.node.Close()
	electLeaderAmong(t, []*testNode{nodes[2]}, nodes, 1)

	c2 := client.New(nodes[1].addr())
	v, ok, err := c2.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(x) on new leader after failover = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
}

// TestRealTCPReadIndexHealthyPath is item 94 (mandatory): a real,
// healthy three-node cluster over real TCP serves GET through the full
// ReadIndex path — quorum probe AppendEntries, real socket responses,
// quorum confirmation, WaitApplied, then the local KV read.
func TestRealTCPReadIndexHealthyPath(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	readIndex, err := nodes[0].svc.node.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex over real TCP: %v", err)
	}
	if readIndex == 0 {
		t.Fatalf("ReadIndex() = 0, want a real committed index")
	}
	v, ok, err := c.Get(ctx, []byte("k"))
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get(k) = %q, %v, %v; want v, true, nil", v, ok, err)
	}
}
