package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/clientproto"
	"quorumkv/internal/raft"
	"quorumkv/internal/reqid"
	"quorumkv/internal/transport"
)

// testClientID returns a fixed, non-zero ClientID for tests that build a
// clientproto.Request by hand (bypassing internal/client) and need a
// valid request identity for PUT/DELETE.
func testClientID() (id reqid.ClientID) {
	id[0] = 0xAB
	return id
}

type testNode struct {
	id  raft.NodeID
	svc *Service
	tr  *transport.Transport
}

// startCluster brings up n real nodes on real loopback TCP listeners,
// each running its own raft.Node + Service, wired to each other. It does
// not elect a leader — callers do that explicitly for determinism.
func startCluster(t *testing.T, n int) []*testNode {
	t.Helper()
	nodes := make([]*testNode, n)
	rNodes := make([]*raft.Node, n)
	for i := 0; i < n; i++ {
		id := raft.NodeID(i + 1)
		dir := t.TempDir()
		store := raft.NewStore(filepath.Join(dir, "state"))
		log, err := raft.OpenLog(filepath.Join(dir, "log"))
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		commitStore := raft.NewCommitStore(filepath.Join(dir, "commit"))
		svc := New(nil)
		node, err := raft.NewNode(id, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, svc.Apply, svc.Snapshot, svc.Restore)
		if err != nil {
			t.Fatalf("NewNode: %v", err)
		}
		svc.Attach(node)
		rNodes[i] = node
		nodes[i] = &testNode{id: id, svc: svc}
	}

	peers := make(map[raft.NodeID]string, n)
	for _, tn := range nodes {
		tr, err := transport.Listen("127.0.0.1:0", tn.svc.Handler())
		if err != nil {
			t.Fatalf("Listen: %v", err)
		}
		tn.tr = tr
		peers[tn.id] = tr.Addr()
	}
	for i, tn := range nodes {
		nodePeers := make(map[raft.NodeID]string, n-1)
		for id, addr := range peers {
			if id != tn.id {
				nodePeers[id] = addr
			}
		}
		rNodes[i].SetPeers(nodePeers)
		tn.svc.peers = nodePeers
		// t.Cleanup runs LIFO: register the node's own Close first and its
		// transport's Close second, so the transport (registered last)
		// stops accepting/dispatching inbound RPCs *before* the node
		// begins closing — otherwise a still-open listener can dispatch a
		// HandleAppendEntries call into a node that Close() is
		// concurrently tearing down, a genuine (if narrow) data race.
		t.Cleanup(rNodes[i].Close)
		tr := tn.tr
		t.Cleanup(func() { tr.Close() })
	}
	return nodes
}

func (tn *testNode) addr() string { return tn.tr.Addr() }

// electLeader triggers an election on nodes[leaderIdx] and then waits
// until every other still-alive node in voters has learned it is the
// leader (via that leader's first heartbeat actually landing over real
// TCP) — StartElection returning only means the vote was won, not that
// followers have heard from the new leader yet, and callers that
// immediately contact a follower need the latter. voters lets a caller
// exclude nodes it has already shut down (e.g. during a failover test).
func electLeader(t *testing.T, nodes []*testNode, leaderIdx int) {
	t.Helper()
	electLeaderAmong(t, nodes, nodes, leaderIdx)
}

func electLeaderAmong(t *testing.T, voters []*testNode, nodes []*testNode, leaderIdx int) {
	t.Helper()
	leader := nodes[leaderIdx]
	// PreVote's leader-contact safeguard (see docs/raft-election.md)
	// means a voter that has recently accepted AppendEntries from a
	// leader rejects a hypothetical PreVote for a real amount of wall
	// time (up to ~150ms) — correct production behavior, but this test
	// helper is also used for a failover election immediately after
	// stopping the old leader, well within that window. raft.Node has no
	// exported hook to fast-forward this from outside its own package, so
	// this waits it out for real rather than racing it.
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := leader.svc.node.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if leader.svc.node.Role() != raft.Leader {
		t.Fatalf("node %d Role() = %v, want Leader", leader.id, leader.svc.node.Role())
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allKnow := true
		for _, n := range voters {
			if n.id == leader.id {
				continue
			}
			id, ok := n.svc.node.LeaderHint()
			if !ok || id != leader.id {
				allKnow = false
				break
			}
		}
		if allKnow {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all nodes learned about the new leader within timeout")
		}
		time.Sleep(3 * time.Millisecond)
	}
}

func waitForClusterCommit(t *testing.T, timeout time.Duration, nodes []*testNode, index raft.LogIndex) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allDone := true
		for _, n := range nodes {
			if n.svc.node.LastApplied() < index {
				allDone = false
				break
			}
		}
		if allDone {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("not all nodes reached lastApplied >= %d within %v", index, timeout)
}

func TestPutGetDeleteEndToEnd(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)

	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(v) != "1" {
		t.Fatalf("Get(x) = %q, %v; want 1, true", v, ok)
	}

	if err := c.Delete(ctx, []byte("x")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, err = c.Get(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if ok {
		t.Fatalf("x should be absent after delete")
	}
}

func TestFollowerReturnsNotLeaderWithHint(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)

	// Contact a follower directly, bypassing the client's redirect logic,
	// to prove the raw protocol behavior and that the follower never
	// proposed anything.
	before := nodes[0].svc.node.LastLogIndex()

	req, _ := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpPut, ClientID: testClientID(), Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	respMsg, err := transport.Send(ctx, nodes[1].addr(), transport.NewMessage(transport.MessageClientRequest, req))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if resp.Status != clientproto.StatusNotLeader {
		t.Fatalf("Status = %v, want NOT_LEADER", resp.Status)
	}
	if string(resp.LeaderHint) != nodes[0].addr() {
		t.Fatalf("LeaderHint = %q, want %q", resp.LeaderHint, nodes[0].addr())
	}
	if nodes[0].svc.node.LastLogIndex() != before {
		t.Fatalf("leader's log changed after a follower rejected a PUT — follower must never propose")
	}
}

func TestUnknownLeaderReturnsEmptyHint(t *testing.T) {
	nodes := startCluster(t, 3) // no election triggered — nobody knows a leader

	req, _ := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpGet, Key: []byte("x")})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	respMsg, err := transport.Send(ctx, nodes[0].addr(), transport.NewMessage(transport.MessageClientRequest, req))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if resp.Status != clientproto.StatusNotLeader {
		t.Fatalf("Status = %v, want NOT_LEADER", resp.Status)
	}
	if len(resp.LeaderHint) != 0 {
		t.Fatalf("LeaderHint = %q, want empty (no leader known, must not fabricate one)", resp.LeaderHint)
	}
}

func TestClientFollowsRedirectToLeader(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)

	// Client starts by contacting the follower, not the leader.
	c := client.New(nodes[1].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// The write only succeeded via the leader (A) proposing it — B's own
	// refusal to propose anything as a follower is proven directly in
	// TestFollowerReturnsNotLeaderWithHint; here we confirm the redirect
	// path actually reached A by checking A committed it.
	if nodes[0].svc.node.CommitIndex() == 0 {
		t.Fatalf("leader A never committed the redirected write")
	}
	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(v) != "1" {
		t.Fatalf("Get(x) = %q, %v; want 1, true", v, ok)
	}
}

func TestFollowersEventuallyApplyCommittedWrites(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)

	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	index := nodes[0].svc.node.LastLogIndex()
	waitForClusterCommit(t, 2*time.Second, nodes, index)

	for _, n := range nodes {
		n.svc.mu.Lock()
		v, ok := n.svc.sm.Get([]byte("x"))
		n.svc.mu.Unlock()
		if !ok || string(v) != "1" {
			t.Fatalf("node %d state = %q, %v; want 1, true", n.id, v, ok)
		}
	}
}

func TestOrderedMultiCommand(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	if err := c.Put(ctx, []byte("x"), []byte("2")); err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	if err := c.Delete(ctx, []byte("x")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := c.Put(ctx, []byte("x"), []byte("3")); err != nil {
		t.Fatalf("Put 3: %v", err)
	}
	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(v) != "3" {
		t.Fatalf("Get(x) = %q, %v; want 3, true", v, ok)
	}
}

func TestConcurrentPuts(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	c := client.New(nodes[0].addr())

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			key := []byte(fmt.Sprintf("k%d", i))
			if err := c.Put(ctx, key, []byte("v")); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Put error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		v, ok, err := c.Get(ctx, key)
		if err != nil || !ok || string(v) != "v" {
			t.Errorf("Get(%s) = %q, %v, %v; want v, true, nil", key, v, ok, err)
		}
	}
	if nodes[0].svc.node.LastLogIndex() != n {
		t.Fatalf("LastLogIndex() = %d, want %d (unique indexes, no lost writes)", nodes[0].svc.node.LastLogIndex(), n)
	}
}

func TestNoMajorityClientWriteTimesOut(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)

	// Isolate the leader from both followers by closing their listeners.
	nodes[1].tr.Close()
	nodes[2].tr.Close()

	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := c.Put(ctx, []byte("x"), []byte("1"))
	if err == nil {
		t.Fatalf("Put succeeded despite no majority, want a bounded failure")
	}

	if nodes[0].svc.node.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0 — a local append without majority must never commit", nodes[0].svc.node.CommitIndex())
	}
	if nodes[0].svc.node.LastLogIndex() != 1 {
		t.Fatalf("LastLogIndex() = %d, want 1 (appended locally despite no majority)", nodes[0].svc.node.LastLogIndex())
	}
}

func TestMajorityWithOneNodeDown(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	nodes[2].tr.Close() // C unavailable

	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if nodes[0].svc.node.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d, want 1 (majority via leader+B)", nodes[0].svc.node.CommitIndex())
	}
}

func TestLeaderFailoverClient(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)

	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put x=1: %v", err)
	}
	waitForClusterCommit(t, 2*time.Second, nodes, nodes[0].svc.node.LastLogIndex())

	// A stops.
	nodes[0].tr.Close()
	nodes[0].svc.node.Close()

	// B is elected among the survivors; only wait on C, since A is dead
	// and will never learn about anything again.
	electLeaderAmong(t, []*testNode{nodes[2]}, nodes, 1)

	c2 := client.New(nodes[1].addr())
	if err := c2.Put(ctx, []byte("y"), []byte("2")); err != nil {
		t.Fatalf("Put y=2: %v", err)
	}

	vx, ok, err := c2.Get(ctx, []byte("x"))
	if err != nil || !ok || string(vx) != "1" {
		t.Fatalf("Get(x) = %q, %v, %v; want 1, true, nil", vx, ok, err)
	}
	vy, ok, err := c2.Get(ctx, []byte("y"))
	if err != nil || !ok || string(vy) != "2" {
		t.Fatalf("Get(y) = %q, %v, %v; want 2, true, nil", vy, ok, err)
	}
}

func TestRestartRebuildsKVStateFromCommittedPrefix(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "log")
	commitPath := filepath.Join(dir, "commit")

	// First run: commit three writes as a single-node cluster.
	func() {
		store := raft.NewStore(statePath)
		log, err := raft.OpenLog(logPath)
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		commitStore := raft.NewCommitStore(commitPath)
		svc := New(nil)
		node, err := raft.NewNode(1, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, svc.Apply, svc.Snapshot, svc.Restore)
		if err != nil {
			t.Fatalf("NewNode: %v", err)
		}
		svc.Attach(node)
		defer node.Close()

		if err := node.StartElection(context.Background()); err != nil {
			t.Fatalf("StartElection: %v", err)
		}
		id := testClientID()
		for _, r := range []clientproto.Request{
			{Operation: clientproto.OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")},
			{Operation: clientproto.OpPut, ClientID: id, Sequence: 2, Key: []byte("y"), Value: []byte("2")},
			{Operation: clientproto.OpDelete, ClientID: id, Sequence: 3, Key: []byte("x")},
		} {
			resp := svc.dispatch(context.Background(), r)
			if resp.Status != clientproto.StatusOK {
				t.Fatalf("dispatch(%+v) = %+v, want OK", r, resp)
			}
		}
	}()

	// Second run: fresh Service/Node from the same files.
	store := raft.NewStore(statePath)
	log, err := raft.OpenLog(logPath)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := raft.NewCommitStore(commitPath)
	svc := New(nil)
	node, err := raft.NewNode(1, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, svc.Apply, svc.Snapshot, svc.Restore)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	svc.Attach(node)
	defer node.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := node.WaitApplied(ctx, node.CommitIndex(), 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if node.Role() != raft.Follower {
		t.Fatalf("Role() = %v, want Follower after restart", node.Role())
	}

	svc.mu.Lock()
	_, xOK := svc.sm.Get([]byte("x"))
	yVal, yOK := svc.sm.Get([]byte("y"))
	svc.mu.Unlock()
	if xOK {
		t.Fatalf("x should be absent (deleted) after rebuild")
	}
	if !yOK || string(yVal) != "2" {
		t.Fatalf("y = %q, %v; want 2, true", yVal, yOK)
	}
}

func TestApplyFailureClientNeverGetsOK(t *testing.T) {
	dir := t.TempDir()
	store := raft.NewStore(filepath.Join(dir, "state"))
	log, err := raft.OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := raft.NewCommitStore(filepath.Join(dir, "commit"))
	failing := func(index raft.LogIndex, cmd []byte) error {
		return errors.New("injected apply failure")
	}
	node, err := raft.NewNode(1, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, failing, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer node.Close()
	svc := New(nil)
	svc.Attach(node)

	if err := node.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	resp := svc.dispatch(context.Background(), clientproto.Request{Operation: clientproto.OpPut, ClientID: testClientID(), Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	if resp.Status == clientproto.StatusOK {
		t.Fatalf("dispatch returned OK despite apply failure")
	}
	if node.LastApplied() != 0 {
		t.Fatalf("LastApplied() = %d, want 0", node.LastApplied())
	}
}
