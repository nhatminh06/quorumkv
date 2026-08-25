package service

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/clientproto"
	"quorumkv/internal/kv"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// dedupTestNode is like testNode but also tracks how many times this
// node's state machine genuinely mutated state for an identified
// command — real production code has no such counter (see
// docs/request-dedup.md's "no increments/counters" scope note); this is
// purely test instrumentation, computed by asking the state machine
// (LookupRequest) whether a command about to be applied will actually
// mutate before handing it to the real Service.Apply, since classification
// is a pure function of already-applied state.
type dedupTestNode struct {
	id      raft.NodeID
	dir     string
	peers   map[raft.NodeID]string // this node's own peer table, for reopenDedupNode
	svc     *Service
	tr      *transport.Transport
	applied atomic.Int64 // count of genuinely new (mutating) applies of an identified command
}

func (tn *dedupTestNode) addr() string { return tn.tr.Addr() }

// startDedupCluster is startCluster (internal/service/service_test.go)
// plus per-node mutation counting.
func startDedupCluster(t *testing.T, n int) []*dedupTestNode {
	t.Helper()
	nodes := make([]*dedupTestNode, n)
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
		tn := &dedupTestNode{id: id, dir: dir, svc: svc}
		countingApply := func(index raft.LogIndex, command []byte) error {
			cmd, decErr := kv.DecodeCommand(command)
			if decErr == nil && !cmd.ClientID.IsZero() {
				svc.mu.Lock()
				willMutate := svc.sm.LookupRequest(cmd.ClientID, cmd.Sequence, kv.Fingerprint(cmd)) == kv.AppliedNew
				svc.mu.Unlock()
				if willMutate {
					tn.applied.Add(1)
				}
			}
			return svc.Apply(index, command)
		}
		node, err := raft.NewNode(id, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, countingApply, svc.Snapshot, svc.Restore)
		if err != nil {
			t.Fatalf("NewNode: %v", err)
		}
		svc.Attach(node)
		rNodes[i] = node
		nodes[i] = tn
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
		tn.peers = nodePeers
		// t.Cleanup runs LIFO: register the node's own Close first and its
		// transport's Close second (see startCluster in service_test.go
		// for why this order matters — a still-open listener can
		// otherwise dispatch into a node concurrently tearing down).
		t.Cleanup(rNodes[i].Close)
		tr := tn.tr
		t.Cleanup(func() { tr.Close() })
	}
	return nodes
}

// reopenDedupNode reconstructs a fresh Raft + Service from old's on-disk
// directory and peer table — a genuine restart (new Store/Log/
// CommitStore/SnapshotStore/Service/Node), never reusing old's in-memory
// state — with the same counting-apply instrumentation, listening on a
// freshly assigned port. old must already be closed (transport and node)
// before calling this.
func reopenDedupNode(t *testing.T, old *dedupTestNode) *dedupTestNode {
	t.Helper()
	store := raft.NewStore(filepath.Join(old.dir, "state"))
	log, err := raft.OpenLog(filepath.Join(old.dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := raft.NewCommitStore(filepath.Join(old.dir, "commit"))
	svc := New(old.peers)
	tn := &dedupTestNode{id: old.id, dir: old.dir, peers: old.peers, svc: svc}
	countingApply := func(index raft.LogIndex, command []byte) error {
		cmd, decErr := kv.DecodeCommand(command)
		if decErr == nil && !cmd.ClientID.IsZero() {
			svc.mu.Lock()
			willMutate := svc.sm.LookupRequest(cmd.ClientID, cmd.Sequence, kv.Fingerprint(cmd)) == kv.AppliedNew
			svc.mu.Unlock()
			if willMutate {
				tn.applied.Add(1)
			}
		}
		return svc.Apply(index, command)
	}
	node, err := raft.NewNode(old.id, store, log, commitStore, raft.NewSnapshotStore(filepath.Join(old.dir, "snapshot")), old.peers, countingApply, svc.Snapshot, svc.Restore)
	if err != nil {
		t.Fatalf("NewNode (reopen): %v", err)
	}
	svc.Attach(node)
	t.Cleanup(node.Close)

	tr, err := transport.Listen("127.0.0.1:0", svc.Handler())
	if err != nil {
		t.Fatalf("Listen (reopen): %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	tn.tr = tr
	return tn
}

func electDedupLeader(t *testing.T, nodes []*dedupTestNode, leaderIdx int) {
	t.Helper()
	leader := nodes[leaderIdx]
	// See electLeaderAmong in service_test.go: PreVote's leader-contact
	// safeguard needs a real amount of wall time to pass since any prior
	// leader's last AppendEntries before a voter will grant a new
	// hypothetical vote — this helper is reused for failover elections
	// too, not only a cluster's very first election.
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
		for _, n := range nodes {
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

// dropResponseOnce wraps a node's real Handler so that the FIRST client
// request matching (clientID, seq) is processed completely for real
// (mutating/committing/applying exactly as it normally would) but its
// response is then discarded and replaced with a transport-level error —
// simulating "the leader fully processed this write and only the
// response was lost," per docs/request-dedup.md item 107: this is not
// simulated by rejecting the request before the leader ever sees it.
func dropResponseOnce(real transport.Handler) (wrapped transport.Handler, arm func(id [16]byte, seq uint64)) {
	var mu sync.Mutex
	var armed bool
	var wantID [16]byte
	var wantSeq uint64

	arm = func(id [16]byte, seq uint64) {
		mu.Lock()
		defer mu.Unlock()
		armed, wantID, wantSeq = true, id, seq
	}
	wrapped = func(ctx context.Context, m transport.Message) (transport.Message, error) {
		resp, err := real(ctx, m)
		if err != nil || m.Type != transport.MessageClientRequest {
			return resp, err
		}
		req, decErr := clientproto.DecodeRequest(m.Payload)
		if decErr != nil {
			return resp, err
		}
		mu.Lock()
		drop := armed && req.ClientID == wantID && uint64(req.Sequence) == wantSeq
		if drop {
			armed = false
		}
		mu.Unlock()
		if drop {
			return transport.Message{}, errors.New("simulated response drop: the leader already fully processed this request")
		}
		return resp, err
	}
	return wrapped, arm
}

// dropResponsesAlways is like dropResponseOnce but keeps dropping every
// matching request's response (never auto-disarms) — used when a test
// needs the client to keep failing against this node across several
// retries (e.g. until the node is deliberately crashed), rather than
// succeeding on its very next attempt.
func dropResponsesAlways(real transport.Handler) (wrapped transport.Handler, arm func(id [16]byte, seq uint64)) {
	var mu sync.Mutex
	var armed bool
	var wantID [16]byte
	var wantSeq uint64

	arm = func(id [16]byte, seq uint64) {
		mu.Lock()
		defer mu.Unlock()
		armed, wantID, wantSeq = true, id, seq
	}
	wrapped = func(ctx context.Context, m transport.Message) (transport.Message, error) {
		resp, err := real(ctx, m)
		if err != nil || m.Type != transport.MessageClientRequest {
			return resp, err
		}
		req, decErr := clientproto.DecodeRequest(m.Payload)
		if decErr != nil {
			return resp, err
		}
		mu.Lock()
		drop := armed && req.ClientID == wantID && uint64(req.Sequence) == wantSeq
		mu.Unlock()
		if drop {
			return transport.Message{}, errors.New("simulated response drop: the leader already fully processed this request")
		}
		return resp, err
	}
	return wrapped, arm
}

// TestResponseLostAfterCommitPutRetrySucceedsOnce is item 60 (mandatory):
// the leader commits+applies a PUT, its response is then lost, the
// client sees a transport failure and retries with the same identity —
// the retry must succeed (OK) and the write must have mutated state
// exactly once, not twice.
func TestResponseLostAfterCommitPutRetrySucceedsOnce(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	leader := nodes[0]

	real := leader.svc.Handler()
	wrapped, arm := dropResponseOnce(real)
	leader.tr.Close()
	tr, err := transport.Listen(leader.addr(), wrapped)
	if err != nil {
		t.Fatalf("re-Listen: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	// Re-point every peer's route to the leader at the same address —
	// the listener was closed and reopened, but transport.Listen was
	// given the exact same address, so no peer table update is needed.
	_ = tr

	c := client.New(leader.addr())
	arm(c.ID(), 1) // this client's first write will use sequence 1

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	v, ok, err := c.Get(ctx, []byte("x"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(x) = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
	if got := leader.applied.Load(); got != 1 {
		t.Fatalf("state mutated %d times, want exactly 1 (response-loss retry must not double-apply)", got)
	}
}

// TestResponseLostAfterCommitDeleteRetrySucceedsOnce is item 61: the same
// proof for DELETE.
func TestResponseLostAfterCommitDeleteRetrySucceedsOnce(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	leader := nodes[0]

	seedClient := client.New(leader.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := seedClient.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	baseline := leader.applied.Load()

	real := leader.svc.Handler()
	wrapped, arm := dropResponseOnce(real)
	leader.tr.Close()
	tr, err := transport.Listen(leader.addr(), wrapped)
	if err != nil {
		t.Fatalf("re-Listen: %v", err)
	}
	t.Cleanup(func() { tr.Close() })

	c := client.New(leader.addr())
	arm(c.ID(), 1)
	if err := c.Delete(ctx, []byte("x")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, err := c.Get(ctx, []byte("x")); err != nil || ok {
		t.Fatalf("Get(x) after delete = ok=%v err=%v, want absent", ok, err)
	}
	if got := leader.applied.Load() - baseline; got != 1 {
		t.Fatalf("delete mutated state %d times, want exactly 1", got)
	}
}

// TestRetryToNewLeaderAfterFailoverRecognizesDedup is item 63 — the
// strongest milestone proof: A commits+applies a write, A's response to
// the client is lost, A then crashes before the client can retry against
// it, B is elected, and the client retries the SAME request against B.
// B must recognize it via its own replicated dedup state (never a
// leader-local cache from A) and return OK without a second mutation.
func TestRetryToNewLeaderAfterFailoverRecognizesDedup(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	a, b, cNode := nodes[0], nodes[1], nodes[2]

	real := a.svc.Handler()
	wrapped, arm := dropResponsesAlways(real)
	a.tr.Close()
	tr, err := transport.Listen(a.addr(), wrapped)
	if err != nil {
		t.Fatalf("re-Listen: %v", err)
	}
	_ = tr

	c := client.New(a.addr())
	arm(c.ID(), 1)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	if err := c.Put(ctx, []byte("x"), []byte("1")); err == nil {
		cancel()
		t.Fatalf("Put succeeded despite the simulated response drop and A about to crash")
	}
	cancel()

	// Verify true ambiguity before crashing A (item 108): the write
	// really did commit and apply on A.
	if a.svc.node.CommitIndex() == 0 || a.svc.node.LastApplied() == 0 {
		t.Fatalf("precondition failed: A never committed/applied the write before crashing")
	}
	a.svc.mu.Lock()
	v, ok := a.svc.sm.Get([]byte("x"))
	a.svc.mu.Unlock()
	if !ok || string(v) != "1" {
		t.Fatalf("precondition failed: A's local KV does not reflect the write before crashing")
	}

	// A crashes for real.
	tr.Close()
	a.svc.node.Close()

	// B is elected among the survivors (deterministically, per the
	// leaderIdx=1 passed below).
	electLeaderAmongDedup(t, []*dedupTestNode{b, cNode}, nodes, 1)

	// The retry must reuse c's exact (ClientID, Sequence) — a fresh
	// Client would allocate a new ClientID and defeat dedup entirely, so
	// this reconstructs a Client from c's identity, pointed at the
	// surviving nodes.
	retry := client.NewWithID(c.ID(), b.addr(), cNode.addr())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := retry.Put(ctx2, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("retry against surviving cluster: %v", err)
	}

	b.svc.mu.Lock()
	v2, ok2 := b.svc.sm.Get([]byte("x"))
	b.svc.mu.Unlock()
	if !ok2 || string(v2) != "1" {
		t.Fatalf("Get(x) on new leader B = %q, %v; want 1, true", v2, ok2)
	}
	// The real invariant is per-state-machine: no single replica may
	// ever apply this (ClientID, Sequence) more than once. Each replica
	// independently applies its own copy of the same committed log
	// entry as ordinary replication (not a bug), so a *combined* count
	// across replicas is not the right thing to assert — B and C may
	// each individually have applied it once already, from before A
	// crashed.
	if got := b.applied.Load(); got > 1 {
		t.Fatalf("B applied this request %d times, want at most 1", got)
	}
	if got := cNode.applied.Load(); got > 1 {
		t.Fatalf("C applied this request %d times, want at most 1", got)
	}
}

// electLeaderAmongDedup mirrors electLeaderAmong for dedupTestNode.
func electLeaderAmongDedup(t *testing.T, voters []*dedupTestNode, nodes []*dedupTestNode, leaderIdx int) {
	t.Helper()
	leader := nodes[leaderIdx]
	// See electLeaderAmong in service_test.go: PreVote's leader-contact
	// safeguard needs a real amount of wall time to pass before a
	// failover election immediately following the old leader's stop.
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
			t.Fatalf("not all voters learned about the new leader within timeout")
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// TestLeaderCrashBeforeCommitRetryAppliesOnce is item 111: a request that
// was only ever appended locally (never committed) before the leader
// crashed must be treated as unseen by the next leader and applied
// exactly once there.
func TestLeaderCrashBeforeCommitRetryAppliesOnce(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	a, b, cNode := nodes[0], nodes[1], nodes[2]

	// Isolate A from both followers so its next proposal can never
	// commit, then stop it before any retry can reach it.
	a.svc.node.SetAppendSend(func(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
		return raft.AppendEntriesResponse{}, errors.New("isolated")
	})

	c := client.New(a.addr())
	shortCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	if err := c.Put(shortCtx, []byte("x"), []byte("1")); err == nil {
		cancel()
		t.Fatalf("Put succeeded despite the leader being isolated from the majority")
	}
	cancel()
	if a.svc.node.CommitIndex() != 0 {
		t.Fatalf("A committed the isolated write, want it to remain uncommitted")
	}

	a.svc.node.Close()
	electLeaderAmongDedup(t, []*dedupTestNode{b, cNode}, nodes, 1)

	retryClient := client.NewWithID(c.ID(), b.addr(), cNode.addr())
	ctx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := retryClient.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("retry against surviving cluster: %v", err)
	}

	if got := b.applied.Load() + cNode.applied.Load(); got != 1 {
		t.Fatalf("surviving nodes mutated state %d times combined, want exactly 1", got)
	}
}

// TestRequestConflictOverRealTCP is items 13/28: the same (ClientID,
// Sequence) reused for a different operation must be rejected as
// REQUEST_CONFLICT, never treated as a duplicate.
func TestRequestConflictOverRealTCP(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	leader := nodes[0]

	id := testClientID()
	putReq, _ := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	respMsg, err := transport.Send(ctx, leader.addr(), transport.NewMessage(transport.MessageClientRequest, putReq))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil || resp.Status != clientproto.StatusOK {
		t.Fatalf("first request: status=%v err=%v, want OK", resp.Status, err)
	}

	conflictReq, _ := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("DIFFERENT")})
	respMsg2, err := transport.Send(ctx, leader.addr(), transport.NewMessage(transport.MessageClientRequest, conflictReq))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp2, err := clientproto.DecodeResponse(respMsg2.Payload)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if resp2.Status != clientproto.StatusRequestConflict {
		t.Fatalf("Status = %v, want REQUEST_CONFLICT", resp2.Status)
	}
	leader.svc.mu.Lock()
	v, _ := leader.svc.sm.Get([]byte("x"))
	leader.svc.mu.Unlock()
	if string(v) != "1" {
		t.Fatalf("Get(x) = %q, want unchanged 1 after a conflicting request", v)
	}
}

// TestStaleSequenceOverRealTCP is item 29/93: a sequence behind the last
// applied one is rejected as STALE_REQUEST.
func TestStaleSequenceOverRealTCP(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	leader := nodes[0]

	c := client.New(leader.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	if err := c.Put(ctx, []byte("x"), []byte("2")); err != nil {
		t.Fatalf("Put 2: %v", err)
	}

	staleReq, _ := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpPut, ClientID: c.ID(), Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	respMsg, err := transport.Send(ctx, leader.addr(), transport.NewMessage(transport.MessageClientRequest, staleReq))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if resp.Status != clientproto.StatusStaleRequest {
		t.Fatalf("Status = %v, want STALE_REQUEST", resp.Status)
	}
}
