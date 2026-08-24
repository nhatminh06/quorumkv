package raft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/transport"
)

// fakeNetwork dispatches RequestVote RPCs directly to a registered peer
// Node's HandleRequestVote, in process — no sockets, no timing. This lets
// pure election tests exercise the exact same StartElection/
// applyVoteResponse code path that the real TCP transport uses, with an
// address optionally marked unreachable to simulate a dropped/delayed RPC.
type fakeNetwork struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[string]bool
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{nodes: make(map[string]*Node), blocked: make(map[string]bool)}
}

func (f *fakeNetwork) register(addr string, n *Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[addr] = n
}

func (f *fakeNetwork) setBlocked(addr string, blocked bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked[addr] = blocked
}

func (f *fakeNetwork) send(_ context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	f.mu.Lock()
	peer, ok := f.nodes[addr]
	blocked := f.blocked[addr]
	f.mu.Unlock()
	if !ok {
		return RequestVoteResponse{}, errors.New("fakeNetwork: unknown address " + addr)
	}
	if blocked {
		return RequestVoteResponse{}, errors.New("fakeNetwork: " + addr + " unreachable")
	}
	return peer.HandleRequestVote(req)
}

func newFakeNode(t *testing.T, id NodeID, peers map[NodeID]string) *Node {
	t.Helper()
	store := NewStore(filepath.Join(t.TempDir(), "state"))
	n, err := NewNode(id, store, peers)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// TestThreeNodePureElection models nodes A(1), B(2), C(3) with an
// in-process fake network. Triggering A's election must elect A leader:
// A self-votes, B and C both grant (a real log-freshness/HandleRequestVote
// call, not a canned response), giving A a 2-of-3 majority.
func TestThreeNodePureElection(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c := newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	a.send, b.send, c.send = net.send, net.send, net.send
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	if a.Role() != Leader {
		t.Fatalf("A.Role() = %v, want Leader", a.Role())
	}
	if a.CurrentTerm() != 1 {
		t.Fatalf("A.CurrentTerm() = %d, want 1", a.CurrentTerm())
	}
	if v := b.VotedFor(); v == nil || *v != 1 {
		t.Fatalf("B.VotedFor() = %v, want 1 (persisted vote for A)", v)
	}
	if b.CurrentTerm() != 1 || c.CurrentTerm() != 1 {
		t.Fatalf("B/C term = %d/%d, want both 1", b.CurrentTerm(), c.CurrentTerm())
	}
}

// TestSplitVoteThenRecovery models the classic Raft split-vote scenario
// with three nodes: A and B both become candidates in term 1 while the
// network is fully partitioned, so each persists a self-vote but neither
// learns about the other or reaches C — no one gets the 2-of-3 majority.
// A then retries in term 2 once the partition heals, reaching both B and
// C, and wins outright.
//
// (Round 1 must isolate every node from every other, not just the third
// voter: StartElection sends its RequestVote RPCs immediately after
// persisting its own self-vote with no automatic retry, so if A and B
// could reach each other, whichever of the two happened to run first
// would simply win the other's vote before the second one ever became a
// candidate — that is a normal election, not a split vote.)
func TestSplitVoteThenRecovery(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c := newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	a.send, b.send, c.send = net.send, net.send, net.send
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	// Round 1: the network is fully partitioned.
	net.setBlocked("A", true)
	net.setBlocked("B", true)
	net.setBlocked("C", true)

	if err := b.StartElection(context.Background()); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("A StartElection: %v", err)
	}

	if a.Role() == Leader || b.Role() == Leader {
		t.Fatalf("split vote should elect no leader: A=%v B=%v", a.Role(), b.Role())
	}
	if a.Role() != Candidate || b.Role() != Candidate {
		t.Fatalf("both A and B should remain Candidate after a split vote: A=%v B=%v", a.Role(), b.Role())
	}
	if a.CurrentTerm() != 1 || b.CurrentTerm() != 1 {
		t.Fatalf("A/B term = %d/%d, want both 1", a.CurrentTerm(), b.CurrentTerm())
	}

	// Round 2: the partition heals; A retries in a higher term and wins.
	net.setBlocked("A", false)
	net.setBlocked("B", false)
	net.setBlocked("C", false)
	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("A StartElection (round 2): %v", err)
	}

	if a.Role() != Leader {
		t.Fatalf("A.Role() = %v, want Leader after recovery election", a.Role())
	}
	if a.CurrentTerm() != 2 {
		t.Fatalf("A.CurrentTerm() = %d, want 2", a.CurrentTerm())
	}
}

// TestRealTCPThreeNodeElection wires three nodes together over real
// loopback TCP transport (Milestone 2), rather than the in-process fake
// network, and proves the same election logic reaches a majority and
// elects a leader.
func TestRealTCPThreeNodeElection(t *testing.T) {
	node1 := newFakeNode(t, 1, nil)
	node2 := newFakeNode(t, 2, nil)
	node3 := newFakeNode(t, 3, nil)

	tr1, err := transport.Listen("127.0.0.1:0", node1.Handler())
	if err != nil {
		t.Fatalf("Listen node1: %v", err)
	}
	defer tr1.Close()
	tr2, err := transport.Listen("127.0.0.1:0", node2.Handler())
	if err != nil {
		t.Fatalf("Listen node2: %v", err)
	}
	defer tr2.Close()
	tr3, err := transport.Listen("127.0.0.1:0", node3.Handler())
	if err != nil {
		t.Fatalf("Listen node3: %v", err)
	}
	defer tr3.Close()

	node1.SetPeers(map[NodeID]string{2: tr2.Addr(), 3: tr3.Addr()})
	node2.SetPeers(map[NodeID]string{1: tr1.Addr(), 3: tr3.Addr()})
	node3.SetPeers(map[NodeID]string{1: tr1.Addr(), 2: tr2.Addr()})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := node1.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	if node1.Role() != Leader {
		t.Fatalf("node1.Role() = %v, want Leader", node1.Role())
	}
	if node1.CurrentTerm() != 1 {
		t.Fatalf("node1.CurrentTerm() = %d, want 1", node1.CurrentTerm())
	}
	votedForNode1 := 0
	for _, n := range []*Node{node2, node3} {
		if v := n.VotedFor(); v != nil && *v == 1 {
			votedForNode1++
		}
	}
	if votedForNode1 < 1 {
		t.Fatalf("expected at least one peer to have persisted a vote for node1")
	}
}

// TestRunStartsElectionOnTimeout proves the production timer path (Run)
// actually starts an election when no vote/heartbeat activity resets it,
// using a short injected timeout so the test stays fast and deterministic
// rather than depending on the real 150-300ms production range.
func TestRunStartsElectionOnTimeout(t *testing.T) {
	n := newFakeNode(t, 1, nil) // single-node cluster: election succeeds with no network
	n.timeoutFunc = func() time.Duration { return 5 * time.Millisecond }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		n.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if n.Role() == Leader {
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	t.Fatalf("Run did not start an election within 1s; role = %v", n.Role())
}
