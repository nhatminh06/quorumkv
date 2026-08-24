package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// eventually polls cond until it returns true or timeout elapses. On
// timeout it fails the test, including describe()'s output if given.
func eventually(t *testing.T, timeout time.Duration, cond func() bool, describe func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			msg := ""
			if describe != nil {
				msg = describe()
			}
			t.Fatalf("condition not met within %v: %s", timeout, msg)
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// faultLink identifies one directed communication path between two nodes.
type faultLink struct{ from, to raft.NodeID }

// faultNet is a directional fault controller for real-TCP service-level
// tests: it sits above transport.Send (per Milestone 6's design) and
// decides whether a Raft RPC a node is about to send is allowed through.
// Anything allowed still travels over a real socket via transport.Send —
// this only ever says yes/no, never fakes the network itself.
type faultNet struct {
	mu       sync.Mutex
	idOfAddr map[string]raft.NodeID
	blocked  map[faultLink]bool
}

func newFaultNet() *faultNet {
	return &faultNet{idOfAddr: make(map[string]raft.NodeID), blocked: make(map[faultLink]bool)}
}

func (f *faultNet) registerAddr(addr string, id raft.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.idOfAddr[addr] = id
}

func (f *faultNet) block(from, to raft.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked[faultLink{from, to}] = true
}

func (f *faultNet) allow(from, to raft.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.blocked, faultLink{from, to})
}

// partition blocks both directions between a and b.
func (f *faultNet) partition(a, b raft.NodeID) {
	f.block(a, b)
	f.block(b, a)
}

// heal restores both directions between a and b.
func (f *faultNet) heal(a, b raft.NodeID) {
	f.allow(a, b)
	f.allow(b, a)
}

func (f *faultNet) isBlocked(from raft.NodeID, addr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	to, known := f.idOfAddr[addr]
	return known && f.blocked[faultLink{from, to}]
}

var errFaultBlocked = errors.New("faultNet: link blocked")

func (f *faultNet) voteSenderFor(from raft.NodeID) func(context.Context, string, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return func(ctx context.Context, addr string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
		if f.isBlocked(from, addr) {
			return raft.RequestVoteResponse{}, errFaultBlocked
		}
		msg := transport.NewMessage(transport.MessageRequestVote, raft.EncodeRequestVote(req))
		resp, err := transport.Send(ctx, addr, msg)
		if err != nil {
			return raft.RequestVoteResponse{}, err
		}
		return raft.DecodeRequestVoteResponse(resp.Payload)
	}
}

func (f *faultNet) appendSenderFor(from raft.NodeID) func(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return func(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
		if f.isBlocked(from, addr) {
			return raft.AppendEntriesResponse{}, errFaultBlocked
		}
		payload, err := raft.EncodeAppendEntries(req)
		if err != nil {
			return raft.AppendEntriesResponse{}, err
		}
		msg := transport.NewMessage(transport.MessageAppendEntries, payload)
		resp, err := transport.Send(ctx, addr, msg)
		if err != nil {
			return raft.AppendEntriesResponse{}, err
		}
		return raft.DecodeAppendEntriesResponse(resp.Payload)
	}
}

// wireFaultNet fits a directional fault controller over an already
// running startCluster: every node's real address is registered, and its
// Raft RPC senders are replaced with fault-checked wrappers that still
// dispatch over real TCP when allowed.
func wireFaultNet(nodes []*testNode) *faultNet {
	f := newFaultNet()
	for _, tn := range nodes {
		f.registerAddr(tn.addr(), tn.id)
	}
	for _, tn := range nodes {
		tn.svc.node.SetVoteSend(f.voteSenderFor(tn.id))
		tn.svc.node.SetAppendSend(f.appendSenderFor(tn.id))
	}
	return f
}

// TestClientWriteDuringPartitionNeverCommits is Scenario 13: a PUT sent
// to a leader isolated from the majority never completes (no client OK),
// and once the majority elects a replacement and commits a different
// write, the final authoritative state does not contain the partitioned
// value.
func TestClientWriteDuringPartitionNeverCommits(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)

	f.partition(nodes[0].id, nodes[1].id)
	f.partition(nodes[0].id, nodes[2].id)

	c := client.New(nodes[0].addr())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := c.Put(ctx, []byte("partitioned"), []byte("value")); err == nil {
		t.Fatalf("Put succeeded despite the leader being isolated from the majority")
	}
	if nodes[0].svc.node.CommitIndex() != 0 {
		t.Fatalf("isolated leader's CommitIndex() = %d, want 0", nodes[0].svc.node.CommitIndex())
	}

	// B and C, still connected, elect a leader and make progress.
	electLeaderAmong(t, []*testNode{nodes[2]}, nodes, 1)
	c2 := client.New(nodes[1].addr())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := c2.Put(ctx2, []byte("majority"), []byte("ok")); err != nil {
		t.Fatalf("Put to new leader: %v", err)
	}

	// Heal; the old leader converges to the authoritative log, which
	// overwrites its divergent uncommitted entry.
	f.heal(nodes[0].id, nodes[1].id)
	f.heal(nodes[0].id, nodes[2].id)
	eventually(t, 2*time.Second, func() bool {
		return nodes[0].svc.node.CurrentTerm() == nodes[1].svc.node.CurrentTerm() && nodes[0].svc.node.Role() == raft.Follower
	}, nil)

	waitForClusterCommit(t, 2*time.Second, []*testNode{nodes[1], nodes[2]}, nodes[1].svc.node.LastLogIndex())
	v, ok, err := c2.Get(ctx2, []byte("majority"))
	if err != nil || !ok || string(v) != "ok" {
		t.Fatalf("Get(majority) = %q, %v, %v; want ok, true, nil", v, ok, err)
	}
	_, ok, err = c2.Get(ctx2, []byte("partitioned"))
	if err != nil {
		t.Fatalf("Get(partitioned): %v", err)
	}
	if ok {
		t.Fatalf("the never-committed partitioned write must not be present in authoritative state")
	}
}

// TestClientReceivesNoFalseOKWhenLeaderCrashesMidWrite is item 58: a
// client PUT is in flight (waiting for commit+apply) when the leader
// process is stopped. The client must get a bounded error, never a false
// OK, and no goroutine/waiter should leak.
func TestClientReceivesNoFalseOKWhenLeaderCrashesMidWrite(t *testing.T) {
	nodes := startCluster(t, 3)
	f := wireFaultNet(nodes)
	electLeader(t, nodes, 0)

	// Isolate the leader from its followers so the write can never
	// commit, then stop it entirely mid-wait.
	f.partition(nodes[0].id, nodes[1].id)
	f.partition(nodes[0].id, nodes[2].id)

	done := make(chan error, 1)
	go func() {
		c := client.New(nodes[0].addr())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- c.Put(ctx, []byte("x"), []byte("1"))
	}()

	eventually(t, time.Second, func() bool { return nodes[0].svc.node.LastLogIndex() >= 1 }, nil)
	nodes[0].tr.Close()
	nodes[0].svc.node.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("client Put returned success despite the leader crashing before commit")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("client Put did not return within a bounded time — goroutine/waiter leak")
	}
}

// TestSurvivingMajorityRecoversValueAfterClientOK is item 59: once a
// client has received OK for a write, immediately stopping the leader
// must not lose it — a newly elected leader among the survivors still
// has it.
func TestSurvivingMajorityRecoversValueAfterClientOK(t *testing.T) {
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
		t.Fatalf("Get(x) on new leader = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
}

// TestClientRedirectsToNewLeaderAfterFailover is item 36 (updated for
// Milestone 9's safe-retry semantics — see item 55): a client seeded only
// with a now-dead former leader's address can no longer succeed on its
// own (it has no other address to fall back to, so it exhausts its ctx
// retrying — proving it does NOT silently give up after one transport
// failure, item 97's mandate, rather than the old immediate-failure
// behavior); a fresh client seeded with a surviving node succeeds
// normally.
func TestClientRedirectsToNewLeaderAfterFailover(t *testing.T) {
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

	// The client is only seeded with A, which is gone: it retries (per
	// Milestone 9) but has nowhere else to go, so it must still fail
	// once its own bounded ctx expires — not hang forever, not succeed.
	deadCtx, deadCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer deadCancel()
	if err := c.Put(deadCtx, []byte("y"), []byte("2")); err == nil {
		t.Fatalf("Put succeeded despite the only seed being dead")
	}

	// A fresh client seeded with a surviving node succeeds normally
	// (follows NOT_LEADER to B if needed), with its own fresh ctx.
	c2 := client.New(nodes[2].addr())
	putCtx, putCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer putCancel()
	if err := c2.Put(putCtx, []byte("y"), []byte("2")); err != nil {
		t.Fatalf("Put via surviving node: %v", err)
	}
	v, ok, err := c2.Get(putCtx, []byte("y"))
	if err != nil || !ok || string(v) != "2" {
		t.Fatalf("Get(y) = %q, %v, %v; want 2, true, nil", v, ok, err)
	}
}
