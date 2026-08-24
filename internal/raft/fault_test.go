package raft

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// link identifies one directed communication path between two nodes.
type link struct{ from, to NodeID }

// directedNetwork dispatches RequestVote/AppendEntries directly to a
// registered peer Node's real handler, in process — no sockets — while
// honoring per-directional link blocking: blocking (A, B) drops only
// messages FROM A TO B, independent of the reverse direction. This is
// deliberately not the same as fakeNetwork in election_test.go (which
// blocks an address symmetrically for every sender) — Milestone 6 needs
// asymmetric partitions.
type directedNetwork struct {
	mu      sync.Mutex
	nodes   map[NodeID]*Node
	addrOf  map[NodeID]string
	idOf    map[string]NodeID
	blocked map[link]bool
}

func newDirectedNetwork() *directedNetwork {
	return &directedNetwork{
		nodes:   make(map[NodeID]*Node),
		addrOf:  make(map[NodeID]string),
		idOf:    make(map[string]NodeID),
		blocked: make(map[link]bool),
	}
}

func (d *directedNetwork) register(id NodeID, addr string, n *Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes[id] = n
	d.addrOf[id] = addr
	d.idOf[addr] = id
}

func (d *directedNetwork) unregister(id NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.nodes, id)
}

// block drops messages sent FROM from TO to; the reverse direction is
// unaffected unless blocked separately.
func (d *directedNetwork) block(from, to NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blocked[link{from, to}] = true
}

func (d *directedNetwork) allow(from, to NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.blocked, link{from, to})
}

// partition blocks both directions between a and b.
func (d *directedNetwork) partition(a, b NodeID) {
	d.block(a, b)
	d.block(b, a)
}

// heal restores both directions between a and b.
func (d *directedNetwork) heal(a, b NodeID) {
	d.allow(a, b)
	d.allow(b, a)
}

func (d *directedNetwork) isBlocked(from, to NodeID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.blocked[link{from, to}]
}

// senderFor returns a RequestVote sender bound to "from": it resolves
// the destination address back to a NodeID, checks directional blocking
// and liveness (the target node must still be registered — i.e. not
// stopped), and only then dispatches straight to the peer's real
// HandleRequestVote.
func (d *directedNetwork) senderFor(from NodeID) sender {
	return func(_ context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		d.mu.Lock()
		to, known := d.idOf[addr]
		peer, alive := d.nodes[to]
		blocked := d.blocked[link{from, to}]
		d.mu.Unlock()
		if !known || !alive {
			return RequestVoteResponse{}, fmt.Errorf("directedNetwork: %s unreachable", addr)
		}
		if blocked {
			return RequestVoteResponse{}, fmt.Errorf("directedNetwork: %d->%d blocked", from, to)
		}
		return peer.HandleRequestVote(req)
	}
}

func (d *directedNetwork) appendSenderFor(from NodeID) appendSender {
	return func(_ context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
		d.mu.Lock()
		to, known := d.idOf[addr]
		peer, alive := d.nodes[to]
		blocked := d.blocked[link{from, to}]
		d.mu.Unlock()
		if !known || !alive {
			return AppendEntriesResponse{}, fmt.Errorf("directedNetwork: %s unreachable", addr)
		}
		if blocked {
			return AppendEntriesResponse{}, fmt.Errorf("directedNetwork: %d->%d blocked", from, to)
		}
		return peer.HandleAppendEntries(req)
	}
}

// faultCluster is a small deterministic test harness: n nodes, each with
// its own persistent temp directory and a fixed synthetic address, wired
// together through a directedNetwork. Node lifecycle (stop/restart)
// genuinely opens/closes Store/Log/CommitStore/Node — never reuses old
// in-memory state — matching what "restart" must mean for this milestone.
type faultCluster struct {
	t     *testing.T
	net   *directedNetwork
	addrs map[NodeID]string
	dirs  map[NodeID]string
	nodes map[NodeID]*Node
}

// newFaultCluster creates n nodes (IDs 1..n) and starts every one of
// them fresh. applyFn, if non-nil, is used for every node's ApplyFunc.
func newFaultCluster(t *testing.T, n int, applyFn ApplyFunc) *faultCluster {
	t.Helper()
	c := &faultCluster{
		t:     t,
		net:   newDirectedNetwork(),
		addrs: make(map[NodeID]string, n),
		dirs:  make(map[NodeID]string, n),
		nodes: make(map[NodeID]*Node, n),
	}
	for i := 1; i <= n; i++ {
		id := NodeID(i)
		c.addrs[id] = fmt.Sprintf("node-%d", id)
		c.dirs[id] = t.TempDir()
	}
	for i := 1; i <= n; i++ {
		c.start(NodeID(i), applyFn)
	}
	return c
}

func (c *faultCluster) peersFor(self NodeID) map[NodeID]string {
	m := make(map[NodeID]string, len(c.addrs)-1)
	for id, addr := range c.addrs {
		if id != self {
			m[id] = addr
		}
	}
	return m
}

// start (re)creates this node's runtime state from its persistent
// directory and registers it with the fault network. Used both for
// initial cluster startup and for restart.
func (c *faultCluster) start(id NodeID, applyFn ApplyFunc) *Node {
	c.t.Helper()
	dir := c.dirs[id]
	store := NewStore(filepath.Join(dir, "state"))
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		c.t.Fatalf("OpenLog(%d): %v", id, err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	n, err := NewNode(id, store, log, commitStore, c.peersFor(id), applyFn)
	if err != nil {
		c.t.Fatalf("NewNode(%d): %v", id, err)
	}
	n.send = c.net.senderFor(id)
	n.sendAppend = c.net.appendSenderFor(id)
	c.net.register(id, c.addrs[id], n)
	c.nodes[id] = n
	c.t.Cleanup(n.Close)
	return n
}

// stop closes the node's background work (heartbeats, apply loop,
// pending waiters) and removes it from the network so no peer can reach
// it — simulating a crashed/down process. Its persistent files are left
// exactly as they were.
func (c *faultCluster) stop(id NodeID) {
	c.t.Helper()
	if n, ok := c.nodes[id]; ok {
		n.Close()
		delete(c.nodes, id)
	}
	c.net.unregister(id)
}

// restart stops (if still running) and genuinely reconstructs the node
// from its persistent directory: a new Store, Log, CommitStore, and
// Node — proving recovery reads from disk rather than surviving in
// memory.
func (c *faultCluster) restart(id NodeID, applyFn ApplyFunc) *Node {
	c.t.Helper()
	c.stop(id)
	return c.start(id, applyFn)
}

func (c *faultCluster) node(id NodeID) *Node { return c.nodes[id] }

// eventually polls cond until it returns true or timeout elapses. On
// timeout it fails the test with describe()'s output for diagnostics.
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
		time.Sleep(2 * time.Millisecond)
	}
}

// statusString renders a compact multi-node diagnostic snapshot for test
// failure messages.
func statusString(nodes map[NodeID]*Node) string {
	var b bytes.Buffer
	for id, n := range nodes {
		fmt.Fprintf(&b, "[node %d role=%v term=%d lastLog=%d commit=%d applied=%d] ",
			id, n.Role(), n.CurrentTerm(), n.LastLogIndex(), n.CommitIndex(), n.LastApplied())
	}
	return b.String()
}

// logsAgreeUpTo asserts that every node in nodes has an identical entry
// (term and command bytes) at every index 1..upTo. It fails the test on
// the first disagreement, isolating exactly index/term/command rather
// than only comparing final KV state or last index.
func logsAgreeUpTo(t *testing.T, nodes map[NodeID]*Node, upTo LogIndex) {
	t.Helper()
	if upTo == 0 || len(nodes) < 2 {
		return
	}
	var refID NodeID
	var ref *Node
	for id, n := range nodes {
		refID, ref = id, n
		break
	}
	for index := LogIndex(1); index <= upTo; index++ {
		refEntry, ok := ref.LogEntry(index)
		if !ok {
			t.Fatalf("reference node %d missing entry at index %d", refID, index)
		}
		for id, n := range nodes {
			if id == refID {
				continue
			}
			e, ok := n.LogEntry(index)
			if !ok {
				t.Fatalf("node %d missing entry at index %d (node %d has term=%d command=%q)",
					id, index, refID, refEntry.Term, refEntry.Command)
			}
			if e.Term != refEntry.Term || !bytes.Equal(e.Command, refEntry.Command) {
				t.Fatalf("log disagreement at index %d: node %d = {term=%d cmd=%q}, node %d = {term=%d cmd=%q}",
					index, refID, refEntry.Term, refEntry.Command, id, e.Term, e.Command)
			}
		}
	}
}
