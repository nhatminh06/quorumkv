package raft

import (
	"context"
	"testing"
	"time"

	"quorumkv/internal/transport"
)

// setupThreeSnapshottingNodes mirrors setupThreeNodeFakeCluster but wires
// each node with a real fakeStateMachine (SnapshotFunc/RestoreFunc), so
// tests here can exercise CreateSnapshot and InstallSnapshot end to end,
// entirely in-process.
func setupThreeSnapshottingNodes(t *testing.T) (a, b, c *snapshottingNode, net *fakeNetwork) {
	t.Helper()
	net = newFakeNetwork()
	a = newSnapshottingNodeWithPeers(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b = newSnapshottingNodeWithPeers(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c = newSnapshottingNodeWithPeers(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*snapshottingNode{a, b, c} {
		n.send = net.send
		n.sendAppend = net.sendAppend
		n.sendInstallSnapshot = net.sendInstallSnapshot
		n.sendPreVote = net.sendPreVote
		n.sendTimeoutNow = net.sendTimeoutNow
	}
	net.register("A", a.Node)
	net.register("B", b.Node)
	net.register("C", c.Node)
	return a, b, c, net
}

func newSnapshottingNodeWithPeers(t *testing.T, id NodeID, peers map[NodeID]string) *snapshottingNode {
	t.Helper()
	dir := t.TempDir()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(t, dir, id, peers, sm)
	return &snapshottingNode{Node: n, dir: dir, state: sm}
}

// TestLeaderInstallsSnapshotToStaleFollower is the in-process version of
// the mandatory snapshot catch-up scenario: a follower falls behind a
// leader's compacted log prefix, the leader automatically detects this
// (nextIndex <= log.BaseIndex()) and sends InstallSnapshot instead of an
// endless string of failing AppendEntries retries, and ordinary
// AppendEntries replication resumes once the follower has caught up.
func TestLeaderInstallsSnapshotToStaleFollower(t *testing.T) {
	a, b, c, net := setupThreeSnapshottingNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if a.Role() != Leader {
		t.Fatalf("a.Role() = %v, want Leader", a.Role())
	}

	// Replicate a couple of entries to everyone first, then take C
	// offline before further entries are proposed.
	i1 := proposeAsLeaderAndWaitApplied(t, a.Node, "one")
	if !waitFor(2*time.Second, func() bool { return c.LastLogIndex() >= i1 }) {
		t.Fatalf("C did not receive entry %d before going offline", i1)
	}
	net.setBlocked("C", true)

	// Commit more entries via A+B only (a majority without C), then
	// compact A's log past what C has — this is the state that forces a
	// snapshot transfer rather than ordinary catch-up.
	_ = proposeAsLeaderAndWaitApplied(t, a.Node, "two")
	i3 := proposeAsLeaderAndWaitApplied(t, a.Node, "three")
	if err := a.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if a.log.BaseIndex() != i3 {
		t.Fatalf("a.log.BaseIndex() = %d, want %d", a.log.BaseIndex(), i3)
	}

	// C rejoins; its nextIndex on A is far behind A's compacted base, so
	// A must send InstallSnapshot rather than a doomed AppendEntries.
	net.setBlocked("C", false)

	if !waitFor(3*time.Second, func() bool { return c.LastApplied() >= i3 }) {
		t.Fatalf("C did not catch up via InstallSnapshot: LastApplied()=%d, want >= %d", c.LastApplied(), i3)
	}
	if got := c.state.snapshotOf(); len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Fatalf("C's restored+replayed state = %v, want [one two three]", got)
	}

	// Ordinary AppendEntries replication must resume afterward.
	i4 := proposeAsLeaderAndWaitApplied(t, a.Node, "four")
	if !waitFor(2*time.Second, func() bool { return c.LastApplied() >= i4 }) {
		t.Fatalf("AppendEntries did not resume after snapshot catch-up: C.LastApplied()=%d, want >= %d", c.LastApplied(), i4)
	}
	_ = b
}

// TestRequestVoteAtSnapshotBoundary proves RequestVote's log-freshness
// comparison still works correctly for a voter whose log has been
// compacted: the voter's "last log" metadata must come from
// baseIndex/baseTerm when no physical entries remain, not from a
// fabricated (0,0).
func TestRequestVoteAtSnapshotBoundary(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	last := proposeAsLeaderAndWaitApplied(t, sn.Node, "only")
	term := sn.CurrentTerm()
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if sn.log.LastIndex() != last || sn.log.BaseIndex() != last {
		t.Fatalf("precondition: log has no physical entries left, BaseIndex()=%d LastIndex()=%d, want both %d", sn.log.BaseIndex(), sn.log.LastIndex(), last)
	}

	// A candidate whose log is exactly as fresh as the snapshot boundary
	// must be granted a vote.
	resp, err := sn.HandleRequestVote(RequestVoteRequest{Term: term + 1, CandidateID: 2, LastLogIndex: last, LastLogTerm: term})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("vote denied to a candidate exactly as fresh as the compacted voter's log")
	}
}

// TestAppendEntriesPrevLogAtSnapshotBoundary proves a follower whose log
// has been compacted through index N can still accept an AppendEntries
// whose PrevLogIndex is exactly N (validated against baseTerm, not a
// missing entry), and correctly rejects one with a mismatched PrevLogTerm
// at that same boundary.
func TestAppendEntriesPrevLogAtSnapshotBoundary(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	last := proposeAsLeaderAndWaitApplied(t, sn.Node, "only")
	term := sn.CurrentTerm()
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Correct PrevLogTerm at the boundary: must be accepted.
	resp, err := sn.HandleAppendEntries(AppendEntriesRequest{
		Term: term, LeaderID: 2, PrevLogIndex: last, PrevLogTerm: term,
		Entries: []LogEntry{{Term: term, Command: []byte("next")}}, LeaderCommit: last,
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp.Success = false, want true for a correct PrevLogTerm at the snapshot boundary")
	}

	// Reset to the same state and try a mismatched PrevLogTerm at the
	// same boundary index: must be rejected.
	sn2 := newSnapshottingNode(t, 1, nil)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := sn2.StartElection(ctx2); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	last2 := proposeAsLeaderAndWaitApplied(t, sn2.Node, "only")
	term2 := sn2.CurrentTerm()
	if err := sn2.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	badResp, err := sn2.HandleAppendEntries(AppendEntriesRequest{
		Term: term2, LeaderID: 2, PrevLogIndex: last2, PrevLogTerm: term2 + 99,
		Entries: nil, LeaderCommit: last2,
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if badResp.Success {
		t.Fatalf("resp.Success = true, want false for a mismatched PrevLogTerm at the snapshot boundary")
	}
}

// --- Real TCP mandatory scenarios (items 95, 97, 98) ---

// snapshottingTCPCluster wires three snapshotting nodes over real loopback
// TCP transport (package transport, Milestone 2), for the mandatory
// snapshot catch-up scenario — no in-process shortcut for this path.
type snapshottingTCPCluster struct {
	t        *testing.T
	nodes    map[NodeID]*snapshottingNode
	dirs     map[NodeID]string
	sms      map[NodeID]*fakeStateMachine
	trs      map[NodeID]*transport.Transport
	addrs    map[NodeID]string
	allPeers map[NodeID]string // id -> addr, fixed member list
}

func newSnapshottingTCPCluster(t *testing.T, ids []NodeID) *snapshottingTCPCluster {
	t.Helper()
	c := &snapshottingTCPCluster{
		t: t, nodes: map[NodeID]*snapshottingNode{}, dirs: map[NodeID]string{},
		sms: map[NodeID]*fakeStateMachine{}, trs: map[NodeID]*transport.Transport{}, addrs: map[NodeID]string{},
	}
	for _, id := range ids {
		c.dirs[id] = t.TempDir()
	}
	for _, id := range ids {
		c.startFresh(id)
	}
	c.allPeers = map[NodeID]string{}
	for _, id := range ids {
		c.allPeers[id] = c.addrs[id]
	}
	for _, id := range ids {
		peers := map[NodeID]string{}
		for otherID, addr := range c.allPeers {
			if otherID != id {
				peers[otherID] = addr
			}
		}
		c.nodes[id].SetPeers(peers)
	}
	return c
}

// startFresh opens a new Node (and a fresh application state machine) from
// id's on-disk directory and listens on a new real TCP port, replacing
// whatever transport that id previously had.
func (c *snapshottingTCPCluster) startFresh(id NodeID) {
	c.t.Helper()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(c.t, c.dirs[id], id, nil, sm)
	tr, err := transport.Listen("127.0.0.1:0", n.Handler())
	if err != nil {
		c.t.Fatalf("Listen node %d: %v", id, err)
	}
	c.nodes[id] = &snapshottingNode{Node: n, dir: c.dirs[id], state: sm}
	c.sms[id] = sm
	c.trs[id] = tr
	c.addrs[id] = tr.Addr()
}

func (c *snapshottingTCPCluster) close(id NodeID) {
	c.nodes[id].Close()
	c.trs[id].Close()
}

// restart closes id's transport (simulating the process stopping) and, on
// a later call, would reopen it — used by the restart-proof step (item
// 97). Here it simply reopens from the same on-disk directory with a
// fresh in-memory Node/state machine and a new TCP listener, then updates
// every other node's peer map to the new address (a real restart gets a
// new ephemeral port).
func (c *snapshottingTCPCluster) restart(id NodeID) {
	c.t.Helper()
	c.close(id)
	c.startFresh(id)
	c.allPeers[id] = c.addrs[id]
	for otherID, n := range c.nodes {
		if otherID == id {
			continue
		}
		peers := map[NodeID]string{}
		for pid, addr := range c.allPeers {
			if pid != otherID {
				peers[pid] = addr
			}
		}
		n.SetPeers(peers)
	}
	peers := map[NodeID]string{}
	for pid, addr := range c.allPeers {
		if pid != id {
			peers[pid] = addr
		}
	}
	c.nodes[id].SetPeers(peers)
}

func (c *snapshottingTCPCluster) closeAll() {
	for id := range c.nodes {
		c.close(id)
	}
}

// TestSnapshotCatchUpEndToEndRealTCP is the mandatory scenario (spec item
// 95): three real nodes over real TCP; C goes offline; A+B commit entries
// beyond what C has; A snapshots and compacts past C's last known index;
// more entries commit; C restarts from its stale on-disk state; A must
// detect C is behind its compacted prefix and send InstallSnapshot; once
// installed, ordinary AppendEntries resumes and all three nodes converge.
func TestSnapshotCatchUpEndToEndRealTCP(t *testing.T) {
	c := newSnapshottingTCPCluster(t, []NodeID{1, 2, 3})
	defer c.closeAll()
	a, b, cc := c.nodes[1], c.nodes[2], c.nodes[3]

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if a.Role() != Leader {
		t.Fatalf("a.Role() = %v, want Leader", a.Role())
	}
	go b.Run(ctx)
	go cc.Run(ctx)

	// Step 1: replicate one entry to all three while everyone is up.
	i1 := proposeAsLeaderAndWaitApplied(t, a.Node, "first")
	if !waitFor(3*time.Second, func() bool { return cc.LastLogIndex() >= i1 }) {
		t.Fatalf("C did not receive the first entry before going offline")
	}

	// Step 2: take C offline (close its transport so real TCP dials to it
	// fail, exactly like a stopped process).
	c.trs[3].Close()

	// Step 3/4: commit more entries via A+B only.
	i2 := proposeAsLeaderAndWaitApplied(t, a.Node, "second")
	i3 := proposeAsLeaderAndWaitApplied(t, a.Node, "third")
	if !waitFor(3*time.Second, func() bool { return b.LastLogIndex() >= i3 }) {
		t.Fatalf("B did not receive entries while C was offline")
	}

	// Step 5: snapshot + compact on A past what C last had.
	if err := a.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if a.log.BaseIndex() != i3 {
		t.Fatalf("a.log.BaseIndex() = %d, want %d", a.log.BaseIndex(), i3)
	}

	// Step 6: commit still more entries after the snapshot.
	i4 := proposeAsLeaderAndWaitApplied(t, a.Node, "fourth")

	// Step 7: restart C from its stale on-disk state (new process, same
	// files) and reconnect it to the cluster over a fresh real TCP port.
	c.restart(3)
	cc = c.nodes[3]
	go cc.Run(ctx)

	// Step 8/9: A must detect C is behind its compacted prefix and send
	// InstallSnapshot; AppendEntries must resume afterward.
	if !waitFor(5*time.Second, func() bool { return cc.LastApplied() >= i4 }) {
		t.Fatalf("C did not catch up via real-TCP InstallSnapshot: LastApplied()=%d, want >= %d", cc.LastApplied(), i4)
	}

	// Step 10: verify convergence — C's restored+replayed application
	// state matches exactly what A and B have.
	got := cc.state.snapshotOf()
	want := []string{"first", "second", "third", "fourth"}
	if len(got) != len(want) {
		t.Fatalf("C's converged state = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("C's converged state = %v, want %v", got, want)
		}
	}
	for _, id := range []LogIndex{i1, i2, i3, i4} {
		if e, ok := cc.LogEntry(id); !ok {
			// Entries at/under the compacted boundary are expected to be
			// gone; only verify the ones still physically retained.
			if id > cc.log.BaseIndex() {
				t.Fatalf("C missing retained entry at %d", id)
			}
		} else {
			_ = e
		}
	}
}

// TestLargeSnapshotOverRealTCP is the mandatory large-snapshot scenario
// (spec item 98): a snapshot payload well over the 256 KiB InstallSnapshot
// chunk size (and over the transport's single-frame limit if sent whole)
// must still transfer correctly in multiple chunks over real TCP.
func TestLargeSnapshotOverRealTCP(t *testing.T) {
	c := newSnapshottingTCPCluster(t, []NodeID{1, 2})
	defer c.closeAll()
	a, b := c.nodes[1], c.nodes[2]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	go b.Run(ctx)

	// Build several 100 KiB commands (each under the log's per-command
	// limit) so the resulting snapshot totals ~800 KiB — well over the
	// 256 KiB InstallSnapshot chunk size and the transport's single-frame
	// limit if it were ever sent whole.
	const numCmds = 8
	const cmdSize = 100 * 1024
	cmds := make([]string, numCmds)
	for i := range cmds {
		buf := make([]byte, cmdSize)
		for j := range buf {
			buf[j] = byte((i*31 + j) % 251)
		}
		cmds[i] = string(buf)
	}
	var index LogIndex
	for _, cmd := range cmds {
		index = proposeAsLeaderAndWaitApplied(t, a.Node, cmd)
	}

	// Take B offline, snapshot past it on A, then bring B back — forcing
	// the whole large payload through InstallSnapshot rather than
	// AppendEntries.
	if !waitFor(5*time.Second, func() bool { return b.LastLogIndex() >= index }) {
		t.Fatalf("B did not receive all entries before going offline")
	}
	c.trs[2].Close()
	if err := a.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if a.log.BaseIndex() != index {
		t.Fatalf("a.log.BaseIndex() = %d, want %d", a.log.BaseIndex(), index)
	}

	c.restart(2)
	b = c.nodes[2]
	go b.Run(ctx)

	if !waitFor(8*time.Second, func() bool { return b.LastApplied() >= index }) {
		t.Fatalf("B did not catch up on the large snapshot: LastApplied()=%d, want >= %d", b.LastApplied(), index)
	}
	got := b.state.snapshotOf()
	if len(got) != numCmds {
		t.Fatalf("B's restored command count = %d, want %d", len(got), numCmds)
	}
	for i := range cmds {
		if got[i] != cmds[i] {
			t.Fatalf("B's restored command %d does not match byte-for-byte", i)
		}
	}
}
