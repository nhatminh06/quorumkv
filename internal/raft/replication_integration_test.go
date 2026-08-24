package raft

import (
	"bytes"
	"context"
	"testing"
	"time"

	"quorumkv/internal/transport"
)

// setupThreeNodeFakeCluster wires three nodes (1="A", 2="B", 3="C")
// together with an in-process fakeNetwork for both RequestVote and
// AppendEntries — no sockets, no timing assumptions beyond each node's
// own timers/heartbeat loop.
func setupThreeNodeFakeCluster(t *testing.T) (a, b, c *Node, net *fakeNetwork) {
	t.Helper()
	net = newFakeNetwork()
	a = newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b = newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c = newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*Node{a, b, c} {
		n.send = net.send
		n.sendAppend = net.sendAppend
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)
	return a, b, c, net
}

// waitFor polls cond until it's true or timeout elapses, sleeping briefly
// between checks — used instead of a fixed sleep so tests finish as soon
// as the condition is met rather than always waiting the full timeout.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return cond()
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestBasicLogReplicationThreeNodes(t *testing.T) {
	a, b, c, _ := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if a.Role() != Leader {
		t.Fatalf("a.Role() = %v, want Leader", a.Role())
	}

	index, _, err := a.Propose([]byte("alpha"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	ok := waitFor(2*time.Second, func() bool {
		return b.LastLogIndex() >= index && c.LastLogIndex() >= index
	})
	if !ok {
		t.Fatalf("replication did not reach majority: b=%d c=%d, want >= %d", b.LastLogIndex(), c.LastLogIndex(), index)
	}
	for _, n := range []*Node{a, b, c} {
		e, ok := n.LogEntry(index)
		if !ok || string(e.Command) != "alpha" {
			t.Fatalf("node entry at %d = %+v, ok=%v, want alpha", index, e, ok)
		}
	}
}

func TestMajorityWithOneFollowerDown(t *testing.T) {
	a, b, _, net := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("C", true) // C becomes unavailable after the election

	index, _, err := a.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	ok := waitFor(2*time.Second, func() bool { return a.CommitIndex() >= index })
	if !ok {
		t.Fatalf("commitIndex did not advance with C down: commitIndex=%d, want >= %d", a.CommitIndex(), index)
	}
	if b.LastLogIndex() < index {
		t.Fatalf("b did not replicate the entry: LastLogIndex()=%d", b.LastLogIndex())
	}
}

func TestNoMajorityCommit(t *testing.T) {
	a, _, _, net := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("B", true)
	net.setBlocked("C", true) // leader is now isolated from both followers

	index, _, err := a.Propose([]byte("lonely"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if a.LastLogIndex() != index {
		t.Fatalf("local append did not happen: LastLogIndex()=%d, want %d", a.LastLogIndex(), index)
	}

	// Give replication attempts a real chance to run (and fail) before
	// asserting commitIndex never moved — this is checking an invariant
	// holds throughout a window, not polling for a condition to become
	// true.
	time.Sleep(200 * time.Millisecond)
	if a.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0 — a local append without majority replication must never be committed", a.CommitIndex())
	}
}

// TestConflictRepairShortFollowerCatchUp proves the leader's real
// nextIndex backtracking loop (not a direct HandleAppendEntries call)
// converges: the leader starts with a pre-existing 3-entry log (as if
// elected after previously being leader), a follower joins empty, and
// repeated AppendEntries failures back nextIndex off one at a time until
// it finds the (here, empty) matching prefix and the follower catches up
// to the leader's full log.
func TestConflictRepairShortFollowerCatchUp(t *testing.T) {
	a, b, c, _ := setupThreeNodeFakeCluster(t)
	// Give A a 3-entry log from "a previous term" before any election, so
	// it legitimately wins the election on log freshness (B/C are empty).
	if err := a.log.Append([]LogEntry{
		{Term: 1, Command: []byte("alpha")},
		{Term: 1, Command: []byte("beta")},
		{Term: 1, Command: []byte("gamma")},
	}); err != nil {
		t.Fatalf("preload log: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if a.Role() != Leader {
		t.Fatalf("a.Role() = %v, want Leader", a.Role())
	}

	ok := waitFor(3*time.Second, func() bool {
		return b.LastLogIndex() == 3 && c.LastLogIndex() == 3
	})
	if !ok {
		t.Fatalf("followers did not catch up: b=%d c=%d, want 3", b.LastLogIndex(), c.LastLogIndex())
	}
	for _, n := range []*Node{b, c} {
		for i, want := range []string{"alpha", "beta", "gamma"} {
			e, ok := n.LogEntry(LogIndex(i + 1))
			if !ok || string(e.Command) != want {
				t.Fatalf("entry %d = %+v, ok=%v, want %q", i+1, e, ok, want)
			}
		}
	}
}

// TestHeartbeatStabilityKeepsLeader runs a real three-node cluster with
// production timers for ~1.2s (well over the 300ms max election timeout,
// far more than the 50ms heartbeat interval) and verifies the elected
// leader stays leader, the term never changes, and followers never start
// their own election — the first scenario in this project where a stable
// leader is demonstrated.
func TestHeartbeatStabilityKeepsLeader(t *testing.T) {
	a, b, c, _ := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if a.Role() != Leader {
		t.Fatalf("a.Role() = %v, want Leader", a.Role())
	}
	term := a.CurrentTerm()

	go b.Run(ctx)
	go c.Run(ctx)

	time.Sleep(1200 * time.Millisecond)

	if a.Role() != Leader {
		t.Fatalf("a.Role() = %v, want still Leader", a.Role())
	}
	if b.Role() != Follower || c.Role() != Follower {
		t.Fatalf("b.Role()=%v c.Role()=%v, want both Follower", b.Role(), c.Role())
	}
	if a.CurrentTerm() != term || b.CurrentTerm() != term || c.CurrentTerm() != term {
		t.Fatalf("terms = %d/%d/%d, want all %d (no surprise elections)", a.CurrentTerm(), b.CurrentTerm(), c.CurrentTerm(), term)
	}
}

// TestRequestVoteRealLogStaleCandidateDenied proves RequestVote now uses
// real log metadata (not the Milestone 3 hardcoded (0,0)): a voter with a
// longer, newer-term log denies a candidate whose log has a higher index
// but an older term — index cannot compensate for a stale term.
func TestRequestVoteRealLogStaleCandidateDenied(t *testing.T) {
	// Voter's real log ends at lastLogTerm=4, lastLogIndex=8.
	entries := make([]LogEntry, 8)
	for i := range entries {
		entries[i] = LogEntry{Term: 4, Command: []byte("e")}
	}
	voter := newTestNode(t, 1, PersistentState{}, nil)
	if err := voter.log.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Candidate claims lastLogTerm=3, lastLogIndex=100: a much higher
	// index, but an older term — must still be denied.
	resp, err := voter.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2, LastLogIndex: 100, LastLogTerm: 3})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("vote granted to a candidate with a higher index but older term than the voter's real log")
	}
}

// TestElectionAfterReplicationStaleCandidateLoses builds real replicated
// history across three nodes, then proves a node with a stale log cannot
// win an election even though it can reach a majority of voters.
func TestElectionAfterReplicationStaleCandidateLoses(t *testing.T) {
	a, b, c, _ := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	index, _, err := a.Propose([]byte("committed-data"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return b.LastLogIndex() >= index && c.LastLogIndex() >= index }) {
		t.Fatalf("replication did not complete")
	}

	// C is stale relative to A/B: manually roll it back to simulate a
	// node that missed the last entry and has an older view of the log.
	c.mu.Lock()
	c.log.TruncateAndAppend(1, nil)
	c.mu.Unlock()

	// C tries to become leader; A and B must deny it (A denies directly
	// since A itself is more up to date; B denies since B is at least as
	// up to date as C).
	if err := c.StartElection(ctx); err != nil {
		t.Fatalf("C StartElection: %v", err)
	}
	if c.Role() == Leader {
		t.Fatalf("stale-log candidate C must not win the election")
	}
}

// TestLeaderFailureUncommittedEntryNeverTreatedAsCommitted proves an
// entry the (now-gone) leader only ever appended to its own log — never
// replicated to a majority — is not committed.
func TestLeaderFailureUncommittedEntryNeverTreatedAsCommitted(t *testing.T) {
	a, _, _, net := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("B", true)
	net.setBlocked("C", true)

	index, _, err := a.Propose([]byte("orphaned"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let replication attempts fail

	if a.CommitIndex() >= index {
		t.Fatalf("CommitIndex() = %d, want < %d — never-replicated entry must not be committed", a.CommitIndex(), index)
	}
	a.Close() // "A disappears"
}

// TestLeaderFailureCommittedEntryPreserved proves that once an entry is
// committed (replicated to and acknowledged by a majority), it survives
// the original leader's failure: a newly elected leader among the
// remaining nodes still has it.
func TestLeaderFailureCommittedEntryPreserved(t *testing.T) {
	a, b, c, net := setupThreeNodeFakeCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	index, _, err := a.Propose([]byte("durable"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return a.CommitIndex() >= index }) {
		t.Fatalf("entry never committed")
	}

	a.Close() // "A stops"
	net.setBlocked("A", true)

	// B, the more caught-up-looking remaining follower, starts an
	// election among the survivors.
	if err := b.StartElection(ctx); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("b.Role() = %v, want Leader (majority of remaining nodes = B + C)", b.Role())
	}
	e, ok := b.LogEntry(index)
	if !ok || string(e.Command) != "durable" {
		t.Fatalf("new leader lost the committed entry: LogEntry(%d) = %+v, ok=%v", index, e, ok)
	}
	_ = c
}

// TestRealTCPReplication wires three nodes over real loopback TCP
// (Milestone 2 transport) and exercises: heartbeat, one-entry
// AppendEntries, and majority commit end to end.
func TestRealTCPReplication(t *testing.T) {
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

	// A heartbeat with no proposed entries should already be flowing via
	// the leader's heartbeat loop; wait for a follower's election timer
	// to have been reset at least once by observing it stay Follower
	// past its own minimum timeout without this test driving anything.
	time.Sleep(200 * time.Millisecond)
	if node2.Role() != Follower || node3.Role() != Follower {
		t.Fatalf("node2/node3 roles = %v/%v, want both Follower (heartbeats should prevent an election)", node2.Role(), node3.Role())
	}

	index, _, err := node1.Propose([]byte("payload"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !waitFor(3*time.Second, func() bool { return node1.CommitIndex() >= index }) {
		t.Fatalf("commitIndex did not advance: %d, want >= %d", node1.CommitIndex(), index)
	}
	for _, n := range []*Node{node2, node3} {
		if !waitFor(2*time.Second, func() bool { return n.LastLogIndex() >= index }) {
			t.Fatalf("follower did not replicate entry")
		}
		e, ok := n.LogEntry(index)
		if !ok || !bytes.Equal(e.Command, []byte("payload")) {
			t.Fatalf("follower entry = %+v, ok=%v, want payload", e, ok)
		}
	}
}
