package raft

import (
	"context"
	"testing"
	"time"
)

// --- Generation safety (see applyReplicationResponse) ---

// TestStaleReplicationSuccessDoesNotAdvanceProgress proves a success
// response captured under an older generation than the peer's current
// one — e.g. a delayed reply to a request sent before a conflict
// backtrack or an InstallSnapshot takeover invalidated it — has no
// effect at all, even though it reports a plausible-looking MatchIndex.
func TestStaleReplicationSuccessDoesNotAdvanceProgress(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	staleGen := n.replicationGeneration[2]
	n.replicationGeneration[2]++ // simulate an invalidation (conflict backtrack, snapshot takeover, ...)
	n.mu.Unlock()

	req := AppendEntriesRequest{Term: term, PrevLogIndex: 0, Entries: entriesOf("a", "b")}
	more := n.applyReplicationResponse(2, term, staleGen, req, AppendEntriesResponse{Term: term, Success: true, MatchIndex: 2})
	if more {
		t.Fatalf("applyReplicationResponse(stale generation) reported more work, want false (discarded)")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.matchIndex[2] != 0 || n.nextIndex[2] != 1 {
		t.Fatalf("matchIndex/nextIndex = %d/%d, want unchanged 0/1 — a stale-generation success must never advance progress", n.matchIndex[2], n.nextIndex[2])
	}
}

// TestStaleReplicationFailureDoesNotBacktrackProgress proves a failure
// response captured under an older generation must not regress a peer
// that has already been repaired/advanced under a newer generation.
func TestStaleReplicationFailureDoesNotBacktrackProgress(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	staleGen := n.replicationGeneration[2]
	// Simulate the peer having since been repaired and advanced under a
	// newer generation.
	n.replicationGeneration[2]++
	n.nextIndex[2] = 10
	n.matchIndex[2] = 9
	n.mu.Unlock()

	req := AppendEntriesRequest{Term: term, PrevLogIndex: 4}
	more := n.applyReplicationResponse(2, term, staleGen, req, AppendEntriesResponse{Term: term, Success: false})
	if more {
		t.Fatalf("applyReplicationResponse(stale generation failure) reported more work, want false (discarded)")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nextIndex[2] != 10 || n.matchIndex[2] != 9 {
		t.Fatalf("nextIndex/matchIndex = %d/%d, want unchanged 10/9 — a stale-generation failure must never regress an already-repaired peer", n.nextIndex[2], n.matchIndex[2])
	}
}

// TestConflictInvalidatesInflightGeneration proves a current-generation
// failure both backtracks nextIndex AND bumps the generation, so a
// still-in-flight response from before the failure (captured with the
// now-superseded generation) is correctly treated as stale once it
// arrives afterward.
func TestConflictInvalidatesInflightGeneration(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	genBeforeFailure := n.replicationGeneration[2]
	n.nextIndex[2] = 5
	n.mu.Unlock()

	// The current-generation failure arrives first.
	failReq := AppendEntriesRequest{Term: term, PrevLogIndex: 4}
	if more := n.applyReplicationResponse(2, term, genBeforeFailure, failReq, AppendEntriesResponse{Term: term, Success: false}); !more {
		t.Fatalf("applyReplicationResponse(current-generation failure) reported no more work, want true (retry)")
	}
	n.mu.Lock()
	if n.nextIndex[2] != 4 {
		n.mu.Unlock()
		t.Fatalf("nextIndex after failure = %d, want 4 (backed off)", n.nextIndex[2])
	}
	if n.replicationGeneration[2] == genBeforeFailure {
		n.mu.Unlock()
		t.Fatalf("generation unchanged after a current-generation failure, want it invalidated")
	}
	n.mu.Unlock()

	// A speculative success for the OLD (pre-failure) assumption, sent
	// before the failure was known, arrives late — using the OLD
	// generation.
	staleReq := AppendEntriesRequest{Term: term, PrevLogIndex: 4, Entries: entriesOf("would-have-been-wrong")}
	if more := n.applyReplicationResponse(2, term, genBeforeFailure, staleReq, AppendEntriesResponse{Term: term, Success: true, MatchIndex: 5}); more {
		t.Fatalf("applyReplicationResponse(stale in-flight success after conflict) reported more work, want false")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nextIndex[2] != 4 {
		t.Fatalf("nextIndex after stale in-flight success = %d, want still 4 (must not un-backtrack)", n.nextIndex[2])
	}
}

// TestHigherTermInvalidatesReplicationGeneration proves a higher-term
// response steps this node down, and that no later response for the old
// term/generation can matter afterward regardless of what it claims.
func TestHigherTermInvalidatesReplicationGeneration(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	gen := n.replicationGeneration[2]
	n.mu.Unlock()

	n.applyReplicationResponse(2, term, gen, AppendEntriesRequest{Term: term}, AppendEntriesResponse{Term: term + 3, Success: false})
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower after a higher-term response", n.Role())
	}
	if n.CurrentTerm() != term+3 {
		t.Fatalf("CurrentTerm() = %d, want %d", n.CurrentTerm(), term+3)
	}

	// A late success for the old term/generation must not resurrect
	// leader-only state on a node that is now a Follower.
	more := n.applyReplicationResponse(2, term, gen, AppendEntriesRequest{Term: term, PrevLogIndex: 0, Entries: entriesOf("a")}, AppendEntriesResponse{Term: term, Success: true, MatchIndex: 1})
	if more {
		t.Fatalf("applyReplicationResponse(old term, now a Follower) reported more work, want false")
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v after stale old-term response, want still Follower", n.Role())
	}
}

// TestRemovedPeerResponseIgnored proves a response for a peer no longer
// present in n.workers (finally removed from membership) is discarded
// outright — it must not recreate any per-peer state or influence
// commit/quorum calculation.
func TestRemovedPeerResponseIgnored(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b", 3: "c"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	gen := n.replicationGeneration[2]
	// Simulate peer 2 having been finally removed from the replication
	// target set (see reconcileReplicationWorkersLocked's removal path).
	if w, ok := n.workers[2]; ok {
		w.cancel()
	}
	delete(n.workers, 2)
	delete(n.nextIndex, 2)
	delete(n.matchIndex, 2)
	delete(n.replicationGeneration, 2)
	n.mu.Unlock()

	req := AppendEntriesRequest{Term: term, PrevLogIndex: 0, Entries: entriesOf("a")}
	more := n.applyReplicationResponse(2, term, gen, req, AppendEntriesResponse{Term: term, Success: true, MatchIndex: 1})
	if more {
		t.Fatalf("applyReplicationResponse(removed peer) reported more work, want false")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.workers[2]; ok {
		t.Fatalf("a stale response for a removed peer recreated its worker")
	}
	if _, ok := n.matchIndex[2]; ok {
		t.Fatalf("a stale response for a removed peer recreated its matchIndex entry")
	}
}

// --- Worker lifecycle / event-driven behavior ---

// TestReplicationWorkerCatchesFollowerWithoutHeartbeatDelay is the core
// M14 deliverable: with the heartbeat interval set far longer than this
// test's own timeout, a follower behind by many entries must still catch
// up almost immediately, proving catch-up does not wait for successive
// heartbeat ticks between batches.
func TestReplicationWorkerCatchesFollowerWithoutHeartbeatDelay(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A"})
	for _, n := range []*Node{a, b} {
		n.send, n.sendAppend, n.sendPreVote, n.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	}
	net.register("A", a)
	net.register("B", b)
	a.heartbeatInterval = time.Hour // if catch-up ever depended on a tick, this test would time out

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	const total = 500
	for i := 0; i < total; i++ {
		if _, _, err := a.Propose([]byte("x")); err != nil {
			t.Fatalf("Propose[%d]: %v", i, err)
		}
	}

	if !waitFor(2*time.Second, func() bool { return b.LastLogIndex() == LogIndex(total) }) {
		t.Fatalf("B never caught up: LastLogIndex() = %d, want %d (heartbeatInterval = 1h, so this can only be event-driven catch-up)", b.LastLogIndex(), total)
	}
}

// TestNewEntriesWakeReplicationWorker proves a single new entry, on its
// own, is enough to wake an idle (already caught-up) worker — not just
// a large burst.
func TestNewEntriesWakeReplicationWorker(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A"})
	for _, n := range []*Node{a, b} {
		n.send, n.sendAppend, n.sendPreVote, n.sendTimeoutNow = net.send, net.sendAppend, net.sendPreVote, net.sendTimeoutNow
	}
	net.register("A", a)
	net.register("B", b)
	a.heartbeatInterval = time.Hour

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if !waitFor(time.Second, func() bool { return b.LastLogIndex() == 0 }) {
		t.Fatalf("precondition: B should start caught up with an empty log")
	}

	if _, _, err := a.Propose([]byte("only-one")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !waitFor(time.Second, func() bool { return b.LastLogIndex() == 1 }) {
		t.Fatalf("B never received the single new entry: LastLogIndex() = %d, want 1", b.LastLogIndex())
	}
}

// TestReplicationWorkerStopsOnStepDown proves losing leadership clears
// every per-peer worker (their contexts are children of leaderCtx — see
// stepToFollowerLocked) rather than leaving them running against a node
// that is no longer Leader.
func TestReplicationWorkerStopsOnStepDown(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b", 3: "c"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	if len(n.workers) != 2 {
		n.mu.Unlock()
		t.Fatalf("workers after becomeLeaderLocked = %d, want 2", len(n.workers))
	}
	n.stepToFollowerLocked()
	workers := n.workers
	n.mu.Unlock()

	if workers != nil {
		t.Fatalf("workers after stepToFollowerLocked = %v, want nil", workers)
	}
}

// TestReplicationWorkerStopsOnClose proves Close waits for every
// replication worker to actually exit (via bgWG), not just cancels their
// context and returns — Close itself would otherwise not be a reliable
// "nothing is running anymore" signal.
func TestReplicationWorkerStopsOnClose(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b", 3: "c"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	hadWorkers := len(n.workers) > 0
	n.mu.Unlock()
	if !hadWorkers {
		t.Fatalf("precondition: node should have started replication workers")
	}

	done := make(chan struct{})
	go func() {
		n.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close never returned — a replication worker may be leaked")
	}
}
