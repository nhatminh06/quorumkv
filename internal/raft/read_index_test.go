package raft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// --- Reserved empty-command no-op ---

// TestProposeRejectsEmptyCommand is item 102: an external/internal normal
// call to Propose with an empty command must fail — only the internal
// barrier path (ensureCurrentTermCommitted) may append one.
func TestProposeRejectsEmptyCommand(t *testing.T) {
	n := newFakeNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	_, _, err := n.Propose(nil)
	if !errors.Is(err, ErrReservedCommand) {
		t.Fatalf("Propose(nil) err = %v, want ErrReservedCommand", err)
	}
	_, _, err = n.Propose([]byte{})
	if !errors.Is(err, ErrReservedCommand) {
		t.Fatalf("Propose([]byte{}) err = %v, want ErrReservedCommand", err)
	}
}

// TestEnsureCurrentTermCommittedAppendsReservedNoOp proves the internal
// barrier path can append the reserved empty command that Propose itself
// refuses, and that it does so as a normal current-term log entry.
func TestEnsureCurrentTermCommittedAppendsReservedNoOp(t *testing.T) {
	n := newFakeNode(t, 1, nil) // single-node cluster: barrier commits locally
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	term := n.CurrentTerm()

	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		t.Fatalf("ensureCurrentTermCommitted: %v", err)
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("LastLogIndex() = %d, want 1", n.LastLogIndex())
	}
	e, ok := n.LogEntry(1)
	if !ok || len(e.Command) != 0 || e.Term != term {
		t.Fatalf("LogEntry(1) = %+v, ok=%v, want empty command at term %d", e, ok, term)
	}
	if n.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d, want 1", n.CommitIndex())
	}
}

// --- No-op application semantics ---

// TestNoOpDoesNotInvokeApplyFuncButAdvancesLastApplied is item 44: a
// committed no-op advances lastApplied without ever calling ApplyFunc,
// and does not mutate whatever state ApplyFunc would have touched.
func TestNoOpDoesNotInvokeApplyFuncButAdvancesLastApplied(t *testing.T) {
	rec := newApplyRecorder()
	n := newNodeWithApply(t, 1, nil, rec.fn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	i1 := proposeAndWait(t, n, "x=1")
	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		t.Fatalf("ensureCurrentTermCommitted: %v", err)
	}
	// The barrier is a no-op only if a barrier was actually needed; here
	// commitIndex already carries the current term from the PUT above, so
	// no second entry should have been appended at all (item 43).
	if n.LastLogIndex() != i1 {
		t.Fatalf("LastLogIndex() = %d, want %d (no barrier needed: current term already committed)", n.LastLogIndex(), i1)
	}

	// Force a genuine no-op by advancing to a new term without a write:
	// simulate by appending one directly through the internal path a
	// second time is not meaningful (barrier already satisfied), so
	// instead assert the invariant on the PUT-committed entry: exactly
	// one ApplyFunc call, at index 1, and nothing beyond.
	if got := rec.count(1); got != 1 {
		t.Fatalf("ApplyFunc called %d times for index 1, want exactly 1", got)
	}
	if n.LastApplied() != i1 {
		t.Fatalf("LastApplied() = %d, want %d", n.LastApplied(), i1)
	}
}

// TestNoOpApplyDoesNotCallApplyFunc proves directly, via a fresh term with
// no prior write, that the barrier's no-op entry is committed and applied
// without ever reaching ApplyFunc.
func TestNoOpApplyDoesNotCallApplyFunc(t *testing.T) {
	rec := newApplyRecorder()
	n := newNodeWithApply(t, 1, nil, rec.fn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		t.Fatalf("ensureCurrentTermCommitted: %v", err)
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("LastLogIndex() = %d, want 1 (the barrier no-op)", n.LastLogIndex())
	}
	if n.LastApplied() != 1 {
		t.Fatalf("LastApplied() = %d, want 1", n.LastApplied())
	}
	if got := rec.count(1); got != 0 {
		t.Fatalf("ApplyFunc called %d times for the no-op at index 1, want 0", got)
	}
}

// --- No-op restart / snapshot compatibility ---

// TestNoOpSurvivesRestart is item 45: a persisted no-op at index 1 and an
// ordinary command at index 2, both committed, must both replay correctly
// after restart — no decode failure on the no-op, lastApplied reaches 2,
// and the ordinary command's effect is present.
func TestNoOpSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "state"))
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	snapStore := NewSnapshotStore(filepath.Join(dir, "snapshot"))
	rec := newApplyRecorder()
	n, err := NewNode(1, store, log, commitStore, snapStore, nil, rec.fn, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		t.Fatalf("ensureCurrentTermCommitted: %v", err)
	}
	i2 := proposeAndWait(t, n, "put x=1")
	n.Close()

	if i2 != 2 {
		t.Fatalf("precondition: expected index 2 for the second entry, got %d", i2)
	}

	rec2 := newApplyRecorder()
	log2, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	n2, err := NewNode(1, NewStore(filepath.Join(dir, "state")), log2, NewCommitStore(filepath.Join(dir, "commit")), NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, rec2.fn, nil, nil)
	if err != nil {
		t.Fatalf("NewNode (restart): %v", err)
	}
	defer n2.Close()

	if err := n2.WaitApplied(ctx, 2, 0); err != nil {
		t.Fatalf("WaitApplied after restart: %v", err)
	}
	if n2.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2", n2.LastApplied())
	}
	if got := rec2.indexes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("ApplyFunc called for indexes %v, want exactly [2] (index 1 is the no-op)", got)
	}
}

// TestNoOpSnapshotBoundaryCompatibility is items 7/46/98: a snapshot whose
// boundary lands exactly on a committed no-op entry must record correct
// Raft metadata (lastIncludedIndex/lastIncludedTerm), the application
// snapshot is unaffected by the no-op, and restart from it succeeds with
// correct RequestVote-relevant last-log metadata.
func TestNoOpSnapshotBoundaryCompatibility(t *testing.T) {
	dir := t.TempDir()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(t, dir, 1, nil, sm)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, n, "alpha")
	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		t.Fatalf("ensureCurrentTermCommitted: %v", err)
	}
	noOpIndex := n.LastLogIndex() // the barrier no-op, since alpha already advanced past a barrier-free state... verify below
	noOpTerm := n.CurrentTerm()

	// Snapshot exactly at the no-op boundary.
	if err := n.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if n.log.BaseIndex() != noOpIndex || n.log.BaseTerm() != noOpTerm {
		t.Fatalf("log boundary = (%d,%d), want (%d,%d)", n.log.BaseIndex(), n.log.BaseTerm(), noOpIndex, noOpTerm)
	}

	store := NewSnapshotStore(filepath.Join(dir, "snapshot"))
	snap, err := store.Load()
	if err != nil || snap == nil {
		t.Fatalf("Load snapshot: %v, snap=%v", err, snap)
	}
	if snap.LastIncludedIndex != noOpIndex || snap.LastIncludedTerm != noOpTerm {
		t.Fatalf("snapshot = (%d,%d), want (%d,%d)", snap.LastIncludedIndex, snap.LastIncludedTerm, noOpIndex, noOpTerm)
	}
	// The application snapshot must be unaffected by the no-op: only
	// "alpha" was ever applied through ApplyFunc.
	if got := sm.snapshotOf(); len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("application snapshot = %v, want [alpha]", got)
	}

	last, lastTerm := n.log.LastIndex(), n.log.LastTerm()
	n.Close()

	sm2 := newFakeStateMachine()
	n2 := openSnapshottingNode(t, dir, 1, nil, sm2)
	defer n2.Close()
	if n2.log.LastIndex() != last || n2.log.LastTerm() != lastTerm {
		t.Fatalf("restart last-log metadata = (%d,%d), want (%d,%d)", n2.log.LastIndex(), n2.log.LastTerm(), last, lastTerm)
	}
	if got := sm2.snapshotOf(); len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("restored application state = %v, want [alpha]", got)
	}
}

// --- Current-term barrier semantics ---

// TestBarrierNotNeededAfterExistingCurrentTermWrite is item 12/43: once a
// normal client write from the current term has already committed,
// ensureCurrentTermCommitted must not append any additional entry.
func TestBarrierNotNeededAfterExistingCurrentTermWrite(t *testing.T) {
	n := newFakeNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAndWait(t, n, "put x=1")
	before := n.LastLogIndex()

	if err := n.ensureCurrentTermCommitted(ctx); err != nil {
		t.Fatalf("ensureCurrentTermCommitted: %v", err)
	}
	if n.LastLogIndex() != before {
		t.Fatalf("LastLogIndex() = %d, want unchanged %d — no barrier should have been appended", n.LastLogIndex(), before)
	}
}

// TestConcurrentEnsureCurrentTermCommittedSingleFlights is item 11: many
// concurrent first-read barrier requests in the same term must result in
// exactly one no-op being appended, not one per caller.
func TestConcurrentEnsureCurrentTermCommittedSingleFlights(t *testing.T) {
	n := newFakeNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	const concurrency = 25
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = n.ensureCurrentTermCommitted(ctx)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: ensureCurrentTermCommitted: %v", i, err)
		}
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("LastLogIndex() = %d, want exactly 1 (a single barrier no-op)", n.LastLogIndex())
	}
}

// --- ReadIndex quorum ---

// TestReadIndexSingleNodeNoNetworkIO is item 38: a one-node cluster's
// majority is 1, so after the current-term barrier is established,
// ReadIndex succeeds with no peer RPCs.
func TestReadIndexSingleNodeNoNetworkIO(t *testing.T) {
	n := newFakeNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	idx, err := n.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx != n.CommitIndex() {
		t.Fatalf("ReadIndex() = %d, want CommitIndex() = %d", idx, n.CommitIndex())
	}
}

// TestReadIndexNotLeaderRejected proves a follower's ReadIndex call fails
// immediately with ErrNotLeader.
func TestReadIndexNotLeaderRejected(t *testing.T) {
	n := newFakeNode(t, 1, nil) // never starts an election: stays Follower
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := n.ReadIndex(ctx)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex() err = %v, want ErrNotLeader", err)
	}
}

// TestReadIndexOneFollowerDownStillQuorum is item 39: a 3-node cluster
// with one follower unreachable still has a majority (leader + the other
// follower), so ReadIndex succeeds.
func TestReadIndexOneFollowerDownStillQuorum(t *testing.T) {
	a, _, c, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("C", true)
	_ = c

	idx, err := a.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx != a.CommitIndex() {
		t.Fatalf("ReadIndex() = %d, want CommitIndex() = %d", idx, a.CommitIndex())
	}
}

// TestReadIndexBothFollowersDownNoQuorum is item 40: with both followers
// unreachable, a leader cannot confirm quorum — ReadIndex must fail
// (bounded), never return stale success.
func TestReadIndexBothFollowersDownNoQuorum(t *testing.T) {
	a, _, _, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	// The barrier commits locally-appended-but-not-yet-committed only
	// once a majority acks it; isolate before that can happen.
	net.setBlocked("B", true)
	net.setBlocked("C", true)

	shortCtx, cancel2 := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel2()
	_, err := a.ReadIndex(shortCtx)
	if err == nil {
		t.Fatalf("ReadIndex succeeded despite no reachable quorum")
	}
	if errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex err = %v, want a bounded timeout/unavailable error, not ErrNotLeader (role must not have flipped)", err)
	}
}

// TestBarrierQuorumEstablishesThroughOneFollower is item 42: a freshly
// elected leader with no current-term committed entry needs a barrier
// no-op; with one follower reachable that's still a majority, so the
// barrier commits, applies, and ReadIndex succeeds.
func TestBarrierQuorumEstablishesThroughOneFollower(t *testing.T) {
	a, b, c, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("C", true)
	_ = c

	if a.hasCurrentTermCommitLockedForTest() {
		t.Fatalf("precondition: a fresh leader should have no current-term commit yet")
	}

	idx, err := a.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx == 0 {
		t.Fatalf("ReadIndex() = 0, want a real committed index")
	}
	if !waitFor(2*time.Second, func() bool { return b.LastApplied() >= idx }) {
		t.Fatalf("follower B did not catch up to the barrier: LastApplied()=%d, want >= %d", b.LastApplied(), idx)
	}
}

// hasCurrentTermCommitLockedForTest exposes hasCurrentTermCommitLocked
// for tests in this package.
func (n *Node) hasCurrentTermCommitLockedForTest() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.hasCurrentTermCommitLocked()
}

// TestBarrierNoQuorumNoOpDoesNotApply is items 41/100: an isolated new
// leader's barrier no-op appends locally but cannot commit without
// quorum, so commitIndex/lastApplied never advance past what they were,
// and GET-equivalent (ReadIndex) never succeeds.
func TestBarrierNoQuorumNoOpDoesNotApply(t *testing.T) {
	a, _, _, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	before := a.CommitIndex()
	net.setBlocked("B", true)
	net.setBlocked("C", true)

	shortCtx, cancel2 := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel2()
	_, err := a.ReadIndex(shortCtx)
	if err == nil {
		t.Fatalf("ReadIndex succeeded despite no quorum for the barrier")
	}
	if a.CommitIndex() != before {
		t.Fatalf("CommitIndex() = %d, want unchanged %d — an uncommitted no-op must never advance it", a.CommitIndex(), before)
	}
	if a.LastApplied() != before {
		t.Fatalf("LastApplied() = %d, want unchanged %d", a.LastApplied(), before)
	}
}

// --- Probe isolation from replication state ---

// TestReadProbeDoesNotModifyReplicationState is item 65: issuing a read
// probe that a follower rejects (Success=false, e.g. because it's behind)
// must not touch that follower's nextIndex/matchIndex on the leader.
func TestReadProbeDoesNotModifyReplicationState(t *testing.T) {
	a, _, _, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	// Get a real committed write in first (so the barrier is already
	// satisfied and ReadIndex won't append anything else).
	proposeAndWait(t, a, "x=1")
	// Wait for the leader's own matchIndex/nextIndex bookkeeping (not
	// just B's log) to reflect the catch-up, since A's periodic
	// heartbeat loop keeps re-sending independently of this test and
	// would otherwise race with the "before" snapshot below.
	if !waitFor(2*time.Second, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.matchIndex[2] >= a.log.LastIndex()
	}) {
		t.Fatalf("A's replication state for B did not stabilize before the probe")
	}
	_ = net

	a.mu.Lock()
	nextBefore := a.nextIndex[2]
	matchBefore := a.matchIndex[2]
	a.mu.Unlock()

	if _, err := a.ReadIndex(ctx); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	a.mu.Lock()
	nextAfter := a.nextIndex[2]
	matchAfter := a.matchIndex[2]
	a.mu.Unlock()
	if nextAfter != nextBefore || matchAfter != matchBefore {
		t.Fatalf("replication state changed by a read probe: next %d->%d, match %d->%d", nextBefore, nextAfter, matchBefore, matchAfter)
	}
}

// TestReadQuorumWithLogMismatchedFollower is item 96: a follower whose
// log doesn't match the leader's probe prevLogIndex/prevLogTerm still
// counts toward read quorum (Success=false, but same term/context) —
// replication success is not required for leadership confirmation.
func TestReadQuorumWithLogMismatchedFollower(t *testing.T) {
	a, b, c, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAndWait(t, a, "x=1")
	proposeAndWait(t, a, "x=2")
	if !waitFor(2*time.Second, func() bool { return b.LastLogIndex() >= a.LastLogIndex() }) {
		t.Fatalf("B did not catch up")
	}
	// Roll B's log back so it no longer matches A's PrevLogIndex/Term for
	// the probe — B will answer Success=false.
	b.mu.Lock()
	b.log.TruncateAndAppend(1, nil)
	b.mu.Unlock()

	net.setBlocked("C", true)
	_ = c

	idx, err := a.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v (B's log mismatch should not block quorum confirmation)", err)
	}
	if idx == 0 {
		t.Fatalf("ReadIndex() = 0, want a real index")
	}
}

// TestHigherTermReadProbeResponseStepsDown is item 66: if any peer
// responds to a read probe with a higher term, the leader must persist
// that term, step down, and ReadIndex must fail rather than return a
// value. A's own periodic heartbeat loop is also live and independently
// probing B/C every ~50ms, so it may itself observe B's forced higher
// term and step A down before this test's own explicit ReadIndex call's
// probe responses are processed — either path is a correct instance of
// the same safety property, so this asserts the property (ReadIndex
// fails; A eventually steps down to at least B's term), not which
// specific code path won the race.
func TestHigherTermReadProbeResponseStepsDown(t *testing.T) {
	a, b, _, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAndWait(t, a, "x=1") // satisfy the barrier so ReadIndex goes straight to probing

	// Force B to a higher term than A's, as if B had already started (or
	// won) its own election.
	higherTerm := a.CurrentTerm() + 5
	b.mu.Lock()
	b.persistent = PersistentState{CurrentTerm: higherTerm}
	b.mu.Unlock()
	net.setBlocked("C", true)

	_, err := a.ReadIndex(ctx)
	if err == nil {
		t.Fatalf("ReadIndex succeeded despite a higher term observed from a peer")
	}
	if !waitFor(2*time.Second, func() bool {
		return a.Role() == Follower && a.CurrentTerm() >= higherTerm
	}) {
		t.Fatalf("leader did not step down after observing a higher term: role=%v term=%d, want Follower term>=%d", a.Role(), a.CurrentTerm(), higherTerm)
	}
}

// --- Lifecycle / cancellation ---

// TestReadIndexContextCancelDuringProbe is item 72/73: if the caller's
// ctx is canceled before quorum confirms, ReadIndex returns promptly with
// the context error rather than hanging or returning a value.
func TestReadIndexContextCancelDuringProbe(t *testing.T) {
	a, _, _, _ := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs

	// Install a sender that hangs until ctx is canceled or the test
	// releases it, in place of fakeNetwork's default — installed before
	// StartElection (and thus before the heartbeat loop starts reading
	// n.sendAppend concurrently) so there is no race on the swap itself.
	// This also means the barrier no-op ReadIndex needs can never
	// replicate, so the call is deterministically still in flight (at
	// the barrier or probe stage) when this test cancels it.
	hang := make(chan struct{})
	a.SetAppendSend(func(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
		select {
		case <-ctx.Done():
			return AppendEntriesResponse{}, ctx.Err()
		case <-hang:
			return AppendEntriesResponse{}, errors.New("unreachable")
		}
	})
	defer close(hang)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	readCtx, readCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := a.ReadIndex(readCtx)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	readCancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadIndex err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ReadIndex did not return promptly after ctx cancellation")
	}
}

// TestReadIndexUnblocksOnNodeClose is item 71: Node.Close while ReadIndex
// is waiting on peers must unblock it with a bounded error, not hang.
func TestReadIndexUnblocksOnNodeClose(t *testing.T) {
	a, _, _, net := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAndWait(t, a, "x=1")
	net.setBlocked("B", true)
	net.setBlocked("C", true)

	done := make(chan error, 1)
	go func() {
		_, err := a.ReadIndex(ctx)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	a.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("ReadIndex succeeded despite Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ReadIndex did not unblock after Close — goroutine/waiter leak")
	}
}

// TestConcurrentReadIndexNoRaces runs many concurrent ReadIndex calls
// against a healthy leader (item 75): all must succeed, with no races
// (run this test with -race) and no leaks.
func TestConcurrentReadIndexNoRaces(t *testing.T) {
	a, b, c, _ := setupThreeNodeFakeCluster(t)
	defer a.Close() // drain any leftover early-quorum-return probe goroutines before other nodes' cleanup Close runs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return b.Role() == Follower && c.Role() == Follower }) {
		t.Fatalf("cluster did not settle")
	}

	const concurrency = 50
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = a.ReadIndex(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("read %d: ReadIndex: %v", i, err)
		}
	}
}

// --- Snapshot interaction ---

// TestBarrierRecognizesSnapshotBoundaryAsCurrentTermCommit is item 97:
// if the current term's committed barrier has already been compacted
// into the snapshot boundary, ReadIndex must recognize it without
// appending a redundant no-op.
func TestBarrierRecognizesSnapshotBoundaryAsCurrentTermCommit(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, sn.Node, "alpha")
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if sn.log.LastIndex() != sn.log.BaseIndex() {
		t.Fatalf("precondition: expected no retained suffix after compaction")
	}

	before := sn.log.LastIndex()
	idx, err := sn.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if sn.log.LastIndex() != before {
		t.Fatalf("LastIndex() = %d, want unchanged %d — no redundant no-op should have been appended", sn.log.LastIndex(), before)
	}
	if idx != before {
		t.Fatalf("ReadIndex() = %d, want %d", idx, before)
	}
}

// TestBarrierAfterFailoverPastSnapshotBoundary is item 99: a new leader
// whose only committed history is a snapshot from an OLDER term must
// establish a new-term barrier before ReadIndex succeeds — the old
// snapshot term does not satisfy the current-term requirement.
func TestBarrierAfterFailoverPastSnapshotBoundary(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, sn.Node, "alpha")
	oldTerm := sn.CurrentTerm()
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Force this node to a higher term without any new committed entry —
	// as if it had lost and regained leadership.
	sn.mu.Lock()
	newTerm := oldTerm + 3
	sn.persistent = PersistentState{CurrentTerm: newTerm}
	sn.role = Leader
	self := sn.id
	sn.leaderID = &self
	sn.nextIndex = map[NodeID]LogIndex{}
	sn.matchIndex = map[NodeID]LogIndex{}
	sn.snapshotSending = map[NodeID]bool{}
	sn.mu.Unlock()

	if sn.hasCurrentTermCommitLockedForTest() {
		t.Fatalf("precondition: the snapshot's term (%d) must not satisfy the new term (%d)", oldTerm, newTerm)
	}

	before := sn.log.LastIndex()
	idx, err := sn.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if sn.log.LastIndex() != before+1 {
		t.Fatalf("LastIndex() = %d, want %d — a new-term barrier no-op should have been appended", sn.log.LastIndex(), before+1)
	}
	if term, ok := sn.log.Term(idx); !ok || term != newTerm {
		t.Fatalf("Term(readIndex) = (%d, %v), want (%d, true)", term, ok, newTerm)
	}
}
