package raft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newApplyRecorder returns an ApplyFunc that records the exact sequence
// of (index, command) calls it receives, and the recorder to inspect it
// with — used to prove ordering, exactly-once application, and that
// uncommitted entries are never applied.
type applyRecorder struct {
	mu    sync.Mutex
	calls []struct {
		index LogIndex
		cmd   string
	}
	fail map[LogIndex]bool
}

func newApplyRecorder() *applyRecorder {
	return &applyRecorder{fail: map[LogIndex]bool{}}
}

func (r *applyRecorder) fn(index LogIndex, command []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail[index] {
		return errors.New("recorder: injected apply failure")
	}
	r.calls = append(r.calls, struct {
		index LogIndex
		cmd   string
	}{index, string(command)})
	return nil
}

func (r *applyRecorder) indexes() []LogIndex {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LogIndex, len(r.calls))
	for i, c := range r.calls {
		out[i] = c.index
	}
	return out
}

func (r *applyRecorder) count(index LogIndex) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.calls {
		if c.index == index {
			n++
		}
	}
	return n
}

func newNodeWithApply(t *testing.T, id NodeID, peers map[NodeID]string, fn ApplyFunc) *Node {
	t.Helper()
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "state"))
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	n, err := NewNode(id, store, log, commitStore, NewSnapshotStore(filepath.Join(dir, "snapshot")), peers, fn, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(n.Close)
	return n
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func TestApplyOrderStrictlyAscending(t *testing.T) {
	rec := newApplyRecorder()
	n := newNodeWithApply(t, 1, nil, rec.fn)
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, err := n.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	waitForCondition(t, time.Second, func() bool { return n.LastApplied() >= 3 })

	got := rec.indexes()
	want := []LogIndex{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("indexes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("indexes = %v, want %v", got, want)
		}
	}
}

func TestApplyOnlyAppliesCommittedEntries(t *testing.T) {
	rec := newApplyRecorder()
	n := newNodeWithApply(t, 1, map[NodeID]string{2: "unreachable-peer"}, rec.fn)
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		return RequestVoteResponse{}, errors.New("unreachable")
	}
	n.sendAppend = func(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
		return AppendEntriesResponse{}, errors.New("unreachable")
	}
	// startRealElection directly: this test is about apply-only-committed
	// behavior as a Candidate, not PreVote.
	if err := n.startRealElection(context.Background()); err != nil {
		t.Fatalf("startRealElection: %v", err)
	}
	if n.Role() != Candidate {
		t.Fatalf("Role() = %v, want Candidate (no majority without the peer)", n.Role())
	}

	// Directly append 3 entries to the local log without going through
	// Propose's role check, to construct "log has 3, nothing committed"
	// deterministically regardless of role.
	n.mu.Lock()
	n.log.Append([]LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}, {Term: 1, Command: []byte("c")}})
	n.mu.Unlock()

	time.Sleep(50 * time.Millisecond) // give any (incorrect) apply a chance to happen
	if n.LastApplied() != 0 {
		t.Fatalf("LastApplied() = %d, want 0 — nothing is committed", n.LastApplied())
	}
	if len(rec.indexes()) != 0 {
		t.Fatalf("apply was called for uncommitted entries: %v", rec.indexes())
	}
}

func TestApplyExactlyOncePerRuntime(t *testing.T) {
	rec := newApplyRecorder()
	n := newNodeWithApply(t, 1, nil, rec.fn)
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	index, _, err := n.Propose([]byte("once"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	waitForCondition(t, time.Second, func() bool { return n.LastApplied() >= index })

	// Simulate ten more heartbeats/commit triggers carrying the same
	// commitIndex — must not reapply.
	for i := 0; i < 10; i++ {
		n.mu.Lock()
		n.kickApplyLocked()
		n.mu.Unlock()
	}
	time.Sleep(20 * time.Millisecond)

	if got := rec.count(index); got != 1 {
		t.Fatalf("apply called %d times for index %d, want exactly 1", got, index)
	}
}

func TestApplyFailureBlocksFurtherApplication(t *testing.T) {
	rec := newApplyRecorder()
	rec.fail[2] = true
	n := newNodeWithApply(t, 1, nil, rec.fn)
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, err := n.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}

	waitForCondition(t, time.Second, func() bool { return n.ApplyError() != nil })
	if n.LastApplied() != 1 {
		t.Fatalf("LastApplied() = %d, want 1 (halted before the failing index 2)", n.LastApplied())
	}
	time.Sleep(20 * time.Millisecond) // give a wrongly-continuing loop a chance to apply index 3
	if got := rec.count(3); got != 0 {
		t.Fatalf("index 3 was applied %d times despite index 2 failing first", got)
	}
}

func TestWaitAppliedReturnsOnceApplied(t *testing.T) {
	n := newNodeWithApply(t, 1, nil, nil)
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	index, term, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, index, term); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
}

func TestWaitAppliedFailsOnApplyError(t *testing.T) {
	rec := newApplyRecorder()
	rec.fail[1] = true
	n := newNodeWithApply(t, 1, nil, rec.fn)
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	index, term, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, index, term); err == nil {
		t.Fatalf("WaitApplied succeeded despite apply failure, want error")
	}
}

func TestWaitAppliedReturnsOnContextTimeout(t *testing.T) {
	// Two-node cluster where the peer never responds: the entry never
	// commits, so WaitApplied must return via ctx, not hang forever.
	n := newNodeWithApply(t, 1, map[NodeID]string{2: "unreachable"}, nil)
	n.sendAppend = func(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
		<-ctx.Done()
		return AppendEntriesResponse{}, ctx.Err()
	}
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		// The peer grants the vote so n can become Leader, but never
		// acknowledges AppendEntries (see sendAppend above), so a
		// proposed entry can never reach majority replication.
		return RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
	}
	n.sendPreVote = func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error) {
		return PreVoteResponse{Term: req.ProspectiveTerm - 1, VoteGranted: true}, nil
	}
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader", n.Role())
	}
	index, term, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = n.WaitApplied(ctx, index, term)
	if err == nil {
		t.Fatalf("WaitApplied succeeded despite no majority, want timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("WaitApplied took %v, want well under 1s", elapsed)
	}
}

func TestWaitAppliedReturnsOnNodeClose(t *testing.T) {
	n := newNodeWithApply(t, 1, map[NodeID]string{2: "unreachable"}, nil)
	n.sendAppend = func(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
		<-ctx.Done()
		return AppendEntriesResponse{}, ctx.Err()
	}
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		return RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
	}
	n.sendPreVote = func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error) {
		return PreVoteResponse{Term: req.ProspectiveTerm - 1, VoteGranted: true}, nil
	}
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader", n.Role())
	}
	index, term, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- n.WaitApplied(context.Background(), index, term)
	}()
	time.Sleep(20 * time.Millisecond) // ensure WaitApplied has registered its waiter
	n.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrNodeClosed) {
			t.Fatalf("err = %v, want ErrNodeClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("WaitApplied did not return after Close — waiter leaked")
	}
}

func TestWaitAppliedDetectsSupersededEntry(t *testing.T) {
	net := newFakeNetwork()
	a := newNodeWithApply(t, 1, map[NodeID]string{2: "B", 3: "C"}, nil)
	b := newNodeWithApply(t, 2, map[NodeID]string{1: "A", 3: "C"}, nil)
	c := newNodeWithApply(t, 3, map[NodeID]string{1: "A", 2: "B"}, nil)
	a.send, b.send, c.send = net.send, net.send, net.send
	a.sendAppend, b.sendAppend, c.sendAppend = net.sendAppend, net.sendAppend, net.sendAppend
	a.sendPreVote, b.sendPreVote, c.sendPreVote = net.sendPreVote, net.sendPreVote, net.sendPreVote
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	// A becomes leader in term 1, but is isolated from B/C immediately
	// after proposing so the entry never replicates.
	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	net.setBlocked("B", true)
	net.setBlocked("C", true)
	index, term, err := a.Propose([]byte("lost"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	// Propose triggers replication in the background; give that doomed
	// (blocked) attempt time to actually fail before B/C are unblocked
	// below, so it can't race in and deliver the entry after all.
	time.Sleep(20 * time.Millisecond)

	// B and C elect a new leader (B) in a higher term without that entry.
	net.setBlocked("A", true)
	net.setBlocked("B", false)
	net.setBlocked("C", false)
	// PreVote's leader-contact safeguard would otherwise make C reject B's
	// PreVote — C only just (moments of real wall-clock time ago) accepted
	// AppendEntries from A — so simulate enough real time having passed
	// rather than sleeping for it (see electAndWaitLeader in
	// fault_recovery_test.go for the same technique).
	for _, n := range []*Node{a, b, c} {
		n.mu.Lock()
		n.lastLeaderContact = time.Time{}
		n.mu.Unlock()
	}
	if err := b.StartElection(context.Background()); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("b.Role() = %v, want Leader", b.Role())
	}

	// The new leader proposes its own entry at index 1 and, once A is
	// reachable again, replicates it — a plain heartbeat wouldn't touch
	// A's stale suffix (it makes no claim beyond prevLogIndex), but a
	// real conflicting entry at that index forces A to truncate it.
	if _, _, err := b.Propose([]byte("new-leader-entry")); err != nil {
		t.Fatalf("B Propose: %v", err)
	}
	net.setBlocked("A", false)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = a.WaitApplied(ctx, index, term)
	if !errors.Is(err, ErrEntryLost) {
		t.Fatalf("err = %v, want ErrEntryLost", err)
	}
}
