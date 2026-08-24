package raft

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func entriesOf(cmds ...string) []LogEntry {
	out := make([]LogEntry, len(cmds))
	for i, c := range cmds {
		out[i] = LogEntry{Term: 1, Command: []byte(c)}
	}
	return out
}

// --- Term handling ---

func TestHandleAppendEntriesLowerTermRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5}, nil)
	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 4, LeaderID: 2})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if resp.Success || resp.Term != 5 {
		t.Fatalf("resp = %+v, want {Term:5 Success:false}", resp)
	}
	if n.LastLogIndex() != 0 {
		t.Fatalf("log must not change on a lower-term request")
	}
}

func TestHandleAppendEntriesHigherTermStepsDownAndPersists(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 2, VotedFor: mustVoter(9)}, nil)
	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 5, LeaderID: 2})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.Success || resp.Term != 5 {
		t.Fatalf("resp = %+v, want {Term:5 Success:true}", resp)
	}
	if n.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want 5", n.CurrentTerm())
	}
	if n.VotedFor() != nil {
		t.Fatalf("VotedFor() = %v, want nil (cleared on step-down)", n.VotedFor())
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower", n.Role())
	}
}

func TestHandleAppendEntriesSameTermCandidateStepsDown(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer-b"})
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		return RequestVoteResponse{}, errors.New("unreachable in this test")
	}
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Candidate {
		t.Fatalf("Role() = %v, want Candidate", n.Role())
	}
	term := n.CurrentTerm()

	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: term, LeaderID: 3})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower after valid same-term leader contact", n.Role())
	}
	if n.CurrentTerm() != term {
		t.Fatalf("CurrentTerm() = %d, want unchanged %d", n.CurrentTerm(), term)
	}
	if !resp.Success {
		t.Fatalf("resp = %+v, want success (empty log, matching prevLog sentinel)", resp)
	}
}

// --- Election timer reset ---

func TestHandleAppendEntriesResetsElectionTimerEvenOnLogMismatch(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.log.Append(entriesOf("a")) // local log has 1 entry at term 1

	// A request whose prevLog doesn't match locally still comes from the
	// valid current-term leader and must reset the timer, even though the
	// consistency check below will fail.
	drained := false
	select {
	case <-n.resetCh:
	default:
		drained = true
	}
	if !drained {
		t.Fatalf("resetCh unexpectedly had a pending signal before the call")
	}

	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 9})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if resp.Success {
		t.Fatalf("resp.Success = true, want false (prevLogIndex 5 doesn't exist)")
	}
	select {
	case <-n.resetCh:
	default:
		t.Fatalf("election timer was not reset despite valid current-term leader contact")
	}
}

// --- prevLog check ---

func TestHandleAppendEntriesMissingPrevIndexRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.log.Append(entriesOf("a"))

	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 1})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if resp.Success {
		t.Fatalf("resp.Success = true, want false")
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("log must not change: LastLogIndex() = %d, want 1", n.LastLogIndex())
	}
}

func TestHandleAppendEntriesPrevTermMismatchRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 3}, nil)
	n.log.Append([]LogEntry{{Term: 2, Command: []byte("a")}})

	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 3, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 7})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if resp.Success {
		t.Fatalf("resp.Success = true, want false (term mismatch at prevLogIndex)")
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("log must not change on prevLogTerm mismatch")
	}
}

func TestHandleAppendEntriesSentinelPrevLogMatchesEmptyLog(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	resp, err := n.HandleAppendEntries(AppendEntriesRequest{
		Term: 1, LeaderID: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: entriesOf("a"),
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp = %+v, want success", resp)
	}
	if n.LastLogIndex() != 1 {
		t.Fatalf("LastLogIndex() = %d, want 1", n.LastLogIndex())
	}
}

// --- Heartbeat does not modify log ---

func TestHeartbeatDoesNotModifyLog(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.log.Append(entriesOf("a", "b"))

	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 2, PrevLogTerm: 1})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp = %+v, want success", resp)
	}
	if n.LastLogIndex() != 2 {
		t.Fatalf("LastLogIndex() = %d, want unchanged 2", n.LastLogIndex())
	}
}

// --- Conflict repair ---

func TestConflictRepairPreservesMatchingPrefixAndRemovesDivergentSuffix(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 4}, nil)
	n.log.Append([]LogEntry{
		{Term: 1, Command: []byte("A")},
		{Term: 2, Command: []byte("B")},
		{Term: 3, Command: []byte("X")},
		{Term: 3, Command: []byte("Y")},
	})

	leaderEntries := []LogEntry{
		{Term: 4, Command: []byte("C")},
		{Term: 4, Command: []byte("D")},
	}
	resp, err := n.HandleAppendEntries(AppendEntriesRequest{
		Term: 4, LeaderID: 2, PrevLogIndex: 2, PrevLogTerm: 2, Entries: leaderEntries,
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp = %+v, want success", resp)
	}

	want := []LogEntry{
		{Term: 1, Command: []byte("A")},
		{Term: 2, Command: []byte("B")},
		{Term: 4, Command: []byte("C")},
		{Term: 4, Command: []byte("D")},
	}
	if n.LastLogIndex() != 4 {
		t.Fatalf("LastLogIndex() = %d, want 4", n.LastLogIndex())
	}
	for i, w := range want {
		got, ok := n.LogEntry(LogIndex(i + 1))
		if !ok || got.Term != w.Term || !bytes.Equal(got.Command, w.Command) {
			t.Fatalf("Entry(%d) = %+v, want %+v", i+1, got, w)
		}
	}
}

func TestConflictRepairPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	logPath := dir + "/log"

	store := NewStore(dir + "/state")
	if err := store.Save(PersistentState{CurrentTerm: 4}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	log, err := OpenLog(logPath)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	n, err := NewNode(1, store, log, workingCommitStore(t), NewSnapshotStore(filepath.Join(dir, "snapshot")), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer n.Close()

	n.log.Append([]LogEntry{
		{Term: 1, Command: []byte("A")},
		{Term: 3, Command: []byte("X")},
	})
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{
		Term: 4, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 4, Command: []byte("C")}},
	}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	n.Close()

	reopened, err := OpenLog(logPath)
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	if reopened.LastIndex() != 2 {
		t.Fatalf("LastIndex() = %d, want 2", reopened.LastIndex())
	}
	e, _ := reopened.Entry(2)
	if e.Term != 4 || string(e.Command) != "C" {
		t.Fatalf("Entry(2) = %+v, want {4 C}", e)
	}
}

// --- Persistence-before-success ---

func TestHandleAppendEntriesFailsIfLogPersistenceFails(t *testing.T) {
	n, err := NewNode(1, NewStore(tempStatePath(t)), brokenLog(t), workingCommitStore(t), NewSnapshotStore(filepath.Join(t.TempDir(), "snapshot")), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	_, err = n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, Entries: entriesOf("a")})
	if err == nil {
		t.Fatalf("HandleAppendEntries succeeded despite log persistence failure, want error")
	}
	if n.LastLogIndex() != 0 {
		t.Fatalf("log must remain empty after a failed persist")
	}
}

func TestHandleAppendEntriesHigherTermFailsIfStatePersistenceFails(t *testing.T) {
	n, err := NewNode(1, brokenStore(t), workingLog(t), workingCommitStore(t), NewSnapshotStore(filepath.Join(t.TempDir(), "snapshot")), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	_, err = n.HandleAppendEntries(AppendEntriesRequest{Term: 5, LeaderID: 2})
	if err == nil {
		t.Fatalf("HandleAppendEntries succeeded despite term-persistence failure, want error")
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("CurrentTerm() = %d, want unchanged 0 — must not act as though the higher term was accepted", n.CurrentTerm())
	}
}

// --- Leader replication state (nextIndex/matchIndex) and backtracking ---

func TestApplyAppendEntriesResponseSuccessAdvancesMatchAndNext(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	req := AppendEntriesRequest{Term: term, PrevLogIndex: 0, Entries: entriesOf("a", "b")}
	n.applyAppendEntriesResponse(term, 2, req, AppendEntriesResponse{Term: term, Success: true, MatchIndex: 2})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.matchIndex[2] != 2 || n.nextIndex[2] != 3 {
		t.Fatalf("matchIndex/nextIndex = %d/%d, want 2/3", n.matchIndex[2], n.nextIndex[2])
	}
}

func TestApplyAppendEntriesResponseFailureBacksOffNextIndex(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	n.nextIndex[2] = 5
	n.mu.Unlock()

	req := AppendEntriesRequest{Term: term, PrevLogIndex: 4}
	n.applyAppendEntriesResponse(term, 2, req, AppendEntriesResponse{Term: term, Success: false})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nextIndex[2] != 4 {
		t.Fatalf("nextIndex = %d, want 4 (backed off by one)", n.nextIndex[2])
	}
}

func TestApplyAppendEntriesResponseNextIndexNeverBelowOne(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	n.nextIndex[2] = 1
	n.mu.Unlock()

	req := AppendEntriesRequest{Term: term, PrevLogIndex: 0}
	n.applyAppendEntriesResponse(term, 2, req, AppendEntriesResponse{Term: term, Success: false})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nextIndex[2] != 1 {
		t.Fatalf("nextIndex = %d, want floor of 1", n.nextIndex[2])
	}
}

func TestApplyAppendEntriesResponseMatchIndexNeverRegresses(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	// A newer, higher success arrives first...
	n.applyAppendEntriesResponse(term, 2, AppendEntriesRequest{Term: term, PrevLogIndex: 0, Entries: entriesOf("a", "b", "c")},
		AppendEntriesResponse{Term: term, Success: true, MatchIndex: 3})
	// ...then a stale, older success for a smaller prefix arrives late.
	n.applyAppendEntriesResponse(term, 2, AppendEntriesRequest{Term: term, PrevLogIndex: 0, Entries: entriesOf("a")},
		AppendEntriesResponse{Term: term, Success: true, MatchIndex: 1})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.matchIndex[2] != 3 {
		t.Fatalf("matchIndex = %d, want 3 (must not regress from a stale response)", n.matchIndex[2])
	}
}

func TestApplyAppendEntriesResponseHigherTermStepsDown(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	n.applyAppendEntriesResponse(term, 2, AppendEntriesRequest{Term: term}, AppendEntriesResponse{Term: term + 5, Success: false})

	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower", n.Role())
	}
	if n.CurrentTerm() != term+5 {
		t.Fatalf("CurrentTerm() = %d, want %d", n.CurrentTerm(), term+5)
	}
}

func TestApplyAppendEntriesResponseStaleTermIgnored(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "b"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	staleTerm := n.persistent.CurrentTerm
	n.mu.Unlock()

	// This node moved on to a new term (e.g. it stepped down and won a
	// later election) before this response for the old term arrives.
	n.mu.Lock()
	n.persistent.CurrentTerm = staleTerm + 1
	n.mu.Unlock()

	n.applyAppendEntriesResponse(staleTerm, 2, AppendEntriesRequest{Term: staleTerm, PrevLogIndex: 0, Entries: entriesOf("a")},
		AppendEntriesResponse{Term: staleTerm, Success: true, MatchIndex: 1})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.matchIndex[2] != 0 {
		t.Fatalf("matchIndex = %d, want unchanged 0 (stale-term response must be ignored)", n.matchIndex[2])
	}
}

// --- Propose ---

func TestProposeFailsIfNotLeader(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	_, _, err := n.Propose([]byte("x"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

func TestProposeAppendsCurrentTermEntry(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil) // single-node: self-vote is a majority
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	index, _, err := n.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if index != 1 {
		t.Fatalf("index = %d, want 1", index)
	}
	e, ok := n.LogEntry(1)
	if !ok || e.Term != n.CurrentTerm() || string(e.Command) != "hello" {
		t.Fatalf("LogEntry(1) = %+v, ok=%v", e, ok)
	}
}

func TestProposeDoesNotAliasCallerSlice(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	cmd := []byte("hello")
	if _, _, err := n.Propose(cmd); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	cmd[0] = 'H'

	e, _ := n.LogEntry(1)
	if string(e.Command) != "hello" {
		t.Fatalf("stored command changed after caller mutation: got %q", e.Command)
	}
}

func TestProposeFailsIfLogPersistenceFails(t *testing.T) {
	n, err := NewNode(1, NewStore(tempStatePath(t)), brokenLog(t), workingCommitStore(t), NewSnapshotStore(filepath.Join(t.TempDir(), "snapshot")), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()
	defer n.Close()

	_, _, err = n.Propose([]byte("x"))
	if err == nil {
		t.Fatalf("Propose succeeded despite log persistence failure, want error")
	}
	if n.LastLogIndex() != 0 {
		t.Fatalf("log must remain empty after a failed Propose")
	}
	if n.CommitIndex() != 0 {
		t.Fatalf("CommitIndex() = %d, want 0 — a failed local append is never committed", n.CommitIndex())
	}
}

// --- Commit rule ---

func TestCommitRuleCurrentTermMajority(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 4}, map[NodeID]string{2: "b", 3: "c"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.log.Append([]LogEntry{{Term: 4, Command: []byte("x")}})
	n.matchIndex[2] = 1 // one follower has replicated it: self+peer = majority of 3
	n.maybeAdvanceCommitIndexLocked()
	commit := n.commitIndex
	n.mu.Unlock()

	if commit != 1 {
		t.Fatalf("commitIndex = %d, want 1", commit)
	}
}

func TestCommitRuleOldTermNotDirectlyCommitted(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 4}, map[NodeID]string{2: "b", 3: "c"})
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.log.Append([]LogEntry{{Term: 3, Command: []byte("old")}}) // index1 term3 (old term)
	n.matchIndex[2] = 1
	n.matchIndex[3] = 1 // full majority has index1, but it's an old-term entry
	n.maybeAdvanceCommitIndexLocked()
	commitAfterOldTerm := n.commitIndex

	// Now append a current-term entry and replicate it to majority.
	n.log.Append([]LogEntry{{Term: 4, Command: []byte("new")}}) // index2 term4
	n.matchIndex[2] = 2
	n.maybeAdvanceCommitIndexLocked()
	commitAfterCurrentTerm := n.commitIndex
	n.mu.Unlock()

	if commitAfterOldTerm != 0 {
		t.Fatalf("commitIndex after old-term majority = %d, want 0 (must not commit an old-term entry directly)", commitAfterOldTerm)
	}
	if commitAfterCurrentTerm != 2 {
		t.Fatalf("commitIndex after current-term majority = %d, want 2 (implicitly commits index 1 too)", commitAfterCurrentTerm)
	}
}

func TestCommitIndexNeverDecreases(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.log.Append(entriesOf("a", "b", "c"))
	n.maybeAdvanceCommitIndexLocked() // single-node: commits straight to 3
	first := n.commitIndex
	n.maybeAdvanceCommitIndexLocked() // calling again must not regress anything
	second := n.commitIndex
	n.mu.Unlock()

	if first != 3 || second != 3 {
		t.Fatalf("commitIndex = %d then %d, want 3 then 3", first, second)
	}
}

// --- Follower commit advancement ---

func TestFollowerCommitIndexAdvancesFromLeaderCommit(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.log.Append(entriesOf("a", "b", "c"))

	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 2}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.CommitIndex() != 2 {
		t.Fatalf("CommitIndex() = %d, want 2", n.CommitIndex())
	}
}

func TestFollowerCommitIndexNeverExceedsLastLogIndex(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.log.Append(entriesOf("a"))

	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 100}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.CommitIndex() != 1 {
		t.Fatalf("CommitIndex() = %d, want capped at LastLogIndex 1", n.CommitIndex())
	}
}

func TestFollowerCommitIndexNeverDecreases(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, nil)
	n.log.Append(entriesOf("a", "b", "c"))
	n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 3})
	if n.CommitIndex() != 3 {
		t.Fatalf("CommitIndex() = %d, want 3", n.CommitIndex())
	}
	n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 1})
	if n.CommitIndex() != 3 {
		t.Fatalf("CommitIndex() = %d, want unchanged 3 (must never decrease)", n.CommitIndex())
	}
}
