package raft

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func newTestNode(t *testing.T, id NodeID, initial PersistentState, peers map[NodeID]string) *Node {
	t.Helper()
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "state"))
	if initial != (PersistentState{}) {
		if err := store.Save(initial); err != nil {
			t.Fatalf("Save initial state: %v", err)
		}
	}
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	n, err := NewNode(id, store, log, commitStore, NewSnapshotStore(filepath.Join(dir, "snapshot")), peers, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(n.Close)
	return n
}

// brokenStore always fails Save, simulating a disk write failure. It
// wraps a real Store so Load still works normally.
func brokenStore(t *testing.T) *Store {
	t.Helper()
	// A store whose directory does not exist: os.CreateTemp inside Save
	// fails deterministically, without needing a mock interface.
	return NewStore(filepath.Join(t.TempDir(), "missing-dir", "state"))
}

// brokenLog always fails Append/TruncateAndAppend, simulating a disk
// write failure, the same way brokenStore does for Store.
func brokenLog(t *testing.T) *Log {
	t.Helper()
	l, err := OpenLog(filepath.Join(t.TempDir(), "missing-dir", "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	return l
}

// brokenCommitStore always fails Save, the same way brokenStore/brokenLog
// simulate a disk write failure for their respective files.
func brokenCommitStore(t *testing.T) *CommitStore {
	t.Helper()
	return NewCommitStore(filepath.Join(t.TempDir(), "missing-dir", "commit"))
}

func workingCommitStore(t *testing.T) *CommitStore {
	t.Helper()
	return NewCommitStore(filepath.Join(t.TempDir(), "commit"))
}

func mustVoter(id NodeID) *NodeID { return &id }

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower", n.Role())
	}
}

func TestHandleRequestVoteLowerTermRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5}, nil)
	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: 4, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if resp.VoteGranted || resp.Term != 5 {
		t.Fatalf("resp = %+v, want {Term:5 VoteGranted:false}", resp)
	}
	if n.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want unchanged 5", n.CurrentTerm())
	}
}

func TestHandleRequestVoteHigherTermUpdatesLocalTerm(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 3}, nil)
	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if resp.Term != 5 {
		t.Fatalf("resp.Term = %d, want 5", resp.Term)
	}
	if n.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want 5", n.CurrentTerm())
	}
}

func TestHandleRequestVoteFirstCandidateGetsVote(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5}, nil)
	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("resp = %+v, want granted", resp)
	}
	if v := n.VotedFor(); v == nil || *v != 2 {
		t.Fatalf("VotedFor() = %v, want 2", v)
	}
}

func TestHandleRequestVoteRepeatSameCandidateGranted(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5, VotedFor: mustVoter(2)}, nil)
	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("repeat vote for same candidate should be idempotently granted, got %+v", resp)
	}
}

func TestHandleRequestVoteDifferentCandidateSameTermRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5, VotedFor: mustVoter(2)}, nil)
	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 3})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("different candidate in same term must be denied, got %+v", resp)
	}
}

// TestHandleRequestVoteCandidateSameTermCompetingRequestRejected proves a
// node that became Candidate (and so voted for itself) denies a same-term
// RequestVote from a different candidate — Raft's log-freshness pure
// comparison is covered independently in request_vote_test.go; this test
// covers the node-level "already voted for self" path specifically.
func TestHandleRequestVoteCandidateSameTermCompetingRequestRejected(t *testing.T) {
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

	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: n.CurrentTerm(), CandidateID: 3})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("a candidate that voted for itself must deny another candidate's same-term request")
	}
}

func TestHandleRequestVoteLeaderStepsDownOnHigherTerm(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil) // no peers: self-vote is already a majority
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader", n.Role())
	}

	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: n.CurrentTerm() + 1, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower after higher-term RequestVote", n.Role())
	}
	if !resp.VoteGranted {
		t.Fatalf("resp = %+v, want granted (fresh term, no prior vote)", resp)
	}
}

func workingLog(t *testing.T) *Log {
	t.Helper()
	l, err := OpenLog(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	return l
}

func TestHandleRequestVoteGrantFailsIfPersistenceFails(t *testing.T) {
	n, err := NewNode(1, brokenStore(t), workingLog(t), workingCommitStore(t), NewSnapshotStore(filepath.Join(t.TempDir(), "snapshot")), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	_, err = n.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2})
	if err == nil {
		t.Fatalf("HandleRequestVote succeeded despite persistence failure, want error")
	}
	if n.VotedFor() != nil {
		t.Fatalf("VotedFor() = %v, want nil — a vote must never be granted without persisting first", n.VotedFor())
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("CurrentTerm() = %d, want unchanged 0", n.CurrentTerm())
	}
}

func TestStartElectionFailsIfPersistenceFails(t *testing.T) {
	n, err := NewNode(1, brokenStore(t), workingLog(t), workingCommitStore(t), NewSnapshotStore(filepath.Join(t.TempDir(), "snapshot")), map[NodeID]string{2: "peer-b"}, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	err = n.StartElection(context.Background())
	if err == nil {
		t.Fatalf("StartElection succeeded despite persistence failure, want error")
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want unchanged Follower", n.Role())
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("CurrentTerm() = %d, want unchanged 0 — must not advertise an unpersisted term", n.CurrentTerm())
	}
}

func TestSingleNodeElectionRequiresNoNetwork(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		t.Fatalf("send was called for a single-node cluster; self-vote alone is already a majority")
		return RequestVoteResponse{}, nil
	}

	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader", n.Role())
	}
	if n.CurrentTerm() != 1 {
		t.Fatalf("CurrentTerm() = %d, want 1", n.CurrentTerm())
	}
}

func TestApplyVoteResponseDeduplicatesGrants(t *testing.T) {
	// Five-node cluster: self + four peers, majority = 3.
	peers := map[NodeID]string{2: "b", 3: "c", 4: "d", 5: "e"}
	n := newTestNode(t, 1, PersistentState{}, peers)
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		return RequestVoteResponse{}, errors.New("no automatic responses in this test")
	}
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	term := n.CurrentTerm()

	n.applyVoteResponse(term, 2, RequestVoteResponse{Term: term, VoteGranted: true})
	n.applyVoteResponse(term, 2, RequestVoteResponse{Term: term, VoteGranted: true}) // duplicate
	if got := len(n.votes); got != 2 {
		t.Fatalf("votes = %d, want 2 (self + one distinct grant, duplicate ignored)", got)
	}
	if n.Role() != Candidate {
		t.Fatalf("Role() = %v, want still Candidate (2 of 5 is not a majority)", n.Role())
	}

	n.applyVoteResponse(term, 3, RequestVoteResponse{Term: term, VoteGranted: true})
	if got := len(n.votes); got != 3 {
		t.Fatalf("votes = %d, want 3", got)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader once majority (3 of 5) is reached", n.Role())
	}
}

func TestApplyVoteResponseIgnoresStaleTermResponse(t *testing.T) {
	peers := map[NodeID]string{2: "b", 3: "c"}
	n := newTestNode(t, 1, PersistentState{}, peers)
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		return RequestVoteResponse{}, errors.New("no automatic responses in this test")
	}
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	staleTerm := n.CurrentTerm() // term 1

	// A higher-term response forces step-down to term 2.
	n.applyVoteResponse(staleTerm, 2, RequestVoteResponse{Term: staleTerm + 1, VoteGranted: false})
	if n.CurrentTerm() != staleTerm+1 || n.Role() != Follower {
		t.Fatalf("after higher-term response: term=%d role=%v, want term=%d Follower", n.CurrentTerm(), n.Role(), staleTerm+1)
	}

	// A late grant for the old (now stale) term must not affect anything.
	n.applyVoteResponse(staleTerm, 3, RequestVoteResponse{Term: staleTerm, VoteGranted: true})
	if n.CurrentTerm() != staleTerm+1 || n.Role() != Follower {
		t.Fatalf("stale response changed state: term=%d role=%v", n.CurrentTerm(), n.Role())
	}
}

func TestApplyVoteResponseHigherTermForcesFollower(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 3, VotedFor: mustVoter(1)}, map[NodeID]string{2: "b"})
	electionTerm := n.CurrentTerm()
	n.applyVoteResponse(electionTerm, 2, RequestVoteResponse{Term: electionTerm + 2, VoteGranted: false})

	if n.CurrentTerm() != electionTerm+2 {
		t.Fatalf("CurrentTerm() = %d, want %d", n.CurrentTerm(), electionTerm+2)
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower", n.Role())
	}
	if n.VotedFor() != nil {
		t.Fatalf("VotedFor() = %v, want nil (cleared on step-down)", n.VotedFor())
	}
}
