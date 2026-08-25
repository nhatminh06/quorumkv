package raft

import (
	"context"
	"testing"
	"time"
)

// TestTimeoutNowValidRequestTriggersImmediateElection proves an
// authorized TimeoutNow (matching this node's current term and
// recognized leader) is accepted and kicks off a real election in the
// background, bypassing PreVote — the target wins immediately since it
// is (in this test) the only other voter besides the "leader" that sent
// it.
func TestTimeoutNowValidRequestTriggersImmediateElection(t *testing.T) {
	// A single-node effective configuration: self-vote alone already
	// wins the real election, so this isolates HandleTimeoutNow's own
	// authorization logic from real-election vote-counting (already
	// covered elsewhere) — no network calls are needed to observe the
	// win.
	n := newTestNode(t, 2, PersistentState{}, nil)
	// n must first recognize node 1 as its leader at term 1 (real
	// AppendEntries contact), matching HandleTimeoutNow's identity check.
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	n.sendPreVote = func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error) {
		t.Fatalf("PreVote was sent for a TimeoutNow-triggered election; TimeoutNow must bypass PreVote entirely")
		return PreVoteResponse{}, nil
	}
	n.send = func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
		t.Fatalf("RequestVote was sent despite self alone already being quorum")
		return RequestVoteResponse{}, nil
	}

	resp, err := n.HandleTimeoutNow(TimeoutNowRequest{Term: 1, LeaderID: 1})
	if err != nil {
		t.Fatalf("HandleTimeoutNow: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("resp.Accepted = false, want true for a valid authorized TimeoutNow")
	}
	if !waitFor(time.Second, func() bool { return n.Role() == Leader }) {
		t.Fatalf("node never won the TimeoutNow-triggered election: role=%v", n.Role())
	}
	if n.CurrentTerm() != 2 {
		t.Fatalf("CurrentTerm() = %d, want 2 (the real election incremented past the recognized term 1)", n.CurrentTerm())
	}
}

func TestTimeoutNowOldTermRejected(t *testing.T) {
	n := newTestNode(t, 2, PersistentState{CurrentTerm: 5}, map[NodeID]string{1: "leader"})
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 5, LeaderID: 1}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	resp, err := n.HandleTimeoutNow(TimeoutNowRequest{Term: 4, LeaderID: 1})
	if err != nil {
		t.Fatalf("HandleTimeoutNow: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("accepted a TimeoutNow for a stale term")
	}
	if n.Role() == Candidate || n.Role() == Leader {
		t.Fatalf("node campaigned despite rejecting the TimeoutNow: role=%v", n.Role())
	}
}

// TestTimeoutNowWrongLeaderRejected proves a follower rejects a
// TimeoutNow whose claimed LeaderID does not match who it actually
// recognizes as leader for that term — a random voter cannot force an
// immediate election just by naming itself.
func TestTimeoutNowWrongLeaderRejected(t *testing.T) {
	n := newTestNode(t, 3, PersistentState{}, map[NodeID]string{1: "leader", 2: "other"})
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	resp, err := n.HandleTimeoutNow(TimeoutNowRequest{Term: 1, LeaderID: 2})
	if err != nil {
		t.Fatalf("HandleTimeoutNow: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("accepted a TimeoutNow from a node this follower does not recognize as its leader")
	}
}

func TestTimeoutNowFromNonLeaderRejected(t *testing.T) {
	// Y never received any AppendEntries at all, so it has no recognized
	// leader — X (a plain follower, not actually leading anything) sends
	// TimeoutNow anyway.
	y := newTestNode(t, 2, PersistentState{}, map[NodeID]string{1: "X"})
	resp, err := y.HandleTimeoutNow(TimeoutNowRequest{Term: 0, LeaderID: 1})
	if err != nil {
		t.Fatalf("HandleTimeoutNow: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("Y accepted TimeoutNow from X, which Y has no reason to recognize as leader")
	}
	if y.Role() == Candidate || y.Role() == Leader {
		t.Fatalf("Y campaigned despite rejecting the TimeoutNow: role=%v", y.Role())
	}
}

// TestTimeoutNowPassiveTargetRejected proves a node outside its own
// effective voter set rejects TimeoutNow even from its recognized
// leader — a removed/not-yet-added node must never campaign.
func TestTimeoutNowPassiveTargetRejected(t *testing.T) {
	n := newTestNode(t, 3, PersistentState{}, map[NodeID]string{1: "leader", 2: "other"})
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	n.mu.Lock()
	n.membership = StableMembership(cfg(1, 2)) // node 3 (self) is not a member
	n.mu.Unlock()

	resp, err := n.HandleTimeoutNow(TimeoutNowRequest{Term: 1, LeaderID: 1})
	if err != nil {
		t.Fatalf("HandleTimeoutNow: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("a passive (non-voter) node accepted TimeoutNow")
	}
}

// TestTimeoutNowHigherTermProcessedButNotAuthorized is item 113: a
// genuinely higher term in a TimeoutNow request is still processed as
// ordinary higher-term evidence (persisted, stepped down), but that
// alone must not authorize a campaign — stepping down clears leaderID,
// so the identity check correctly still fails for what turns out not to
// be a currently-recognized leader at that term.
func TestTimeoutNowHigherTermProcessedButNotAuthorized(t *testing.T) {
	n := newTestNode(t, 2, PersistentState{CurrentTerm: 1}, map[NodeID]string{1: "leader"})
	resp, err := n.HandleTimeoutNow(TimeoutNowRequest{Term: 9, LeaderID: 1})
	if err != nil {
		t.Fatalf("HandleTimeoutNow: %v", err)
	}
	if resp.Accepted {
		t.Fatalf("accepted a TimeoutNow whose higher term was never established via real leader contact")
	}
	if n.CurrentTerm() != 9 {
		t.Fatalf("CurrentTerm() = %d, want 9 — a genuinely higher term must still be processed as real evidence", n.CurrentTerm())
	}
	if n.Role() == Candidate || n.Role() == Leader {
		t.Fatalf("node campaigned despite the TimeoutNow not being authorized: role=%v", n.Role())
	}
}
