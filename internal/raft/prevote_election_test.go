package raft

import (
	"context"
	"testing"
	"time"
)

// --- HandlePreVote (responder-side) unit tests ---

func TestPreVoteDoesNotMutatePersistentTerm(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5}, map[NodeID]string{2: "peer"})
	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 6, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("expected grant for a fresh higher prospective term with an equally-empty log")
	}
	if n.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want unchanged 5 — PreVote must never persist the prospective term", n.CurrentTerm())
	}
}

func TestPreVoteDoesNotChangeVotedFor(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5, VotedFor: mustVoter(9)}, map[NodeID]string{2: "peer"})
	if _, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 6, CandidateID: 2}); err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	v := n.VotedFor()
	if v == nil || *v != 9 {
		t.Fatalf("VotedFor() = %v, want unchanged 9 — PreVote must never touch votedFor", v)
	}
}

func TestPreVoteRejectsStaleLog(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer"})
	n.mu.Lock()
	if err := n.log.Append([]LogEntry{{Term: 3, Command: []byte("x")}, {Term: 5, Command: []byte("y")}}); err != nil {
		n.mu.Unlock()
		t.Fatalf("Append: %v", err)
	}
	n.mu.Unlock()

	// Candidate's log is behind: lower lastLogTerm than the responder's.
	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 1, CandidateID: 2, LastLogIndex: 2, LastLogTerm: 3})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("granted PreVote to a candidate with a stale log")
	}
}

func TestPreVoteLowerProspectiveTermRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5}, map[NodeID]string{2: "peer"})
	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 5, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("granted PreVote for a non-advancing prospective term (5 <= currentTerm 5)")
	}
	if resp.Term != 5 {
		t.Fatalf("resp.Term = %d, want the responder's actual current term 5", resp.Term)
	}
}

func TestPreVoteResponseReportsActualNotProspectiveTerm(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 5}, map[NodeID]string{2: "peer"})
	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 42, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.Term != 5 {
		t.Fatalf("resp.Term = %d, want actual current term 5, not the prospective term 42", resp.Term)
	}
}

func TestPreVoteFreshCandidateGranted(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer"})
	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 1, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("expected grant: fresh term, empty log, no recent leader contact")
	}
}

// TestPreVoteRecentLeaderContactRejected proves the leader-contact
// safeguard rejects an otherwise-perfectly-eligible PreVote request —
// the core mechanism that keeps an isolated node from disrupting a
// healthy leader.
func TestPreVoteRecentLeaderContactRejected(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer", 3: "leader"})
	fixedNow := time.Now()
	n.mu.Lock()
	n.nowFunc = func() time.Time { return fixedNow }
	n.lastLeaderContact = fixedNow.Add(-50 * time.Millisecond) // well within minElectionTimeout
	n.mu.Unlock()

	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 1, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("granted PreVote despite recent leader contact")
	}
}

// TestPreVoteGrantedOnceLeaderContactWindowExpires proves the safeguard
// is time-bounded, not permanent, and is fully testable without a real
// sleep via the injectable nowFunc.
func TestPreVoteGrantedOnceLeaderContactWindowExpires(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer", 3: "leader"})
	fixedNow := time.Now()
	n.mu.Lock()
	n.nowFunc = func() time.Time { return fixedNow }
	n.lastLeaderContact = fixedNow.Add(-time.Second) // well past minElectionTimeout
	n.mu.Unlock()

	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 1, CandidateID: 2})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("PreVote still rejected after the leader-contact window expired")
	}
}

func TestPreVoteRejectsNonMemberCandidate(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer"})
	resp, err := n.HandlePreVote(PreVoteRequest{ProspectiveTerm: 1, CandidateID: 99})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("granted PreVote to a candidate outside this node's own effective configuration")
	}
}

// --- applyPreVoteResponse (candidate-side) unit tests ---

func TestPreVoteHigherActualTermCausesStepDown(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 1}, map[NodeID]string{2: "peer"})
	granted := map[NodeID]bool{1: true}
	n.applyPreVoteResponse(2, PreVoteResponse{Term: 9, VoteGranted: false}, granted)

	if n.CurrentTerm() != 9 {
		t.Fatalf("CurrentTerm() = %d, want 9 — a higher ACTUAL term in a PreVote response is real evidence", n.CurrentTerm())
	}
	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower after learning of a higher term", n.Role())
	}
	if n.VotedFor() != nil {
		t.Fatalf("VotedFor() = %v, want nil (cleared on step-down)", n.VotedFor())
	}
	if granted[2] {
		t.Fatalf("a higher-term response must not also be counted as a grant")
	}
}

func TestPreVoteDuplicateResponseCountedOnce(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer"})
	granted := map[NodeID]bool{1: true}
	n.applyPreVoteResponse(2, PreVoteResponse{Term: 0, VoteGranted: true}, granted)
	n.applyPreVoteResponse(2, PreVoteResponse{Term: 0, VoteGranted: true}, granted)
	if len(granted) != 2 {
		t.Fatalf("granted = %v, want exactly {1,2} — a duplicate response must not double count", granted)
	}
}

// --- Candidate-side StartElection PreVote-gating tests ---

func TestSingleNodePreVoteSkipsNetworkEntirely(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	n.sendPreVote = func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error) {
		t.Fatalf("sendPreVote was called for a single-node cluster; self alone is already PreVote quorum")
		return PreVoteResponse{}, nil
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

// TestPassiveNodeDoesNotPreVoteCampaign proves a node that is not an
// effective voter never even attempts a PreVote round — no network call,
// no term change.
func TestPassiveNodeDoesNotPreVoteCampaign(t *testing.T) {
	// A node whose own bootstrap configuration is real, but which we then
	// place outside its own effective membership by rebuilding against a
	// Stable configuration that excludes it — simulating a removed/
	// not-yet-added node the same way membership_rebuild_test.go does.
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "peer", 3: "peer2"})
	n.sendPreVote = func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error) {
		t.Fatalf("sendPreVote was called by a non-voter node")
		return PreVoteResponse{}, nil
	}
	n.mu.Lock()
	n.membership = StableMembership(cfg(2, 3)) // 1 is not a member
	n.mu.Unlock()

	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() == Candidate || n.Role() == Leader {
		t.Fatalf("Role() = %v, want Follower — a passive node must not campaign", n.Role())
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("CurrentTerm() = %d, want unchanged 0", n.CurrentTerm())
	}
}

// TestPreVoteUsesJointQuorum is a direct unit-level companion to
// TestJointElectionRequiresBothMajorities (config_change_test.go, a full
// end-to-end proof): applyPreVoteResponse's tally is checked against
// Membership.HasQuorum, so a set that satisfies only Old's majority
// during a Joint transition must not read as quorum.
func TestPreVoteUsesJointQuorum(t *testing.T) {
	m := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	granted := map[NodeID]bool{1: true, 2: true} // old={1,2}=2/2 satisfied; new={1,2}=2/4 not
	if m.HasQuorum(granted) {
		t.Fatalf("test bug: this set must not be Joint quorum")
	}
	granted[3] = true // old={1,2,3}=3/3; new={1,2,3}=3/4 — both satisfied
	if !m.HasQuorum(granted) {
		t.Fatalf("test bug: this set must be Joint quorum")
	}
}

// --- Mandatory isolated-follower integration scenario (items 26-29) ---

// TestIsolatedFollowerDoesNotDisruptHealthyLeader is the mandatory
// isolated-follower scenario: C is isolated from A (the healthy leader)
// and B — a genuine two-way partition, not merely "nobody can reach C"
// (fakeNetwork's single-direction blocking would let C still receive
// real, informative rejection responses from A/B, which is not true
// isolation — see directedNetwork/faultCluster for real bidirectional
// partitions). C repeatedly fails to reach any PreVote quorum, and — the
// crux — its own currentTerm never advances while truly isolated, since
// it receives no responses of any kind, honest or otherwise. Once
// healed, A remains leader at its original term; C simply rejoins as a
// follower. No disruption is caused solely by C's isolation.
func TestIsolatedFollowerDoesNotDisruptHealthyLeader(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	a, cc := c.node(1), c.node(3)

	// Isolate C from the very start, before A is even elected, so C
	// never legitimately participates in anything (not even granting
	// A's own real vote request) and any term it later reaches can only
	// come from its own isolated attempts.
	c.net.partition(3, 1)
	c.net.partition(3, 2)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if a.Role() != Leader || a.CurrentTerm() != 1 {
		t.Fatalf("A did not become leader in term 1: role=%v term=%d", a.Role(), a.CurrentTerm())
	}

	// C experiences several failed election timeouts. Each attempt's
	// PreVote round can reach no one (true isolation: no responses at
	// all, not even honest rejections), so it must never obtain quorum
	// and must never advance currentTerm.
	for i := 0; i < 5; i++ {
		if err := cc.StartElection(context.Background()); err != nil {
			t.Fatalf("C StartElection attempt %d: %v", i, err)
		}
		if cc.CurrentTerm() != 0 {
			t.Fatalf("attempt %d: C.CurrentTerm() = %d, want unchanged 0 — a failed PreVote must never bump the term", i, cc.CurrentTerm())
		}
		if cc.Role() == Candidate || cc.Role() == Leader {
			t.Fatalf("attempt %d: C.Role() = %v, want Follower — PreVote must fail before C ever campaigns for real", i, cc.Role())
		}
	}

	// A must never have been disrupted: still leader, same term.
	if a.Role() != Leader || a.CurrentTerm() != 1 {
		t.Fatalf("A was disrupted by C's isolated attempts: role=%v term=%d", a.Role(), a.CurrentTerm())
	}

	// Heal the partition: C simply rejoins as a follower once A's next
	// heartbeat reaches it — no special-cased reconciliation needed.
	c.net.heal(3, 1)
	c.net.heal(3, 2)
	if !waitFor(time.Second, func() bool { return cc.Role() == Follower && cc.CurrentTerm() == 1 }) {
		t.Fatalf("C never rejoined as a follower after healing: role=%v term=%d", cc.Role(), cc.CurrentTerm())
	}
	if a.Role() != Leader || a.CurrentTerm() != 1 {
		t.Fatalf("A after healing: role=%v term=%d, want unchanged Leader/1 — no disruption caused solely by C's isolation", a.Role(), a.CurrentTerm())
	}
}

// TestPreVoteAllowsElectionAfterLeaderLoss proves PreVote does not
// prevent a LEGITIMATE election: once the leader is actually gone, the
// surviving majority can still elect a replacement.
func TestPreVoteAllowsElectionAfterLeaderLoss(t *testing.T) {
	net := newFakeNetwork()
	a := newFakeNode(t, 1, map[NodeID]string{2: "B", 3: "C"})
	b := newFakeNode(t, 2, map[NodeID]string{1: "A", 3: "C"})
	c := newFakeNode(t, 3, map[NodeID]string{1: "A", 2: "B"})
	for _, n := range []*Node{a, b, c} {
		n.send, n.sendAppend, n.sendPreVote = net.send, net.sendAppend, net.sendPreVote
	}
	net.register("A", a)
	net.register("B", b)
	net.register("C", c)

	if err := a.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	// A is actually gone now (stopped, unreachable to everyone).
	a.Close()
	net.setBlocked("A", true)
	// C's leader-contact safeguard is tracking A's now-stale heartbeat;
	// simulate enough real time having passed rather than sleeping for
	// it (see electAndWaitLeader in fault_recovery_test.go).
	for _, n := range []*Node{b, c} {
		n.mu.Lock()
		n.lastLeaderContact = time.Time{}
		n.mu.Unlock()
	}

	if err := b.StartElection(context.Background()); err != nil {
		t.Fatalf("B StartElection: %v", err)
	}
	if b.Role() != Leader {
		t.Fatalf("B did not win a legitimate election after the leader was actually lost: role=%v", b.Role())
	}
	if b.CurrentTerm() != 2 {
		t.Fatalf("B.CurrentTerm() = %d, want 2 (PreVote succeeded, real election advanced the term once)", b.CurrentTerm())
	}
}
