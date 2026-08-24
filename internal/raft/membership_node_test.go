package raft

import "testing"

// TestBootstrapMembershipIncludesSelfAndPeers proves a freshly constructed
// Node's effective membership is Stable, contains self plus every peer
// passed to NewNode, and self is always considered an effective voter
// before any real membership change ever happens.
func TestBootstrapMembershipIncludesSelfAndPeers(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, map[NodeID]string{2: "addr2", 3: "addr3"})
	n.mu.Lock()
	m := n.membership
	n.mu.Unlock()

	if m.Mode != ModeStable {
		t.Fatalf("Mode = %v, want ModeStable", m.Mode)
	}
	for _, id := range []NodeID{1, 2, 3} {
		if !m.Stable.Has(id) {
			t.Fatalf("bootstrap Stable configuration missing voter %d", id)
		}
	}
	if !m.IsVoter(1) {
		t.Fatalf("self (1) must be an effective voter under bootstrap membership")
	}
}

// TestSetSelfAddrUpdatesBootstrapMembership proves SetSelfAddr's address
// is reflected in the bootstrap Configuration (needed so a future joiner
// or snapshot could resolve this node by a real address, not a
// placeholder).
func TestSetSelfAddrUpdatesBootstrapMembership(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	n.SetSelfAddr("127.0.0.1:9001")

	n.mu.Lock()
	addr, ok := n.membership.Stable.Voters[1]
	n.mu.Unlock()
	if !ok || addr != "127.0.0.1:9001" {
		t.Fatalf("Stable.Voters[1] = %q, ok=%v, want 127.0.0.1:9001, true", addr, ok)
	}
}

// TestSetPeersUpdatesBootstrapMembership proves SetPeers' new peer table
// is reflected in bootstrap membership immediately, since no real
// membership-change history exists yet in this scenario.
func TestSetPeersUpdatesBootstrapMembership(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{}, nil)
	n.SetPeers(map[NodeID]string{2: "addr2"})

	n.mu.Lock()
	has2 := n.membership.Stable.Has(2)
	n.mu.Unlock()
	if !has2 {
		t.Fatalf("membership does not reflect SetPeers' new peer table")
	}
}

// TestTargetsMatchesLegacyPeersBehavior proves the membership-derived
// replication/election target set is exactly self's peers — the same set
// legacy code iterated directly over n.peers — so this milestone's
// membership plumbing changes no observable targeting behavior yet
// (until a real joint transition exists).
func TestTargetsMatchesLegacyPeersBehavior(t *testing.T) {
	peers := map[NodeID]string{2: "addr2", 3: "addr3"}
	n := newTestNode(t, 1, PersistentState{}, peers)

	n.mu.Lock()
	targets := n.membership.Targets(n.id)
	n.mu.Unlock()

	if len(targets) != len(peers) {
		t.Fatalf("Targets() = %v, want exactly %v", targets, peers)
	}
	for id, addr := range peers {
		if targets[id] != addr {
			t.Fatalf("Targets()[%d] = %q, want %q", id, targets[id], addr)
		}
	}
}
