package raft

import (
	"context"
	"testing"
	"time"

	"quorumkv/internal/transport"
)

// TestMembershipChangeOverRealTCP is the mandatory real-socket scenario,
// covering add, remove, and self-remove together with the "powerful
// integration test" of a brand-new node catching up via real
// InstallSnapshot (not merely AppendEntries) because it joins well behind
// the leader's already-compacted log prefix.
func TestMembershipChangeOverRealTCP(t *testing.T) {
	c := newSnapshottingTCPCluster(t, []NodeID{1, 2, 3})
	defer c.closeAll()
	a := c.nodes[1]

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := a.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	// Commit several entries, then snapshot+compact past them, so a
	// brand-new joiner cannot catch up via AppendEntries alone — it must
	// go through real InstallSnapshot.
	var last LogIndex
	for _, cmd := range []string{"one", "two", "three"} {
		last = proposeAsLeaderAndWaitApplied(t, a.Node, cmd)
	}
	if err := a.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if a.log.BaseIndex() != last {
		t.Fatalf("a.log.BaseIndex() = %d, want %d (snapshot must have compacted the log)", a.log.BaseIndex(), last)
	}

	// D is a brand-new node: no prior log, no prior knowledge of the
	// cluster, listening on a real TCP port of its own.
	dDir := t.TempDir()
	smD := newFakeStateMachine()
	d := openSnapshottingNode(t, dDir, 4, nil, smD)
	dtr, err := transport.Listen("127.0.0.1:0", d.Handler())
	if err != nil {
		t.Fatalf("Listen(D): %v", err)
	}
	defer func() { d.Close(); dtr.Close() }()
	go d.Run(ctx)

	if err := a.AddVoter(ctx, 4, dtr.Addr()); err != nil {
		t.Fatalf("AddVoter(D): %v", err)
	}
	status := a.MembershipStatus()
	if status.Mode != ModeStable {
		t.Fatalf("Mode = %v after AddVoter, want ModeStable", status.Mode)
	}
	for _, id := range []NodeID{1, 2, 3, 4} {
		if !status.Stable.Has(id) {
			t.Fatalf("final Stable configuration missing voter %d: %+v", id, status.Stable)
		}
	}

	// D must have actually caught up via real InstallSnapshot (it never
	// had the pre-compaction entries any other way) and applied them.
	if !waitFor(5*time.Second, func() bool { return d.LastApplied() >= last }) {
		t.Fatalf("D did not catch up over real InstallSnapshot: LastApplied()=%d, want >= %d", d.LastApplied(), last)
	}
	if got := smD.snapshotOf(); len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Fatalf("D's restored+replicated commands = %v, want [one two three]", got)
	}

	// Remove D again.
	if err := a.RemoveVoter(ctx, 4); err != nil {
		t.Fatalf("RemoveVoter(D): %v", err)
	}
	status = a.MembershipStatus()
	if status.Stable.Has(4) {
		t.Fatalf("final Stable configuration still has removed voter 4: %+v", status.Stable)
	}

	// A removes itself; it must stop leading once that becomes final.
	if err := a.RemoveVoter(ctx, 1); err != nil {
		t.Fatalf("RemoveVoter(self): %v", err)
	}
	if a.Role() == Leader {
		t.Fatalf("A still Leader after removing itself, want a passive Follower")
	}
	status = a.MembershipStatus()
	if status.Stable.Has(1) {
		t.Fatalf("final Stable configuration still has self-removed voter 1: %+v", status.Stable)
	}
	for _, id := range []NodeID{2, 3} {
		if !status.Stable.Has(id) {
			t.Fatalf("final Stable configuration missing retained voter %d: %+v", id, status.Stable)
		}
	}
}
