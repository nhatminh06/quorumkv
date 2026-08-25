package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/kv"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// TestInstallSnapshotDedupSurvives is items 79/118 — mandatory, powerful
// evidence that request identity survives stale-follower snapshot
// recovery: C is isolated while A+B commit several writes (including one
// specific client request), A snapshots and compacts that request out of
// its log entirely, C is reconnected and catches up via a real
// InstallSnapshot transfer (not a log replay — the entry no longer
// exists to replay), C is then made leader legitimately, and the client
// retries its original request against C. C must recognize it from its
// installed dedup table and must not mutate state again.
func TestInstallSnapshotDedupSurvives(t *testing.T) {
	nodes := startDedupCluster(t, 3)
	electDedupLeader(t, nodes, 0)
	a, b, cNode := nodes[0], nodes[1], nodes[2]

	// Isolate C: A and B's outbound sends to C's address are dropped: C
	// itself is otherwise fully functional (a real process, just
	// unreachable), matching "C offline" rather than "C crashed."
	aSender, bSender := newIsolatedSender(), newIsolatedSender()
	aSender.wireInto(a.svc.node)
	bSender.wireInto(b.svc.node)
	aSender.block(cNode.addr())
	bSender.block(cNode.addr())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A committed write whose request identity must survive compaction:
	// this is the one we will retry against C later.
	target := client.New(a.addr())
	if err := target.Put(ctx, []byte("tracked"), []byte("v1")); err != nil {
		t.Fatalf("Put(tracked): %v", err)
	}

	// More ordinary writes, then snapshot + compact past the tracked
	// request entirely.
	other := client.New(a.addr())
	if err := other.Put(ctx, []byte("other"), []byte("v2")); err != nil {
		t.Fatalf("Put(other): %v", err)
	}
	if err := a.svc.node.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if _, ok := a.svc.node.LogEntry(1); ok {
		t.Fatalf("precondition: the tracked request's log entry should have been compacted away")
	}

	// More writes after the snapshot too, so C's eventual catch-up must
	// combine InstallSnapshot with at least some ordinary suffix replay.
	if err := other.Put(ctx, []byte("other"), []byte("v3")); err != nil {
		t.Fatalf("Put(other) after snapshot: %v", err)
	}

	// Reconnect C and let it catch up via real InstallSnapshot.
	aSender.allow(cNode.addr())
	bSender.allow(cNode.addr())
	if !eventuallyTrue(5*time.Second, func() bool {
		return cNode.svc.node.LastApplied() >= a.svc.node.LastApplied()
	}) {
		t.Fatalf("C did not catch up: LastApplied()=%d, want >= %d", cNode.svc.node.LastApplied(), a.svc.node.LastApplied())
	}
	cNode.svc.mu.Lock()
	_, hasTracked := cNode.svc.sm.Get([]byte("tracked"))
	trackedFP := kv.Fingerprint(kv.NewIdentifiedPutCommand(target.ID(), 1, []byte("tracked"), []byte("v1")))
	lookup := cNode.svc.sm.LookupRequest(target.ID(), 1, trackedFP)
	cNode.svc.mu.Unlock()
	if !hasTracked {
		t.Fatalf("C's KV state does not have the compacted-and-installed key after catch-up")
	}
	if lookup != kv.AppliedDuplicate {
		t.Fatalf("C's dedup table does not yet recognize the tracked request after catch-up: LookupRequest = %v, want AppliedDuplicate", lookup)
	}

	// Stop A and B, and legitimately elect C leader among the survivors
	// (a real election, not a forced role). Close() stops each node's own
	// background work but does not reset its role: A's RPC handler stays
	// reachable and, being closed rather than truly gone, still correctly
	// insists it is Leader — so C must win using B's vote alone (B, a
	// Follower whose own last leader contact is long past by this point
	// in the test, has no such objection). This models "unreachable" as
	// this test suite already does elsewhere (isolatedSender), not a
	// literal process crash.
	a.svc.node.Close()
	b.svc.node.Close()
	electDedupLeader(t, []*dedupTestNode{cNode}, 0)

	// Snapshot the mutation count here, right before the retry: it
	// already includes the legitimate suffix-replay mutation for the
	// unrelated "other" request from catch-up — the assertion below
	// must check the count did not advance *further*, not that it is
	// zero.
	before := cNode.applied.Load()

	// Retry the tracked request — same ClientID, same sequence — against
	// the now-leader C.
	retry := client.NewWithID(target.ID(), cNode.addr())
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()
	if err := retry.Put(retryCtx, []byte("tracked"), []byte("v1")); err != nil {
		t.Fatalf("retry against C: %v", err)
	}

	v, ok, err := retry.Get(retryCtx, []byte("tracked"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get(tracked) on C = %q, %v, %v; want v1, true, nil", v, ok, err)
	}
	if got := cNode.applied.Load() - before; got != 0 {
		t.Fatalf("C applied %d additional mutation(s) after the retry, want 0 (the tracked request must have been recognized as already-installed, never as unseen)", got)
	}
}

// isolatedSender gates real-transport RPC sends for one node behind a
// blocked-address set, honoring Milestone 6's directional-fault-control
// design (item 92: reuse/extend the existing fault harness rather than
// building a new one) — extended here to also cover Milestone 7's
// InstallSnapshot RPC, which the original internal/service faultNet
// (Milestone 6) predates.
type isolatedSender struct {
	mu      sync.Mutex
	blocked map[string]bool
}

func newIsolatedSender() *isolatedSender { return &isolatedSender{blocked: map[string]bool{}} }

func (s *isolatedSender) block(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocked[addr] = true
}

func (s *isolatedSender) allow(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blocked, addr)
}

func (s *isolatedSender) isBlocked(addr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blocked[addr]
}

func (s *isolatedSender) wireInto(n *raft.Node) {
	n.SetVoteSend(func(ctx context.Context, addr string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
		if s.isBlocked(addr) {
			return raft.RequestVoteResponse{}, errIsolated
		}
		msg := transport.NewMessage(transport.MessageRequestVote, raft.EncodeRequestVote(req))
		resp, err := transport.Send(ctx, addr, msg)
		if err != nil {
			return raft.RequestVoteResponse{}, err
		}
		return raft.DecodeRequestVoteResponse(resp.Payload)
	})
	n.SetAppendSend(func(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
		if s.isBlocked(addr) {
			return raft.AppendEntriesResponse{}, errIsolated
		}
		payload, err := raft.EncodeAppendEntries(req)
		if err != nil {
			return raft.AppendEntriesResponse{}, err
		}
		msg := transport.NewMessage(transport.MessageAppendEntries, payload)
		resp, err := transport.Send(ctx, addr, msg)
		if err != nil {
			return raft.AppendEntriesResponse{}, err
		}
		return raft.DecodeAppendEntriesResponse(resp.Payload)
	})
	n.SetInstallSnapshotSend(func(ctx context.Context, addr string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
		if s.isBlocked(addr) {
			return raft.InstallSnapshotResponse{}, errIsolated
		}
		payload, err := raft.EncodeInstallSnapshot(req)
		if err != nil {
			return raft.InstallSnapshotResponse{}, err
		}
		msg := transport.NewMessage(transport.MessageInstallSnapshot, payload)
		resp, err := transport.Send(ctx, addr, msg)
		if err != nil {
			return raft.InstallSnapshotResponse{}, err
		}
		return raft.DecodeInstallSnapshotResponse(resp.Payload)
	})
}

var errIsolated = errors.New("install_snapshot_dedup_test: destination isolated")

func eventuallyTrue(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(3 * time.Millisecond)
	}
}
