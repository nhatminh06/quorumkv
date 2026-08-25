package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// setTestBeforeBatch installs fn as n's testBeforeBatch hook under
// queueMu — the same lock proposalWorker reads it through — so the
// write is guaranteed visible to the worker's next read rather than
// racing it (both a data race and, worse, a timing race that could let
// the worker persist a batch before ever observing the hook).
func setTestBeforeBatch(n *Node, fn func()) {
	n.queueMu.Lock()
	n.testBeforeBatch = fn
	n.queueMu.Unlock()
}

// singleVoterLeader builds a one-node cluster (its own trivial majority)
// and elects it, so Propose can be exercised without needing real peers.
func singleVoterLeader(t *testing.T) *Node {
	t.Helper()
	n := newFakeNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	return n
}

// TestProposeContiguousIndexesUnderConcurrency is item 33/118/119: many
// concurrent Propose calls must receive a contiguous run of indexes (no
// gaps, no duplicates, in an order consistent with queue arrival being
// possible in any interleaving) and — the actual point of batching —
// the resulting Stats() must show fewer log-rewrite batches than
// proposals, proving concurrent proposals actually shared durable
// writes rather than each getting its own.
func TestProposeContiguousIndexesUnderConcurrency(t *testing.T) {
	n := singleVoterLeader(t)
	const count = 200

	var wg sync.WaitGroup
	indexes := make([]LogIndex, count)
	errs := make([]error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			idx, _, err := n.Propose([]byte("v"))
			indexes[i], errs[i] = idx, err
		}(i)
	}
	wg.Wait()

	seen := make(map[LogIndex]bool, count)
	var min, max LogIndex
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Propose[%d]: %v", i, err)
		}
		if seen[indexes[i]] {
			t.Fatalf("index %d returned to more than one caller", indexes[i])
		}
		seen[indexes[i]] = true
		if min == 0 || indexes[i] < min {
			min = indexes[i]
		}
		if indexes[i] > max {
			max = indexes[i]
		}
	}
	if int(max-min)+1 != count {
		t.Fatalf("indexes span [%d,%d] (%d values), want exactly %d contiguous values", min, max, max-min+1, count)
	}
	for idx := min; idx <= max; idx++ {
		if !seen[idx] {
			t.Fatalf("index %d missing from the contiguous run [%d,%d]", idx, min, max)
		}
	}

	stats := n.Stats()
	if stats.ProposalsAdmitted < count {
		t.Fatalf("Stats().ProposalsAdmitted = %d, want >= %d", stats.ProposalsAdmitted, count)
	}
	if stats.ProposalBatches == 0 {
		t.Fatalf("Stats().ProposalBatches = 0, want > 0")
	}
	if stats.ProposalBatches >= int64(count) {
		t.Fatalf("Stats().ProposalBatches = %d, want < %d proposals — batching under concurrency produced no fewer log rewrites than proposals", stats.ProposalBatches, count)
	}
	t.Logf("%d proposals persisted in %d batches (%.1f entries/batch avg)", count, stats.ProposalBatches, float64(stats.ProposalBatchEntries)/float64(stats.ProposalBatches))
}

// TestProposeBackpressureDeterministic is item 111: with tiny bounds and
// the worker deterministically held still (via testBeforeBatch, not
// timing), filling the queue to exactly its configured capacity must
// make the next Propose fail with ErrBackpressure — every time, not
// just under load.
func TestProposeBackpressureDeterministic(t *testing.T) {
	n := singleVoterLeader(t)
	n.SetProposalLimits(2, 1<<20, DefaultMaxProposalBatchEntries, DefaultMaxProposalBatchBytes)

	release := make(chan struct{})
	held := make(chan struct{}, 1)
	setTestBeforeBatch(n, func() {
		select {
		case held <- struct{}{}:
		default:
		}
		<-release
	})

	// This Propose's entry is the one the worker will dequeue and then
	// block on inside testBeforeBatch — it still counts against the
	// queue bound (see admitProposal's doc comment: queuedProposals
	// counts dequeued-but-unpersisted entries too).
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := n.Propose([]byte("first"))
		firstDone <- err
	}()
	<-held // the worker has dequeued "first" and is now blocked

	// Capacity is 2; "first" already occupies one slot. Exactly one more
	// admission must succeed before the queue is full.
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := n.Propose([]byte("second"))
		secondDone <- err
	}()
	// No deterministic signal for "second" having been admitted short of
	// racing the channel; give it a moment to reach admitProposal (it
	// cannot proceed past there since the worker is held, so once
	// admitted it just sits, making this safe to poll for).
	deadline := time.After(2 * time.Second)
	for {
		n.queueMu.Lock()
		depth := n.queuedProposals
		n.queueMu.Unlock()
		if depth == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("queue never reached depth 2 (stuck at %d)", depth)
		case <-time.After(time.Millisecond):
		}
	}

	if _, _, err := n.Propose([]byte("third")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Propose at capacity: err = %v, want ErrBackpressure", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Propose (after release): %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Propose (after release): %v", err)
	}

	stats := n.Stats()
	if stats.ProposalsRejectedBusy != 1 {
		t.Fatalf("Stats().ProposalsRejectedBusy = %d, want 1", stats.ProposalsRejectedBusy)
	}
}

// TestProposeQueueByteLimitDeterministic is item 112: a byte bound must
// reject admission even when the count bound has room.
func TestProposeQueueByteLimitDeterministic(t *testing.T) {
	n := singleVoterLeader(t)
	n.SetProposalLimits(DefaultMaxQueuedProposals, 1000, DefaultMaxProposalBatchEntries, DefaultMaxProposalBatchBytes)

	release := make(chan struct{})
	held := make(chan struct{}, 1)
	setTestBeforeBatch(n, func() {
		select {
		case held <- struct{}{}:
		default:
		}
		<-release
	})

	first := make([]byte, 400)
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := n.Propose(first)
		firstDone <- err
	}()
	<-held // "first" dequeued and held; still counts as 400 queued bytes

	// "second" can only be ADMITTED while the worker is held (persisting
	// nothing yet) — it cannot itself complete until release is closed,
	// so it must run in its own goroutine, then be waited for via the
	// queue depth rather than its own completion.
	second := make([]byte, 400)
	secondDone := make(chan error, 1)
	go func() {
		_, _, err := n.Propose(second)
		secondDone <- err
	}()
	deadline := time.After(2 * time.Second)
	for {
		n.queueMu.Lock()
		bytes := n.queuedProposalBytes
		n.queueMu.Unlock()
		if bytes == 800 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("queued bytes never reached 800 (stuck at %d)", bytes)
		case <-time.After(time.Millisecond):
		}
	}

	third := make([]byte, 400)
	if _, _, err := n.Propose(third); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("third Propose (would reach 1200/1000 bytes): err = %v, want ErrBackpressure", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Propose (after release): %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Propose (after release): %v", err)
	}
}

// TestProposeFailsAfterLeadershipLost is item 60: a queued proposal must
// fail with ErrNotLeader (never silently append as a follower) if this
// node stops being Leader before the batch containing it is persisted.
func TestProposeFailsAfterLeadershipLost(t *testing.T) {
	n := singleVoterLeader(t)

	release := make(chan struct{})
	setTestBeforeBatch(n, func() { <-release })

	done := make(chan error, 1)
	go func() {
		_, _, err := n.Propose([]byte("x"))
		done <- err
	}()

	// Give the worker a moment to actually dequeue into testBeforeBatch
	// before forcing a step-down, so this exercises "already dequeued,
	// not yet persisted" rather than racing admission itself.
	time.Sleep(20 * time.Millisecond)
	n.mu.Lock()
	n.stepToFollowerLocked()
	n.mu.Unlock()

	close(release)
	if err := <-done; !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose after step-down mid-queue: err = %v, want ErrNotLeader", err)
	}
	if n.LastLogIndex() != 0 {
		t.Fatalf("LastLogIndex() = %d, want 0 — a rejected batch must never append as a follower", n.LastLogIndex())
	}
}

// TestNodeCloseFailsQueuedProposals is item 62/103: every proposal still
// queued when Close runs must resolve (bounded) with ErrNodeClosed, and
// Close itself must return (no goroutine leak) rather than hang waiting
// on work that will never be admitted again.
func TestNodeCloseFailsQueuedProposals(t *testing.T) {
	n := newFakeNode(t, 1, nil)
	// Deliberately not elected leader — proposals will queue and then
	// fail at persist time regardless, but the point here is Close
	// draining ones still sitting admitted-but-unprocessed.
	n.SetProposalLimits(1, 1<<20, DefaultMaxProposalBatchEntries, DefaultMaxProposalBatchBytes)

	release := make(chan struct{})
	setTestBeforeBatch(n, func() { <-release })

	done := make(chan error, 1)
	go func() {
		_, _, err := n.Propose([]byte("x"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // let it reach testBeforeBatch

	closeDone := make(chan struct{})
	go func() {
		n.Close()
		close(closeDone)
	}()

	// Close must be able to complete once the worker unblocks and drains
	// — release it, then require both Close and the pending Propose to
	// resolve promptly.
	time.Sleep(20 * time.Millisecond)
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNodeClosed) {
			t.Fatalf("queued Propose during Close: err = %v, want ErrNotLeader or ErrNodeClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propose never returned after Close")
	}
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close never returned")
	}

	if _, _, err := n.Propose([]byte("y")); !errors.Is(err, ErrNodeClosed) {
		t.Fatalf("Propose after Close: err = %v, want ErrNodeClosed", err)
	}
}
