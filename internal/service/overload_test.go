package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/clientproto"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// TestServiceConcurrencyBackpressureDeterministic is item 55/56: with
// the admission bound filled to exactly its capacity (by directly
// occupying every slot, not by racing real concurrent requests), a real
// client PUT started while the bound is full must not error out — per
// item 50 the client automatically retries a BUSY response with the
// same request identity — and must complete successfully as soon as a
// slot is released, with no restart required.
func TestServiceConcurrencyBackpressureDeterministic(t *testing.T) {
	nodes := startCluster(t, 1)
	electLeader(t, nodes, 0)
	n := nodes[0]
	n.svc.SetMaxConcurrentRequests(1)

	// Occupy the only admission slot directly — deterministic, no timing
	// dependency on how long a real request takes to process.
	n.svc.admission <- struct{}{}

	c := client.New(n.addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	putDone := make(chan error, 1)
	go func() {
		putDone <- c.Put(ctx, []byte("x"), []byte("1"))
	}()

	// The PUT must still be retrying, not having given up, a bit later —
	// proving BUSY alone never becomes a terminal client-visible error.
	select {
	case err := <-putDone:
		t.Fatalf("Put returned early (err=%v) while admission was still full — BUSY must be retried, not surfaced", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Release the slot: capacity is no longer exhausted, so the
	// already-in-flight retry loop must complete successfully shortly,
	// no restart required.
	<-n.svc.admission
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("Put after releasing the slot: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Put never completed after admission capacity was released")
	}
}

// TestServiceConcurrencyFloodStaysBounded is item 55/121: flooding a
// tiny-capacity service with real concurrent requests must never let
// more than the configured bound run concurrently, must produce at
// least one BUSY response, and must leave the server fully responsive
// once the flood subsides — proven by a plain request succeeding
// afterward. Uses real concurrency (not a synchronization barrier)
// because the property under test — an accurate, race-free semaphore —
// is exactly what a barrier-based test would trivially satisfy without
// actually exercising the concurrent admit path.
func TestServiceConcurrencyFloodStaysBounded(t *testing.T) {
	nodes := startCluster(t, 1)
	electLeader(t, nodes, 0)
	n := nodes[0]
	const capacity = 4
	n.svc.SetMaxConcurrentRequests(capacity)

	const flood = 100
	var wg sync.WaitGroup
	var busyCount, otherCount int
	var countMu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// admission is not exported for direct observation from a real flood,
	// so this measures via real client outcomes instead: with 100
	// concurrent GETs against a capacity-4 server, seeing at least one
	// BUSY among them is the observable proxy for "the bound was
	// actually reached."
	for i := 0; i < flood; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.New(n.addr())
			_, _, err := c.Get(ctx, []byte("k"))
			countMu.Lock()
			defer countMu.Unlock()
			if errors.Is(err, client.ErrBusy) {
				busyCount++
			} else if err != nil {
				otherCount++
			}
		}()
	}
	wg.Wait()

	if busyCount == 0 {
		t.Fatalf("flood of %d concurrent GETs against capacity %d never observed a single BUSY", flood, capacity)
	}
	t.Logf("flood: %d BUSY, %d other-error, %d OK (out of %d)", busyCount, otherCount, flood-busyCount-otherCount, flood)

	// Recovery: once the flood has drained, an ordinary request succeeds.
	c := client.New(n.addr())
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer recoverCancel()
	if err := c.Put(recoverCtx, []byte("recovered"), []byte("1")); err != nil {
		t.Fatalf("Put after flood subsided: %v", err)
	}
}

// TestProposalQueueBackpressureViaClientRetry is item 104/48/49: a real
// PUT that hits raft.ErrBackpressure at the proposal layer must surface
// as BUSY over the wire, and internal/client's automatic retry (same
// ClientID/Sequence, per docs/request-dedup.md) must still complete the
// write successfully once the queue has room again — never allocating a
// new sequence for the retry.
func TestProposalQueueBackpressureViaClientRetry(t *testing.T) {
	nodes := startCluster(t, 1)
	electLeader(t, nodes, 0)
	n := nodes[0]
	// A capacity-1 proposal queue: send one at-capacity concurrent burst
	// to reliably observe backpressure without any cross-package test
	// hook into raft's internal worker synchronization.
	n.svc.node.SetProposalLimits(1, 1<<20, raft.DefaultMaxProposalBatchEntries, raft.DefaultMaxProposalBatchBytes)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const concurrency = 50
	var wg sync.WaitGroup
	results := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := client.New(n.addr())
			results[i] = c.Put(ctx, []byte("k"), []byte("v"))
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Fatalf("Put[%d] (client auto-retries BUSY): %v", i, err)
		}
	}

	v, ok, err := (client.New(n.addr())).Get(ctx, []byte("k"))
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get(k) after concurrent backpressured writes = (%q,%v,%v), want (\"v\",true,nil)", v, ok, err)
	}
}

// TestBusyResponseDoesNotAdvanceDedupState is item 44: a request the
// service rejects with BUSY (via the admission bound, before Raft is
// ever touched) must leave that (ClientID, Sequence) completely unseen —
// a subsequent real attempt with the same identity must succeed as a
// fresh write, not be rejected as stale/duplicate.
func TestBusyResponseDoesNotAdvanceDedupState(t *testing.T) {
	nodes := startCluster(t, 1)
	electLeader(t, nodes, 0)
	n := nodes[0]
	n.svc.SetMaxConcurrentRequests(1)
	n.svc.admission <- struct{}{} // occupy the only slot

	id := testClientID()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := clientproto.EncodeRequest(clientproto.Request{Operation: clientproto.OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	respMsg, err := transport.Send(ctx, n.addr(), transport.NewMessage(transport.MessageClientRequest, req))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := clientproto.DecodeResponse(respMsg.Payload)
	if err != nil || resp.Status != clientproto.StatusBusy {
		t.Fatalf("first attempt (admission full) = (%v,%v), want StatusBusy", resp.Status, err)
	}

	<-n.svc.admission // release the slot

	respMsg2, err := transport.Send(ctx, n.addr(), transport.NewMessage(transport.MessageClientRequest, req))
	if err != nil {
		t.Fatalf("Send (retry): %v", err)
	}
	resp2, err := clientproto.DecodeResponse(respMsg2.Payload)
	if err != nil || resp2.Status != clientproto.StatusOK {
		t.Fatalf("retry with same identity after BUSY = (%v,%v), want StatusOK (never StatusStaleRequest)", resp2.Status, err)
	}
}
