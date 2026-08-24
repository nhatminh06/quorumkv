package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/clientproto"
	"quorumkv/internal/transport"
)

// startFakeServer starts a real TCP listener whose client-protocol
// responses are entirely scripted by respond, so client's redirect logic
// can be tested without a real Raft cluster.
func startFakeServer(t *testing.T, respond func(req clientproto.Request) clientproto.Response) *transport.Transport {
	t.Helper()
	tr, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, m transport.Message) (transport.Message, error) {
		req, err := clientproto.DecodeRequest(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		payload, err := clientproto.EncodeResponse(respond(req))
		if err != nil {
			return transport.Message{}, err
		}
		return transport.NewMessage(transport.MessageClientResponse, payload), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func TestClientFollowsSingleRedirect(t *testing.T) {
	var leaderAddrVal atomic.Pointer[string]
	leader := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK}
	})
	addr := leader.Addr()
	leaderAddrVal.Store(&addr)

	follower := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(*leaderAddrVal.Load())}
	})

	c := New(follower.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// TestClientWriteRedirectLoopIsBoundedByContext is item 56: unlike the
// old read-style redirect chase (bounded by a fixed attempt count), a
// write's redirect/retry loop is deliberately unbounded in attempts —
// bounded only by ctx — because a retried write is now safe (same
// request identity, server-side dedup). A and B perpetually redirecting
// to each other must not hang past ctx, and must not succeed.
func TestClientWriteRedirectLoopIsBoundedByContext(t *testing.T) {
	var addrAVal, addrBVal atomic.Pointer[string]
	a := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(*addrBVal.Load())}
	})
	b := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(*addrAVal.Load())}
	})
	addrA, addrB := a.Addr(), b.Addr()
	addrAVal.Store(&addrA)
	addrBVal.Store(&addrB)

	c := New(addrA)
	budget := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	err := c.Put(ctx, []byte("x"), []byte("1"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("Put succeeded despite an infinite redirect loop")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed < budget || elapsed > budget+500*time.Millisecond {
		t.Fatalf("Put took %v, want approximately the %v ctx budget", elapsed, budget)
	}
}

// TestClientRetriesWriteAfterTransportFailureWithSameIdentity is items
// 55/59/94/97: a PUT that fails at the transport level is now safely
// retried (never true for the old conservative behavior) — and every
// retry must carry the exact same ClientID/Sequence (item 59's mandatory
// rule), never a freshly allocated one.
func TestClientRetriesWriteAfterTransportFailureWithSameIdentity(t *testing.T) {
	var calls atomic.Int64
	var firstReq atomic.Pointer[clientproto.Request]
	tr, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, m transport.Message) (transport.Message, error) {
		n := calls.Add(1)
		req, decErr := clientproto.DecodeRequest(m.Payload)
		if decErr == nil {
			if n == 1 {
				firstReq.Store(&req)
			} else if first := firstReq.Load(); first != nil &&
				(req.ClientID != first.ClientID || req.Sequence != first.Sequence) {
				t.Errorf("retry %d used identity (%x,%d), want the original (%x,%d)", n, req.ClientID, req.Sequence, first.ClientID, first.Sequence)
			}
		}
		if n < 3 {
			return transport.Message{}, errors.New("simulated transport failure")
		}
		payload, _ := clientproto.EncodeResponse(clientproto.Response{Status: clientproto.StatusOK})
		return transport.NewMessage(transport.MessageClientResponse, payload), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler called %d times, want exactly 3 (2 failures + 1 success)", got)
	}
}

// TestClientWriteRetriesNotLeaderWithoutHint proves a write facing a
// NOT_LEADER response with no hint (e.g. mid-election) retries — rather
// than giving up immediately the way GET still does — bounded by ctx.
func TestClientWriteRetriesNotLeaderWithoutHint(t *testing.T) {
	var calls atomic.Int64
	tr := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		calls.Add(1)
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := c.Put(ctx, []byte("x"), []byte("1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("handler called %d times, want multiple retries before ctx expired", calls.Load())
	}
}

// TestClientGetUnknownLeaderNoHintFailsImmediately proves GET keeps the
// original (Milestone 5-8) conservative behavior: no hint means no
// retry, immediate ErrNoLeaderKnown.
func TestClientGetUnknownLeaderNoHintFailsImmediately(t *testing.T) {
	tr := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := c.Get(ctx, []byte("x"))
	if !errors.Is(err, ErrNoLeaderKnown) {
		t.Fatalf("err = %v, want ErrNoLeaderKnown", err)
	}
}

func TestClientGetDoesNotRetryAfterTransportFailure(t *testing.T) {
	var calls atomic.Int64
	tr, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, m transport.Message) (transport.Message, error) {
		calls.Add(1)
		return transport.Message{}, errors.New("handler failure")
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := c.Get(ctx, []byte("x")); err == nil {
		t.Fatalf("Get succeeded despite handler failure, want error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want exactly 1 (GET is not retried)", got)
	}
}

func TestClientGetNotFound(t *testing.T) {
	tr := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotFound}
	})
	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, ok, err := c.Get(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false for NOT_FOUND")
	}
}
