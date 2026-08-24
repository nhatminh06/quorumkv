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

func TestClientRedirectLoopIsBounded(t *testing.T) {
	// A and B perpetually redirect to each other — a real misconfigured
	// or flapping cluster could in principle do this; the client must
	// not loop forever.
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := c.Put(ctx, []byte("x"), []byte("1"))
	if err == nil {
		t.Fatalf("Put succeeded despite an infinite redirect loop")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Put took %v, want well under 1s (redirect bound should stop it quickly)", elapsed)
	}
}

func TestClientDoesNotRetryAfterTransportFailure(t *testing.T) {
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
	if err := c.Put(ctx, []byte("x"), []byte("1")); err == nil {
		t.Fatalf("Put succeeded despite handler failure, want error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want exactly 1 (no blind retry)", got)
	}
}

func TestClientUnknownLeaderNoHint(t *testing.T) {
	tr := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Put(ctx, []byte("x"), []byte("1"))
	if !errors.Is(err, ErrNoLeaderKnown) {
		t.Fatalf("err = %v, want ErrNoLeaderKnown", err)
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
