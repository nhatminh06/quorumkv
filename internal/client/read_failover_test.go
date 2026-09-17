package client

import (
	"context"
	"errors"
	"fmt"
	"quorumkv/internal/transport"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/clientproto"
)

func TestClientGetFollowerWithoutLeaderHintTriesOtherSeeds(t *testing.T) {
	var followerCalls, leaderCalls atomic.Int32
	follower := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		followerCalls.Add(1)
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	leader := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		leaderCalls.Add(1)
		return clientproto.Response{Status: clientproto.StatusOK, Value: []byte("world")}
	})
	c := New(follower.Addr(), leader.Addr(), follower.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		value, found, err := c.Get(ctx, []byte("hello"))
		if err != nil || !found || string(value) != "world" {
			t.Fatalf("Get = %q, %v, %v", value, found, err)
		}
	}
	if followerCalls.Load() != 1 || leaderCalls.Load() != 2 {
		t.Fatal("successful leader was not cached")
	}
}

func TestClientGetSeedFailover(t *testing.T) {
	for _, scenario := range []string{"unreachable", "stale", "cycle", "malformed", "cached"} {
		t.Run(scenario, func(t *testing.T) {
			var aHint, bHint string
			var aCalls, bCalls atomic.Int32
			a := startFakeServer(t, func(clientproto.Request) clientproto.Response {
				aCalls.Add(1)
				return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(aHint)}
			})
			b := startFakeServer(t, func(clientproto.Request) clientproto.Response {
				bCalls.Add(1)
				return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(bHint)}
			})
			leader := startFakeServer(t, func(clientproto.Request) clientproto.Response {
				return clientproto.Response{Status: clientproto.StatusOK}
			})
			seeds := []string{a.Addr(), b.Addr(), leader.Addr()}
			switch scenario {
			case "unreachable":
				a.Close()
			case "stale":
				aHint = b.Addr()
				b.Close()
			case "cycle":
				aHint, bHint = b.Addr(), a.Addr()
			case "malformed":
				aHint = "invalid address"
			case "cached":
				seeds = []string{leader.Addr()}
			}
			c := New(seeds...)
			if scenario == "cached" {
				c.leader = a.Addr()
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, _, err := c.Get(ctx, []byte("x")); err != nil {
				t.Fatal(err)
			}
			if aCalls.Load() > 1 || bCalls.Load() > 1 {
				t.Fatal("repeated an address")
			}
		})
	}
}

func TestClientGetExhaustsSeedsOnce(t *testing.T) {
	var calls atomic.Int32
	follower := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		calls.Add(1)
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := New(follower.Addr(), follower.Addr()).Get(ctx, []byte("x"))
	if !errors.Is(err, ErrNoLeaderKnown) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestClientGetCancellationDuringFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	follower := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		cancel()
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	var calls atomic.Int32
	other := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		calls.Add(1)
		return clientproto.Response{Status: clientproto.StatusOK}
	})
	start := time.Now()
	_, _, err := New(follower.Addr(), other.Addr()).Get(ctx, []byte("x"))
	if !errors.Is(err, context.Canceled) || calls.Load() != 0 || time.Since(start) > time.Second {
		t.Fatalf("err=%v calls=%d elapsed=%v", err, calls.Load(), time.Since(start))
	}
}

func TestClientGetHintBudgetStillTriesSeeds(t *testing.T) {
	var calls atomic.Int32
	hint := ""
	for i := 0; i < maxRedirects+2; i++ {
		next := hint
		node := startFakeServer(t, func(clientproto.Request) clientproto.Response {
			calls.Add(1)
			return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(next)}
		})
		hint = node.Addr()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := New(hint).Get(ctx, []byte("x"))
	if !errors.Is(err, ErrTooManyRedirects) || calls.Load() != int32(maxRedirects+1) {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	leader := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK}
	})
	if _, _, err := New(hint, leader.Addr()).Get(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestClientGetTerminalStatusesDoNotFailOver(t *testing.T) {
	for _, status := range []clientproto.Status{clientproto.StatusNotFound, clientproto.StatusTimeout, clientproto.StatusBusy, clientproto.StatusBadRequest, clientproto.StatusInternalError} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			first := startFakeServer(t, func(clientproto.Request) clientproto.Response { return clientproto.Response{Status: status} })
			var calls atomic.Int32
			second := startFakeServer(t, func(clientproto.Request) clientproto.Response {
				calls.Add(1)
				return clientproto.Response{Status: clientproto.StatusOK}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _, err := New(first.Addr(), second.Addr()).Get(ctx, []byte("x"))
			if !errors.Is(err, statusErr(status)) || calls.Load() != 0 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestClientGetDeadlineDuringFailover(t *testing.T) {
	entered := make(chan struct{})
	stalled, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, _ transport.Message) (transport.Message, error) {
		close(entered)
		<-ctx.Done()
		return transport.Message{}, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	follower := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err = New(follower.Addr(), stalled.Addr()).Get(ctx, []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("err=%v elapsed=%v", err, time.Since(start))
	}
	select {
	case <-entered:
	default:
		t.Fatal("did not reach second seed")
	}
}

func TestClientGetAllSeedsUnreachable(t *testing.T) {
	var seeds []string
	for i := 0; i < 2; i++ {
		node := startFakeServer(t, func(clientproto.Request) clientproto.Response { return clientproto.Response{} })
		seeds = append(seeds, node.Addr())
		node.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := New(seeds...).Get(ctx, []byte("x"))
	if err == nil || ctx.Err() != nil {
		t.Fatalf("want transport error before deadline, got %v", err)
	}
}
