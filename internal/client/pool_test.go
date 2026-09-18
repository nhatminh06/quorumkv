package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/clientproto"
)

func TestClientReusesConnectionAndClose(t *testing.T) {
	tr := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK, Value: append([]byte(nil), req.Key...)}
	})
	c := New(tr.Addr())
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		got, ok, err := c.Get(context.Background(), key)
		if err != nil || !ok || string(got) != string(key) {
			t.Fatalf("Get(%q) = %q, %v, %v", key, got, ok, err)
		}
	}
	stats := c.Stats()
	if stats.ConnectionsDialed != 1 || stats.ConnectionsReused != 99 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := c.Stats(); stats.ActiveConnections != 0 {
		t.Fatalf("active after Close = %d", stats.ActiveConnections)
	}
	if _, _, err := c.Get(context.Background(), []byte("closed")); err == nil {
		t.Fatal("Get after Close succeeded")
	}
}

func TestClientConcurrentGetResponsesMatch(t *testing.T) {
	tr := startFakeServer(t, func(req clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK, Value: append([]byte(nil), req.Key...)}
	})
	c := New(tr.Addr())
	defer c.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("%d-%d", worker, i))
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				got, ok, err := c.Get(ctx, key)
				cancel()
				if err != nil || !ok || string(got) != string(key) {
					t.Errorf("Get(%q) = %q, %v, %v", key, got, ok, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	stats := c.Stats()
	if stats.ConnectionsDialed > 8 || stats.ActiveConnections > 8 {
		t.Fatalf("pool exceeded bound: %+v", stats)
	}
	if stats.ConnectionsReused < 3000 {
		t.Fatalf("too few reuses: %+v", stats)
	}
}

func TestFollowerResponseDoesNotPoisonConnection(t *testing.T) {
	tr := startFakeServer(t, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader}
	})
	c := New(tr.Addr())
	defer c.Close()
	for i := 0; i < 2; i++ {
		if _, _, err := c.Get(context.Background(), []byte("key")); err == nil {
			t.Fatal("Get unexpectedly succeeded")
		}
	}
	stats := c.Stats()
	if stats.ConnectionsDialed != 1 || stats.ConnectionsReused != 1 {
		t.Fatalf("valid NOT_LEADER poisoned session: %+v", stats)
	}
}
