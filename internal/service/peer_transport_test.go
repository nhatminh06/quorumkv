package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/transport"
)

func TestPeerTransportReconnectAndFollowerCatchUp(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	leader, follower := nodes[0], nodes[1]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := client.New(leader.addr())
	if err := c.Put(ctx, []byte("before"), []byte("break")); err != nil {
		t.Fatal(err)
	}
	waitForClusterCommit(t, time.Second, nodes, leader.svc.node.LastLogIndex())
	before := leader.svc.node.TransportStats()
	addr := follower.addr()
	// Closing the real listener forcibly closes its established TCP sessions.
	// Keep it offline while the other majority commits a suffix, then restore
	// the same address. No injected successful responses or Raft state edits.
	follower.tr.Close()
	for i := 0; i < 20; i++ {
		if err := c.Put(ctx, []byte(fmt.Sprintf("offline-%d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := transport.Listen(addr, follower.svc.Handler())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	target := leader.svc.node.LastLogIndex()
	waitForClusterCommit(t, 5*time.Second, []*testNode{follower}, target)
	if err := follower.svc.node.WaitApplied(ctx, target, 0); err != nil {
		t.Fatal(err)
	}
	after := leader.svc.node.TransportStats()
	if after.ConnectionsDialed <= before.ConnectionsDialed || after.SendFailures <= before.SendFailures || after.ConnectionsReused <= before.ConnectionsReused {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	value, found, err := c.Get(ctx, []byte("offline-19"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("Get: %q %v %v", value, found, err)
	}
}

func TestClientPoolFollowsLeaderFailoverAndReusesNewLeader(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	c := client.New(nodes[0].addr(), nodes[1].addr(), nodes[2].addr())
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("before-failover"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	beforeIndex := nodes[0].svc.node.LastLogIndex()
	waitForClusterCommit(t, 2*time.Second, nodes, beforeIndex)
	for _, node := range nodes {
		if err := node.svc.node.WaitApplied(ctx, beforeIndex, 0); err != nil {
			t.Fatal(err)
		}
	}
	nodes[0].tr.Close()
	nodes[0].svc.node.Close()
	electLeaderAmong(t, []*testNode{nodes[2]}, nodes, 1)
	if err := c.Put(ctx, []byte("after-failover"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, found, err := c.Get(ctx, []byte("after-failover"))
		if err != nil || !found || string(got) != "value" {
			t.Fatalf("Get after failover = %q, %v, %v", got, found, err)
		}
	}
	stats := c.Stats()
	if stats.SendFailures == 0 || stats.ConnectionsDialed < 2 || stats.ConnectionsReused < 19 {
		t.Fatalf("failover/reuse not demonstrated: %+v", stats)
	}
}

func TestClientPoolReconnectsAfterServerRestartSameAddress(t *testing.T) {
	nodes := startCluster(t, 1)
	electLeader(t, nodes, 0)
	n := nodes[0]
	c := client.New(n.addr())
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	addr := n.addr()
	n.tr.Close()
	restarted, err := transport.Listen(addr, n.svc.Handler())
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	// The first exchange may discover that the retained socket was closed.
	_, _, _ = c.Get(ctx, []byte("key"))
	got, found, err := c.Get(ctx, []byte("key"))
	if err != nil || !found || string(got) != "value" {
		t.Fatalf("Get after server restart = %q, %v, %v", got, found, err)
	}
	stats := c.Stats()
	if stats.SendFailures == 0 || stats.ConnectionsDialed < 2 {
		t.Fatalf("restart did not discard and redial: %+v", stats)
	}
}

// Extend the existing real-TCP cluster harness with independent writer
// identities and read-after-write checks. All workers and transport resources
// are joined; race runs exercise the same workload without exclusions.
func TestPersistentPeerConcurrentPutGetStress(t *testing.T) {
	nodes := startCluster(t, 3)
	electLeader(t, nodes, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const workers, rounds = 32, 100
	var wg sync.WaitGroup
	var clientDialed, clientReused, clientClosed atomic.Uint64
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c := client.New(nodes[0].addr())
			defer func() {
				before := c.Stats()
				clientDialed.Add(before.ConnectionsDialed)
				clientReused.Add(before.ConnectionsReused)
				if err := c.Close(); err != nil {
					t.Errorf("Close client: %v", err)
				}
				after := c.Stats()
				clientClosed.Add(after.ConnectionsClosed)
				if after.ActiveConnections != 0 || after.Waiters != 0 {
					t.Errorf("client resources remain after Close: %+v", after)
				}
			}()
			key := []byte(fmt.Sprintf("worker-%d", worker))
			for round := 0; round < rounds; round++ {
				want := fmt.Sprintf("%d/%d", worker, round)
				if err := c.Put(ctx, key, []byte(want)); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				got, found, err := c.Get(ctx, key)
				if err != nil || !found || string(got) != want {
					t.Errorf("Get: %q %v %v want %q", got, found, err, want)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	if clientDialed.Load() > workers*transport.DefaultClientPoolWidth || clientReused.Load() < workers*(rounds*2-2) || clientClosed.Load() != clientDialed.Load() {
		t.Fatalf("client pool reuse/bound/cleanup failed: dialed=%d reused=%d closed=%d", clientDialed.Load(), clientReused.Load(), clientClosed.Load())
	}
	waitForClusterCommit(t, 5*time.Second, nodes, nodes[0].svc.node.LastLogIndex())
	stats := nodes[0].svc.node.TransportStats()
	if stats.ConnectionsReused < workers*rounds || stats.ConnectionsDialed >= stats.ConnectionsReused {
		t.Fatalf("reuse not demonstrated: %+v", stats)
	}
	t.Logf("6400 PUT/GET operations; client dialed=%d reused=%d; peer stats: %+v", clientDialed.Load(), clientReused.Load(), stats)
	// Assert all owned outgoing sockets are released after concurrent work.
	for _, node := range nodes {
		node.tr.Close()
		node.svc.node.Close()
	}
	for _, node := range nodes {
		s := node.svc.node.TransportStats()
		if s.ConnectionsDialed != s.ConnectionsClosed {
			t.Fatalf("unclosed peer sockets: %+v", s)
		}
	}
}
