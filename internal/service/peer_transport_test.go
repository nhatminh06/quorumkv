package service

import (
	"context"
	"fmt"
	"sync"
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
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c := client.New(nodes[0].addr())
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
	waitForClusterCommit(t, 5*time.Second, nodes, nodes[0].svc.node.LastLogIndex())
	stats := nodes[0].svc.node.TransportStats()
	if stats.ConnectionsReused < workers*rounds || stats.ConnectionsDialed >= stats.ConnectionsReused {
		t.Fatalf("reuse not demonstrated: %+v", stats)
	}
	t.Logf("6400 PUT/GET operations; peer stats: %+v", stats)
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
