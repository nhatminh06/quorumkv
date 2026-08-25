package service

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// This file is the M13 reproducible benchmark harness: real 3-node
// clusters over real loopback TCP (reusing startCluster/electLeader from
// service_test.go — no separate fake/in-memory cluster implementation),
// the real client protocol, and the real durable log path (no failpoint,
// no skipped fsync — see docs/performance.md's "no unsafe fast mode").
// These are Go benchmarks (Benchmark...), never run by `go test ./...`
// (see -run '^$' in the documented commands in docs/performance.md).

// latencies is a benchmark-local, goroutine-safe latency collector — not
// a monitoring subsystem, just enough to report p50/p95/p99 for one
// benchmark run.
type latencies struct {
	mu  sync.Mutex
	obs []time.Duration
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.obs = append(l.obs, d)
	l.mu.Unlock()
}

func (l *latencies) percentile(p float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.obs) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), l.obs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func (l *latencies) report(b *testing.B) {
	b.Helper()
	l.mu.Lock()
	n := len(l.obs)
	l.mu.Unlock()
	if n == 0 {
		return
	}
	b.ReportMetric(float64(l.percentile(0.50).Microseconds()), "p50-us")
	b.ReportMetric(float64(l.percentile(0.95).Microseconds()), "p95-us")
	b.ReportMetric(float64(l.percentile(0.99).Microseconds()), "p99-us")
}

func valueOfSize(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

// benchCluster brings up a real n-node cluster and elects nodes[0]
// leader, reusing exactly the same helpers the correctness test suite
// uses (see service_test.go) — no separate benchmark-only cluster
// implementation (item 6).
func benchCluster(b *testing.B, n int) ([]*testNode, *testNode) {
	b.Helper()
	nodes := startCluster(b, n)
	electLeader(b, nodes, 0)
	return nodes, nodes[0]
}

// --- Workload A: 3-node cluster, sequential PUT, one client ---

func BenchmarkThreeNodeSequentialPut(b *testing.B) {
	for _, sz := range []int{16, 1024} {
		b.Run(fmt.Sprintf("value=%dB", sz), func(b *testing.B) {
			_, leader := benchCluster(b, 3)
			c := client.New(leader.addr())
			value := valueOfSize(sz)
			lat := &latencies{}
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Appendf(nil, "k%d", i)
				start := time.Now()
				if err := c.Put(ctx, key, value); err != nil {
					b.Fatalf("Put: %v", err)
				}
				lat.add(time.Since(start))
			}
			b.StopTimer()
			lat.report(b)
		})
	}
}

// runConcurrent drives exactly `concurrency` goroutines, each with its
// own Client (own ClientID — item 11), dividing b.N total operations
// evenly across them and calling op(ctx, client, opIndex) for each. This
// is deliberately manual (not b.RunParallel, whose parallelism scales
// with GOMAXPROCS rather than an exact requested count) so "32 clients"
// in docs/performance.md means exactly that on any machine.
func runConcurrent(b *testing.B, addr string, concurrency int, lat *latencies, op func(ctx context.Context, c *client.Client, i int) error) {
	b.Helper()
	ctx := context.Background()
	var wg sync.WaitGroup
	var next atomic.Int64
	b.ResetTimer()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.New(addr)
			for {
				i := next.Add(1) - 1
				if i >= int64(b.N) {
					return
				}
				start := time.Now()
				if err := op(ctx, c, int(i)); err != nil {
					b.Errorf("op[%d]: %v", i, err)
					return
				}
				lat.add(time.Since(start))
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
}

// --- Workload B: 3-node cluster, concurrent PUT, 32 clients ---

func BenchmarkThreeNodeConcurrentPut(b *testing.B) {
	for _, sz := range []int{16, 1024} {
		b.Run(fmt.Sprintf("value=%dB", sz), func(b *testing.B) {
			_, leader := benchCluster(b, 3)
			value := valueOfSize(sz)
			lat := &latencies{}
			runConcurrent(b, leader.addr(), 32, lat, func(ctx context.Context, c *client.Client, i int) error {
				return c.Put(ctx, fmt.Appendf(nil, "k%d", i), value)
			})
			lat.report(b)
		})
	}
}

// --- Workload C: 3-node cluster, concurrent ReadIndex GET, 32 clients ---

func BenchmarkThreeNodeConcurrentGet(b *testing.B) {
	_, leader := benchCluster(b, 3)
	seedClient := client.New(leader.addr())
	value := valueOfSize(16)
	if err := seedClient.Put(context.Background(), []byte("bench-key"), value); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	lat := &latencies{}
	runConcurrent(b, leader.addr(), 32, lat, func(ctx context.Context, c *client.Client, i int) error {
		_, _, err := c.Get(ctx, []byte("bench-key"))
		return err
	})
	lat.report(b)
}

// --- Workload D: 3-node cluster, mixed 80% GET / 20% PUT, 32 clients ---

func BenchmarkThreeNodeMixedReadWrite(b *testing.B) {
	_, leader := benchCluster(b, 3)
	value := valueOfSize(16)
	seedClient := client.New(leader.addr())
	if err := seedClient.Put(context.Background(), []byte("bench-key"), value); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	lat := &latencies{}
	runConcurrent(b, leader.addr(), 32, lat, func(ctx context.Context, c *client.Client, i int) error {
		if i%5 == 0 { // 20% writes
			return c.Put(ctx, []byte("bench-key"), value)
		}
		_, _, err := c.Get(ctx, []byte("bench-key")) // 80% reads
		return err
	})
	lat.report(b)
}

// --- Workload E: follower falls behind, heal, measure catch-up ---

// BenchmarkFollowerCatchUp is not a throughput/latency micro-benchmark
// like A-D — it reports one measurement per b.N iteration (rebuilding
// the whole scenario each time), following the standard go test bench
// convention. See docs/performance.md for how this is actually invoked
// (typically -benchtime=1x, since each iteration is itself an entire
// multi-thousand-entry catch-up).
func BenchmarkFollowerCatchUp(b *testing.B) {
	const laggingEntries = 5000
	value := valueOfSize(256)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodes := startCluster(b, 3)
		electLeader(b, nodes, 0)
		leader := nodes[0]
		follower := nodes[1]

		// Stop the follower's transport so it cannot receive anything,
		// then build up a large retained suffix on the leader.
		follower.tr.Close()
		c := client.New(leader.addr())
		ctx := context.Background()
		for j := 0; j < laggingEntries; j++ {
			key := fmt.Appendf(nil, "k%d", j)
			if err := c.Put(ctx, key, value); err != nil {
				b.Fatalf("Put (building lag): %v", err)
			}
		}
		lastIndex := leader.svc.node.LastLogIndex()

		// Heal: reopen the follower's transport on a fresh port serving
		// the SAME underlying node/service (its log is untouched — only
		// its listener was closed), and tell the leader its new address.
		newTr, err := transport.Listen("127.0.0.1:0", follower.svc.Handler())
		if err != nil {
			b.Fatalf("reopen follower transport: %v", err)
		}
		b.Cleanup(func() { newTr.Close() })
		follower.tr = newTr
		leader.svc.node.SetPeers(map[raft.NodeID]string{
			follower.id: newTr.Addr(),
			nodes[2].id: nodes[2].addr(),
		})

		b.StartTimer()
		start := time.Now()
		waitForClusterCommit(b, 30*time.Second, []*testNode{follower}, lastIndex)
		elapsed := time.Since(start)
		b.StopTimer()

		b.ReportMetric(float64(laggingEntries)/elapsed.Seconds(), "entries/sec")
		b.ReportMetric(elapsed.Seconds(), "catchup-sec")
	}
}
