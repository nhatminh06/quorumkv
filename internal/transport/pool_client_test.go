package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"
)

func poolEchoServer(t testing.TB, handler Handler) *Transport {
	t.Helper()
	tr, err := Listen("127.0.0.1:0", handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func TestPoolClientSequentialReuse(t *testing.T) {
	tr := poolEchoServer(t, func(_ context.Context, m Message) (Message, error) { return m, nil })
	c := NewPoolClient(4)
	defer c.Close()
	for i := 0; i < 100; i++ {
		if _, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, []byte("x")), nil); err != nil {
			t.Fatal(err)
		}
	}
	stats := c.Stats()
	if stats.ConnectionsDialed != 1 || stats.ConnectionsReused != 99 {
		t.Fatalf("stats = %+v, want 1 dial and 99 reuses", stats)
	}
}

func TestPoolClientBoundAndWaitCancellation(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	tr := poolEchoServer(t, func(_ context.Context, m Message) (Message, error) {
		entered <- struct{}{}
		<-release
		return m, nil
	})
	c := NewPoolClient(2)
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), nil)
		}()
		<-entered
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.Send(ctx, tr.Addr(), NewMessage(MessageClientRequest, nil), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v", err)
	}
	if got := c.Stats().ActiveConnections; got != 2 {
		t.Fatalf("active connections = %d, want pool bound 2", got)
	}
	close(release)
	wg.Wait()
}

func TestPoolClientMalformedResponseDiscardsOnlySession(t *testing.T) {
	tr := poolEchoServer(t, func(_ context.Context, m Message) (Message, error) { return m, nil })
	c := NewPoolClient(2)
	defer c.Close()
	bad := errors.New("malformed")
	if _, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), func(Message) error { return bad }); !errors.Is(err, bad) {
		t.Fatalf("error = %v", err)
	}
	if _, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), nil); err != nil {
		t.Fatal(err)
	}
	stats := c.Stats()
	if stats.ConnectionsDialed != 2 || stats.ConnectionsClosed != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestPoolClientConcurrentClose(t *testing.T) {
	tr := poolEchoServer(t, func(_ context.Context, m Message) (Message, error) { return m, nil })
	c := NewPoolClient(4)
	if _, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), nil); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Close() }()
	}
	wg.Wait()
	if stats := c.Stats(); stats.ActiveConnections != 0 {
		t.Fatalf("stats after close = %+v", stats)
	}
	if _, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Close = %v", err)
	}
}

func TestPoolClientCloseCancelsActiveAndQueuedSends(t *testing.T) {
	entered := make(chan struct{})
	releaseServer := make(chan struct{})
	tr := poolEchoServer(t, func(_ context.Context, m Message) (Message, error) {
		close(entered)
		<-releaseServer
		return m, nil
	})
	c := NewPoolClient(1)
	results := make(chan error, 2)
	go func() {
		_, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), nil)
		results <- err
	}()
	<-entered
	go func() {
		_, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, nil), nil)
		results <- err
	}()
	deadline := time.Now().Add(time.Second)
	for c.Stats().Waiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := <-results; !errors.Is(err, ErrClosed) {
			t.Fatalf("Send error = %v, want ErrClosed", err)
		}
	}
	close(releaseServer)
	if stats := c.Stats(); stats.ActiveConnections != 0 || stats.Waiters != 0 {
		t.Fatalf("stats after Close = %+v", stats)
	}
}

func TestPoolClientAddressMetadataBound(t *testing.T) {
	c := NewPoolClient(1)
	defer c.Close()
	c.dial = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("dial failed")
	}
	for i := 0; i < MaxClientPoolAddresses; i++ {
		_, _ = c.Send(context.Background(), fmt.Sprintf("address-%d", i), Message{}, nil)
	}
	if _, err := c.Send(context.Background(), "one-too-many", Message{}, nil); !errors.Is(err, ErrAddressLimit) {
		t.Fatalf("address beyond bound error = %v", err)
	}
}

func BenchmarkPoolClientWidths(b *testing.B) {
	tr := poolEchoServer(b, func(_ context.Context, m Message) (Message, error) { return m, nil })
	for _, width := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			c := NewPoolClient(width)
			defer c.Close()
			var latMu sync.Mutex
			latencies := make([]time.Duration, 0, b.N)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					if _, err := c.Send(context.Background(), tr.Addr(), NewMessage(MessageClientRequest, []byte("x")), nil); err != nil {
						b.Error(err)
						return
					}
					latMu.Lock()
					latencies = append(latencies, time.Since(start))
					latMu.Unlock()
				}
			})
			stats := c.Stats()
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			percentile := func(p float64) float64 {
				return float64(latencies[int(p*float64(len(latencies)-1))].Microseconds())
			}
			b.ReportMetric(float64(stats.ConnectionsDialed), "connections")
			b.ReportMetric(float64(stats.ConnectionsReused), "reuses")
			b.ReportMetric(percentile(.50), "p50-us")
			b.ReportMetric(percentile(.95), "p95-us")
			b.ReportMetric(percentile(.99), "p99-us")
		})
	}
}
