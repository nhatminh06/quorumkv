package client

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/clientproto"
	"quorumkv/internal/transport"
)

func benchmarkClientServer(b testing.TB, respond func(clientproto.Request) clientproto.Response) *transport.Transport {
	b.Helper()
	tr, err := transport.Listen("127.0.0.1:0", func(_ context.Context, m transport.Message) (transport.Message, error) {
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
		b.Fatal(err)
	}
	b.Cleanup(func() { tr.Close() })
	return tr
}

func reportClientLatency(b *testing.B, observations []time.Duration) {
	b.Helper()
	if len(observations) == 0 {
		return
	}
	sort.Slice(observations, func(i, j int) bool { return observations[i] < observations[j] })
	at := func(p float64) float64 {
		return float64(observations[int(p*float64(len(observations)-1))].Microseconds())
	}
	b.ReportMetric(at(.50), "p50-us")
	b.ReportMetric(at(.95), "p95-us")
	b.ReportMetric(at(.99), "p99-us")
}

func reportPoolStats(b *testing.B, c *Client) {
	b.Helper()
	stats := c.Stats()
	b.ReportMetric(float64(stats.ConnectionsDialed)/float64(b.N), "connections/op")
	b.ReportMetric(float64(stats.ConnectionsReused)/float64(b.N), "reuses/op")
}

func BenchmarkClientSequentialGet(b *testing.B) {
	tr := benchmarkClientServer(b, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK, Value: []byte("value")}
	})
	c := New(tr.Addr())
	defer c.Close()
	lat := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, _, err := c.Get(context.Background(), []byte("key")); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	b.StopTimer()
	reportPoolStats(b, c)
	reportClientLatency(b, lat)
}

func BenchmarkClientSequentialPut(b *testing.B) {
	tr := benchmarkClientServer(b, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK}
	})
	c := New(tr.Addr())
	defer c.Close()
	lat := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := c.Put(context.Background(), []byte("key"), []byte("value")); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	b.StopTimer()
	reportPoolStats(b, c)
	reportClientLatency(b, lat)
}

func BenchmarkClientConcurrentGet(b *testing.B) {
	tr := benchmarkClientServer(b, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK, Value: []byte("value")}
	})
	c := New(tr.Addr())
	defer c.Close()
	var next atomic.Int64
	lat := make([]time.Duration, b.N)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= b.N {
					return
				}
				start := time.Now()
				if _, _, err := c.Get(context.Background(), []byte("key")); err != nil {
					b.Error(err)
					return
				}
				lat[i] = time.Since(start)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	reportPoolStats(b, c)
	reportClientLatency(b, lat)
}

func BenchmarkClientMixed(b *testing.B) {
	tr := benchmarkClientServer(b, func(req clientproto.Request) clientproto.Response {
		if req.Operation == clientproto.OpGet {
			return clientproto.Response{Status: clientproto.StatusOK, Value: []byte("value")}
		}
		return clientproto.Response{Status: clientproto.StatusOK}
	})
	c := New(tr.Addr())
	defer c.Close()
	lat := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if i%5 == 0 {
			if err := c.Put(context.Background(), []byte("key"), []byte("value")); err != nil {
				b.Fatal(err)
			}
		} else if _, _, err := c.Get(context.Background(), []byte("key")); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	b.StopTimer()
	reportPoolStats(b, c)
	reportClientLatency(b, lat)
}

func BenchmarkClientLeaderRedirect(b *testing.B) {
	leader := benchmarkClientServer(b, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusOK, Value: []byte("value")}
	})
	follower := benchmarkClientServer(b, func(clientproto.Request) clientproto.Response {
		return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: []byte(leader.Addr())}
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := New(follower.Addr())
		if _, _, err := c.Get(context.Background(), []byte("key")); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
	b.ReportMetric(2, "connections/op")
}
