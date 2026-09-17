package transport

import (
	"context"
	"sync/atomic"
	"testing"
)

// The same server and frame payload compare fresh dialing with reuse. Dial
// counters are measured directly; throughput includes gate contention for
// concurrent callers to the same peer, rather than hiding serialization.
func benchmarkRPC(b *testing.B, concurrent bool) {
	for _, persistent := range []bool{false, true} {
		name := "fresh"
		if persistent {
			name = "persistent"
		}
		b.Run(name, func(b *testing.B) {
			tr := echoServer(b)
			c := testPeerClient(b)
			var freshDials atomic.Uint64
			msg := Message{Type: MessageTest, Payload: make([]byte, 64)}
			send := func() {
				var err error
				if persistent {
					_, err = c.Send(context.Background(), tr.Addr(), msg, nil)
				} else {
					_, err = Send(context.Background(), tr.Addr(), msg)
					if err == nil {
						freshDials.Add(1)
					}
				}
				if err != nil {
					b.Error(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			if concurrent {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						send()
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					send()
				}
			}
			b.StopTimer()
			dials := freshDials.Load()
			if persistent {
				dials = c.Stats().ConnectionsDialed
			}
			b.ReportMetric(float64(dials), "connections")
			b.ReportMetric(float64(dials)/float64(b.N), "connections/op")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "RPC/s")
		})
	}
}

func BenchmarkTransportSequentialRPC(b *testing.B) { benchmarkRPC(b, false) }
func BenchmarkTransportConcurrentRPC(b *testing.B) { benchmarkRPC(b, true) }
