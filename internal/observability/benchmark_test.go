package observability

import (
	"bytes"
	"testing"
	"time"
)

func benchmarkScrape(b *testing.B, concurrent bool) {
	m := New()
	for peer := uint64(1); peer <= 32; peer++ {
		m.RecordReplication(peer, peer*10, peer*10+1, 1024, false, false)
	}
	n := NodeSnapshot{Term: 7, Role: "leader", Leader: true, CommitIndex: 500, LastApplied: 499, LastLogIndex: 510, ActiveConnections: 2}
	stop := make(chan struct{})
	if concurrent {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					m.RecordRequest("put", "ok", time.Millisecond)
					m.RecordRPC("append_entries", time.Millisecond)
				}
			}
		}()
	}
	var buf bytes.Buffer
	if err := m.WritePrometheus(&buf, n); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(buf.Len()), "response-bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := m.WritePrometheus(&buf, n); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
}

func BenchmarkMetricsScrapeIdle(b *testing.B)              { benchmarkScrape(b, false) }
func BenchmarkMetricsScrapeConcurrentUpdates(b *testing.B) { benchmarkScrape(b, true) }
