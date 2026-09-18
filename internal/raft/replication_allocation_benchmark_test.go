package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// Leader-only step/catch-up accounting excludes follower decoding, persistence,
// and transport; BenchmarkFollowerCatchUp measures those over real TCP.
func BenchmarkReplicationStep(b *testing.B) {
	for _, count := range []int{0, 1, 64, 5000, 10000, 25000} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			n := payloadTestNode(b)
			n.log.entries = make([]LogEntry, count)
			for i := range n.log.entries {
				n.log.entries[i] = LogEntry{Term: 1, Command: make([]byte, 1024)}
			}
			n.sendEncodedAppend = func(_ context.Context, _ string, _ []byte) (AppendEntriesResponse, error) {
				return AppendEntriesResponse{Term: 2, Success: true}, nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n.nextIndex[2] = 1
				n.matchIndex[2] = 0
				for n.replicationStep(context.Background(), 2) {
				}
			}
		})
	}
}

// This function follows the production lock boundary. Keep the benchmark's
// fixtures and cases unchanged when comparing revisions.
func benchmarkReplicationPayload(l *Log, mu *sync.Mutex) ([]byte, error) {
	mu.Lock()
	n := Node{log: l}
	payload, _, err := n.encodeReplicationLocked(1, AppendEntriesRequest{Term: 2, LeaderID: 1})
	mu.Unlock()
	return payload, err
}

func BenchmarkReplicationPayload(b *testing.B) {
	for _, c := range []struct{ count, size int }{{0, 0}, {1, 16}, {1, 1024}, {64, 16}, {64, 1024}, {64, 8192}, {1, maxCommandSize}} {
		b.Run(fmt.Sprintf("entries=%d/value=%d", c.count, c.size), func(b *testing.B) {
			l := &Log{entries: make([]LogEntry, c.count)}
			for i := range l.entries {
				l.entries[i] = LogEntry{Term: 1, Command: make([]byte, c.size)}
			}
			var mu sync.Mutex
			b.Run("select", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = l.EntriesRange(1, maxEntriesPerAppend, MaxAppendEntriesBytes)
				}
			})
			b.Run("construct", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					p, err := benchmarkReplicationPayload(l, &mu)
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(len(p)), "encoded-B")
				}
			})
			b.Run("encode-critical-section", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					mu.Lock()
					_, err := EncodeAppendEntries(AppendEntriesRequest{Entries: l.entriesRangeView(1, maxEntriesPerAppend, MaxAppendEntriesBytes)})
					mu.Unlock()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("contended", func(b *testing.B) {
				b.ReportAllocs()
				b.SetParallelism(2)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := benchmarkReplicationPayload(l, &mu); err != nil {
							b.Error(err)
						}
					}
				})
			})
		})
	}
}
