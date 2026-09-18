package raft

import (
	"fmt"
	"sync"
	"testing"
)

// This function follows the production lock boundary. Keep the benchmark's
// fixtures and cases unchanged when comparing revisions.
func benchmarkReplicationPayload(l *Log, mu *sync.Mutex) ([]byte, error) {
	mu.Lock()
	entries := l.EntriesRange(1, maxEntriesPerAppend, MaxAppendEntriesBytes)
	mu.Unlock()
	return EncodeAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: 1, Entries: entries})
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
