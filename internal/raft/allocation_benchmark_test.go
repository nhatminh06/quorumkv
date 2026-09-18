package raft

import (
	"fmt"
	"testing"
)

func BenchmarkAllocationEntryRecord(b *testing.B) {
	for _, size := range []int{16, 1024, 16 * 1024, maxCommandSize} {
		b.Run(fmt.Sprintf("command=%dB", size), func(b *testing.B) {
			entry := LogEntry{Term: 7, Kind: EntryApplication, Command: make([]byte, size)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := encodeEntryRecord(entry); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAllocationReplicationEncoding(b *testing.B) {
	log, _, _ := preparedScalingLog(b, 10000, 1024)
	b.Run("entries-range-64", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = log.EntriesRange(5000, maxEntriesPerAppend, MaxAppendEntriesBytes)
		}
	})
	entries := log.EntriesRange(5000, maxEntriesPerAppend, MaxAppendEntriesBytes)
	req := AppendEntriesRequest{Term: 2, LeaderID: 1, PrevLogIndex: 4999, PrevLogTerm: 1, Entries: entries}
	b.Run("encode-append-entries-64", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := EncodeAppendEntries(req); err != nil {
				b.Fatal(err)
			}
		}
	})
}
