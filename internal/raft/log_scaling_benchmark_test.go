package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"quorumkv/internal/observability"
)

var persistenceScaleEntries = []int{100, 1000, 5000, 10000, 25000, 50000}

func scalingEntries(count, commandBytes int) []LogEntry {
	command := make([]byte, commandBytes)
	for i := range command {
		command[i] = byte('a' + i%26)
	}
	entries := make([]LogEntry, count)
	for i := range entries {
		entries[i] = LogEntry{Term: Term(1 + i/1000), Command: command}
	}
	return entries
}

func preparedScalingLog(b *testing.B, count, commandBytes int) (*Log, string, int64) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "log")
	data, err := encodeLogFile(0, 0, scalingEntries(count, commandBytes))
	if err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatal(err)
	}
	log, err := OpenLog(path)
	if err != nil {
		b.Fatal(err)
	}
	return log, path, int64(len(data))
}

func metricSum(b *testing.B, metrics *observability.Metrics, prefix string) float64 {
	b.Helper()
	var out strings.Builder
	if err := metrics.WritePrometheus(&out, observability.NodeSnapshot{}); err != nil {
		b.Fatal(err)
	}
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				value, err := strconv.ParseFloat(fields[1], 64)
				if err != nil {
					b.Fatal(err)
				}
				return value
			}
		}
	}
	b.Fatalf("metric %q not found", prefix)
	return 0
}

func reportMutation(b *testing.B, metrics *observability.Metrics, path string, logicalBytes int64) {
	b.Helper()
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	physical := info.Size()
	b.ReportMetric(float64(logicalBytes), "logical-B")
	b.ReportMetric(float64(physical), "physical-B")
	b.ReportMetric(float64(physical)/float64(logicalBytes), "write-amplification")
	b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_duration_seconds_sum{domain=\"log\"}")*1e9, "persist-ns")
	b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_fsync_duration_seconds_sum{domain=\"log\"}")*1e9, "fsync-ns")
}

func BenchmarkRaftLogScalingAppendOne(b *testing.B) {
	for _, commandBytes := range []int{16, 1024, 16 * 1024} {
		counts := persistenceScaleEntries
		if commandBytes == 16*1024 {
			counts = []int{100, 1000, 5000}
		}
		for _, count := range counts {
			b.Run(fmt.Sprintf("entries=%d/value=%dB", count, commandBytes), func(b *testing.B) {
				log, path, _ := preparedScalingLog(b, count, commandBytes)
				metrics := observability.New()
				log.setObserver(metrics)
				entry := LogEntry{Term: 99, Command: make([]byte, commandBytes)}
				logical := int64(logLengthPrefixSize + logEntryHeaderSizeV3 + commandBytes + logChecksumSize)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := log.Append([]LogEntry{entry}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				reportMutation(b, metrics, path, logical*int64(b.N))
			})
		}
	}
}

func BenchmarkRaftLogScalingMutations(b *testing.B) {
	const commandBytes = 1024
	for _, count := range persistenceScaleEntries {
		b.Run(fmt.Sprintf("append-batch/entries=%d", count), func(b *testing.B) {
			log, path, _ := preparedScalingLog(b, count, commandBytes)
			metrics := observability.New()
			log.setObserver(metrics)
			batch := scalingEntries(64, commandBytes)
			logical := int64(len(batch) * (logLengthPrefixSize + logEntryHeaderSizeV3 + commandBytes + logChecksumSize))
			b.ReportAllocs()
			b.ResetTimer()
			if err := log.Append(batch); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			reportMutation(b, metrics, path, logical)
		})
		b.Run(fmt.Sprintf("truncate-tail/entries=%d", count), func(b *testing.B) {
			log, path, _ := preparedScalingLog(b, count, commandBytes)
			metrics := observability.New()
			log.setObserver(metrics)
			replacement := scalingEntries(10, commandBytes)
			logical := int64(len(replacement) * (logLengthPrefixSize + logEntryHeaderSizeV3 + commandBytes + logChecksumSize))
			b.ReportAllocs()
			b.ResetTimer()
			if err := log.TruncateAndAppend(LogIndex(count-9), replacement); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			reportMutation(b, metrics, path, logical)
		})
		b.Run(fmt.Sprintf("truncate-deep/entries=%d", count), func(b *testing.B) {
			log, path, _ := preparedScalingLog(b, count, commandBytes)
			metrics := observability.New()
			log.setObserver(metrics)
			replacement := scalingEntries(10, commandBytes)
			logical := int64(len(replacement) * (logLengthPrefixSize + logEntryHeaderSizeV3 + commandBytes + logChecksumSize))
			b.ReportAllocs()
			b.ResetTimer()
			if err := log.TruncateAndAppend(LogIndex(count/2), replacement); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			reportMutation(b, metrics, path, logical)
		})
		b.Run(fmt.Sprintf("compact/entries=%d", count), func(b *testing.B) {
			log, path, _ := preparedScalingLog(b, count, commandBytes)
			metrics := observability.New()
			log.setObserver(metrics)
			base := LogIndex(count / 2)
			term, _ := log.Term(base)
			b.ReportAllocs()
			b.ResetTimer()
			if err := log.Compact(base, term); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			reportMutation(b, metrics, path, 16)
		})
	}
}

func BenchmarkRaftLogScalingRead(b *testing.B) {
	const commandBytes = 1024
	for _, count := range persistenceScaleEntries {
		b.Run(fmt.Sprintf("open/entries=%d", count), func(b *testing.B) {
			_, path, physical := preparedScalingLog(b, count, commandBytes)
			b.ReportMetric(float64(physical), "log-B")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := OpenLog(path); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("range/entries=%d", count), func(b *testing.B) {
			log, _, physical := preparedScalingLog(b, count, commandBytes)
			want := 128
			if available := count - count/2 + 1; available < want {
				want = available
			}
			b.ReportMetric(float64(physical), "log-B")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := log.EntriesRange(LogIndex(count/2), 128, 1<<20); len(got) != want {
					b.Fatalf("range length = %d", len(got))
				}
			}
		})
	}
}

// BenchmarkFollowerLogPersistenceCatchUp isolates the follower-side durable
// AppendEntries cost using the production 128-entry batch bound. The existing
// service benchmark includes network, protocol, and apply work at 5,000
// entries; this benchmark makes 10,000 and 25,000-entry persistence scaling
// practical without constructing the leader suffix through timed proposals.
func BenchmarkFollowerLogPersistenceCatchUp(b *testing.B) {
	const commandBytes = 256
	for _, count := range []int{5000, 10000, 25000} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			log, err := OpenLog(filepath.Join(b.TempDir(), "log"))
			if err != nil {
				b.Fatal(err)
			}
			metrics := observability.New()
			log.setObserver(metrics)
			entries := scalingEntries(count, commandBytes)
			logical := int64(count * (logLengthPrefixSize + logEntryHeaderSizeV3 + commandBytes + logChecksumSize))
			b.ReportAllocs()
			b.ResetTimer()
			for from := 0; from < count; from += 128 {
				to := min(from+128, count)
				if err := log.Append(entries[from:to]); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			elapsed := b.Elapsed()
			b.ReportMetric(float64(count)/elapsed.Seconds(), "entries/sec")
			b.ReportMetric(float64(logical)/elapsed.Seconds(), "logical-B/sec")
			b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_bytes_total{domain=\"log\"}"), "physical-B")
			b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_writes_total{domain=\"log\"}"), "log-writes")
			b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_duration_seconds_sum{domain=\"log\"}")*1e9, "persist-ns")
			b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_fsync_duration_seconds_sum{domain=\"log\"}")*1e9, "fsync-ns")
			b.ReportMetric(metricSum(b, metrics, "quorumkv_persistence_bytes_total{domain=\"log\"}")/float64(logical), "write-amplification")
		})
	}
}
