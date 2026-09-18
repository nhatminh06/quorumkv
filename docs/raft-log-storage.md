# Raft log storage and persistence scaling

Milestone 20 replaces whole-file Raft log rewrites with a segmented,
append-oriented format. This document records the evidence that justified the
change and the resulting on-disk and crash-recovery model. The state-machine
WAL in `internal/wal` is separate and unchanged.

## Measurement and decision

The Phase A benchmarks constructed logs outside the timed region at 100,
1,000, 5,000, 10,000, 25,000, and 50,000 retained entries. They measured a
single append with 16 B, 1 KiB, and 16 KiB commands; 64-entry batches; shallow
and deep conflict repair; compaction; open; range reads; and follower-side
128-entry catch-up batches. Metrics recorded logical bytes, encoded and
written bytes, persistence and fsync duration, allocations, and write count.

For a one-entry 1 KiB append, the original implementation produced:

| Retained entries | Approximate append time | Physical bytes | Amplification | Allocated bytes/op |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.136 ms | 105,566 | 101x | 0.63 MiB |
| 1,000 | 1.92 ms | 1,046,066 | 1,001x | 6.36 MiB |
| 5,000 | 7.66 ms | 5,226,066 | 5,001x | 37.4 MiB |
| 10,000 | 15.9 ms | 10,451,066 | 10,001x | 69.9 MiB |
| 25,000 | 35.9 ms | 26,126,066 | 25,001x | 171.7 MiB |
| 50,000 | 81.1 ms | 52,251,066 | 50,001x | 340.9 MiB |

The physical byte figures are measured encoded output, not estimates from the
source. Fsync was unusually cheap on the workspace filesystem; encoding,
copying, and garbage collection dominated. Slower durable storage would add
the same amplified bytes to the device cost.

Follower persistence showed the same scaling:

| Entries caught up | Wall time | Entries/s | Physical bytes | Amplification |
| ---: | ---: | ---: | ---: | ---: |
| 5,000 | 47.4 ms | 105,800 | 29.0 MiB | 20.97x |
| 10,000 | 174.3 ms | 57,400 | 112 MiB | 40.44x |
| 25,000 | 1.036 s | 24,100 | 684.5 MiB | 98.84x |

This is Decision A: whole-log rewriting caused linear append latency, extreme
write amplification, and allocation growth, so a storage redesign was
justified.

## Layout and format

For a logical path `log`, storage consists of:

```text
log                         legacy v1-v3 file, retained after migration
log.manifest                atomically replaced current-generation metadata
log.segments/
  g00000000000000000001/
    00000000000000000001.seg
    00000000000000003997.seg
```

The small, CRC32C-protected manifest contains a format version, generation,
base index, and base term. Each segment header contains magic `RSG1`, a format
version, and its first logical index. A record contains a bounded length,
term, explicit entry kind, command length, command, and CRC32C. A checksummed
internal batch-commit record makes a multi-entry append visible as one unit;
recovery discards all records after the last complete commit record.
Application, no-op, and configuration entries therefore round-trip without
inspecting command contents.

Segments target 4 MiB. This keeps boundary rewrites bounded while avoiding a
file per entry. A record larger than the remaining segment space starts the
next segment; the maximum command remains 256 KiB. The implementation opens
an active segment only for a write and closes it immediately, so retained
file-descriptor use does not grow with segment count.

## Append and durability

Ordinary append encodes only the new records, appends them to the active
segment, calls `fsync`, and then updates the in-memory log. Rotation creates
and fsyncs a new header and syncs the generation directory before using the
new segment. A successful return therefore means every returned entry's
record has reached the segment fsync. Checksums and Raft commit rules are
unchanged.

The active segment may end with an incomplete length or body if a process dies
during append. Startup validates every complete record and truncates only that
incomplete final active tail to the last record boundary, then fsyncs the
repair. A bad checksum, invalid length, or malformed record is fatal even in
the active segment. Any damage or truncation in a non-active segment is fatal.

## Truncation and compaction

Conflict repair and snapshot compaction build a new generation and publish it
by atomically replacing the manifest. Complete segments whose index range and
contents are unchanged are hard-linked into the new generation. Only a
boundary segment and replacement records are encoded again. This preserves
unaffected prefix segments during suffix truncation and unaffected suffix
segments during compaction.

The old manifest remains authoritative while the new generation is built and
its files and directories are synced. The atomic manifest rename is the commit
point. Before it, recovery selects the complete old generation; after it,
recovery selects the complete new generation. Old generation directory
removal is best effort after publication. Generation numbers skip abandoned
crash debris, which startup never selects.

Compaction stores the new `baseIndex` and `baseTerm` in the manifest. Segments
entirely below the boundary disappear from the new generation; a crossing
segment is rewritten from the first retained entry; later complete segments
are reused.

## Startup and corruption behavior

Startup validates the manifest checksum and version, enumerates segment names
in index order, verifies that each filename matches its header start index,
and rejects gaps and overlaps. It validates record bounds, entry kinds, and
checksums while reconstructing the in-memory representation.
Readers and replication workers continue to use `BaseIndex`, `BaseTerm`,
`LastIndex`, `LastTerm`, `Term`, `Entry`, and `EntriesRange`; physical segments
do not enter the Raft API. M23 retains immutable raw segment buffers behind
compact metadata; safe public entry APIs still return copies, and rewrites
materialize surviving commands before old backing references are released. See
[startup-memory-performance.md](startup-memory-performance.md).

Only an incomplete final active record is recoverable. Missing segments,
overlaps, filename/header mismatch, unsupported versions, invalid lengths,
sealed-segment truncation, and checksum corruption return `ErrCorruptLog`.

## Legacy migration

Single-file versions 1, 2, and 3 remain readable through the existing parser.
The first mutation writes and syncs a complete segmented generation and then
atomically publishes its manifest. The legacy file is retained as a fallback
artifact. A crash before manifest publication continues to select the legacy
file; a crash after publication selects the complete segmented generation.
At no point is the only complete representation removed.

## Observability

The metrics endpoint exports cumulative logical and physical Raft-log bytes,
rotation and truncation counts, and current segment count. Their ratio gives
write amplification without unbounded labels. Existing persistence metrics
record total operation time, bytes, writes, and measured fsync duration.
Structured logs are emitted for migration, rotation, torn-tail recovery,
truncation, and compaction, but not for ordinary appends.

## Post-change scaling

One-entry appends now write one entry plus a small batch-commit record (1.024x
amplification for a 1 KiB command), use ten allocations in the measured path,
and remain approximately flat as retained history grows. On the same
workspace, representative 1 KiB results were roughly 6-14 microseconds at
every size from 100 through 50,000 entries, apart from one 61-microsecond
outlier. The
workspace's cached fsync measurements were roughly 3-4 microseconds, so these
absolute timings should not be extrapolated to production storage.

Follower-side persistence improved as follows:

| Entries caught up | Before | After | After entries/s | After amplification |
| ---: | ---: | ---: | ---: | ---: |
| 5,000 | 47.4 ms | 4.0-4.2 ms | 1.20-1.25 M/s | 1.001x |
| 10,000 | 174.3 ms | 6.0-8.1 ms | 1.23-1.68 M/s | 1.001x |
| 25,000 | 1.036 s | 13.3-14.2 ms | 1.76-1.88 M/s | 1.001x |

These are storage-isolation results. Network, protocol, apply, and persistence
can overlap in a live cluster, so the benchmark does not claim perfect
end-to-end attribution. The real-process test separately verifies a three-node
multi-segment follower catch-up and a second restart.

## Established service benchmarks and profiles

Means of three unchanged M20 runs, compared with the recorded M19 means:

| Workload | M19 ns/op | M20 ns/op | Change |
| --- | ---: | ---: | ---: |
| Sequential PUT, 16 B | 146,409 | 158,519 | +8.3% |
| Sequential PUT, 1 KiB | 329,939 | 193,534 | -41.3% |
| Concurrent PUT, 16 B | 54,793 | 47,137 | -14.0% |
| Concurrent PUT, 1 KiB | 141,166 | 51,113 | -63.8% |
| Concurrent GET | 25,075 | 44,709 | +78.3% |
| Mixed 80/20 | 28,076 | 50,427 | +79.6% |

The targeted PUT paths improved strongly for 1 KiB values, while 16 B
sequential PUT was roughly flat to slightly slower. GET and mixed workloads
regressed in these short runs even though their storage path was not changed;
the runs had substantial tail outliers, so no storage causation is claimed.
The complete output is in `service-benchmark-m20.txt`.

The concurrent 1 KiB PUT CPU profile was sparse: syscalls, mutex contention,
and GC scanning led the samples; whole-log encoding no longer appeared as a
hotspot. Its allocation profile attributed 17.2% directly to segmented append,
17.0% to state-machine byte cloning, 10.7% to request fingerprinting, and 7.2%
to entry-record encoding. The isolated catch-up allocation profile attributed
60.3% to segmented append buffers, 21.5% to record encoding, and 9.5% to
command cloning. The isolated large-append allocation profile is dominated by
its intentional 50,000-entry setup; the timed result (10 allocs/op, 4,832
bytes/op) is the useful hot-path measurement. No binary profiles are committed.

## Tradeoffs and limits

Startup still decodes all retained entries into memory, so open cost and
resident memory scale with retained history. Boundary operations must compare
candidate reusable segment contents and rewrite at most the partial boundary
plus new records. Hard links require the old and new generation directories to
share a filesystem, which is guaranteed by their common root. Physical media
and power-loss behavior remain subject to the POSIX filesystem assumptions in
`crash-consistency.md`.

The evidence suggests a future milestone should focus on reducing decoded
entry cloning and per-record allocation during open and catch-up, after fresh
profiles establish whether that work is more important than transport or
state-machine apply costs.

Milestone 21 subsequently replaced per-record body/wrapper allocations with
one exact-size batch buffer while preserving this byte format and crash model.
See [allocation-performance.md](allocation-performance.md) for the ownership
proof and measured allocation reduction.
