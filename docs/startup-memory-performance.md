# Startup memory performance (M23)

Milestone 23 changes retained segmented-log commands from individually cloned
byte slices to slices backed by immutable `os.ReadFile` segment buffers. The
M20 on-disk format, manifest, CRC32C validation, batch commits, 4 MiB target,
and torn-tail recovery are unchanged.

## Previous path

```text
segment file bytes
  -> decode record
  -> allocate command []byte
  -> copy command
  -> retain LogEntry
```

M21 recorded approximately 0.5 ms and 2.24 MB for 1,000 entries, 5-6 ms and
23.2 MB for 10,000 entries, and 33-34 ms, 121.1 MB, and about one command
allocation per entry for 50,000 entries. Those figures are the startup
allocation baseline, not retained heap size.

## M23 representation

```text
Log
  -> entry metadata and command slices
  -> immutable raw segment backings (one per retained segment)
  -> owned command slices for entries appended in this process
```

The segment scanner validates headers, lengths, kinds, CRC32C, and batch-commit
records before publishing entries. A loaded command slice aliases only the
raw buffer retained by `Log.segmentBackings`; no public API exposes that slice
for mutation. `Entry`, `EntriesFrom`, and `EntriesRange` return defensive
copies. Internal Raft paths use borrowed views only while their existing
synchronization is held. M22 replication encodes its final independent payload
before releasing `Node.mu`.

A rewrite caused by truncation, compaction, or a large append materializes the
current logical entries into owned command slices before releasing old
backings. The new physical generation can then be published and old backing
references dropped. A crossing segment may remain retained while a surviving
entry points into it; retention is bounded by segment size rather than entry
count. Deleting an old generation from disk is independent from the lifetime
of its Go-owned byte buffer.

## Startup results

Go 1.26.5, linux/amd64, 11th Gen Intel(R) Core(TM) i5-11400H, GOMAXPROCS=12.
Fixtures are prepared outside the timed region. Results are one-iteration
samples from `BenchmarkRaftLogStartup`; rerun with `-count` for local medians.

| Entries x command | Startup | B/op | allocs/op | backing buffers |
| --- | ---: | ---: | ---: | ---: |
| 1,000 x 16 B | 0.068 ms | 0.20 MB | 41 | 1 |
| 10,000 x 16 B | 0.775 ms | 2.87 MB | 49 | 1 |
| 50,000 x 16 B | 6.12 ms | 16.09 MB | 58 | 1 |
| 100,000 x 16 B | 11.36 ms | 33.85 MB | 104 | 2 |
| 1,000 x 1 KiB | 0.401 ms | 1.21 MB | 45 | 1 |
| 10,000 x 1 KiB | 5.17 ms | 12.99 MB | 107 | 3 |
| 50,000 x 1 KiB | 22.24 ms | 69.86 MB | 372 | 13 |
| 100,000 x 1 KiB | 34.74 ms | 141.32 MB | 702 | 26 |
| 1,000 x 16 KiB | 4.44 ms | 16.63 MB | 108 | 4 |
| 10,000 x 16 KiB | 63.63 ms | 167.31 MB | 817 | 40 |

The M23 50,000 x 1 KiB run retains 50,000 entries and 51.2 MB of command
bytes, while the raw segment backings retain approximately 51.4 MB including
headers, lengths, checksums, and batch commits. Allocation volume falls because
commands are not copied into 50,000 separate objects; resident memory still
includes the immutable encoded segment representation and compact entry
metadata. M23 is not zero-copy storage and does not use mmap or unsafe code.

## Lifecycle and compatibility

- Startup publishes only entries covered by complete batch-commit records.
- An incomplete final active record or batch is truncated/discarded exactly as
  before; complete checksum or format corruption remains fatal.
- Appended commands remain ordinary owned slices until a restart or rewrite.
- Conflict truncation and snapshot compaction drop old backing references after
  rebuilding the logical generation; tests assert the backing list is cleared.
- `Term`, `LastTerm`, and index lookups use retained metadata without command
  decoding or filesystem reads.
- Legacy flat log versions still load and migrate through the existing path.
- State-machine apply receives a fresh command copy, preserving application
  ownership isolation.

## Verification

The M23 tests cover backing-count bounds, public `Entry` alias safety, segment
recovery, corruption, truncation, compaction, restart, M22 replication payload
independence, and race behavior. The benchmark is:

```bash
go test ./internal/raft -run '^$' -bench BenchmarkRaftLogStartup -benchmem
```

The remaining tradeoff is metadata plus encoded-record retention. A future
milestone may investigate transport or GC costs only after fresh profiles; M23
does not add mmap, lazy disk access, a custom allocator, a new disk format, or
segment-size changes.
