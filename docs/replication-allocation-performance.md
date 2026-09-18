# Replication allocation performance (M22)

M22 removes the leader's intermediate deep snapshot before AppendEntries
encoding. A 64 × 1 KiB batch falls from **141,952 B / 66 allocations** to
**73,728 B / 1 allocation**. The independent contiguous wire payload remains.
This is a memory-ownership optimization, with no protocol or storage change.

## Ownership and synchronization

```text
M21:
Node.mu LOCKED
    +-- EntriesRange
          +-- deep clone entries and commands
Node.mu UNLOCKED
    +-- EncodeAppendEntries
          +-- allocate final payload and copy commands
    +-- network

M22:
Node.mu LOCKED
    +-- bounded borrowed log view
    +-- encode final independent payload
    +-- capture scalar response metadata
Node.mu UNLOCKED
    +-- network
```

`Log.entriesRangeView` is unexported and aliases retained log memory. Its caller
must provide synchronization, never mutate the view, and never retain it beyond
that boundary. Production only consumes it inside `encodeReplicationLocked`.
The request containing the view is local to that helper; the helper returns
only a fresh payload and scalar metadata. `Log.EntriesRange` still deep-copies
its result, using exactly the same selection logic. Bounds, compaction-boundary
clamping, and the one-entry exception to the byte target remain unchanged.

The existing `EncodeAppendEntries` already validates entry counts, command
lengths and hard payload size, then allocates one exact-size buffer. Reusing it
avoids a second encoder or size formula. Wire layout and ReadContext are
unchanged. The 512 KiB target includes entry headers; 64 × 8 KiB selects **63**
entries and produces 516,967 wire bytes. A maximum 256 KiB command produces
262,209 wire bytes and is sent alone in the single-entry fixture.

The default `encodedAppendSender` transfers the payload through
`transport.NewOwnedMessage`, without decoding, re-encoding, or cloning it.
Socket framing remains a separate allocation. Follower decoding still owns
independent command buffers. There are no pools, caches, unsafe conversions,
scatter/gather changes, or multiple in-flight requests per peer socket.

`replicationAttempt` retains `prevLogIndex`, `entryCount`, and `logicalBytes`.
Term and generation remain separate scalar arguments to response application.
Success advances to the **sent** previous index plus **sent** count. Current
log mutations cannot change that calculation. Role, term, worker existence,
generation and higher-term checks, conflict backoff, snapshot invalidation,
membership and logical-byte metrics retain their previous behavior. Retries
build a fresh payload from current state.

`SetAppendSend` remains usable, including direct package-test assignments to
`sendAppend`. It installs an optional structured adapter: replication decodes
the completed payload after unlock, then calls the override. Production never
takes this adapter. Nil restores encoded transport. ReadIndex keeps structured
probes and selects the same override/default sender safely under the mutex.

## Method and raw evidence

Measurements used Go 1.26.5, linux/amd64, an i5-11400H, and GOMAXPROCS=12.
M21 production was merged at `fe9f191`; baseline benchmark commit `ba3d78f`
preceded production edits. Additional matched harnesses ran against an archive
of that baseline. Tables report medians across repetitions unless specified.
Raw `replication-*.txt` files retain initial, confirmation and sustained runs.

`BenchmarkReplicationPayload` measures selection, combined selection/encoding,
and shared-mutex contention (24 goroutines). Baseline construction clones under
the lock and encodes after unlock; M22 calls the production helper under lock.
Fixtures are outside timing. `BenchmarkReplicationStep` executes every complete
step/catch-up for each b.N iteration, with a successful synthetic sender and
1 KiB commands. The baseline sender encodes the structured request; the M22
sender consumes the already-encoded bytes. This isolates leader allocations;
it excludes framing, follower decoding and persistence. It uses five iterations,
so its timing is illustrative, not a latency distribution.

The real-TCP service catch-up benchmark retains 256 B values and includes all
nodes' allocations while the timer runs. Setup is excluded from benchmark
counters, but **included in process-wide profiles**. Its original heal operation
does not wake existing workers: the 50 ms heartbeat phase and 3 ms observation
poll can dominate small runs. The triggered variant adds one timed PUT after
healing to wake replication, while waiting for the original backlog to apply.
Both revisions use the same harness. No p50/p95 per-entry catch-up latency is
available; entire-run medians are reported.

## Batch construction

| Entries × bytes | M21 ns/op | M22 ns/op | M21 B/op | M22 B/op | Allocations before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Heartbeat | 64.6 | 76.2 | 64 | 64 | 1 → 1 |
| 1 × 16 | 133.8 | 90.0 | 160 | 96 | 3 → 1 |
| 1 × 1024 | 431.9 | 232.1 | 2,224 | 1,152 | 3 → 1 |
| 64 × 16 | 2,739 | 1,328 | 5,760 | 2,048 | 66 → 1 |
| 64 × 1024 | 20,852 | 9,099 | 141,952 | 73,728 | 66 → 1 |
| Near 512 KiB | 191,043 | 50,212 | 1,043,074 | 524,288 | 65 → 1 |
| 1 × 256 KiB | 70,800 | 44,845 | 532,530 | 270,336 | 3 → 1 |

Allocation size classes explain differences between wire length and B/op.
Heartbeat construction adds about 12 ns in this run, with no extra allocation.
Public selection still has its original allocation cost by design.

## Lock duration and contention

The focused critical-section benchmark includes lock/unlock, bounded selection,
validation, allocation and encoding; it excludes network and response work.
These are amortized ns/op, not maximum mutex hold times or a scheduling bound.

| Batch | M22 critical-section ns/op |
| --- | ---: |
| Heartbeat | 36.7 |
| 1 × 16 B | 56.2 |
| 1 × 1 KiB | 189.4 |
| 64 × 16 B | 1,143 |
| 64 × 1 KiB | 7,703 |
| Near 512 KiB | 55,873 |
| 1 × 256 KiB | 36,503 |

With 24 concurrent constructors sharing one mutex, 64 × 1 KiB improves from
24.0 to 14.3 µs/op; near 512 KiB improves from 157.4 to 66.5 µs/op. Encoding
serializes peer workers, but removing dozens of allocations also removes work
previously done under that lock. These results do not justify a ref-counted
snapshot design. GC or scheduling can still extend individual lock holds.

## Leader-only catch-up

| Entries | M21 ms | M22 ms | M21 B/op | M22 B/op | Allocations before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 2.48 | 1.31 | 11,111,601 | 5,780,550 | 5,317 → 237 |
| 10,000 | 5.92 | 3.86 | 22,220,246 | 11,560,192 | 10,628 → 471 |
| 25,000 | 25.13 | 18.38 | 55,553,280 | 28,903,168 | 26,564 → 1,173 |

The isolated heartbeat step stays at 320 B / 3 allocations; 64-entry steps
fall from 142,208 B / 68 allocations to 73,984 B / 3 allocations. Remaining
fixed step allocations include target-map construction.

## Real-TCP catch-up

The original three-repeat, untriggered 5k/10k/25k medians were 28.3/56.5/135.4 ms
for M21 and 37.9/31.4/123.7 ms for M22. A five-repeat 5k confirmation was
25.9 → 49.5 ms, with M21 individual runs ranging from 14.5 to 62.4 ms. These
results retain the heartbeat-phase delay described above and cannot isolate
replication throughput.

With an explicit timed proposal wake, five-repeat medians are:

| Entries | M21 ms | M22 ms | M21 entries/s | M22 entries/s | M21 B/op | M22 B/op | Allocations before → after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 12.84 | 13.27 | 389,281 | 376,809 | 17,168,176 | 15,354,824 | 45,498 → 40,389 |
| 10,000 | 27.92 | 25.06 | 358,198 | 399,010 | 34,670,440 | 31,045,104 | 90,742 → 80,538 |
| 25,000 | 105.44 | 104.30 | 237,095 | 239,695 | 86,297,952 | 77,248,320 | 226,101 → 200,717 |

Catch-up allocations fall about 10.5%. The 5k timing difference is 0.43 ms,
smaller than the harness's 3 ms polling interval; larger runs are similar or
faster. This supports no material catch-up regression in the measured cases,
without claiming precise latency percentiles from a coarse polling harness.

## Service throughput and tails

The required 50-operation sequential and 300-operation concurrent runs, plus
1,000-operation GET/mixed repeats, are retained in the raw files. Initial M22
runs overlapped race validation and showed large latency increases. Serial
confirmation still showed substantial short-run variability: 300-operation
PUT-1KiB medians were 43.2 versus 58.8 µs/op, whereas 1,000-operation mixed
medians were 32.8 versus 31.4 µs/op. Sequential medians were 147.6 → 247.8 µs
(16 B) and 184.9 → 212.5 µs (1 KiB); their p50s were 97 → 104 and 105 → 113 µs.
These short samples do not establish a throughput improvement.

To assess sustained behavior, both revisions ran serially with 32 clients,
10,000 operations and five repeats, with no concurrent verification jobs:

| Workload | M21 ns/op | M22 ns/op | M21 B/op | M22 B/op | Allocs before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| PUT 16 B | 18,171 | 17,609 | 6,634 | 6,411 | 84 → 81 |
| PUT 1 KiB | 21,383 | 20,632 | 34,096 | 31,714 | 84 → 81 |
| 80% GET / 20% PUT, 1 KiB | 21,590 | 21,724 | 17,144 | 16,677 | 103 → 102 |

| Latency (µs) | M21 p50/p95/p99 | M22 p50/p95/p99 |
| --- | --- | --- |
| PUT 16 B | 531 / 977 / 1459 | 521 / 898 / 1371 |
| PUT 1 KiB | 625 / 1151 / 1910 | 606 / 1053 / 1762 |
| GET under write load, GET observations only | 568 / 1025 / 1596 | 568 / 1059 / 1777 |

Sustained PUT throughput improves about 3–4%. Mixed throughput is within 1%;
GET p95 rises 3.3% and p99 rises 11.3% (181 µs). This is a remaining tail-latency
tradeoff/uncertainty, not evidence of universally faster requests. A separate
3,000-operation confirmation had GET p95 improve 1142 → 1086 µs and p99 worsen
2594 → 2965 µs. Longer tests on dedicated hardware are needed for firm p99
claims. All stress benchmark operations completed without BUSY/errors.

## Profiles and next bottleneck

`replication-profiles-m22.txt` records CPU, alloc_space and alloc_objects tops.
The isolated 20,000-batch profile drops from 2,724.86 MB / 1,402,239 sampled
objects to about 1.34 GB / 30,948 objects. `EntriesRange`/`cloneEntries` disappears
from this path; its baseline cumulative allocation was 1,336.39 MB. Sampled
CPU falls from 930 to 380 ms, and memmove from 170 to 60 ms in this fixture.

The real catch-up profiles include backlog creation and all three nodes:
7,544.06 → 4,657.87 MB and 33,834,238 → 23,981,578 sampled objects. Total sampled
CPU is 45.49 → 43.89 seconds; memmove is 0.44 → 0.23 seconds. Setup and retry
activity dominate this profile, so these totals are not per-catch-up counters.

The 2,000-operation concurrent PUT profiles do **not** establish a total CPU or
allocation win: CPU is 180 ms in both; allocation estimates are 67.29 → 68.71 MB
and 230,244 → 409,439 objects. Heap sampling and runtime/setup activity are
significant in this short profile. Measured per-operation allocations above
are stronger evidence. Transport `EncodeFrame` remains a leading allocation
(7.62 MB in the M22 PUT profile), alongside safe KV copies, receive buffers and
the final AppendEntries payload. Request structs and scalar response metadata
introduce no proportional heap allocation.

For M23, first investigate bounded transport-frame allocation reuse with an
explicit ownership/release contract and read-tail measurements. Framing is a
measured cost; its optimization is not included here. Final payload allocation
also remains large during catch-up, but caching or pooling needs separate
lifetime and retained-capacity analysis.

## Correctness and verification

New tests compare 1,000 randomized and seven deterministic requests against
both the public encoder and an independent M21 wire oracle. They cover entry
kinds, heartbeat, ReadIndex, maximum commands, count/byte selection, public
copy isolation, decode ownership, payload independence after source mutation,
append/truncate, allocation bounds, sender equivalence and unlocked sending.
Encoded in-flight response tests cover generation invalidation/snapshot
takeover, removal, stepdown and fresh-payload retries. Existing deterministic
fault and real snapshot/membership/leadership tests continue exercising their
original behavior through structured overrides.

A delayed encoded sender test lets the fast peer, a 256 KiB proposal and
ReadIndex finish while the slow peer is blocked. New tests pass ten times under
the race detector. Full verification includes formatting, vet, build, all tests,
all race tests and `make check`, plus ten repetitions each of real-process,
Raft race/fault and persistence tests. Storage layout, fsync, CRC, migration,
truncation and compaction code are unchanged.

Primary implementation files: `internal/raft/log.go`, `node.go`,
`replication_worker.go`, and `read_index.go`. Test and benchmark changes are in
the corresponding Raft tests, `replication_payload_test.go`,
`replication_allocation_benchmark_test.go` and `internal/service/benchmark_test.go`.
