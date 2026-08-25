# Performance

This document records Milestone 13's actual, measured before/after
result for QuorumKV's client write path, on the environment described
below. It follows the project's measure-first discipline: the benchmark
harness (`internal/service/benchmark_test.go`) was written before any
optimization, a baseline was recorded, then proposal batching was
implemented, then the same benchmarks were re-run unchanged.

No number here is a hardware-independent throughput claim. Every figure
below is "on the documented local benchmark environment," nothing more.

## Environment

```text
Go version:      go1.26.5 linux/amd64
GOOS/GOARCH:      linux/amd64
CPU:              11th Gen Intel(R) Core(TM) i5-11400H @ 2.70GHz
Logical CPUs:     12
GOMAXPROCS:       12 (default)
```

## What changed

Milestone 13's only change to the write path is proposal batching (see
`internal/raft/proposal.go`): `Node.Propose` previously appended and
fsynced one entry per call while holding `Node.mu` for the entire durable
write, serializing every concurrent proposal against every other one (and
against ordinary Raft state-transition locking). It now admits into a
bounded queue drained by a single coordinator goroutine, which persists
whatever is available in one shared `Log.Append`/fsync — concurrent
proposals that arrive while a batch is being drained share that one
durable write instead of each paying for their own.

GET (ReadIndex) and single-client sequential PUT do not go through this
queue's batching benefit at all — a sequential caller is always a batch
of one — so they are not expected to improve, and a small per-request
admission-semaphore/channel-hop overhead (added for backpressure — see
below) is expected to show up as a small latency cost on those paths.

## Benchmark commands

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeSequentialPut' -benchtime=50x -count=3
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeConcurrentPut|BenchmarkThreeNodeConcurrentGet|BenchmarkThreeNodeMixedReadWrite' -benchtime=300x -count=3
go test ./internal/service -run '^$' -bench 'BenchmarkFollowerCatchUp' -benchtime=1x
```

Baseline was measured by checking out the Milestone 12 merge commit
(`0706e3da5d0e1736d253bc7f9e459e8a3f32df2a`) into a separate worktree,
copying only `benchmark_test.go` and the `testing.TB` widening of
`service_test.go`'s cluster helpers onto it (no production code changed),
and running the identical commands above. "After" is the same commands
run on this branch. Both used the same machine, same Go version, same
30-second-per-run wall clock budget, same value sizes, same client
concurrency (32 independent `client.Client`s, each its own ClientID — see
item 11), same operation counts.

Each reported number below is the mean `ns/op` across the 3 `-count=3`
runs (or `benchtime=50x`'s single set for sequential PUT); the
concurrency benchmarks divide `b.N` total operations across exactly 32
goroutines, so `ns/op` here is per-operation wall time under that shared
concurrent load, not a single serialized client's latency.

## Results

### Workload B — concurrent PUT, 32 clients

| Value size | Baseline ns/op | M13 ns/op | Baseline throughput | M13 throughput | Change |
|---|---|---|---|---|---|
| 16 B | 575,444 | 256,477 | ~1,738 ops/s | ~3,899 ops/s | **+124%** |
| 1024 B | 1,297,875 | 558,989 | ~771 ops/s | ~1,789 ops/s | **+132%** |

This is the targeted bottleneck (item 126) and shows a clear improvement
well beyond run-to-run noise: roughly 2.2-2.3x more concurrent write
throughput, from letting concurrent proposals share durable log rewrites
instead of serializing one fsync per proposal.

### Workload C — concurrent ReadIndex GET, 32 clients

| Baseline ns/op | M13 ns/op | Baseline throughput | M13 throughput | Change |
|---|---|---|---|---|
| 130,032 | 143,154 | ~7,691 ops/s | ~6,986 ops/s | **-9%** |

A small regression, not hidden (item 125): GET never touches the
proposal queue, so this is not a batching effect — it is the fixed cost
of `handleClient`'s new admission-semaphore channel operation on every
request, reads included. It is well within the kind of overhead a
bounded concurrency gate is expected to add, and item 55/56 (bounded
concurrency, fast recovery under overload) was an explicit Milestone 13
requirement, not optional — this is the tradeoff for it.

### Workload D — mixed 80% GET / 20% PUT, 32 clients

| Baseline ns/op | M13 ns/op | Baseline throughput | M13 throughput | Change |
|---|---|---|---|---|
| 177,982 | 154,564 | ~5,619 ops/s | ~6,470 ops/s | **+15%** |

Net positive: the write portion's batching gain outweighs the small
per-request admission overhead once any meaningful write fraction is
present.

### Workload A — sequential PUT, 1 client (latency, not throughput)

| Value size | Baseline ns/op | M13 ns/op | Change |
|---|---|---|---|
| 16 B | 835,169 | 974,544 | **+17% latency** |
| 1024 B | 1,287,993 | 1,424,131 | **+11% latency** |

The expected latency/throughput tradeoff (item 127): a single sequential
caller is always a batch of one, so it gets no amortization benefit, and
now pays a small extra scheduling hop (admission → the proposal
worker's channel round trip → result delivery) that the old direct,
synchronous append did not have. This is a real, reported cost, not
hidden — a single low-concurrency writer is measurably slower per
request in exchange for the concurrent-throughput gain above.

### Workload E — follower catch-up (5000-entry lagging suffix)

| Baseline | M13 | Change |
|---|---|---|
| 3.934 s, ~1271 entries/s | 3.959 s, ~1263 entries/s | ~unchanged |

Expected: Milestone 13's batching change touches proposal admission and
local log persistence only, not the AppendEntries replication/pipeline
path. Bounded AppendEntries batch sizing, the byte-accurate replication
range API, and event-driven per-peer replication workers (items 67-89 of
the milestone scope) were not implemented this milestone — see
"Deferred" below — so no improvement here is expected or claimed.

## Batching actually occurring (not just present in code)

`internal/raft/proposal_test.go`'s
`TestProposeContiguousIndexesUnderConcurrency` fires 200 concurrent
`Propose` calls against a single-node cluster and checks `Node.Stats()`:
on a representative run, 200 proposals were persisted in as few as 2-4
`Log.Append` batches (50-100 entries/batch average) rather than 200
individual rewrites — direct evidence (not inference from code reading)
that concurrent proposals are actually sharing durable writes.

## CPU profile

A CPU profile was taken of `BenchmarkThreeNodeConcurrentPut/value=16B`
(the concurrent-write hot path) via:

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeConcurrentPut/value=16B$' \
  -benchtime=300x -cpuprofile /tmp/cpu.prof
go tool pprof -top /tmp/cpu.prof
```

By cumulative time, the top real (non-runtime-internal) consumers were
`internal/transport.Send` (~31% of samples) and connection
dial/close (`net.(*Dialer).DialContext`, `net.(*netFD).Close`, ~11%),
with background GC marking also significant (~25%, itself mostly driven
by the allocation churn of constantly opening and tearing down
connections). Nothing pointed at a new hotspot introduced by batching
itself — the original full-log-rewrite-per-proposal cost this milestone
targeted is no longer the dominant line item. Instead, this profile
directly confirms the milestone brief's own observation that
"replication can generate... many independent TCP connections": this
benchmark's `transport.Send` opens a new connection per RPC (both
client→leader and leader→follower), and that per-RPC connection
overhead is now the largest visible cost in the write path — a natural
target for future work, not something this milestone's proposal-batching
change addresses. Profile files are not committed (item 18); reproduce
with the command above.

## Deferred (not implemented this milestone)

The milestone's full scope was substantially larger than what is above;
the following were deliberately not implemented, to keep this a single
reviewable, well-tested change rather than a sprawling one:

- **Bounded/byte-accurate AppendEntries batches and a bounded log-range
  API** (items 67-73): `Log.EntriesFrom` still clones an entire matched
  suffix before `replicateToAllPeers` truncates it to
  `maxEntriesPerAppend` — real waste for a far-behind follower with a
  large retained suffix, and the batch bound is still count-only (64
  entries), not byte-accurate.
- **Event-driven per-peer replication workers with a bounded pipeline**
  (items 74-89, 98-103): replication remains "one goroutine per
  replication round, call-response, wait for the next heartbeat/Propose
  trigger" rather than a persistent per-peer worker with an in-flight
  window, generation-based stale-response suppression, and immediate
  catch-up loops. This is the reason Workload E shows no improvement.
- **Membership/leadership-transfer interaction with a replication
  pipeline** (items 63-66, 91-97, 101-102): moot without the pipeline
  above; existing Milestone 10/11 behavior is unaffected and unchanged
  (verified by the full existing test suite passing unmodified).
- **The full observational stats surface** (item 114): `Node.Stats()`
  covers proposal admission/batching only, not AppendEntries RPC/pipeline
  counters, since no pipeline exists yet to instrument.
- **Deterministic load/stress test matrix beyond the overload tests
  added** (items 130-133): `internal/service/overload_test.go` covers
  deterministic queue/concurrency saturation and recovery; a
  32-client/100-write-each combined correctness-under-load stress test,
  a one-follower-down load test, and a leader-failover-under-load test
  were not added.

These are legitimate follow-up work, not silently dropped requirements —
each depends on the replication pipeline redesign, which is
correctness-sensitive enough (stale-response suppression, generation
invalidation, InstallSnapshot/membership/transfer interaction) to
deserve its own dedicated measurement-driven milestone rather than being
folded in here.
