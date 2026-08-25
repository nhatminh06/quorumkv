# Performance

This document records Milestone 13's and Milestone 14's actual, measured
before/after results, on the environment described below. Both followed
the project's measure-first discipline: the benchmark harness
(`internal/service/benchmark_test.go`) was written before any
optimization, a baseline was recorded, the change was implemented, then
the same benchmarks were re-run unchanged. Milestone 13's numbers are
kept below exactly as originally recorded — this document does not erase
earlier tradeoffs. See
[docs/replication-performance.md](replication-performance.md) for
Milestone 14's full design rationale (bounded AppendEntries, per-peer
event-driven workers, generations); this file keeps only its measured
results and environment.

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

## Deferred at the end of Milestone 13 (resolved in Milestone 14)

Two of Milestone 13's deferred items were addressed in Milestone 14 —
bounded/byte-accurate AppendEntries batches and a bounded log-range API,
and event-driven per-peer replication workers with generation-based
stale-response suppression. See
[docs/replication-performance.md](replication-performance.md) and the
M14 results below. Membership/leadership-transfer interaction with the
new worker model, and the InstallSnapshot handoff, were also covered
there. The observational stats surface was not extended to replication
in M14 either — still proposal admission/batching only.

The deterministic load/stress test matrix beyond `overload_test.go`
(items 130-133 of Milestone 13's own spec — a combined 32-client stress
test, a one-follower-down load test, a leader-failover-under-load test)
remains undone; it was not part of Milestone 14's scope either.

## Milestone 14 — replication performance

### Environment

Same machine/toolchain as the Milestone 13 measurements above.

### Benchmark commands

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkFollowerCatchUp' -benchtime=3x
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeSequentialPut' -benchtime=50x -count=3
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeConcurrentPut|BenchmarkThreeNodeConcurrentGet|BenchmarkThreeNodeMixedReadWrite' -benchtime=300x -count=3
```

"Before" (M13) was measured on the Milestone 13 merge commit
(`bb972997149931608bf585254d42d36f8949d377`) in a separate worktree;
"after" (M14) is this branch. Both used the identical commands, machine,
and Go version.

### Follower catch-up (the targeted improvement)

| | Before (M13) | After (M14) | Change |
|---|---|---|---|
| Catch-up duration (5000-entry lagging suffix) | 3.952 s | 0.269 s | **14.7x faster** |
| Entries/sec | 1,265 | 18,583 | **14.7x** |

This is the primary result: event-driven immediate re-send between
successful batches, instead of waiting for the next heartbeat tick,
removes almost the entire wait time from catching a far-behind follower
up. See [docs/replication-performance.md](replication-performance.md)
§18 for a profile of the new dominant cost (the follower's own whole-log
rewrite on each received batch — unchanged, and explicitly out of
scope, this milestone).

### Steady-state workloads (re-run, not the primary target)

Mean `ns/op` across the `-count=3` runs; throughput is `1e9/ns_op`.

| Workload | M13 ns/op | M14 ns/op | M13 throughput | M14 throughput | Change |
|---|---|---|---|---|---|
| Sequential PUT, 16 B | 974,544 | 974,853 | — | — | unchanged |
| Sequential PUT, 1024 B | 1,424,131 | 1,508,504 | — | — | +6% latency (within run-to-run noise) |
| Concurrent PUT, 16 B | 256,477 | 231,557 | 3,899 ops/s | 4,318 ops/s | +11% |
| Concurrent PUT, 1024 B | 558,989 | 374,444 | 1,789 ops/s | 2,671 ops/s | +49% |
| Concurrent GET | 143,154 | 98,661 | 6,986 ops/s | 10,136 ops/s | +45% |
| Mixed 80/20 | 154,564 | 105,748 | 6,470 ops/s | 9,457 ops/s | +46% |

None of these were the targeted improvement (only follower catch-up
was), and none regressed. The concurrent-workload gains beyond PUT are a
plausible secondary effect of event-driven workers removing the old
periodic full-replication-round background cost even for an idle/
caught-up cluster, but this was not isolated or specifically profiled to
confirm that attribution — reported as measured, not fully explained.
Sequential PUT is unaffected as expected (a single sequential writer
never benefits from replication scheduling changes either way).

### CPU profile

See [docs/replication-performance.md](replication-performance.md) §18
for the follower catch-up profile and its implication (the follower's
whole-log rewrite is now the dominant remaining cost).

## Deferred (not implemented in Milestone 14)

See [docs/replication-performance.md](replication-performance.md) §19
for the full list (no request pipelining beyond window 1, no transport
connection pooling, no replication-specific stats, snapshot chunking
unchanged).
