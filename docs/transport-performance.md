# Persistent peer transport measurements

## Design and scope

Previously every Raft or client exchange dialed, sent one frame, read one
frame, and closed its TCP socket. Now every Raft node owns one bounded
`PeerClient`: a map of peer addresses to sequential sessions. All five Raft
RPC families use it, including heartbeats and ReadIndex's AppendEntries
probes. CLI/client/admin traffic still uses the original fresh-connection
function. The server accepts multiple sequential frames on either path.

A context-aware channel gate permits one in-flight RPC per peer. There is
no request multiplexing. Different peers use independent gates; incoming
handlers do not acquire outgoing gates, and Raft holds no state mutex across
network I/O. Response type and payload validation complete before reuse.
Cancellation while queued leaves the owner's socket alone. Cancellation or
any failure during an exchange discards that socket; only a future Send
reconnects, with one dial and no hidden retry loop. A stale connection can
therefore cost one failed RPC. Raft's existing retry scheduling is unchanged.

Sessions and address metadata are capped at 256 per node. At capacity, an
idle session is evicted; when all slots are occupied, admission returns an
error. Historical peer addresses may retain idle sockets until eviction or
shutdown. Client Close rejects admission, cancels gate waits/dials/I/O, joins
Sends and cancellation callbacks, then closes idle sockets. Server Close
cancels handlers and joins all accepted sessions. A handler that ignores
cancellation still has to return before server Close can finish. There are
no background connection-manager workers. See [transport.md](transport.md)
for the detailed lifecycle and counter semantics.

Raft decisions, quorum rules, ReadIndex, checksums, fsync, request deduplication,
and persistence formats are unchanged. Sender overrides still support fake,
delayed, blocked, and dropped RPC tests.

## Environment and reproducibility

Measured on the same Linux/amd64 machine: Intel Core i5-11400H @ 2.70 GHz,
Go 1.26.5, default GOMAXPROCS=12, loopback TCP, temporary persistent directories.
Before: main commit `e7a365d`; after: production code committed as `b706851`.
Baseline benchmarks and profiles completed before production transport edits.
No other benchmarks or correctness suites ran concurrently with these
measurements. Normal machine scheduling and filesystem caching still introduce
noise; these short runs are evidence for this environment, not universal
throughput guarantees. Both versions use the same real service benchmark
harness and durable storage path without unsafe options.

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeSequentialPut' -benchtime=50x -count=3
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeConcurrentPut|BenchmarkThreeNodeConcurrentGet|BenchmarkThreeNodeMixedReadWrite' -benchtime=300x -count=3
go test ./internal/service -run '^$' -bench 'BenchmarkFollowerCatchUp' -benchtime=3x
go test ./internal/transport -run '^$' -bench 'BenchmarkTransport' -benchtime=1000x -count=3
```

[Raw benchmark output](transport-benchmark-results.txt) includes every run,
latency percentile, allocation count, and catch-up metric.

## Service results

Arithmetic mean ns/op across three runs; throughput is 1e9 / mean ns/op.
Concurrent ns/op is aggregate elapsed time per operation, not individual
request latency; p50/p95/p99 in the raw results measure individual latency.

| Workload | Before ns/op | After ns/op | Change | Before ops/s | After ops/s |
|---|---:|---:|---:|---:|---:|
| Sequential PUT, 16 B | 241,330 | 210,251 | -12.9% | 4,144 | 4,756 |
| Sequential PUT, 1024 B | 445,440 | 344,716 | -22.6% | 2,245 | 2,901 |
| Concurrent PUT, 16 B | 71,560 | 67,174 | -6.1% | 13,974 | 14,887 |
| Concurrent PUT, 1024 B | 188,173 | 182,351 | -3.1% | 5,314 | 5,484 |
| Concurrent GET | 38,354 | 30,252 | -21.1% | 26,073 | 33,055 |
| Mixed 80% GET / 20% PUT | 41,369 | 33,441 | -19.2% | 24,173 | 29,903 |

The small concurrent PUT differences overlap run-to-run variation and should
not be treated as a demonstrated universal speedup. Tail latency does not
uniformly improve: mean p95 for concurrent 1024 B PUT rose from 8,956 µs to
9,961 µs (11.2%), even while aggregate throughput rose. Mean p99 rose from
10,076 µs to 10,866 µs (7.8%). Serializing a peer stream creates contention
between replication and read probes; no quorum or cancellation semantics
were relaxed to hide that tradeoff.

| Follower catch-up, 5,000 entries | Before | After |
|---|---:|---:|
| Mean timed ns/op over three iterations | 175,089,929 | 141,891,947 |
| Reported final-iteration catchup-sec | 0.1874 | 0.1339 |
| Reported final-iteration entries/sec | 26,684 | 37,335 |

The harness overwrites its custom catchup-sec and entries/sec metrics each
iteration; those are the final iteration, not three-run averages. Lag-building
work is outside its benchmark timer. Total command time actually increased
from 58.987 s to 63.861 s; the timed catch-up result must not be presented as
an improvement to the whole command or the lag-building phase.

## Focused RPC results and connection evidence

64-byte payload, same persistent-capable echo server, original fresh Send
versus PeerClient.Send. Mean of three runs:

| Workload | Fresh ns/op | Persistent ns/op | Fresh RPC/s | Persistent RPC/s | Connections per 1,000 RPCs |
|---|---:|---:|---:|---:|---:|
| Sequential RPC | 42,638 | 10,187 | 23,453 | 98,164 | 1,000 → 1 |
| Concurrent RPC, same peer | 9,832 | 16,694 | 101,705 | 59,902 | 1,000 → 1 |

That is 99.9% less connection churn and about 4.19× sequential throughput.
The concurrent comparison **regresses**: elapsed ns/op rises about 70% and
throughput drops about 41%. Fresh Send allows multiple independent sockets;
the persistent client deliberately queues all callers to one peer. This
benchmark exposes that serialization rather than adding connections or
multiplexing to improve its result. Sequential ns/op approximates RPC latency;
concurrent ns/op is an aggregate throughput measure, not queue-inclusive
per-request latency.

Successful fresh Sends each count one connection by construction; persistent
dials are counted by the manager. `TestPeerClientReusesOneConnection` also
asserts exactly one dial, 99 reuses, no failures, and one live server socket
for 100 sequential exchanges. Concurrent-response tests verify payload identity
across 640 exchanges on one socket. No timing inference is used to establish
reuse. Allocation counts fall from 34 to 27 allocations/op, but sequential
bytes/op increase slightly (~1,402 → 1,418) due to context/callback bookkeeping.

## CPU profiles

Both versions ran the requested short profile:

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeConcurrentPut/value=16B$' -benchtime=300x -cpuprofile /tmp/quorumkv-m17.prof
go tool pprof -top /tmp/quorumkv-m17.prof
```

The baseline short profile had only 90 ms of CPU samples. To avoid drawing
conclusions from a few samples, both versions also ran the same workload with
`-count=10` and separate profile paths; cumulative results from
`go tool pprof -top -cum PROFILE` follow. Setup/election/teardown are included
in process-wide CPU profiles even though benchmark timers exclude them.

| Cumulative CPU attribution | Before | After |
|---|---:|---:|
| Total sampled CPU | 930 ms | 900 ms |
| `net.(*Dialer).DialContext` | 110 ms (11.83%) | 130 ms (14.44%) |
| `net.(*netFD).Close` | 80 ms (8.60%) | 80 ms (8.89%) |
| Fresh `transport.Send` | 200 ms (21.51%) | 180 ms (20.00%) |
| `transport.(*PeerClient).Send` | absent | 20 ms (2.22%) |
| Transport server `handleConn` | 230 ms (24.73%) | 250 ms (27.78%) |
| `runtime.gcBgMarkWorker` | 200 ms (21.51%) | 140 ms (15.56%) |

These profiles **do not establish a material total dial/close CPU reduction**.
After the change, fresh `transport.Send` is still the external client path,
which performs one connection per write. Before, 150 of its 200 cumulative ms
were attributed to the client call path; after, all 180 ms were. Profiles are
sampled and cumulative rows overlap; adding these percentages is invalid.
Connection counters provide stronger evidence for peer reuse than these short
profiles provide for total CPU savings. Binary profiles are not committed.

The largest after-profile flat cost remains syscalls (340 ms, 37.78%); memory
allocation has 190 ms cumulative (21.11%) and fresh client dialing 130 ms.
Remaining work includes client connection churn, allocations, and syscall
cost. Whole-log rewriting and fsync remain part of write/catch-up costs; this
profile does not isolate one of them as the unique next bottleneck. None was
changed in this milestone.

## Correctness and stress evidence

- Persistent-frame regression failed against the original server on response
  2 with EOF; the new server processes 100 frames on the same connection.
- Peer-client tests cover reuse, same-peer concurrency, independent peers,
  forced socket close/reconnect, invalid frames and protocol replies, queued
  cancellation, blocked dialing/writing/reading, server/client shutdown,
  concurrent Close/Send, and bounded admission/idle eviction.
- Raft adapters reject wrong-type and truncated typed responses before reuse.
- Real three-node integration closes the follower transport and its existing
  sessions, commits 20 writes on the surviving majority, reopens the same
  address, and waits for the follower to commit and apply the suffix. Dial,
  reuse, and failure counters must all advance.
- The 32-client stress test performs 3,200 PUTs and 3,200 read-after-write GETs
  with independent client identities, matching every returned value, then
  joins workers and closes every node/server. Dialed and closed socket totals
  must match. Three race runs passed: leader dials 151/152/179, reuse attempts
  6,483/6,356/6,459, failures 869/972/907. Failures include safely canceled
  redundant quorum work; they are not 869 failed application operations.
- Existing election, PreVote, ReadIndex, dedup, membership, snapshot, transfer,
  crash/restart, and real-process suites remain unchanged. Their default peer
  adapters now use persistent transport; explicitly injected fake/fault senders
  retain their configured behavior.

A canceled in-flight quorum probe must discard its stream, so real clusters
will not retain exactly one connection per peer forever. Slow snapshots or
replication can delay probes queued to that peer. Other peers proceed, and
all queue/I/O waits obey context cancellation. No availability or throughput
guarantee is implied by socket reuse.

## Verification

All commands passed on the measured branch with exit status 0:

| Command | Result |
|---|---|
| `gofmt -w .` / `gofmt -l .` | formatted / no output |
| `go vet ./...` | passed |
| `go build ./...` | passed |
| `go test ./...` | all 11 packages passed; service 15.536 s |
| `go test -race ./...` | all 11 packages passed; service 19.572 s |
| `make check` | vet, binary builds, and tests passed |
| `go test ./cmd/quorumkv -run 'TestRealProcess' -count=10` | passed, 11.723 s |
| `go test -race ./internal/transport -count=5` | passed, 2.454 s |
| `go test -race ./internal/service -run 'TestPeerTransportReconnectAndFollowerCatchUp\|TestPersistentPeerConcurrentPutGetStress' -count=3 -v` | passed, 13.749 s |
| `go test -race ./internal/raft -run 'Test.*(Partition\|Crash\|Transfer\|PreVote\|Snapshot\|ReplicationWorker)' -count=10` | passed, 17.270 s |

No race exclusions were added. As before, process tests build ordinary node
and CLI binaries; `go test -race` instruments the test packages, including
the real-TCP in-process Raft/service stress tests, not separately built
subprocess executables. Existing CI retains gofmt, vet, build, test, and race.
