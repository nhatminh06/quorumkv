# QuorumKV

QuorumKV is a distributed key-value store being built to study consensus
and failure recovery. Raft is implemented from scratch in this repository.

Target architecture (not yet implemented beyond the state machine, WAL,
and transport):

```text
Client
 ↓
Raft
 ↓
Replicated Log
 ↓
KV State Machine
```

## Current milestone

QuorumKV now bounds follower replication by entry count and encoded
bytes, and uses event-driven per-peer replication workers to catch a
stale follower up without waiting for successive heartbeat intervals
between batches. Replication responses are guarded by a per-peer
generation, so a stale or superseded response can never regress
progress that has already moved on. Measured on a 5000-entry lagging
follower: catch-up dropped from 3.95s to 0.27s (about 14.7x), with no
observed regression to steady-state write/read throughput. See
[docs/replication-performance.md](docs/replication-performance.md) for
the full design and [docs/performance.md](docs/performance.md) for
measured results.

This builds on Milestone 13's proposal batching and bounded backpressure
(concurrent proposals share one durable log write instead of each
paying for its own; a full proposal queue or a full service admission
bound fails fast with a retryable `BUSY` status rather than accepting
unbounded work) and Milestone 12's deterministic crash-consistency
proofs (every durable file recovers as exactly its old content or
exactly its new content after a process killed at any point during a
write — proven with genuine subprocess crashes, not just simulated
errors). See [docs/performance.md](docs/performance.md) and
[docs/crash-consistency.md](docs/crash-consistency.md).

QuorumKV also runs a PreVote phase before every ordinary election: a
node asks, hypothetically, "would you vote for me?" without touching
any persistent state, and only proceeds to a real election if that
round reaches quorum. A voter that recently heard from a healthy leader
(including the leader itself, protecting its own term) rejects the
hypothetical vote outright, so a node that has been isolated and
repeatedly times out never bumps the cluster's term — proven by fully
(bidirectionally) partitioning a follower away from a healthy leader:
across repeated failed election timeouts its term never advances and
the leader is never disrupted, while a follower-losing-its-leader
scenario still elects a legitimate replacement normally. Leaders can
also perform a controlled leadership transfer to a specific, fully
caught-up voter: the target is brought current through ordinary
replication (diverting through real `InstallSnapshot` if it's behind a
compacted log — composing with Milestone 10's membership changes), new
write/read/membership admission is frozen only once handoff begins, and
an authorized `TimeoutNow` triggers the target's real election
(deliberately bypassing PreVote, since the current leader has already
authorized the handoff) — success is only reported once the old leader
observes real evidence the target actually won, never merely that
`TimeoutNow` was accepted. See
[docs/raft-election.md](docs/raft-election.md) and
[docs/leadership-transfer.md](docs/leadership-transfer.md) for the full
protocol and test list.

This builds on QuorumKV's Raft joint-consensus membership changes —
adding or removing exactly one voter at a time, where every quorum-based
decision during a transition requires a majority of the old
configuration **and** a majority of the new one simultaneously, never a
majority of their union (see [docs/membership.md](docs/membership.md)) —
and on giving PUT/DELETE a stable request identity
(`ClientID` + a monotonic per-client sequence number) so an ambiguous write — a
transport failure, a server-side `TIMEOUT`, or a `NOT_LEADER` redirect —
can be safely retried: the replicated KV state machine deduplicates a
retried request and applies its effect at most once, even across a
leader failover. This closes Milestone 5-8's prohibition on write
retries. Proven with a real three-node cluster over real TCP: a leader
commits and applies a write, its response is lost, the leader crashes
before the client can retry against it, a new leader is elected, and the
client's retry — recognized purely from the new leader's own replicated
dedup state, never a leader-local cache — returns success without a
second mutation; the same holds when the request's original log entry
has been compacted away and the new leader only ever received it via a
real `InstallSnapshot` transfer. See
[docs/request-dedup.md](docs/request-dedup.md) for the mechanism, the
exact-next-sequence policy, and the full test list.

GET is unaffected: it carries no request identity and continues to use
ReadIndex-style quorum confirmation
([docs/read-index.md](docs/read-index.md)), eliminating the stale-read
path on an isolated old leader — a leader must prove a current-term
committed entry and confirm, by contacting a quorum, that it is still
recognized as leader before reading local state.

This builds on: bounded Raft log growth via snapshots and chunked
`InstallSnapshot` catch-up ([docs/snapshots.md](docs/snapshots.md));
deterministic failure tests covering leader loss, partitions,
stale-follower catch-up, divergent-log repair, and persistent restart
recovery ([docs/failure-testing.md](docs/failure-testing.md)); committed
Raft entries applied to the KV state machine, a leader-aware binary
PUT/GET/DELETE client API ([docs/state-machine.md](docs/state-machine.md),
[docs/client-protocol.md](docs/client-protocol.md)); and persistent Raft
election/log replication/heartbeats
([docs/raft-election.md](docs/raft-election.md),
[docs/raft-log-replication.md](docs/raft-log-replication.md)), plus
[docs/wal.md](docs/wal.md) and [docs/transport.md](docs/transport.md).

The correct claim after verification: **replicated request identity
provides at-most-once state-machine effects for retried PUT/DELETE
operations that reuse the same ClientID and sequence number** — not
exactly-once networking, delivery, or execution under arbitrary client
misuse (the transport remains at-most/unreliable request-response), and
not a claim about GET, which was already quorum-confirmed but carries no
request identity. The dedup table's size grows with the number of
distinct `ClientID`s a node has ever seen (no GC/quota yet — a known
limitation). QuorumKV implements quorum-confirmed linearizable GET,
proven by targeted deterministic tests — not a formally verified
linearizability proof and not Byzantine fault tolerant. Snapshotting is
caller-triggered only (no automatic threshold/schedule policy, no
client-facing snapshot API, no distributed snapshot coordination between
nodes); this is not a general-purpose storage engine. Membership changes
are limited to one voter at a time, with no batched multi-node changes,
no learner/observer promotion as a public feature, and no automatic
rebalancing or discovery (see [docs/membership.md](docs/membership.md)).
There is no client-side session persistence across a client process
restart. PreVote reduces disruptive elections caused by isolated or
stale followers; it does not eliminate all disruption in every possible
scenario, and is not a formal proof of election stability. Leadership
transfer provides an intentional, best-effort handoff to a specified
up-to-date voter — a caller-specified operation, not automatic leader
balancing, and not a guarantee that a transfer can never fail (a
partitioned or crashed target simply fails the call cleanly).

## Layout

```text
internal/reqid/       client request identity types (ClientID, Sequence) shared across layers
internal/kv/          command representation, codec, deterministic KV state machine + request dedup
internal/wal/         append-only write-ahead log (application command history, not the Raft log)
internal/transport/   bounded message framing and TCP request/response transport
internal/raft/        persistent Raft state, log replication, RequestVote/AppendEntries, leader election
internal/clientproto/ bounded binary client PUT/GET/DELETE wire protocol
internal/service/     wires a raft.Node to a kv.StateMachine and serves the client protocol
internal/client/      reusable leader-aware Go client with safe write retry
docs/                  format and design notes
```

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```
