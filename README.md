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

QuorumKV now supports changing cluster membership — adding or removing
exactly one voter at a time — through Raft's joint-consensus protocol:
a transition passes through a joint phase (`C_old,new`) where every
quorum-based decision (election, log commitment, ReadIndex, the
current-term barrier, client writes) requires a majority of the old
configuration **and** a majority of the new one simultaneously, never a
majority of their union. Proven over a real three-node TCP cluster: a
brand-new node with no prior log joins past a leader whose log is
already compacted, catching up entirely through a real `InstallSnapshot`
transfer; it is later removed; the leader removes itself and stops
leading once that becomes final. Also proven: a cluster that loses its
leader mid-transition — at either of two distinct crash points — always
finishes the transition automatically once a new leader is elected,
never getting stuck in the joint phase. See
[docs/membership.md](docs/membership.md) for the full protocol, the API
(`Node.AddVoter`/`Node.RemoveVoter`), and the complete test list.

This builds on QuorumKV giving PUT/DELETE a stable request identity
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
restart.

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
