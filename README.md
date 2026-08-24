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

QuorumKV now uses a ReadIndex-style quorum confirmation before serving
GET, eliminating the previously documented stale-read path on an
isolated old leader: a leader must prove it holds a current-term
committed entry (appending an internal no-op barrier first if needed)
and confirm, by contacting a quorum, that it is still recognized as
leader before reading local state. This was proven with a real
three-node cluster over real TCP: a leader isolated from the majority
while a replacement is elected and commits a different write can no
longer return the stale value on a direct GET — it gets `TIMEOUT` or
`NOT_LEADER`, never a stale `OK` — while the majority-side leader serves
the current value normally. See [docs/read-index.md](docs/read-index.md)
for the mechanism, safety argument, and full test list.

PUT/DELETE remain commit-and-apply acknowledged, unchanged by this
milestone. GET now waits for a current-term quorum-confirmed read index
and local application through that index before answering.

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

QuorumKV implements quorum-confirmed linearizable GET within its current
static-membership Raft model, proven by targeted deterministic tests —
not a formally verified linearizability proof and not Byzantine fault
tolerant. Every GET pays for a quorum round trip (no lease reads, no
follower reads, no clock assumptions). Snapshotting is caller-triggered
only (no automatic threshold/schedule policy, no client-facing snapshot
API, no distributed snapshot coordination between nodes); this is not a
general-purpose storage engine. There is no membership-change support, no
request deduplication, and no exactly-once write claim.

## Layout

```text
internal/kv/          command representation, codec, and the deterministic KV state machine
internal/wal/         append-only write-ahead log (application command history, not the Raft log)
internal/transport/   bounded message framing and TCP request/response transport
internal/raft/        persistent Raft state, log replication, RequestVote/AppendEntries, leader election
internal/clientproto/ bounded binary client PUT/GET/DELETE wire protocol
internal/service/     wires a raft.Node to a kv.StateMachine and serves the client protocol
internal/client/      reusable leader-aware Go client
docs/                  format and design notes
```

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```
