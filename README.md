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

QuorumKV now includes deterministic failure tests covering leader loss,
majority/minority partitions, stale-follower catch-up, divergent-log
repair, and persistent restart recovery — proving, with executable
evidence rather than a formal proof, that the Raft implementation from
earlier milestones behaves correctly under controlled node and network
failures. A three-node test cluster was verified to: elect a replacement
leader and preserve committed writes after a leader crash; continue
committing writes with one node unavailable; refuse to commit on a leader
isolated from the majority; repair a partitioned follower's divergent
uncommitted suffix while leaving its matching prefix untouched; and
rebuild term/vote/log/commitIndex/applied-KV state from disk after a
restart. See [docs/failure-testing.md](docs/failure-testing.md) for the
full scenario table and current limitations.

This builds on: committed Raft entries applied to the KV state machine,
a leader-aware binary PUT/GET/DELETE client API
([docs/state-machine.md](docs/state-machine.md),
[docs/client-protocol.md](docs/client-protocol.md)), and persistent Raft
election/log replication/heartbeats
([docs/raft-election.md](docs/raft-election.md),
[docs/raft-log-replication.md](docs/raft-log-replication.md)), plus
[docs/wal.md](docs/wal.md) and [docs/transport.md](docs/transport.md).

PUT/DELETE success means the entry was committed by Raft and applied on
the leader. GET is leader-only but quorum-confirmed linearizable reads
are not yet implemented — an isolated former leader may briefly still
believe it is Leader and serve a stale local GET before it learns of a
higher term; this is a known, documented limitation, not a write-safety
failure. There is no snapshotting, no membership changes, no request
deduplication, and no exactly-once write claim.

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
