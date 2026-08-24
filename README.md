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

Single-node deterministic KV state machine, a persistent append-only
write-ahead log (WAL) with crash-safe replay, a bounded TCP transport for
exchanging framed messages between nodes, and persistent Raft term/vote
state with RequestVote-based leader election for an empty-log cluster.
See [docs/wal.md](docs/wal.md), [docs/transport.md](docs/transport.md),
and [docs/raft-election.md](docs/raft-election.md).

Election alone does not make a stable or usable distributed system yet:
there is no AppendEntries or heartbeats (an elected leader has no way to
stay leader), no Raft log or log replication, no commit index, no
client-facing writes through Raft, no snapshots, and no consistency
guarantee of any kind.

## Layout

```text
internal/kv/        command representation and the deterministic KV state machine
internal/wal/       append-only write-ahead log
internal/transport/ bounded message framing and TCP request/response transport
internal/raft/      persistent Raft term/vote state and RequestVote leader election
docs/                format and design notes
```

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```
