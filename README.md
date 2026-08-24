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
write-ahead log (WAL) with crash-safe replay, and a bounded TCP transport
for exchanging framed messages between nodes. See
[docs/wal.md](docs/wal.md) for the WAL record format and
[docs/transport.md](docs/transport.md) for the wire format and transport
behavior.

The transport carries messages only — it has no Raft logic. Not yet
implemented: Raft itself (elections, replication, terms, commit), a
client-facing API, snapshots, and any consistency guarantees.

## Layout

```text
internal/kv/        command representation and the deterministic KV state machine
internal/wal/       append-only write-ahead log
internal/transport/ bounded message framing and TCP request/response transport
docs/                format and design notes
```

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```
