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

Raft leader election, heartbeats, persistent replicated logs, conflict
repair, and majority commit advancement are implemented, alongside the
single-node KV state machine, its application WAL, and the bounded TCP
transport from earlier milestones. See [docs/wal.md](docs/wal.md),
[docs/transport.md](docs/transport.md),
[docs/raft-election.md](docs/raft-election.md), and
[docs/raft-log-replication.md](docs/raft-log-replication.md).

Committed entries are not yet exposed through a distributed client API,
and are not yet applied to the KV state machine — there is no
`lastApplied` pipeline connecting Raft's `commitIndex` to `internal/kv`
yet. There is no snapshotting, no membership changes, and no
linearizability claim.

## Layout

```text
internal/kv/        command representation and the deterministic KV state machine
internal/wal/       append-only write-ahead log (application command history, not the Raft log)
internal/transport/ bounded message framing and TCP request/response transport
internal/raft/      persistent Raft state, log replication, RequestVote/AppendEntries, leader election
docs/                format and design notes
```

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```
