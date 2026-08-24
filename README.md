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

QuorumKV now applies committed Raft entries to the KV state machine and
exposes a leader-aware binary PUT/GET/DELETE client API, on top of
persistent Raft election, log replication, and heartbeats from earlier
milestones. See [docs/wal.md](docs/wal.md),
[docs/transport.md](docs/transport.md),
[docs/raft-election.md](docs/raft-election.md),
[docs/raft-log-replication.md](docs/raft-log-replication.md),
[docs/state-machine.md](docs/state-machine.md), and
[docs/client-protocol.md](docs/client-protocol.md).

PUT/DELETE success means the entry was committed by Raft and applied on
the leader. GET is leader-only but quorum-confirmed linearizable reads
are not yet implemented. There is no snapshotting, no membership changes,
no request deduplication, and no exactly-once write claim.

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
