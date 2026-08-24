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

QuorumKV now bounds Raft log growth with snapshots: a node can serialize
its KV state and compact the log behind that boundary, and a leader whose
compacted prefix has outrun a stale follower's `nextIndex` automatically
falls back to a chunked `InstallSnapshot` RPC instead of an unsatisfiable
AppendEntries retry loop. This was proven with a real three-node cluster
over real TCP — not an in-process shortcut — where a follower taken
offline, left behind while the leader committed more writes and compacted
past it, and restarted from stale on-disk state, is brought back to full
convergence via `InstallSnapshot` followed by ordinary AppendEntries
catch-up; a second real-TCP test moves a snapshot spanning several 256 KiB
chunks end to end. See [docs/snapshots.md](docs/snapshots.md) for the
persistence formats, the persist-snapshot-before-compact-log safety
ordering, and the full test list.

This builds on: deterministic failure tests covering leader loss,
partitions, stale-follower catch-up, divergent-log repair, and persistent
restart recovery ([docs/failure-testing.md](docs/failure-testing.md));
committed Raft entries applied to the KV state machine, a leader-aware
binary PUT/GET/DELETE client API
([docs/state-machine.md](docs/state-machine.md),
[docs/client-protocol.md](docs/client-protocol.md)); and persistent Raft
election/log replication/heartbeats
([docs/raft-election.md](docs/raft-election.md),
[docs/raft-log-replication.md](docs/raft-log-replication.md)), plus
[docs/wal.md](docs/wal.md) and [docs/transport.md](docs/transport.md).

PUT/DELETE success means the entry was committed by Raft and applied on
the leader. GET is leader-only but quorum-confirmed linearizable reads
are not yet implemented — an isolated former leader may briefly still
believe it is Leader and serve a stale local GET before it learns of a
higher term; this is a known, documented limitation, not a write-safety
failure. Snapshotting is caller-triggered only (no automatic
threshold/schedule policy, no client-facing snapshot API, no distributed
snapshot coordination between nodes); this is not a general-purpose
storage engine. There is no membership-change support, no request
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
