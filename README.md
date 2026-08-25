# QuorumKV

QuorumKV is a distributed key-value store built to study consensus,
replication, and failure recovery from first principles. Raft is
implemented from scratch in this repository — no external consensus
library.

## Why QuorumKV

Most Raft implementations are libraries you import. QuorumKV is the
opposite: every mechanism — persistent state, log replication, leader
election, PreVote, snapshotting, joint-consensus membership changes,
leadership transfer, request deduplication, quorum-confirmed reads — is
implemented and tested here, with the invariant each one enforces
stated explicitly rather than assumed. It is not a library; it is a
runnable system with its own CLI and operational tooling.

## What it implements

- Persistent Raft: term/vote/log/commit state survive a restart, with
  explicit checksummed binary formats and deterministic crash-recovery
  tests (real subprocess kills at each meaningful write point).
- Leader election with PreVote, avoiding disruptive term bumps from an
  isolated node.
- Log replication with per-peer event-driven catch-up and bounded
  batching.
- Snapshotting and chunked `InstallSnapshot` catch-up for followers
  behind a compacted log.
- Joint-consensus membership changes (add/remove one voter at a time).
- Deliberate leadership transfer, confirmed by real evidence the target
  won, not just that a handoff was accepted.
- Quorum-confirmed (`ReadIndex`) linearizable GET.
- At-most-once state-machine effects for retried PUT/DELETE, via a
  replicated per-client request identity.
- Proposal batching and bounded backpressure (`BUSY` instead of
  unbounded queuing).
- A real node executable and a client/admin CLI, driven by real OS
  processes over real TCP in the mandatory integration tests and demo
  scripts — not just in-process test harnesses.

## Architecture

```text
Client
  ↓
Leader
  ↓
Replicated Raft Log
  ↓
Committed Commands
  ↓
KV State Machine
```

Full breakdown — process model, write/read paths, election,
replication, persistence, snapshotting, membership, leadership
transfer, and the operational control plane — in
[docs/architecture.md](docs/architecture.md).

## Correctness properties

- **Linearizable GET**: quorum-confirmed via `ReadIndex`, eliminating
  stale reads from an isolated old leader — see
  [docs/read-index.md](docs/read-index.md).
- **At-most-once PUT/DELETE effect** for a retried request that reuses
  the same `ClientID`/sequence, including across a leader failover —
  see [docs/request-dedup.md](docs/request-dedup.md).
- **Joint-consensus membership changes**: every quorum decision during
  a transition requires a majority of both the old and new
  configuration — see [docs/membership.md](docs/membership.md).
- **Deterministic crash recovery**: every persisted file recovers to
  exactly its old or exactly its new content after a process killed at
  any point mid-write, proven with real subprocess crashes — see
  [docs/crash-consistency.md](docs/crash-consistency.md).

These are implemented and tested, not formally proven. See Limitations.

## Performance evidence

Measured on this repository's own benchmark harness (single machine,
see [docs/performance.md](docs/performance.md) for exact environment
and commands — do not extrapolate these numbers to other hardware):

- Concurrent-write throughput improved after proposal batching let
  concurrent `Propose` calls share one durable log write.
- Stale-follower catch-up (5000-entry lagging suffix) dropped from
  3.95s to 0.27s (~14.7x) after event-driven per-peer replication
  workers replaced fixed-heartbeat-interval catch-up, with no observed
  regression to steady-state throughput.

Full methodology in [docs/performance.md](docs/performance.md) and
[docs/replication-performance.md](docs/replication-performance.md).

## Quick start

```bash
git clone <this-repo>
cd quorumkv
make build
./scripts/start-local-cluster.sh
./bin/qkv --addr 127.0.0.1:7001 put x 1
./bin/qkv --addr 127.0.0.1:7001 get x
./bin/qkv --addr 127.0.0.1:7001 status
./scripts/stop-local-cluster.sh
```

`start-local-cluster.sh` starts a real 3-node cluster on
`127.0.0.1:7001-7003`, prints each node's PID, and waits for a leader
to be elected. Full CLI reference in
[docs/operations.md](docs/operations.md).

## Running a node directly

```bash
./bin/quorumkv node \
  --id 1 --listen 127.0.0.1:7001 --data ./data/node1 \
  --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003
```

See [docs/operations.md](docs/operations.md) for startup, shutdown,
data-directory, and membership semantics.

## Failure demo

```bash
./scripts/demo-failover.sh
```

Kills the real leader process (`SIGKILL`), confirms the surviving
majority elects a replacement and previously committed data survives,
writes a second key through the new leader, then restarts the crashed
node from its same on-disk data directory and confirms it catches up —
proving real election, replication, and disk-backed persistence, not
in-memory survival. Four more scripted demos (basic KV, leadership
transfer, snapshot/`InstallSnapshot` catch-up, membership change) are
described in [docs/demo.md](docs/demo.md).

## Repository structure

```text
cmd/quorumkv/          the node server executable
cmd/qkv/                the client/admin CLI
internal/reqid/         client request identity types (ClientID, Sequence)
internal/kv/             command representation, codec, deterministic KV state machine + dedup
internal/wal/            append-only write-ahead log (application command history)
internal/transport/     bounded message framing and TCP request/response transport
internal/raft/           persistent Raft state, replication, election, snapshotting, membership
internal/clientproto/   bounded binary client PUT/GET/DELETE wire protocol
internal/adminproto/    bounded binary operational admin wire protocol
internal/service/       wires a raft.Node to a kv.StateMachine; serves both wire protocols
internal/client/         reusable leader-aware Go client with safe write retry
scripts/                 local cluster lifecycle + reproducible demos
docs/                    protocol design notes, operations guide, runbooks, architecture
```

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```

`make check` runs vet, build, and test together. The mandatory
process-integration tests in `cmd/quorumkv` build and run the actual
`quorumkv`/`qkv` binaries as real OS processes over real TCP with real
persistent directories — the strongest available proof that the
executables work the way an operator would use them, not just that the
internal packages do.

## Design documents

Protocol-level design notes with the reasoning and full test list for
each mechanism live in [docs/](docs/): election, replication,
read-index, request dedup, membership, leadership transfer,
snapshotting, crash consistency, WAL/state-machine format, transport,
and performance. [docs/architecture.md](docs/architecture.md) is the
top-level map; [docs/operations.md](docs/operations.md) and the
`docs/runbook-*.md` files cover running and administering a cluster.

## Limitations

- Crash fault model only — not Byzantine fault tolerant.
- No TLS or authentication on either wire protocol; intended for a
  local or trusted-network/educational environment.
- No client-side session persistence across a process restart — each
  `qkv` invocation is a fresh client identity.
- The write-dedup `ClientID` table has no garbage collection yet.
- Membership changes are one voter at a time; no batched changes, no
  automatic rebalancing or discovery.
- Snapshot creation is manually triggered; no scheduling policy.
- No repair tooling for corrupted persistent storage — a corrupted node
  is replaced via the normal membership procedure, not patched in
  place. See [docs/runbook-failover.md](docs/runbook-failover.md).
- One TCP connection per RPC — no persistent connection pooling.
- No sharding, multi-Raft, transactions, CAS, TTL, follower reads, or
  leader leases.

## Out of scope

Kubernetes/Helm manifests, a REST/gRPC API, a web dashboard,
Prometheus/Grafana integration, automatic cluster discovery, and
performance optimization beyond what's already measured above — none of
these are goals of this project.

## Engineering highlights

- Every persisted format is explicit and checksummed — no `gob` or
  other opaque serialization for correctness-critical state — with
  deterministic tests that kill a real subprocess mid-write and assert
  the file recovers to exactly one of its two valid states.
- Joint-consensus membership changes require a majority of *both* the
  old and new configuration for every quorum decision during a
  transition, never a majority of their union.
- Leadership transfer only reports success once the old leader observes
  real evidence the target won an election — never merely that a
  handoff request was accepted.
- Request deduplication is fully replicated state, not a leader-local
  cache: a retried write is recognized correctly even by a brand-new
  leader that only ever received the original request via
  `InstallSnapshot`.
- The mandatory CLI integration tests spawn the actual production
  binaries as OS processes and simulate a leader crash with `SIGKILL`,
  not `Node.Close()` — proving the on-disk recovery path, not just the
  in-memory one.
