# Operations

How to run, inspect, and administer a QuorumKV cluster with the real
`quorumkv` node executable and the `qkv` client/admin CLI. See
[demo.md](demo.md) for scripted end-to-end walkthroughs and
[architecture.md](architecture.md) for how the pieces fit together.

## Starting a node

```bash
./bin/quorumkv node \
  --id 1 \
  --listen 127.0.0.1:7001 \
  --data ./data/node1 \
  --peer 2=127.0.0.1:7002 \
  --peer 3=127.0.0.1:7003
```

`--id`, `--listen`, and `--data` are required. `--peer ID=ADDR` may be
repeated once per other node. A single listener address serves both
Raft RPCs and the client/admin protocol — there is no separate port.

### Data directory

`--data` owns everything this node persists: Raft state (`state`),
log (`log`), commit index (`commit`), and snapshots (`snapshot`). It is
created if missing; if the path exists and is not a directory, startup
fails with an error naming the path. Never share a data directory
between two node processes.

### Bootstrap membership vs. persisted membership

`--peer` flags only matter the first time a node starts with an empty
data directory — they define who the node bootstraps its Raft group
with (itself plus the given peers, all as voters). On every later
restart, the membership actually persisted in that node's log/snapshot
is authoritative; `--peer` flags are still required (to give the node a
current, dialable address for each other node) but do **not** override
persisted membership, even if they list a different peer set. If a
node's real cluster membership has changed since it last started
(voters added/removed), pass its current peer list — passing a stale
one does not roll membership back.

### Graceful shutdown

`SIGINT`/`SIGTERM` triggers a graceful shutdown: the listener stops
accepting new connections and in-flight requests are given a chance to
finish, then the node closes. A single signal is enough.

### Corrupt persistent storage

If a node's data directory contains corrupted state (a torn write or
mid-log corruption — see [crash-consistency.md](crash-consistency.md)),
startup fails loudly and the process exits non-zero **before** serving
any request. It does not delete, reset, or reinterpret the corrupted
file. Do not delete files to "fix" this — see
[runbook-failover.md](runbook-failover.md#corrupted-node-storage).

## The qkv CLI

```bash
qkv --addr 127.0.0.1:7001 [--addr 127.0.0.1:7002 ...] [--timeout 5s] <command> [args]
```

`--addr` may be repeated to give more than one entry point into the
cluster; `--timeout` bounds every network operation (default 5s) via
`context.WithTimeout` — no command waits forever. Every command supports
`-h`/`--help`.

Values are treated as UTF-8 text throughout — `put`/`get`/`delete` take
plain string keys/values, there is no hex/binary encoding option.

Each `qkv` invocation is a fresh process with a fresh client identity:
there is no session persistence across invocations. A `put`/`delete`
retried by running `qkv` again is a genuinely new request as far as the
cluster's dedup table is concerned — see
[request-dedup.md](request-dedup.md). Within a single invocation, a
write that times out is safe to simply re-run.

### put / get / delete

```bash
qkv --addr 127.0.0.1:7001 put x 1
qkv --addr 127.0.0.1:7001 get x
qkv --addr 127.0.0.1:7001 delete x
```

`put`/`delete` print `OK` on success. `get` prints the value on success;
on a missing key it prints `not found` and exits with status 3 (see
Exit codes below), rather than printing anything ambiguous.

`get` follows a `NOT_LEADER` redirect but does **not** fail over to a
different `--addr` on a connection failure — if the first reachable
seed is down, provide only currently-reachable addresses.

### status

```bash
qkv --addr 127.0.0.1:7001 status
```

Reports the addressed node's own observable state — role, term,
log/commit/apply indices, snapshot boundary, and membership — directly
from that node, without redirecting elsewhere. This is deliberately
**not** a linearizable read: it does not run ReadIndex or touch any
term/log/commit/membership/timer state. It is operational metadata,
useful for understanding what one node currently believes, not for
reading application data (use `get` for that).

`qkv status --all` queries every configured `--addr` in turn and prints
each result — there is no cluster discovery, so it can only ever see
the addresses given.

Example output:

```text
node:           1
role:           leader
term:           4
leader:         1
last-log-index: 12
commit-index:   12
last-applied:   12
snapshot-index: 0
snapshot-term:  0
membership:     stable
voters:
  1 127.0.0.1:7001
  2 127.0.0.1:7002
  3 127.0.0.1:7003
```

During a joint-consensus membership change, `membership: joint` is
shown along with separate `old voters:` and `new voters:` lists instead
of `voters:`.

### snapshot

```bash
qkv --addr 127.0.0.1:7001 snapshot
```

Leader-only. Triggers `Node.CreateSnapshot` on the addressed node and
reports the resulting boundary, e.g. `OK snapshot-index=12
snapshot-term=4`. There is no automatic snapshot scheduling — see
[snapshots.md](snapshots.md).

### transfer-leadership

```bash
qkv --addr 127.0.0.1:7001 transfer-leadership --target 2
```

Leader-only. Only reports success (`leadership transferred to node 2`)
once the current leader has real evidence the target actually became
leader — never merely that the handoff request was accepted. Do not
blindly retry a timed-out transfer; inspect `status` on both nodes
first. See [runbook-leadership-transfer.md](runbook-leadership-transfer.md).

### add-voter / remove-voter

```bash
qkv --addr 127.0.0.1:7001 add-voter --id 4 --peer-address 127.0.0.1:7004
qkv --addr 127.0.0.1:7001 remove-voter --id 4
```

Leader-only. Success means the joint-consensus change has actually
reached its final stable configuration, not merely that the joint
configuration was appended. Change one voter at a time; do not issue an
overlapping change while one is in progress (it is rejected with "a
membership change is already in progress"). See
[runbook-membership.md](runbook-membership.md).

## Handling common conditions

- **`node is not leader; leader hint: ADDR`**: retry the same command
  against the hinted address, or against a different `--addr` you
  already have.
- **`node is not leader; no leader hint known`**: the addressed node
  doesn't currently know who the leader is (mid-election, or isolated);
  wait briefly and retry, or try a different `--addr`.
- **`server is busy (overloaded)`** (`BUSY`): the server's proposal
  queue or admission bound is full; back off briefly and retry — see
  [performance.md](performance.md).
- **`timed out waiting for a definite outcome`**: an admin operation's
  outcome is genuinely unknown from the client's point of view. Check
  `status` before deciding whether to retry — do not blindly resend a
  membership or leadership-transfer request.

## Exit codes

`qkv` uses a small, stable set: `0` success, `1` general operational
failure (server error, timeout, etc.), `2` usage error (bad flags/args),
`3` key not found (only from `get`). These are part of the documented
CLI contract.

## Known operational limitations

- The client/admin wire protocols are unauthenticated and unencrypted —
  intended for a local or trusted-network/educational environment, not
  a public network. No TLS, no authentication.
- No automatic cluster discovery, leader balancing, or reconfiguration —
  every membership and leadership change is operator-initiated.
- No repair tooling for corrupted persistent storage; a corrupted node
  must be replaced via the normal Raft membership procedure
  (`remove-voter` + `add-voter` with a fresh data directory), not
  patched in place.
- `qkv`'s client identity is per-invocation; there is no cross-process
  session persistence.
