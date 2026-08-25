# Demos

Five scripts under `scripts/` exercise QuorumKV as real OS processes
over real TCP with real on-disk state — no in-process test harness, no
mocked networking. Each is self-contained: it resets and starts its own
local cluster, runs, and stops the cluster on exit (including on
failure, via a shell trap).

## Prerequisites

```bash
make build
```

produces `bin/quorumkv` and `bin/qkv`. All scripts use fixed loopback
ports `127.0.0.1:7001-7003` (plus `7004` for the membership demo) and
store data under `.local/quorumkv/` (gitignored).

## Cluster lifecycle scripts

```bash
./scripts/start-local-cluster.sh   # start a real 3-node cluster, print PIDs/ports, wait for a leader
./scripts/stop-local-cluster.sh    # gracefully stop only the nodes it started (by PID file)
./scripts/reset-local-cluster.sh   # stop, then remove .local/quorumkv/ (data/logs/pids)
```

`stop`/`reset` only ever touch PIDs recorded in `.local/quorumkv/pids/`
by `start-local-cluster.sh` itself — they never search for or signal
unrelated processes.

Once a cluster is running:

```bash
./bin/qkv --addr 127.0.0.1:7001 put x 1
./bin/qkv --addr 127.0.0.1:7001 get x
./bin/qkv --addr 127.0.0.1:7001 status
```

## Basic KV demo

```bash
./scripts/demo-basic.sh
```

Starts a fresh cluster, `put`s a key, `get`s it back, `delete`s it,
confirms `get` now reports `not found`, and prints one node's status.

## Failover demo

```bash
./scripts/demo-failover.sh
```

Finds the real elected leader, writes `x=1`, `SIGKILL`s the leader
process (a genuine crash, not a graceful stop), confirms the surviving
majority elects a replacement and `x` is still readable, writes `y=2`
through the new leader, then restarts the crashed node **from its same
on-disk data directory** and confirms — by polling that node's own
`status`, not a redirected `get` — that it catches up on both keys.
Proves real election, replication, and disk-backed persistence, not
in-memory survival.

## Leadership transfer demo

```bash
./scripts/demo-leadership-transfer.sh
```

Asks the current leader to transfer leadership to a specific peer and
confirms the target's own `status` reports it as leader afterward.

## Snapshot / catch-up demo

```bash
./scripts/demo-snapshot.sh
```

Writes five keys, snapshots the leader, takes a follower offline,
writes five more keys and snapshots again (compacting the log past
where the offline follower left off), then restarts that follower and
confirms it catches up — necessarily via `InstallSnapshot`, since the
log entries it's missing no longer exist anywhere in the cluster — and
that all ten keys are readable everywhere afterward.

## Membership demo

```bash
./scripts/demo-membership.sh
```

Starts a 3-voter cluster (A/B/C), starts a 4th node as a standalone
process not yet part of that cluster's consensus group, `add-voter`s it
in through the real leader, confirms all four report a stable 4-voter
configuration, writes and reads through the 4-node cluster, then
`remove-voter`s it back out and confirms A/B/C return to a stable
3-voter configuration.

## Cleanup

```bash
./scripts/reset-local-cluster.sh
```

removes all demo state. Each demo script also does this itself before
starting, so demos can be re-run back to back.
