# Runbook: leader loss and node failure

See [demo-failover.sh](../scripts/demo-failover.sh) /
[demo.md](demo.md#failover-demo) for a scripted walkthrough of this
exact sequence against real processes.

## A node has stopped responding

1. `qkv --addr <that node> status` to confirm it's actually unreachable
   (vs. slow — respect `--timeout`).
2. `qkv --addr <other node> status` against the remaining nodes to see
   whether a majority is still up. If it is, the cluster continues to
   serve writes/reads normally; only the failed node's own data is
   unavailable until it returns.
3. If the failed node was leader, the survivors elect a replacement
   automatically (real election timers — no manual intervention). Poll
   `status` on the survivors, or `qkv status --all`, until one reports
   `role: leader`.

## Restarting a failed node

Restart it with the **same** `--id`, `--listen`, `--data`, and a
current `--peer` list (see [operations.md](operations.md#bootstrap-membership-vs-persisted-membership) —
persisted membership is authoritative, `--peer` just needs to give
correct, current addresses for dialing). It will:

- recover its persisted term/vote/log/commit/snapshot state,
- rejoin as a follower,
- catch up via ordinary replication, or via `InstallSnapshot` if its log
  has been compacted away past what it's missing (see
  [snapshots.md](snapshots.md)).

Confirm catch-up by polling `qkv status` **directly against that node's
own address** (not a redirected `get`, which would just be answered by
whichever node is currently leader) until its `last-applied` reaches
the expected index.

## Corrupted node storage

If a node's data directory is corrupted (a torn write or mid-log
corruption), `quorumkv node` refuses to start and exits non-zero rather
than serving with unverified state — see
[crash-consistency.md](crash-consistency.md). When this happens:

- **Do not** delete the corrupted files and let the node "start fresh."
  A node given a corrupted-then-erased data directory does not
  automatically know it needs to be re-added to the cluster's
  membership — it would start as if brand new.
- **Do not** manually edit persisted files to attempt repair. There is
  no supported repair tool for canonical storage corruption.
- Preserve the corrupted data directory for diagnosis (copy it aside if
  you need the disk).
- Treat the node as permanently failed and replace it through the
  normal Raft membership procedure: `remove-voter` its old ID once a
  quorum of the remaining voters agrees it's gone, bring up a
  replacement node with a fresh (empty) data directory and a new ID,
  and `add-voter` it in — see
  [runbook-membership.md](runbook-membership.md). Do not try to reuse
  the old node's ID for a node with different history.

## A node keeps losing elections it should win, or the cluster won't
## settle on a leader

1. Check every reachable node's `status` — term and role. A term that
   keeps climbing without any node settling into `leader` usually means
   a real network partition, not a bug — see
   [failure-testing.md](failure-testing.md) and
   [raft-election.md](raft-election.md) for how PreVote is meant to
   behave here.
2. Check that every node's `--peer` list actually points at the current,
   reachable address of every other voter. A stale address prevents
   heartbeats from reaching that peer, which looks like election
   instability from the other side.
3. Do not "fix" this by lengthening timeouts as a first move — inspect
   terms, roles, and connectivity first (see the election-timeout
   guidance in `CLAUDE.md`).
