# Runbook: leadership transfer

See [demo-leadership-transfer.sh](../scripts/demo-leadership-transfer.sh)
/ [demo.md](demo.md#leadership-transfer-demo) for a scripted
walkthrough, and [leadership-transfer.md](leadership-transfer.md) for
the underlying protocol.

## Why transfer leadership deliberately

Planned maintenance (restarting or upgrading the current leader's host,
rebalancing load onto a specific node) is safer as an intentional,
observable handoff than as an induced crash-and-reelect.

## Performing a transfer

```bash
qkv --addr <current-leader> transfer-leadership --target <target-id>
```

This only prints `leadership transferred to node <target-id>` once the
old leader has real evidence the target actually won an election as the
new leader — never merely that the target accepted a `TimeoutNow`. The
target is brought fully caught-up (via ordinary replication, or
`InstallSnapshot` if it was behind a compacted log) before the handoff
is attempted; new write/read/membership admission is frozen only once
the handoff itself begins.

## Example maintenance sequence

```bash
qkv --addr 127.0.0.1:7001 status                                  # confirm node 1 is leader
qkv --addr 127.0.0.1:7001 transfer-leadership --target 2          # hand off to node 2
qkv --addr 127.0.0.1:7002 status                                  # confirm node 2 is now leader
# now safe to stop/restart/upgrade node 1
```

## If the transfer fails or times out

- **"the target explicitly declined the leadership transfer"**: the
  target rejected the handoff (it may not be fully caught up, or has
  its own reason to decline). Do not retry blindly — check
  `qkv status` on both the current leader and the intended target
  first.
- **"a leadership transfer is already in progress"**: another transfer
  is in flight; wait for it to resolve (check `status`) rather than
  issuing a second one.
- **Timeout ("outcome is uncertain")**: check `status` on the original
  leader and the intended target before retrying. If the target already
  reports `role: leader`, the transfer in fact succeeded despite the
  timeout — do not attempt a second transfer back based only on the
  original command's exit status.
- **"cannot transfer leadership to this node itself"**: `--target` must
  name a different voter.

Do not automatically retry a leadership transfer on any failure or
timeout without first inspecting `status` — an ambiguous transfer left
unretried is always safer than compounding it with a second one.
