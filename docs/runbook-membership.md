# Runbook: changing cluster membership

See [demo-membership.sh](../scripts/demo-membership.sh) /
[demo.md](demo.md#membership-demo) for a scripted walkthrough of this
exact sequence, and [membership.md](membership.md) for the underlying
joint-consensus protocol.

## Ground rules

- Change **one voter at a time**. Do not issue a second `add-voter` or
  `remove-voter` while a previous one is still in progress — it is
  rejected with "a membership change is already in progress" rather
  than queued or merged.
- Both `add-voter` and `remove-voter` are leader-only and only report
  success once the change has reached its final **stable** configuration
  — not merely once a joint configuration was appended/committed.
- If a command times out, its outcome is genuinely ambiguous from the
  client's point of view — **do not immediately retry**. Check `status`
  on the leader first (see below) before deciding what to do next.

## Adding a voter

1. Start the new node as its own standalone process, with a fresh data
   directory and no `--peer` flags pointing at the existing cluster —
   it does not need to already know about the group it's joining
   (`add-voter` supplies its address to the existing cluster, not the
   other way around).
2. From the current leader:
   ```bash
   qkv --addr <leader> add-voter --id <new-id> --peer-address <new-node-addr>
   ```
3. Confirm: `qkv --addr <new-node-addr> status` should report
   `membership: stable` with the new node included in `voters:`.

## Removing a voter

1. From the current leader:
   ```bash
   qkv --addr <leader> remove-voter --id <id-to-remove>
   ```
2. Confirm: `qkv status` on any remaining voter should report
   `membership: stable` without the removed ID in `voters:`.
3. The removed node's own process is not stopped by this — it simply
   stops being part of the consensus group. Stop it separately if it
   should no longer run at all.

## If a change times out or the outcome is unclear

1. `qkv --addr <leader> status` (or `--addr <any voter>` if the old
   leader itself is now unreachable). While `membership: joint` is
   shown, the change is still in flight — wait and re-check rather than
   retrying.
2. Once it settles to `membership: stable`, compare `voters:` against
   what you intended. If the change you wanted is reflected, it
   succeeded and nothing further is needed even though the original
   command reported a timeout. If it is not reflected, re-issue the
   original command against the current leader.
3. Never issue a second, different membership change to try to "undo" an
   ambiguous one before confirming the first one's actual outcome via
   `status`.

## Removing the last voter / invalid configurations

`remove-voter` on a change that would leave the cluster without a valid
quorum (e.g. removing the last remaining voter) is rejected with "that
change would produce an invalid configuration" — it is never silently
allowed.
