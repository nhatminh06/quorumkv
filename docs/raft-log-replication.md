# Raft log replication

`internal/raft` now implements a persistent replicated Raft log,
AppendEntries (including heartbeats), leader-driven conflict repair, and
majority-based commit advancement. It builds directly on
[docs/raft-election.md](raft-election.md); read that first for roles,
persistent term/vote state, and RequestVote.

## Logical index convention

Index 0 is a sentinel meaning "no entry" (the position before the first
real entry); real entries start at index 1. `Log.Term(0)` is defined to be
`(0, true)`, so the empty-log convention already used by RequestVote
(`lastLogIndex=0, lastLogTerm=0`) and the AppendEntries prevLog sentinel
check both fall out of the same rule with no special-casing.

`internal/raft`'s `Log` is a dedicated component — **not** the Milestone 1
`internal/wal` — since a Raft log needs prefix matching, truncation, and
conflict replacement, which an application command WAL doesn't.

## Log entry and persistent format

```go
type LogEntry struct {
    Term    Term
    Command []byte // opaque; the log never interprets it
}
```

`Log` is rewritten atomically as a whole on every mutation (`Append` or
`TruncateAndAppend`), via the same temp-file/fsync/rename/directory-fsync
sequence as `PersistentState` (`atomicWriteFile`, shared by both). Because
the file is always a complete, atomically-replaced image, there is no
legitimate torn-tail case the way Milestone 1's append-only WAL has: any
structural problem on open — short read, bad checksum, an inconsistent or
oversized declared length — is `ErrCorruptLog`, never silently repaired or
reset to empty.

Per-entry record (big-endian), concatenated after a 5-byte file header
(`"RLG1"` magic + version):

```
recordLength    4B   length of everything below
term            8B
commandLength   4B
command         NB
CRC32C          4B   over term|commandLength|command
```

Limits: `maxCommandSize` = 256 KiB per entry, checked before allocation on
both encode and decode.

## AppendEntries wire format

Both payloads travel inside a `transport.Message`, whose frame already
carries a CRC32C over the whole payload, so neither RPC encoding
duplicates that checksum. All integers big-endian.

`AppendEntriesRequest` — fixed header (52 bytes) + entries:

```
term          8B
leaderID      8B
prevLogIndex  8B
prevLogTerm   8B
leaderCommit  8B
readContext   8B  (since Milestone 8; 0 for ordinary replication/heartbeat)
entryCount    4B
[entries]     each: term(8B) + commandLength(4B) + command(NB)
```

A heartbeat is exactly this with `entryCount = 0` — there is no separate
heartbeat RPC. `entryCount` is validated against `maxEntriesPerAppend`
(64) and each entry's `commandLength` against `maxCommandSize` before any
per-entry allocation, so a corrupt or hostile peer cannot force an
oversized allocation by declaring a huge count or length.

`AppendEntriesResponse` — 25 bytes:

```
term        8B
success     1B  (0 or 1)
matchIndex  8B  meaningful only when success == true
readContext 8B  (since Milestone 8; always echoes the request's)
```

This milestone uses simple `nextIndex--` backtracking on failure rather
than a conflict-term hint, so a failure response's `matchIndex` is unused
(sent as 0).

`readContext` (since Milestone 8) correlates a ReadIndex quorum probe —
an otherwise-ordinary, entries-free AppendEntries — with its response.
Critically, the response echoes it **even when `Success` is false** due
to a `prevLogIndex`/`prevLogTerm` mismatch: a same-term response from a
live peer proves current-term leadership regardless of whether that
peer's log happens to be caught up, so a log-replication failure must
not be confused with a ReadIndex quorum failure. See
[docs/read-index.md](read-index.md) for the full mechanism.

## Replication batch size

A leader sends up to `maxEntriesPerAppend` (64) entries per AppendEntries
call — a fixed cap rather than a dynamic byte-budget calculation, which
combined with the 256 KiB command limit keeps a request comfortably under
transport's 1 MiB frame limit. A leader with more to replicate than that
simply catches a peer up over further rounds (the next heartbeat tick, or
immediately after another Propose).

## Heartbeat behavior and timer reset

`defaultHeartbeatInterval` is 50ms, well below the 150–300ms election
timeout range, so a healthy leader's heartbeats reliably beat a follower's
timeout. A follower resets its election timer on *any* valid current-term
(or higher-term) AppendEntries contact — even if the prevLog consistency
check below fails — since the sender is still that term's leader and a
rejected consistency check is not a reason to start an election.

## Term handling

- `req.Term < currentTerm`: reject (`success=false`), no state change, no
  timer reset.
- `req.Term > currentTerm`: persist the new term with `votedFor` cleared,
  become Follower — before evaluating the rest of the request (reusing
  the same `stepDownLocked` RequestVote already uses).
- `req.Term == currentTerm` and this node is Candidate or Leader: step
  down to Follower without a term change (`stepToFollowerLocked`) — valid
  same-term leader contact proves another leader already exists for this
  term.

## Log consistency check and conflict repair

A follower accepts entries only if `Term(prevLogIndex) == prevLogTerm`
(the sentinel `prevLogIndex=0` always matches). Otherwise it rejects with
`success=false` and makes no log change — the leader will back off
`nextIndex` and retry.

If the check passes and entries are present, the follower scans forward
for the first index where its own entry doesn't match the incoming one
(same rule: missing entirely, or present with a different term). Only
from that point does it truncate its suffix and append the leader's
entries (`Log.TruncateAndAppend`) — the matching prefix before that point
is never touched. If every incoming entry already matches (an idempotent
retransmission, or a plain heartbeat with no entries), nothing is written
to disk at all. The resulting log is persisted before `success=true` is
returned.

## Leader replication state

`nextIndex`/`matchIndex` are volatile, re-initialized fresh every time a
node becomes Leader (`becomeLeaderLocked`): `nextIndex[peer] =
lastLogIndex + 1`, `matchIndex[peer] = 0`. Neither is ever persisted.

On a successful response, `matchIndex[peer]` advances to `prevLogIndex +
len(entries)` from that request and `nextIndex[peer] = matchIndex[peer] +
1` — but only if that's higher than the peer's current `matchIndex`, so a
stale-but-successful older response can never regress it. On failure,
`nextIndex[peer]` decreases by one (never below 1) for a retry on the next
round.

Every applied response is first checked against current state: a response
carrying a higher term forces step-down; a response whose sender's
`sentTerm` no longer matches `currentTerm`, or whose recipient is no
longer Leader, is stale and dropped without being applied.

## Commit rule

```text
commitIndex may advance to N only if:
  N > commitIndex
  a majority (including self) has matchIndex >= N
  log[N].term == currentTerm
```

The `currentTerm` restriction is mandatory: an older-term entry is never
committed by majority replication alone. It can only become committed as
a side effect of committing a later current-term entry — `commitIndex`
jumps straight to the highest qualifying `N`, which implicitly commits
every earlier index too (Raft's Log Matching Property guarantees a
majority holding a later matching entry also holds every entry before it
from the leader's log).

`Propose` also triggers this check locally right after appending, so a
single-node cluster commits its own proposals immediately without waiting
on any network round trip.

## Commit propagation to followers

A leader advancing `commitIndex` does not push that fact to followers
immediately — it rides along as `leaderCommit` on the next AppendEntries
or heartbeat. A follower sets `commitIndex = min(leaderCommit,
lastLogIndex)`, and only if that's greater than its current value, so a
follower's `commitIndex` never exceeds its own log and never decreases.

## Propose (internal only)

```go
func (n *Node) Propose(command []byte) (LogIndex, error)
```

Leader-only: returns `ErrNotLeader` otherwise. Copies `command` so caller
mutation afterward cannot change the persisted entry. Appends `{currentTerm,
command}` and persists it before returning; if that persistence fails, the
log is left unchanged and the entry is never treated as proposed — no fake
commit. On success it kicks off one immediate replication round in the
background (in addition to the regular heartbeat cadence) and returns the
new entry's index; it does not wait for replication or commitment.

There is still no external client protocol — only internal Go code can
call `Propose`. Client-facing writes wait for the next milestone, once
committed entries are actually applied to the KV state machine.

## Concurrency and network-lock discipline

Unchanged from Milestone 3: a single mutex protects all of `Node`'s
state, and RPCs (both RequestVote and now AppendEntries) are never sent
while it's held — `StartElection` and `replicateToAllPeers` each snapshot
what they need, unlock, do the I/O concurrently, and re-lock only to
apply each response.

The leader's heartbeat loop is bound to the `Node`'s own long-lived
background context (`bgCtx`), not to whatever short-lived `ctx` happened
to trigger the leadership transition, so it keeps running on its own
schedule until this node steps down (`stepDownLocked`/
`stepToFollowerLocked` cancel it) or `Node.Close` is called.

## Committed entries feed application (since Milestone 5)

`commitIndex` is now durably persisted (see
[docs/state-machine.md](state-machine.md)) rather than purely volatile,
specifically so restart knows which entries are safe to replay. Every
commit-advancing site (`maybeAdvanceCommitIndexLocked` on the leader, the
`leaderCommit` branch of `HandleAppendEntries` on a follower) persists the
new value before updating `Node`'s in-memory `commitIndex`, then triggers
`Node`'s apply loop. `internal/service.Service` wires a client PUT/DELETE
to wait on that pipeline (`Propose` then `WaitApplied`) before
acknowledging — see [docs/client-protocol.md](client-protocol.md).

## Logical base index and log compaction (since Milestone 7)

`Log` no longer assumes its physical entry slice starts at logical index
1. It tracks a `baseIndex`/`baseTerm` boundary; physical entry `entries[i]`
is logical index `baseIndex + i + 1`. Before any compaction this is
`(0, 0)`, identical to the sentinel this document already describes above
— every invariant in this file (`Term(0) == (0, true)`, the AppendEntries
prevLog sentinel check, conflict repair, the commit rule) is unchanged and
still holds exactly as written; compaction only affects how far back
physical history reaches, never the logical indexing scheme itself.

Once compacted, `Term(index)` for `index < baseIndex` returns `(0,
false)` — an explicit "unavailable," never a fabricated term — and a
leader whose `nextIndex` for some peer has fallen to or below its
`baseIndex` falls back from AppendEntries to the `InstallSnapshot` RPC for
that peer instead of retrying a request it can no longer satisfy. Full
detail, including the snapshot persistence format, the persist-before-
compact safety ordering, and follower-side installation, is in
[docs/snapshots.md](snapshots.md).

## Known limitations

- No AppendEntries conflict-term optimization: backtracking is simple
  `nextIndex--`, one index per failed round.
- No membership changes; the peer set is static.
- No quorum-confirmed linearizable reads (no ReadIndex yet) — see
  docs/client-protocol.md's GET section.
- No request deduplication / exactly-once write semantics.
