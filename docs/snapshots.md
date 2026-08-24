# Snapshots and log compaction

Milestone 7 solves unbounded Raft log growth: `internal/raft`'s log now
supports compaction behind a durable snapshot boundary, and a leader can
bring a badly stale follower up to date via `InstallSnapshot` instead of
replaying its entire retained history. This builds directly on
[docs/raft-log-replication.md](raft-log-replication.md) (persistent log,
AppendEntries, commit rule) and [docs/state-machine.md](state-machine.md)
(the KV state machine being snapshotted); read those first.

## 1. Motivation

Before this milestone, a follower that fell behind recovered only by
replaying the leader's complete retained log from its old `nextIndex`.
That works only as long as the full historical log stays on disk forever
— every write, forever, on every node. A follower offline for a long time,
or a fresh node joining with nothing, had no bounded way to catch up.
Snapshots break that: once a prefix of the log is known to be safely
captured in a point-in-time state-machine snapshot, that prefix can be
discarded, and a stale follower is brought up to date with one bounded
transfer of the snapshot instead of an unbounded replay.

## 2. Logical model: base index and base term

`Log` now tracks `baseIndex`/`baseTerm` in addition to its physical entry
slice: physical entry `entries[i]` corresponds to logical index
`baseIndex + i + 1`. Before any compaction, `baseIndex = 0, baseTerm = 0`
— exactly Milestone 3's original empty-log sentinel — so every
Milestone 3–6 log file and test fixture still loads correctly with no
migration step (see §4).

`LastIndex()`/`LastTerm()`/`Term(index)`/`Entry(index)`/`EntriesFrom(from)`
are all boundary-aware:

- `Term(baseIndex)` returns `(baseTerm, true)` — the boundary itself is
  always answerable, never fabricated.
- `Term(index)` for `index < baseIndex` returns `(0, false)`: an explicit
  "compacted, unavailable" signal, never a fabricated term. Only a
  genuinely out-of-range query (`index < 0` conceptually, or in this log's
  case `index < baseIndex`) gets `false`; nothing downstream is allowed to
  treat that `false` as "index 0" or "term 0 is real."
- `Entry(index)` returns `false` for `index <= baseIndex` — command bytes
  are never retained past the boundary, even for the boundary index
  itself (its term is known; its command is not, and was never meant to
  be — the snapshot is what stands in for it).
- `EntriesFrom(from)` clamps `from <= baseIndex` up to `baseIndex + 1`, so
  a caller that still thinks a compacted prefix exists doesn't get handed
  entries logically before the boundary.

## 3. KV state serialization (`internal/kv`)

`StateMachine.Snapshot() ([]byte, error)` and `StateMachine.Restore(data
[]byte) error` serialize/restore the entire key/value map deterministically:

```
version(1B) | entryCount(4B) | repeated{ keyLen(4B) valLen(4B) key valLen }
```

Keys are sorted (`sort.Strings`) before encoding specifically because Go
map iteration order is randomized — two snapshots of identical state must
produce byte-identical output, proven by
`TestSnapshotIsDeterministicRegardlessOfInsertionOrder`. Bounds
(`MaxSnapshotEntries`, per-key/value size vs. `kv.MaxKeySize`/
`MaxValueSize`) are checked before allocating anything, and `Restore` only
replaces `m.state` after the entire input has decoded successfully — a
malformed snapshot never partially mutates live state
(`TestRestoreIsAtomicOnMalformedInput`).

## 4. Raft snapshot persistence (`internal/raft`)

`SnapshotStore` persists exactly one canonical `Snapshot{LastIncludedIndex,
LastIncludedTerm, Data}` per node, atomically rewritten on every `Save`
(the same temp-file/fsync/rename/directory-fsync sequence as
`PersistentState`/`Log`):

```
magic(4B "SNP1") | version(1B) | lastIncludedIndex(8B) | lastIncludedTerm(8B)
  | payloadLength(8B) | payload(NB) | CRC32C(4B, over version..payload)
```

A missing file is `(nil, nil)` from `Load` — "no snapshot yet," not an
error. `Data` is opaque to this package (it's whatever `kv.Snapshot`
produced); the 64 MiB bound here is kept in sync with `kv.MaxSnapshotSize`
so a legal KV snapshot always fits a legal Raft one.

The on-disk `Log` format grew a version 2 header (`baseIndex(8B) +
baseTerm(8B)` after the existing magic+version) to carry the boundary.
Version 1 files (no such fields) still decode correctly — implicitly
`baseIndex=0, baseTerm=0` — and any subsequent mutation of a v1 file
transparently upgrades it to v2 on the next rewrite
(`TestLogV1FileStillLoads`).

## 5. Compaction: two related but distinct operations

`Log.Compact(newBaseIndex, newBaseTerm)` is the **leader's own routine**
compaction: it only ever trims a prefix of history the log already has and
already trusts, so it asserts `newBaseIndex <= LastIndex()` and is a
no-op if `newBaseIndex <= baseIndex` (never regresses).

`Log.InstallSnapshotBoundary(newBaseIndex, newBaseTerm)` is the more
general operation a **follower installing a leader-sent snapshot** needs:
the follower's own log may be shorter than the snapshot boundary, or
diverge from it entirely. It checks `Term(newBaseIndex)` against
`newBaseTerm` — if they match, the verified suffix beyond the boundary is
retained (identical effect to `Compact`); if they don't match (including
the case where the follower's log doesn't reach that far at all), the
**entire local log is discarded**, since none of it can be trusted to
lead into a snapshot it disagrees with.

These are kept as two separate methods rather than one, because
conflating "my own consistent history" with "state a remote peer just
told me to trust" would blur a real safety distinction.

## 6. Snapshot/apply atomicity (`applyMu`)

A snapshot's `(lastIncludedIndex, data)` pair must describe *exactly* the
same logical state — never one command ahead of, or behind, what
`lastIncludedIndex` claims. `Node` now has a second lock, `applyMu`
(distinct from `mu`, the Raft bookkeeping lock), held by both the apply
loop's `ApplyFunc` call and `CreateSnapshot`'s `SnapshotFunc` call. This
guarantees the two can never interleave, while still never holding the
cheap-to-contend `Node.mu` during a potentially large serialization —
`CreateSnapshot` reads `lastApplied` and unlocks `mu` before calling
`SnapshotFunc`.

## 7. CreateSnapshot: persist-before-compact ordering

```go
func (n *Node) CreateSnapshot() error
```

is this milestone's only snapshot trigger — there is no automatic
size/count threshold policy; a caller (test or future operator surface)
decides when to call it. It captures `index = lastApplied`, serializes via
`SnapshotFunc`, and **only after `SnapshotStore.Save` succeeds** calls
`Log.Compact`. If `Save` fails, the log is left exactly as it was
(`TestCreateSnapshotSaveFailureLeavesLogUncompacted`) — this package never
performs "delete log, hope snapshot succeeds."

## 8. Crash-window recovery on startup

A crash between "snapshot persisted" and "log compacted" is possible and
expected — `NewNode` treats it as ordinary recovery, not corruption. On
startup, if a loaded snapshot's `LastIncludedIndex > log.BaseIndex()`,
`NewNode` calls `Log.Compact` to finish the interrupted step (idempotent:
a no-op if already done) *before* validating `commitIndex <=
log.LastIndex()`. `commitIndex`/`lastApplied` are then raised to at least
the snapshot's boundary if they weren't already there, and `RestoreFunc`
is called to rebuild application state, before any committed suffix beyond
the snapshot is replayed on top (`TestRestartFinishesInterruptedCompaction`,
`TestRestartFromSnapshotOnly`, `TestRestartFromSnapshotPlusSuffix`).

## 9. InstallSnapshot RPC and chunking

Snapshots can exceed transport's 1 MiB single-frame limit, so
`InstallSnapshot` is chunked — bounded to 256 KiB (`maxSnapshotChunkSize`)
per RPC, never one giant frame:

```
InstallSnapshotRequest:  term(8B) leaderID(8B) lastIncludedIndex(8B)
  lastIncludedTerm(8B) offset(8B) done(1B) dataLength(4B) data(NB)
InstallSnapshotResponse: term(8B) success(1B) nextOffset(8B)
```

`dataLength` is validated against `maxSnapshotChunkSize` before
allocation. A follower accumulates chunks in memory
(`Node.incoming`) keyed by `(leaderID, term, lastIncludedIndex,
lastIncludedTerm)`; only the exact next chunk (`offset ==
len(accumulated)`) for the current session is accepted, and any mismatch
in session identity is treated as the start of a fresh session, which
must itself begin at `offset 0`
(`TestHandleInstallSnapshotSessionMismatchRestartsAtZero`). Nothing is
installed until the final (`done=true`) chunk arrives.

## 10. Leader-side detection and sending

`replicateToAllPeers` now branches per peer: if `baseIndex > 0` (this
leader has compacted) and that peer's `nextIndex <= baseIndex`, the
entries it would need have already been discarded — an ordinary
AppendEntries there would fail forever. Instead of building an
AppendEntries request for that peer, the leader starts (or lets continue,
guarded by `snapshotSending[peer]` so a second transfer never starts
concurrently) a background transfer via `sendSnapshotToPeer`, which loops
sending sequential 256 KiB chunks — bound to the node's own long-lived
background context, not the short-lived `ctx` of whichever heartbeat tick
or `Propose` call happened to trigger it, since a transfer can span far
longer than either. On the final chunk's success, `matchIndex`/`nextIndex`
for that peer advance past the snapshot boundary, and ordinary
AppendEntries replication resumes on the very next round
(`TestLeaderInstallsSnapshotToStaleFollower`).

## 11. Follower-side installation ordering

`HandleInstallSnapshot`'s term handling mirrors `AppendEntries` exactly
(stale rejected, higher term steps down, valid same-term contact resets
the election timer). Once the final chunk arrives, installation happens
in this fixed order, matching §7's leader-side rule:

1. Persist the canonical snapshot (`SnapshotStore.Save`).
2. Reconcile the log boundary (`Log.InstallSnapshotBoundary` — retain a
   verified suffix or discard entirely; see §5).
3. Advance and persist `commitIndex` if the snapshot boundary is beyond
   it.
4. Replace application state (`RestoreFunc`).
5. Advance `lastApplied`, wake any waiters, and resume the ordinary apply
   loop for any retained suffix beyond the boundary.

A snapshot at or behind the follower's current `lastApplied` is
acknowledged successfully without reinstalling anything
(`TestHandleInstallSnapshotStaleSnapshotIsIdempotent`) — a
duplicate or superseded transfer is a no-op, not an error, and never
regresses state.

## 12. RequestVote / AppendEntries at the snapshot boundary

Both RPCs already went through `Log.Term`/`LastIndex`, so they
automatically became boundary-aware once `Log` itself was:
`RequestVote`'s log-freshness comparison correctly uses `baseTerm` when a
voter's log has no physical entries left
(`TestRequestVoteAtSnapshotBoundary`), and `AppendEntries`'s
`PrevLogIndex == baseIndex` case is validated against `baseTerm`, not
treated as a missing/always-matching entry
(`TestAppendEntriesPrevLogAtSnapshotBoundary`).

## 13. Testing evidence and current limitations

Unit coverage: KV snapshot determinism and corruption handling
(`internal/kv/snapshot_test.go`), Raft snapshot file format including a
hand-derived byte vector (`internal/raft/snapshot_test.go`),
`InstallSnapshot` codec including a hand-derived byte vector
(`internal/raft/install_snapshot_test.go`), log compaction/boundary
behavior (`internal/raft/log_test.go`), and `CreateSnapshot`/
`HandleInstallSnapshot`/restart-from-snapshot
(`internal/raft/snapshot_node_test.go`) — stale/higher term, session
identity, offset mismatch, multi-chunk accumulation, boundary
retain-vs-discard, mid-transfer term change, and interrupted-compaction
recovery.

Integration coverage (`internal/raft/snapshot_integration_test.go`):
in-process leader-driven detection and catch-up
(`TestLeaderInstallsSnapshotToStaleFollower`); the mandatory real-TCP
three-node scenario (`TestSnapshotCatchUpEndToEndRealTCP`) — C goes
offline, A+B commit past it, A snapshots and compacts, more commits
follow, C restarts from stale disk over a fresh real TCP port, A detects
C is behind its compacted prefix and sends `InstallSnapshot`, ordinary
AppendEntries resumes, and all three converge; and a large-snapshot
scenario over real TCP spanning several 256 KiB chunks
(`TestLargeSnapshotOverRealTCP`). See
[docs/failure-testing.md](failure-testing.md) for the corresponding
fault-injection scenario.

Known limitations, unchanged from the plan: no automatic snapshot
threshold/schedule policy (`CreateSnapshot` is caller-triggered only); no
client-facing snapshot/compact API; no generic pluggable storage engine
(the KV state machine's `Snapshot`/`Restore` are specific to
`internal/kv`); no distributed snapshot coordination between nodes (each
node decides independently when to call `CreateSnapshot`); no membership
changes; no request deduplication — all unchanged from prior milestones'
documented scope. As of Milestone 8, ReadIndex/quorum-confirmed
linearizable GET exists (see [docs/read-index.md](read-index.md)) and is
correctly snapshot-boundary aware: a current-term commit barrier that
gets compacted into a snapshot's `lastIncludedTerm`/`lastIncludedIndex`
still satisfies ReadIndex's current-term-committed check without
requiring the physical log entry to still exist.
