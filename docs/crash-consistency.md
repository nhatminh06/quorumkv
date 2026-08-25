# Crash consistency

This document describes what QuorumKV proves about a node's on-disk state
after the process is killed at an arbitrary point during a durable write,
how it proves it, and what it deliberately does not claim.

## What this is not

This is **not** a proof of formal crash consistency, not a guarantee
against arbitrary physical disk/media failure, and not a claim that every
combination of filesystem and storage device preserves these properties.
It proves one narrower thing: **deterministic process-crash recovery, at
explicitly injected persistence boundaries, under the Linux/POSIX
filesystem semantics this implementation relies on.**

No test here powers off a machine, unmounts a filesystem, or corrupts a
physical device. There is no loopback/ext4 mount, no VM reboot, no sudo.
Every "crash" is a real `os.Exit` of a real OS process at a specific,
named point in a real durable write — proving that no in-process cleanup
(defers, `Close`, in-memory rollback) was ever load-bearing for the
guarantee.

## Persistence dependency graph

```text
currentTerm / votedFor (Store)         — independent of everything else
Raft log (Log)                         — entries after the snapshot boundary
  ├─ baseIndex/baseTerm                — the compaction boundary
  └─ entries[baseIndex+1 .. lastIndex]
commit metadata (CommitStore)          — must be >= the snapshot boundary
                                          once a snapshot exists
Raft snapshot (SnapshotStore)
  ├─ lastIncludedIndex/lastIncludedTerm — the log's baseIndex/baseTerm once applied
  ├─ Configuration                      — the stable membership as of the boundary
  └─ Data (opaque to internal/raft)
        └─ kv.StateMachine's own encoding of:
              ├─ KV entries
              └─ dedup table (ClientRecord map)
```

Four independent files, one dependency rule: once a snapshot exists, the
log's base and the commit index must both be at least the snapshot's
boundary. `NewNode`'s startup path enforces this by reconciling the log
(`Log.InstallSnapshotBoundary`) and commit metadata forward to match a
loaded snapshot, regardless of which of the three files a crash left
furthest behind.

Membership and dedup are not separate persistence concerns: a
configuration-change entry is an ordinary `LogEntry`, persisted by the
same log rewrite as any other command, and the dedup table is encoded
inside the same opaque `Snapshot.Data` blob as KV state — neither has its
own failure mode beyond the log's and the snapshot's.

## The durability primitive

Every durable file this package owns — `Store`, `Log`, `CommitStore`,
`SnapshotStore` — funnels through exactly one function,
`atomicWriteFile` (`internal/raft/atomic_file.go`). There is a single
publication contract for the whole package:

1. write to a temp file in the same directory
2. fsync the temp file
3. close it
4. rename it over the target path (atomic replace on POSIX)
5. fsync the containing directory (so the rename itself survives a crash)

A reader — including a freshly restarted process — always observes
either the complete previous file or the complete new file. There is no
append-only log format in this codebase to reason about separately: the
Raft log is rewritten as a whole file on every mutation, so the exact
same guarantee covers it.

**Checksums are not durability.** Every file format here also carries a
CRC32C checksum, but that only detects corruption after the fact — it is
`atomicWriteFile`'s write/fsync/rename/dir-fsync ordering that decides
*which* version (old or new) is the one a reader ever sees. **Rename
alone is not enough** either: without the preceding fsync, a rename can
be durable while the data it points at is not; without the trailing
directory fsync, the rename itself might not survive a crash on some
filesystems.

## Two distinct failure modes

- **I/O failure injection** (`crashpoint_test.go`): a failpoint returns
  an error from inside `atomicWriteFile` at a named stage. The caller's
  normal Go error handling runs — this is indistinguishable from a real
  disk error, and proves the *return-early* path never leaves a
  malformed file.
- **Crash injection** (`crash_subprocess_test.go`, `crash_matrix_test.go`,
  `internal/service/crash_dedup_test.go`): the test binary re-executes
  itself as a subprocess (the standard
  `exec.Command(os.Args[0], "-test.run=...")` pattern), which performs
  the real operation and calls `os.Exit` at the target point — no
  defers, no `Close`, no in-memory rollback ever run. The parent verifies
  the subprocess actually reached that point (a distinct exit code *and*
  a stderr marker; a subprocess that exits any other way fails the test)
  before inspecting the resulting directory with genuinely fresh
  `Store`/`Log`/`CommitStore`/`SnapshotStore`/`Node` objects.

These are deliberately not conflated: a returned error still allows
cleanup to run; a real crash never does. The failpoint hook
(`internal/raft/failpoint.go`) is a single function type
(`func(name string) error`) that serves both modes — return an error for
the first, call `os.Exit` inside it for the second — but no production
code path is ever gated on an environment variable; the hook is `nil`
(a no-op) unless a test installs one, and only test files ever do.

## Old-or-new, not independently-either

The invariant proved throughout is: after a crash during a durable
write, the observed state is *exactly* the complete old value or
*exactly* the complete new value — never a state that mixes fields from
each in a way that was never legally reachable together. For example, a
term/vote crash test never accepts "term is 5 or 6" and "vote is A or B"
as independent possibilities; it accepts only the single old
`PersistentState` or the single new one, as a whole record.

`publicationCompletedAt(stage)` (`crashpoint_test.go`) formalizes the
boundary: `after-rename` and `after-dir-fsync` are the only stages where
the real `os.Rename` has already happened, so only failures injected at
or after that point may observe the new value; every earlier stage must
observe the old one.

## Crash matrix

Only rows with an executed, currently-passing test are marked PASS.

| Operation | Crash point | Allowed recovered state | Forbidden state | Test | Result |
|---|---|---|---|---|---|
| Term/vote save | before-temp-write / after-temp-write / after-temp-fsync | old term/vote | new term/vote, partial file | `TestStableStateFailpointOldOrNew`, `TestStableStateRealCrashOldOrNew` | PASS |
| Term/vote save | after-rename / after-dir-fsync | new term/vote | old term/vote, partial file | same | PASS |
| Log append | before-temp-write .. after-temp-fsync | old log (entry not present) | new entry partially present | `TestLogFailpointOldOrNew`, `TestLogAppendRealCrashOldOrNew` | PASS |
| Log append | after-rename / after-dir-fsync | new log (entry present) | old log, torn file | same | PASS |
| Log conflict repair (TruncateAndAppend) | before-temp-write .. after-temp-fsync | pre-repair suffix | mixed suffix | `TestLogConflictRepairRealCrashOldOrNew` | PASS |
| Log conflict repair | after-rename / after-dir-fsync | repaired suffix | pre-repair suffix, mixed suffix | same | PASS |
| Commit metadata save | before-temp-write .. after-temp-fsync | old commitIndex | new commitIndex | `TestCommitMetaFailpointOldOrNew`, `TestCommitMetaRealCrashOldOrNew` | PASS |
| Commit metadata save | after-rename / after-dir-fsync | new commitIndex | old commitIndex | same | PASS |
| Snapshot publish | before-temp-write .. after-temp-fsync | old snapshot | new snapshot, partial file | `TestSnapshotFailpointOldOrNew` | PASS |
| Snapshot publish | after-rename / after-dir-fsync | new snapshot | old snapshot, partial file | same | PASS |
| CreateSnapshot (snapshot publish then log compact) | any stage in either file | log base <= a snapshot's boundary that actually covers it; fresh Node opens and starts | log compacted past any covering snapshot | `TestCreateSnapshotRealCrashCrossFileConsistency` | PASS |
| InstallSnapshot (snapshot, then log boundary, then commit meta) | any stage in any of the three files | fresh Node opens; if a snapshot published, log reaches its boundary and commitIndex >= it | snapshot published with log left behind it (see below); commitIndex < snapshot boundary | `TestInstallSnapshotRealCrashCrossFileConsistency` | PASS |
| AppendEntries ack | immediately after a real Success=true response | acked entry present, correct term/command | acked entry missing or altered | `TestAppendEntriesAckedEntryRealCrashSurvives` | PASS |
| Dedup table (inside snapshot) | immediately after CreateSnapshot, real process kill | fresh Service recognizes the identified request as a duplicate; Get returns the original value | request reapplied; Get returns nothing/wrong value | `TestSnapshotDedupSurvivesRealCrash` | PASS |
| writeFull short-write handling | n < len(p), err == nil, repeated | all bytes eventually written | infinite loop, truncated write | `TestWriteFullHandlesShortWrites` | PASS |
| writeFull no-progress handling | n == 0, err == nil | hard error (`errNoWriteProgress`), no hang | infinite loop | `TestWriteFullRejectsNoProgress` | PASS |
| Failpoint coverage | every domain × stage name | fires at least once during normal operation | a declared failpoint no test ever reaches | `TestFailpointStagesAllReached` | PASS |

### The InstallSnapshot bug this matrix found and fixed

`TestInstallSnapshotRealCrashCrossFileConsistency` initially failed:
`NewNode`'s startup reconciliation used `Log.Compact` to advance the log
to a loaded snapshot's boundary, which requires the log to already reach
that index. A crash between `installSnapshot`'s snapshot publish and its
own log-boundary rewrite leaves exactly the opposite — a durable
snapshot whose boundary the log does not yet reach — and `Compact`
rejected it outright, so a restart in that window could fail to open at
all. The fix (`internal/raft/node.go`) replaces that call with
`Log.InstallSnapshotBoundary`, the same general reconciliation
`installSnapshot` itself already uses at runtime, which correctly
discards a non-matching or short local suffix instead of assuming one is
already present.

## No false success

- A follower never returns `AppendEntries.Success = true` for an entry
  that is not yet durable — proved by `TestAppendEntriesAckedEntryRealCrashSurvives`
  killing the process immediately after a real ack and confirming the
  entry survives.
- `HandleInstallSnapshot` never acknowledges a chunk before every
  preceding durable step (snapshot save, log boundary, commit metadata)
  for that installation has completed — see `installSnapshot`'s ordering
  in `internal/raft/snapshot_node.go`.
- A client-visible OK is never returned for a write this node could not
  itself durably persist; existing Milestone 9 dedup/retry tests
  (`internal/service/dedup_test.go`) continue to pass unmodified,
  confirming this milestone did not weaken that path.

## Volatile state that must never be persisted

Confirmed by inspection of `NewNode` and the crash tests above, which
always reconstruct a `Node` from disk with no in-memory carryover: role
always starts `Follower`; `leaderID` starts nil; leadership-transfer
state (target/originalTerm/phase) has no persistent representation at
all; PreVote performs no persistent mutation of its own. None of this
milestone's work touches any of those paths.

## Tested filesystem assumptions

Linux/POSIX only. Specifically relied upon:

- `os.Rename` within the same directory is atomic with respect to a
  concurrent reader.
- `File.Sync` (fsync) on a regular file makes its just-written contents
  durable.
- `File.Sync` on a directory file descriptor makes changes to that
  directory's entries (the rename above) durable.

No claim is made about non-POSIX filesystems, network filesystems, or
filesystems without atomic rename semantics.

## What remains unsupported / not implemented

- No live multi-process 3-node integration test exists yet where a real
  separate OS process is killed mid-operation while two other real nodes
  continue serving traffic over TCP and the restarted node catches up
  (items 91, 92's "cluster continues" half). What is proved instead: (a)
  every individual durable operation survives a real crash in isolation
  (this matrix), and (b) a healthy cluster tolerates a node's *loss*
  during in-process testing (existing `internal/raft/fault_recovery_test.go`,
  `internal/service/fault_test.go` from earlier milestones, using
  graceful `Close()` rather than `os.Exit`). Composing the two into one
  live multi-process scenario is future work.
- No scripted crash-torture sequence (repeated crash/restart/operate
  cycles chained together) or seeded stress test exists yet.
- No dedicated membership-specific crash test exists beyond the generic
  log/snapshot matrix above. This is a deliberate scope decision, not an
  oversight: a configuration-change entry is an ordinary `LogEntry`
  covered by `TestLogAppendRealCrashOldOrNew`/`TestLogConflictRepairRealCrashOldOrNew`,
  and stable membership at a snapshot boundary is an ordinary
  `Snapshot.Configuration` field covered by
  `TestCreateSnapshotRealCrashCrossFileConsistency`/`TestInstallSnapshotRealCrashCrossFileConsistency`
  — no membership-specific durability code path exists that those don't
  already exercise.
- Short-write, fsync-failure, and rename-failure injection are exercised
  generically via the failpoint mechanism (any stage can return an
  error), but no dedicated test isolates a short write specifically
  inside `atomicWriteFile`'s real temp-file write beyond `writeFull`'s
  own unit tests (`atomic_file_test.go`), which use a synthetic writer.
- PreVote/leadership-transfer-specific crash regression tests (proving a
  restarting stale node cannot disrupt a healthy leader through term
  inflation) were not added this milestone; the volatile-state claims
  above are inspection-based, not test-based.
