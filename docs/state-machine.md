# Committed-entry application

This covers how a committed Raft log entry becomes visible KV state:
`internal/kv`'s command codec, and `internal/raft`'s application pipeline
(`lastApplied`, durable commit metadata, restart replay). See
[docs/raft-log-replication.md](raft-log-replication.md) for how an entry
becomes committed in the first place, and
[docs/client-protocol.md](client-protocol.md) for how a client write
actually waits on this pipeline before acknowledging.

## Command format

`internal/kv.EncodeCommand`/`DecodeCommand` define the bytes a Raft log
entry's `Command` field holds for a PUT or DELETE. GET is never encoded —
it is a read, with nothing to replicate.

```
version      1B   = 1
operation    1B   = PUT (1) or DELETE (2)
keyLength    4B
valueLength  4B
key          N bytes
value        M bytes
```

Big-endian. A DELETE must not carry a value (`valueLength` must be 0).
Bounds: `MaxKeySize` = 64 KiB, `MaxValueSize` = 200 KiB — comfortably
under Raft's 256 KiB per-entry limit (`internal/raft`'s
`maxCommandSize`) once this format's overhead is included, so an encoded
command always fits one legal log entry. Declared lengths are validated
before any allocation based on them.

Raft never imports `internal/kv` — it only knows commands as opaque
bytes. The connection is a narrow callback:

```go
type ApplyFunc func(index LogIndex, command []byte) error
```

`internal/service.Service.Apply` implements it: decode with
`kv.DecodeCommand`, apply to a `kv.StateMachine`. That's the only place
Raft's log content is given meaning.

## commitIndex vs. lastApplied

These stay distinct:

```text
appended    — in this node's local log
replicated  — a majority has it (Milestone 4)
committed   — replicated + current-term rule satisfied (Milestone 4)
applied     — this process has run it through ApplyFunc (Milestone 5)
```

`commitIndex` is **durable**, persisted separately from `currentTerm`/
`votedFor`/the log itself (see below) — this milestone needs to
distinguish, after a restart, which of the log's entries are safe to
replay into the application versus still-uncommitted.

`lastApplied` is **volatile**, never persisted directly. It is
reconstructed every process run by replaying entries `1..commitIndex`
through `ApplyFunc` from scratch. Invariant: `0 <= lastApplied <=
commitIndex <= lastLogIndex`, and `lastApplied` never decreases.

## Application order and exactly-once-per-run

`Node` applies committed entries one at a time, strictly in log order,
starting from `lastApplied+1`, never skipping and never reapplying an
index within one process run. A single apply loop (started or re-checked
whenever `commitIndex` might have advanced) owns this — there is no
per-entry goroutine and no parallel application. `Node`'s own lock is
never held while `ApplyFunc` runs; the loop locks only to pick the next
entry and again to record the result before continuing.

Repeated heartbeats carrying the same (already-known) `leaderCommit` do
not reapply anything — the loop only ever starts from `lastApplied+1` and
does nothing if there's no unapplied committed entry.

## Apply failure

If `ApplyFunc` returns an error — including a malformed committed command
found on disk during restart replay — application halts **permanently**
for that process run: `lastApplied` stops advancing past the failing
index, the error is recorded (`Node.ApplyError`), and every waiter for an
index beyond that point (present and future) receives it. Later entries
are never applied out of turn past a failure. Restart-time corruption is
treated the same as a live failure — disk data is validated, not trusted
merely because it originated locally.

## Durable commit metadata

`CommitStore` persists `commitIndex` in its own small file, separate from
`PersistentState` (`currentTerm`/`votedFor`) and the Raft log:

```
magic        4B   "CMT1"
version      1B
commitIndex  8B
checksum     4B   CRC32C over version|commitIndex
```

Same atomic-replace discipline as the rest of this project's persistent
files (temp file, fsync, rename, directory fsync). A missing file means a
fresh node (`commitIndex` 0); an existing-but-invalid file (bad magic,
version, size, or checksum) is `ErrCorruptCommitMeta` — never silently
treated as 0.

Whenever local `commitIndex` would advance — on the leader via majority
replication, on a follower via `leaderCommit` — the new value is
persisted **before** it becomes visible in memory. If that persist fails,
`commitIndex` is left unchanged: the underlying Raft entry may already be
logically committed cluster-wide (a majority has it), but this node
cannot treat it as durably recorded locally, so it does not advance
`commitIndex`, does not trigger application, and (transitively) does not
let a waiting client see success. The next commit-advancing trigger
(heartbeat, response) retries.

`Node.NewNode` rejects a persisted `commitIndex` greater than the log's
own last index — that combination should never occur from this
package's own persistence ordering, and is treated as corruption rather
than silently clamped.

## Restart replay

On startup, `Node` loads `currentTerm`/`votedFor`, the log, and the
durable `commitIndex` (validating `commitIndex <= lastLogIndex`), starts
as Follower (role is never persisted), and kicks off the same apply loop
used during normal operation — there is no separate "replay" code path.
Only entries `1..commitIndex` are ever applied; an uncommitted suffix
stays in the log, untouched, until a future leader's AppendEntries
resolves it one way or another.

## Concurrency

`kv.StateMachine` is not safe for concurrent use. `internal/service`
serializes every access to it (the `ApplyFunc` callback and GET reads)
behind its own mutex — a lock distinct from `raft.Node`'s internal one,
never held across a call into Raft, and never held while `ApplyFunc`
itself is invoked by Raft (Raft already guarantees it isn't called
concurrently with itself; the service lock only protects the state
machine from a concurrent GET).
