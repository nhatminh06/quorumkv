# ReadIndex: quorum-confirmed linearizable GET

Milestone 8 closes QuorumKV's last documented consistency gap: GET was
leader-local and role-check-based, not quorum-confirmed. This builds on
[docs/raft-log-replication.md](raft-log-replication.md) (AppendEntries,
commit rule) and [docs/state-machine.md](state-machine.md) (apply
pipeline); read those first.

## 1. The previous stale-read limitation

Before this milestone, GET checked `Role == Leader` and then waited for
this node's own `lastApplied` to catch up to its own `commitIndex` — both
purely local facts. A node can believe `Role == Leader` after being
partitioned away from the rest of the cluster: nothing forces it to learn
about a higher term promptly. During that window it could, in principle,
answer a GET from state that a majority-elected successor had already
overwritten — a real linearizability violation, previously documented as
a known, un-closed gap (see the Milestone 6/7 failure-testing docs).

## 2. The ReadIndex safety requirement

A leader may serve a linearizable read only after:

1. it knows it has committed an entry from its **current term**, and
2. it has confirmed, by communicating with a quorum in that term, that it
   still holds leadership.

The index it read-confirms this way (`readIndex`) is then safe to read
through once this node's own state machine has caught up to it
(`lastApplied >= readIndex`). This is the standard Raft ReadIndex
protocol; QuorumKV implements it in `raft.Node.ReadIndex` and
`raft.Node.ensureCurrentTermCommitted` (`internal/raft/read_index.go`).

## 3. Why current-term commit is required

Requirement 1 above is not optional. A newly elected leader's
`commitIndex` may still only reference entries from a *previous* term
(Raft's commit rule never lets majority replication alone commit an
old-term entry — see raft-log-replication.md — but the old-term entry
can still be *the* current `commitIndex`, committed as a side effect once
something newer does commit). Serving a read against that state without
first proving a current-term entry is committed risks reading behind a
write a *different* leader already committed and this node hasn't
learned about yet — the scenario Raft's ReadIndex protocol exists to
rule out.

`hasCurrentTermCommitLocked` checks this directly:

```text
commitIndex > 0 AND Log.Term(commitIndex) == currentTerm
```

`Log.Term` is already snapshot-boundary aware (see
[docs/snapshots.md](snapshots.md)): if `commitIndex` equals the log's
compaction boundary, `Term` returns the snapshot's `lastIncludedTerm`
without needing the physical entry to still exist, so this check works
identically whether the current-term commit is a live log entry or a
compacted-away boundary.

## 4. The current-term barrier and the internal no-op

If the check above fails, `ensureCurrentTermCommitted` establishes the
barrier itself: it appends a **reserved empty-command** Raft log entry
(`LogEntry{Term: currentTerm, Kind: EntryNoop, Command: nil}`),
replicates it through the ordinary AppendEntries path, and waits for it
to commit and apply.

An empty command is safe to reserve this way because
`kv.EncodeCommand`'s wire format always emits at least its fixed header
(version + operation + two length fields) — a legitimate application
command is never zero bytes. `Node.Propose` therefore rejects an empty
command outright (`ErrReservedCommand`); only the internal barrier path
(`proposeLocked` called from inside `ensureCurrentTermCommitted`) may
construct one. The apply loop classifies by `Kind` (Milestone 10; before
that, by `len(Command)==0` — see [docs/membership.md](membership.md) §3
for why an explicit field replaced that inference) and advances
`lastApplied` for any non-`EntryApplication` entry **without** ever
invoking `ApplyFunc` — the no-op carries no application meaning and must
never reach the KV decoder. This behaves identically on restart replay
(the same apply-loop code path) and is compatible with snapshot
compaction (a snapshot boundary can legally sit on a no-op entry; the KV
snapshot simply has nothing to say about it).

Concurrent first reads in a new term single-flight onto one barrier:
`Node.pendingBarrier`, guarded by the same `Node.mu` used everywhere else
(no separate lock), tracks at most one in-flight barrier per term. A
second `ensureCurrentTermCommitted` call for the same term joins the
existing barrier's completion instead of appending its own no-op. The
barrier's own commit-wait runs bound to the node's own background
context (`n.bgCtx`), not the calling read's `ctx` — so one caller's
cancellation never aborts the barrier for a different, still-waiting
caller; the barrier keeps trying until it commits, is superseded by
conflict repair (`ErrEntryLost`, reusing the existing apply-waiter
mechanism — see docs/state-machine.md), or the node closes.

## 5. ReadContext: correlating a quorum probe

`AppendEntriesRequest`/`AppendEntriesResponse` both gained a `ReadContext
uint64` field. `0` means "ordinary replication or heartbeat, not a read
probe" — every existing AppendEntries call site continues to send `0`
implicitly. A ReadIndex quorum probe generates a non-zero value
(`Node.nextReadContextLocked`, a simple in-process counter — not
persisted, not cryptographically random, not a client/dedup identifier;
uniqueness only needs to hold among this process's currently active
reads) and sends it on an otherwise-ordinary, entries-free AppendEntries.

## 6. AppendEntries correlation and the response echo

The wire format (`internal/raft/append_entries.go`) grew one `uint64`
field on each side:

```text
AppendEntriesRequest:  term | leaderID | prevLogIndex | prevLogTerm |
                        leaderCommit | readContext | entryCount | entries...
AppendEntriesResponse: term | success(1B) | matchIndex | readContext
```

The response **always echoes the request's `ReadContext`**, even when
`Success` is `false` because the follower's log doesn't match
`prevLogIndex`/`prevLogTerm` — as long as the request was decoded and
handled normally. This is the crux of why the probe works even against a
badly out-of-date follower: log replication success and ReadIndex quorum
confirmation are different properties. A same-term response from a live,
correctly-behaving peer proves it currently recognizes this node as
leader; it says nothing about whether that peer's log happens to be
caught up. `HandleAppendEntries` is otherwise completely unchanged by a
read probe — the same term rules, timer reset, and (if `LeaderCommit`
advances) follower commit/apply advancement apply, since a probe is a
real AppendEntries, not a separate RPC.

A probe never carries `Entries` and is never routed through the
replication bookkeeping (`nextIndex`/`matchIndex`,
`applyAppendEntriesResponse`) that ordinary AppendEntries responses
update — a follower rejecting a probe because it's behind must not affect
its replication state, and a probe must never trigger `InstallSnapshot`.
`ReadIndex` sends probes directly via the same `sendAppend` function
ordinary replication uses (so fault-injection tests intercept both
identically) but processes the responses itself.

## 7. Quorum counting

`ReadIndex` counts exactly as `StartElection`/commit-advancement do:
`Membership.HasQuorum` on an `acked` set built from real responses (an
unreachable node does not shrink the denominator) — a plain majority of
the current Stable configuration outside a transition, or a majority of
*both* Old and New simultaneously during a Milestone 10 Joint transition
(see [docs/membership.md](membership.md) §5; a majority of Old alone is
provably insufficient — `TestJointReadIndexRequiresBothMajorities`). The
leader counts itself once, immediately, with no network I/O — a
single-node cluster's `ReadIndex` never sends an RPC. Each peer's
response counts at most once
per read (a `seen` map keyed by peer ID); a response only counts if its
`Term` equals the term the read was started in and its `ReadContext`
matches this specific probe's — a stale term, an old read's leftover
response, or a lower term are all silently ignored, never subtracted from
quorum. A response carrying a **higher** term forces this node to persist
it and step down (via the same `stepDownLocked` every other higher-term
path uses — no read-specific bypass), and `ReadIndex` aborts with
`ErrNotLeader`. Quorum confirmation returns as soon as enough distinct
peers have acknowledged — it does not wait for every peer, and the
per-read `context.WithCancel` wrapping outbound probes is canceled on
return so stragglers stop being awaited (any peer goroutine still
in-flight is tracked by `Node.bgWG` like all other background work, so
`Close` still waits for it and no goroutine leaks).

## 8. readIndex selection

Once quorum is confirmed, `ReadIndex` re-verifies (under `Node.mu`) that
this node is still Leader in the term the read was started in, then
returns its **current** `commitIndex` — which may have advanced further
than it was when `ReadIndex` was first called. That is safe: a later
commit only means more state is now certifiably durable. `ReadIndex`
never returns an uncommitted `lastLogIndex`.

## 9. WaitApplied: ReadIndex does not itself read

`ReadIndex` only establishes the safe read boundary; it performs no
application-state read at all. The caller (`Service.get`, in
`internal/service/service.go`) does:

```text
readIndex, err := node.ReadIndex(ctx)
node.WaitApplied(ctx, readIndex, 0)
sm.Get(key)
```

exactly mirroring the existing PUT/DELETE path's `Propose` +
`WaitApplied` structure. `term=0` is passed to `WaitApplied` because
`readIndex` is already known-committed (supersession cannot happen for an
already-committed index — see docs/state-machine.md).

## 10. The linearization point

**The read is linearized at the moment `ReadIndex` successfully confirms
quorum for its `ReadContext`** — not at the initial role check, and not
at the later local `WaitApplied`/KV-read step. Once quorum has confirmed,
a *subsequent* loss of leadership does not retroactively invalidate that
already-established point: the read is still safe to complete by locally
catching up to the returned index and reading state, even if this node
stops being leader a moment later. There is no requirement that this node
remain leader until the response bytes leave the socket.

## 11. Partition behavior

An isolated (minority-side) leader can still append its barrier no-op
locally, but cannot commit it without a majority — so `ReadIndex` never
returns a value for it: `commitIndex`/`lastApplied` never advance past
what they already were, and the call fails (context deadline, or
`ErrReadIndexUnavailable` once every reachable peer's probe attempt is
exhausted) rather than hanging forever or fabricating a result. The
majority side elects a new leader and can serve reads normally through
the identical `ReadIndex` mechanism. See
[docs/failure-testing.md](failure-testing.md) scenario 21 and
`internal/service/read_index_test.go`'s
`TestIsolatedOldLeaderCannotServeStaleGet` for the executable proof this
milestone's objective actually holds.

## 12. Snapshot interaction

`ReadIndex` and `ensureCurrentTermCommitted` only ever consult
`currentTerm`, `commitIndex`, `lastApplied`, and `Log.Term` (already
boundary-aware) — never whether committed history physically lives in
the retained log suffix or was compacted into a snapshot. A current-term
barrier that gets compacted away by a later `CreateSnapshot` still
satisfies `hasCurrentTermCommitLocked` afterward, since
`Log.Term(commitIndex)` answers correctly at the boundary. A read probe
against a follower far enough behind to be below the leader's compacted
prefix still counts toward quorum on a term/context match
(`Success=false` is fine) — it does not, and must not, trigger an
`InstallSnapshot` transfer; that remains purely the background
replication path's concern (see [docs/snapshots.md](snapshots.md)).

## 13. Limitations

- No lease reads / clock-based optimizations — every GET pays for one
  quorum round trip (beyond the very first per new leader-term, which
  additionally pays for one no-op commit). This is deliberate: the
  milestone spec explicitly excludes time-based read leases.
- No follower reads, no bounded-staleness reads, no read caching across
  separate GETs — every GET gets its own quorum confirmation.
- `ReadContext` is a correlation value, not authentication: QuorumKV's
  Raft assumes a non-Byzantine cluster, and this milestone adds no
  TLS/authentication.
- No formally verified linearizability proof and no general-purpose
  linearizability history checker (no Jepsen-style tooling) — this
  milestone proves a set of targeted, deterministic ReadIndex scenarios,
  not an exhaustive external audit.
- Prefer: *implemented*, *tested*, *observed* — not *formally verified*,
  *production-ready*, or *Byzantine fault tolerant*.

## 14. Test evidence

Unit/integration coverage in `internal/raft/read_index_test.go`: reserved
empty-command rejection, no-op apply/restart/snapshot-boundary behavior,
barrier single-flight under concurrency, barrier skip when already
satisfied, ReadIndex in a single-node cluster (no network I/O), one- and
both-follower-down quorum scenarios, read-probe isolation from
replication state, quorum confirmation despite a log-mismatched follower,
higher-term step-down from a probe response, context cancellation and
`Node.Close` during an in-flight read, 50 concurrent `ReadIndex` calls,
and snapshot-boundary barrier recognition across a term failover.

Service-level coverage in `internal/service/read_index_test.go`, over
real TCP: the mandatory isolated-old-leader scenario
(`TestIsolatedOldLeaderCannotServeStaleGet`), the new leader serving the
quorum-confirmed current value, the healed old leader returning
`NOT_LEADER` with no stale read, one-follower-partitioned reads still
succeeding, side-by-side majority-vs-minority partition read behavior,
real-time read-after-write ordering, failover read of a
previously-committed write, and an explicit healthy real-TCP `ReadIndex`
round trip.
