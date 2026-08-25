# Replication performance (Milestone 14)

This document explains the bounded, event-driven replication design
introduced in Milestone 14, why each piece exists, and what was actually
measured. [docs/raft-log-replication.md](raft-log-replication.md) covers
the wire format and protocol; this document covers the leader-side
scheduling and measurement.

## 1. Previous replication behavior

Through Milestone 13, replication happened in synchronous rounds: one
goroutine (spawned by a Propose, a configuration change, or the
heartbeat ticker) called `replicateToAllPeers`, which built one
AppendEntries per peer from whatever fit under a count-only bound
(`maxEntriesPerAppend`, 64 entries) and sent them all, then returned. A
follower more than one batch behind advanced roughly one batch per
heartbeat interval, since nothing else prompted another round until the
next trigger.

Two concrete problems followed from this:

- `Log.EntriesFrom` cloned the entire retained suffix from the requested
  index to the end, even when only the first 64 entries would ever be
  used — real, measurable waste for a follower behind by thousands of
  entries.
- The 64-entry count bound was never actually a byte bound: 64 entries
  near the 256 KiB command-size limit would encode to several megabytes,
  far past the transport's 1 MiB payload limit. Nothing in the existing
  test suite happened to exercise 64 large entries in one batch, but the
  invariant "a leader never hands the transport an oversized RPC" was
  not actually enforced.

## 2. Bounded range API

`Log.EntriesRange(from, maxEntries, maxEncodedBytes)` replaces the
unbounded-then-truncated pattern: it stops adding entries once the next
one would push the running encoded-byte total past the budget, and never
copies more of the retained log than the result needs. The first entry
is always included regardless of its own size — see §3.

## 3. AppendEntries byte accounting

`encodedEntrySize`/`appendEntriesEncodedSize`
(`internal/raft/append_entries.go`) are the one place that compute how
many wire bytes an entry or a whole request costs — shared by
`EncodeAppendEntries` (encoding) and `EntriesRange` (deciding what to
include before encoding anything), so there is exactly one calculation,
never two that could drift apart.

Two distinct bounds exist, deliberately different sizes:

- `MaxAppendEntriesBytes` (512 KiB) — the *soft target* `EntriesRange`
  builds a normal batch against.
- `maxAppendEntriesEncodedSize` (`transport.MaxPayloadSize` − 64 KiB) —
  the *hard ceiling* `EncodeAppendEntries` itself rejects anything past.

The gap between them is what lets a single valid entry up to
`maxCommandSize` (256 KiB) always be sent alone even though it alone
exceeds the normal batch target — `EntriesRange` always includes the
first entry unconditionally, so batching a target smaller than the
absolute maximum never makes an otherwise-valid large command
unsendable.

## 4. Batch limits

| Bound | Value | Enforced by |
|---|---|---|
| `maxEntriesPerAppend` | 64 entries | `EntriesRange` (soft), `DecodeAppendEntries` (hard, pre-allocation safety) |
| `MaxAppendEntriesBytes` | 512 KiB | `EntriesRange` (soft target) |
| `maxAppendEntriesEncodedSize` | `transport.MaxPayloadSize` − 64 KiB | `EncodeAppendEntries` (hard ceiling) |
| `maxCommandSize` | 256 KiB | unchanged since Milestone 1 |

## 5. Replication-worker lifecycle

One `replicationWorker` exists per current replication target while
this node is Leader — created and destroyed in exactly one place,
`reconcileReplicationWorkersLocked`, called from `becomeLeaderLocked`
(the initial set) and from every `rebuildMembershipLocked` (whenever
effective membership might have changed, including a just-appended,
not-yet-committed Joint entry adding a new voter). A worker's own
goroutine context is a child of the current leadership term's context,
so losing leadership cancels every worker at once with no separate
bookkeeping — `stepToFollowerLocked` also drops the worker map itself,
so a later `becomeLeaderLocked` starts completely clean.

Workers are purely runtime scheduling state: nothing about them is
persisted, and none exist until a restart's normal election path makes
this node Leader again.

## 6. Trigger coalescing

Each worker has a buffered-1 wake channel. Every site that used to spawn
a replication round directly (a proposal batch persisting, a
configuration entry appending, the ReadIndex no-op barrier appending)
now instead pings every current worker's channel; so does the heartbeat
ticker. A non-blocking send that finds the buffer already occupied is a
no-op — the pending wake will still cause the worker to observe
whatever the current state is once it runs, so 100 proposals landing
before a worker gets scheduled collapse into one wake, not 100.

The worker loop is structured so a wake can never be permanently missed:
after completing a step, it explicitly checks whether more work remains
before going back to waiting, rather than trusting that a wake token is
still sitting in the channel. See §8 for a subtler version of this
requiring an active fix, not just the wake channel's own coalescing.

## 7. Generation model

Each peer carries a `replicationGeneration` counter, incremented when:

- a current-generation AppendEntries failure requires backtracking
  `nextIndex` (the old assumption is no longer trustworthy);
- taking over for InstallSnapshot (invalidating any AppendEntries still
  speculatively in flight for the pre-snapshot assumption);
- resuming ordinary suffix replication after a successful InstallSnapshot
  (a fresh epoch for the post-snapshot state);
- a worker is (re)created (leadership regained, or a peer entering the
  target set for the first time).

Every request captures the generation it was built under; a response is
only allowed to mutate `nextIndex`/`matchIndex`/trigger a commit-index
recheck if the peer's CURRENT generation still matches. Anything else —
an older generation, an older term, a peer no longer present in the
worker map — is discarded exactly like a response that never arrived.

## 8. Stale-response handling

The generation check subsumes the term/role checks that already existed
before Milestone 14 (a higher term still forces step-down; a response
for a role/term this node has moved past is still dropped) and adds the
missing piece: two responses for the SAME term and role can still
disagree about whether they are stale, which only a per-peer,
monotonically-invalidated generation counter — not term or role alone —
can distinguish.

## 9. Failure / backtracking

Unchanged conflict-repair algorithm: a current-generation failure backs
`nextIndex` off by exactly one (never below 1) and lets the worker's own
step loop retry immediately with the corrected value — no conflict-term
hint, no second algorithm invented for this milestone.

## 10. Pipeline window

Milestone 14 deliberately does **not** implement multiple
simultaneously in-flight AppendEntries requests per peer.
`replicationWorker`'s inner loop is single-flight: send one batch, apply
its response, decide whether to send the next — matching the milestone's
own explicit "window 1 is a legitimate, complete answer" scope
allowance. Reasoning:

- The transport's own real bottleneck (see §17, and Milestone 13's own
  profiling) is per-RPC TCP connection setup/teardown, not waiting for a
  response after the connection is already open. A second in-flight
  request against the SAME peer does not avoid paying that same
  per-connection cost again; it does not obviously help the actual
  measured hotspot.
- Multiple in-flight requests introduce real complexity this milestone's
  correctness budget is better spent elsewhere: speculative later
  requests must survive being answered out of order (the current
  transport gives no cross-request ordering guarantee), a mid-window
  conflict failure must correctly invalidate every later speculative
  request without corrupting `nextIndex`, and every one of those paths
  needs its own deterministic reordering tests.
- Event-driven immediate re-send (§21 below) already captures the
  primary, measured win — a follower advances batch after batch with no
  wait in between — independent of whether more than one of those
  batches is ever in flight at once.

Given the transport architecture (§17) and no evidence a wider window
would help it, a window-1 implementation was built and measured;
speculative multi-request pipelining was not implemented rather than
built and left unvalidated. See §20 for what non-goal this leaves
explicitly undone.

## 11. InstallSnapshot interaction

When a peer's `nextIndex` falls at or before the log's compacted
boundary, its worker's step performs the InstallSnapshot chunk-loop
transfer synchronously (still with no network I/O under `Node.mu` —
each chunk round-trip unlocks before sending) instead of a normal
AppendEntries batch, having already bumped the peer's generation before
starting so no AppendEntries response still in flight from before the
takeover can apply itself afterward. On a fully successful transfer,
`matchIndex`/`nextIndex` are advanced to the snapshot boundary, the
generation is bumped again (a fresh epoch for the resumed suffix), and
the worker's step reports whether more (ordinary) catch-up work remains
so it continues immediately rather than waiting for another wake. A
failed transfer changes nothing — the existing retry-on-next-wake
behavior is unchanged.

## 12. Membership interaction

`reconcileReplicationWorkersLocked` is membership-mode-aware only
through `n.membership.Targets()`, which already implements the Joint
union(old, new) rule (Milestone 10) — a worker exists for exactly the
current effective target set, Joint or Stable, with no separate logic
here for which mode is active. A newly added voter (even before its
Joint entry commits) gets a worker immediately; a peer that a final
Stable entry excludes has its worker canceled and its `nextIndex`/
`matchIndex`/`replicationGeneration` entries removed in the same pass.
A stale response arriving after removal finds no worker and no
generation to match, so it is discarded — it cannot recreate the peer's
state or affect quorum.

## 13. Leadership-transfer interaction

`waitForTransferCatchUp` (`leadership_transfer.go`, unchanged by this
milestone) already only watches `matchIndex[target]` via the existing
`transferChanged` signal — it never drove replication itself. Replacing
the replication engine underneath it required no changes there: the
transfer target's own worker now catches it up event-drivenly, and
`waitForTransferCatchUp` simply observes progress faster. The mandatory
queued-proposal-drain-before-handoff behavior (Milestone 13) is
unaffected.

## 14. ReadIndex isolation

Unchanged and unaffected: a ReadIndex quorum probe
(`read_index.go`) sends its own AppendEntries-with-`ReadContext`
directly via `n.sendAppend`, never through a replication worker, and
never calls `applyReplicationResponse` — it inspects only the response's
`Term`/`ReadContext` for quorum confirmation. It does not consume a
worker's wake slot, does not advance `matchIndex`, and does not touch
any peer's generation.

## 15. Benchmark methodology

Same environment, harness, and workloads as
[docs/performance.md](performance.md) (`internal/service/benchmark_test.go`,
unchanged this milestone). "Before" was measured on the Milestone 13
merge commit (`bb972997149931608bf585254d42d36f8949d377`) in a separate
worktree; "after" is this branch. Both used the same machine, Go
version, and benchmark commands.

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkFollowerCatchUp' -benchtime=3x
```

## 16. Before/after catch-up results

| | Before (M13) | After (M14) | Change |
|---|---|---|---|
| Catch-up duration (5000-entry lagging suffix) | 3.952 s | 0.269 s | **14.7x faster** |
| Entries/sec | 1,265 | 18,583 | **14.7x** |

This is the milestone's primary targeted improvement, and it is
material — not noise. See §18 for why: this benchmark's lagging follower
scenario is exactly the "wait for the next heartbeat between batches"
case Milestone 13 documented as unaddressed, and event-driven immediate
re-send removes that wait entirely.

## 17. Pipeline-width comparison

Not run as a full sweep: given §10's reasoning (the transport's
per-RPC connection cost, confirmed by Milestone 13's own CPU profile
showing `transport.Send`/dial overhead as the dominant non-runtime cost
in the write path) and the added correctness surface a wider window
requires, a window-1 implementation was built directly rather than
building window={2,4} variants to disprove them empirically. This is
consistent with the milestone's own explicit guidance to choose 1 when
the transport architecture doesn't support a case for more, rather than
building unused complexity to produce a comparison table. If a future
milestone changes the transport (connection reuse/pooling), revisiting
window width then would have a real chance of showing a different
answer than it would today.

## 18. Remaining bottleneck (profiled)

A CPU profile of `BenchmarkFollowerCatchUp` on this branch:

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkFollowerCatchUp' -benchtime=1x -cpuprofile /tmp/cpu.prof
go tool pprof -top -cum /tmp/cpu.prof
```

shows `Log.rewrite`/`encodeLogFile` (the follower's own whole-log
atomic rewrite on every received batch — see
[docs/raft-log-replication.md](raft-log-replication.md)) at ~41%
cumulative time, with associated GC pressure (`scanObject`,
`memmove`, `growslice`) from repeatedly re-encoding and copying the
growing log file as more of it accumulates. This is exactly the
bottleneck item 129 of the milestone brief anticipated: the follower's
durable-write path was not changed by Milestone 14 and remains the
dominant cost once the "wait for a heartbeat between batches" cost is
removed. **This was not converted to segmented storage in this
milestone** — see §19.

## 19. Limitations

- The follower's log is still rewritten as a whole file on every
  received batch (no segmented WAL / append-only log format) — see §18;
  this is now the primary remaining catch-up bottleneck, deliberately
  left unaddressed per the milestone's own scope boundary.
- No multi-request replication pipelining (window is fixed at 1 — see
  §10); the transport still opens roughly one connection per RPC (also
  unchanged this milestone — see
  [docs/performance.md](performance.md)'s own limitations list).
- No follower-side out-of-order request buffering — not needed at
  window 1, and deliberately not built as speculative infrastructure for
  a wider window that was not implemented.
- No observational stats for the replication path itself (RPC counts,
  bytes sent, pipeline resets, stale-responses-ignored) — Milestone 13's
  `Node.Stats()` covers proposal admission/batching only; extending it
  to replication was not done this milestone.
- Snapshot chunk size/protocol (Milestone 7) unchanged — InstallSnapshot
  now runs inside a peer's own worker step, but the chunking mechanism
  itself was not touched.
