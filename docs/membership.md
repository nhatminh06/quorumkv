# Joint-consensus membership changes

Milestone 10 removes QuorumKV's last static assumption: cluster
membership was fixed at construction (`raft.NewNode`'s `peers` argument,
mutated only via the bootstrap-only `SetPeers`). This builds on
[docs/raft-election.md](raft-election.md) and
[docs/raft-log-replication.md](raft-log-replication.md) (quorum, the
commit rule) and [docs/snapshots.md](snapshots.md) (the log-compaction
boundary a membership history must survive); read those first.

## 1. The model: `C_old -> C_old,new -> C_new`

Adding or removing exactly one voter is a two-phase transition, never a
single atomic swap:

```text
C_old  --(append Joint entry)-->  C_old,new  --(append Stable entry)-->  C_new
```

While the cluster is in the joint (`C_old,new`) phase, **every
quorum-based Raft decision requires a majority of `C_old` AND a majority
of `C_new` simultaneously** — never a majority of their union. This is
the one rule this milestone cannot get wrong: a union-based shortcut is a
materially weaker, incorrect rule that can let a leader be elected (or an
entry committed) with a set of acknowledgments that is not actually safe
under either configuration alone. `Membership.HasQuorum`
(`internal/raft/membership.go`) is the single implementation of this
rule; `TestNoUnionMajorityBug` (`internal/raft/membership_test.go`)
exists specifically to catch a regression back to a union-based shortcut.

Only one voter changes per transition. Multiple simultaneous membership
changes, learner promotion, automatic rebalancing/discovery, witness
nodes, and leadership transfer are all out of scope this milestone.

## 2. `Configuration` and `Membership`

```go
type Configuration struct { Voters map[NodeID]string } // NodeID -> address
type Membership struct {
    Mode         MembershipMode // ModeStable or ModeJoint
    Stable       Configuration  // valid only in ModeStable
    Old, New     Configuration  // valid only in ModeJoint
}
```

`Configuration` validates itself on construction (`NewConfiguration`):
non-empty, no zero NodeID, no empty/oversized address, no more than
`MaxVoters` (31) entries. `Membership.HasQuorum(acked map[NodeID]bool)`
is `Stable.hasQuorum(acked)` in `ModeStable`, and
`Old.hasQuorum(acked) && New.hasQuorum(acked)` in `ModeJoint` — a NodeID
present in both `Old` and `New` (the common case: most voters don't
change) correctly contributes to both majority counts from a single
acknowledgment. `IsVoter` reports membership in the effective voter set
(`Stable`, or `Old ∪ New` during Joint); `Targets(self)` returns every
other effective voter's address, used by replication/election/ReadIndex.

Both are encoded deterministically (`internal/raft/membership_codec.go`,
no JSON/gob — the same project convention as every other persistent/wire
format): `version(1B) | mode(1B) | Stable: voterCount+voters[]` or
`Joint: oldCount+oldVoters[] + newCount+newVoters[]`, each voter
`nodeID(8B) | addrLength(2B) | address`, sorted by NodeID ascending.

## 3. `EntryKind` and the log format v2 -> v3 upgrade

Before this milestone, a `LogEntry`'s meaning was inferred from its
`Command` bytes alone (`len(Command) == 0` meant the Milestone 8
current-term no-op — see [docs/read-index.md](read-index.md)). A
Configuration entry needs a third, explicit kind, so `LogEntry` gained a
real field:

```go
type EntryKind uint8
const (
    EntryApplication EntryKind = iota // deliberately the zero value
    EntryNoop
    EntryConfiguration
)
```

`EntryApplication = iota` (value `0`) is a deliberate choice: every
pre-existing `LogEntry{Term: t, Command: c}` literal across this
package's large test suite continues to mean exactly what it always
meant, with zero required changes, satisfying the project's "avoid
cosmetic churn" rule. The Milestone 8 no-op's `Command` stays empty by
convention; its `Kind` is now `EntryNoop`, set explicitly wherever the
barrier entry is constructed.

The on-disk log format is bumped to version 3
(`internal/raft/log.go`), adding one `kind` byte per entry between
`term` and `commandLength`. A version 1 or 2 file still decodes: legacy
entries have no stored `Kind`, so it is inferred exactly as before
(`len(Command)==0` -> `EntryNoop`, else `EntryApplication`) —
`TestLogV1FileStillLoads` proves an old file still loads and silently
upgrades to v3 on its next mutation. `AppendEntries`'s wire format
(`internal/raft/append_entries.go`) carries the same per-entry `Kind`
byte, validated against the three known kinds on decode.

Configuration entries participate in ordinary Raft log
matching/conflict-repair like any other entry — there is no
special-casing in `HandleAppendEntries`'s consistency check or
truncation path.

## 4. Deriving effective membership from the local log

**A node's effective membership comes from its own local log, not from a
globally agreed "committed" source.** `Node.rebuildMembershipLocked`
(`internal/raft/node.go`) is the single place this is computed: starting
from `baseConfiguration` (the most recent snapshot's stored stable
config, or this node's bootstrap configuration if none exists yet — see
§6), it walks every surviving log entry from `BaseIndex+1` onward and
applies each `EntryConfiguration` entry found, in order:

- **A `Joint` entry activates immediately, as soon as it is locally
  appended — before it ever commits.** This is required, not
  incidental: a leader that has just appended a Joint entry to its own
  log must itself start requiring both majorities for everything from
  that point on, including the Joint entry's own commitment.
- **The final `Stable` entry completing a transition is deliberately
  conservative: it activates only once it is itself committed**
  (`entryIndex <= commitIndex`). Until then, effective membership stays
  at the preceding Joint state — quorum still requires both old and new
  majorities right up to the moment the transition is truly final. This
  asymmetry (immediate activation for Joint, commit-gated activation for
  the final Stable) is a deliberate implementation choice the milestone
  calls out explicitly, not an oversight: it keeps the joint safety
  property in force for exactly as long as any doubt about the
  transition's outcome remains, and only relaxes to new-only quorum once
  that doubt is gone. `TestRebuildKeepsJointQuorumUntilFinalStableCommits`
  is the direct unit proof.

`rebuildMembershipLocked` always re-derives from scratch rather than
patching incrementally, which is what makes it safe to call after
*anything* that can change what persisted history means: a newly
appended entry, log truncation (conflict repair — an uncommitted Joint or
Stable entry on the losing side of a term simply disappears from the next
rebuild, see `TestRebuildRevertsOnTruncation` and
`TestConflictRepairRevertsUncommittedJointEntry`), commit-index
advancing on either the leader or follower path, or an InstallSnapshot
boundary changing.

## 5. Every quorum decision, not just the configuration entry

The milestone's central safety rule is not scoped to committing the
configuration entries themselves — it governs **every** quorum-based Raft
decision while `Mode == ModeJoint`:

| Decision | Where |
| --- | --- |
| Leader election (self-vote fast path, `applyVoteResponse`) | `Node.StartElection`, `Node.applyVoteResponse` |
| Log commitment | `Node.maybeAdvanceCommitIndexLocked` |
| Configuration entry commitment | same path — no special-casing |
| ReadIndex quorum confirmation | `Node.ReadIndex` |
| Current-term no-op barrier commitment | same commit path the barrier entry goes through |
| Client PUT/DELETE commitment | same commit path every entry goes through |

All of these call `Membership.HasQuorum` on an `acked map[NodeID]bool`
built from real acknowledgments (votes, `matchIndex`, ReadIndex probe
responses) — there is exactly one quorum implementation in this package,
not a parallel simplified one for any of these paths.
`TestJointWriteCommitRequiresBothMajorities`,
`TestJointElectionRequiresBothMajorities`, and
`TestJointReadIndexRequiresBothMajorities` are the end-to-end partition
proofs: each shows a majority of `Old` alone is insufficient, and only a
majority of `New` too lets the decision go through — over real
Propose/replication/election, not just `Membership.HasQuorum` unit math.

### Campaign and vote eligibility

A node only starts an election if it is an effective voter in its own
membership (`StartElection` checks `n.membership.IsVoter(n.id)` before
incrementing its term); a node only grants a vote if it is itself a
voter (`HandleRequestVote`). A removed or not-yet-activated node is
never a source of spurious elections or votes it has no standing to
cast.

### Replication targets vs. dial addresses

`Membership.Targets(self)` gives the correct **set** of who to
replicate/heartbeat/probe (joint-quorum-aware — including a newly added,
not-yet-committed peer). But the **address** to dial for each target is
resolved by `Node.resolveTargetsLocked`, which prefers this node's own
freshest `n.peers` entry over whatever address a Configuration entry
happens to carry, falling back to the Configuration's address only for a
peer this node has no direct address for yet (a brand-new joiner known
only through a just-replicated Configuration entry). This distinction
matters in practice, not just in theory: once a snapshot has ever been
taken, a Configuration's embedded address is effectively a historical
snapshot of "what this node's address was at the boundary" — treating it
as authoritative for live dialing would regress a node's own
operationally-current knowledge (e.g. a real re-listen after restart)
back to a stale value.

## 6. Bootstrap vs. persisted history

`Node.NewNode`'s existing `peers` constructor argument becomes a
**bootstrap** configuration, used only when no persisted membership
history exists yet. `Node.SetSelfAddr(addr)` is a new bootstrap-only
setter, parallel to `SetPeers`, giving a node's own dialable address —
needed so its bootstrap Configuration (which must include itself) has a
real address once it is ever serialized to a log entry or snapshot for
another node's benefit. Before it is ever called, a placeholder
(`unresolved-self-<id>`) is used; since `Targets` always excludes self,
correctness of replication/election/quorum never depends on this value
being real — only its later use in a Configuration handed to another
node does.

Once real membership-change history exists, it is authoritative forever:
`rebuildMembershipLocked` always re-derives from `baseConfiguration`
plus the log's surviving `EntryConfiguration` entries, so calling
`SetPeers`/`SetSelfAddr` again cannot regress an already-real transition
history back to a bootstrap guess — there is no separate "has real
history ever happened" flag to get out of sync with reality, because the
rebuild is pure with respect to its two inputs (`baseConfiguration`, the
log) every time.

## 7. Passive non-voters

A newly added node accepts replication, persists its log, and installs
snapshots — but remains passive (does not start elections, does not
grant counted votes, does not count toward quorum) until configuration
history makes it a voter. This falls out of the same two mechanisms
already described: it is simply not in the effective voter set
(`IsVoter` false) until the Joint entry naming it activates locally, and
`StartElection`/`HandleRequestVote` already gate on `IsVoter`. No
separate "learner" type or public API exists — internally, a node not
present in its own effective voter set behaves passively, which is
sufficient.

## 8. Snapshots carry the stable membership boundary

A Raft snapshot now layers: `lastIncludedIndex, lastIncludedTerm, stable
membership, application snapshot` (see
[docs/snapshots.md](snapshots.md) for the application-snapshot layer
itself). The snapshot format is bumped to version 2, adding a
length-prefixed `EncodeMembership(StableMembership(cfg))` section; a
version 1 file still loads, with `Snapshot.ConfigurationPresent = false`
telling the caller to fall back to its own bootstrap configuration as the
historical stable config rather than treating the missing metadata as
corruption (`TestSnapshotStoreLegacyV1FileStillLoads`,
`TestNewNodeFallsBackToBootstrapConfigForLegacySnapshot`). `InstallSnapshotRequest`
carries the same Configuration on every chunk (not only the final one).

**`CreateSnapshot` refuses with `ErrMembershipChangeInProgress` while
`Mode == ModeJoint`.** A snapshot can only ever describe a single Stable
membership; rather than support a joint-config snapshot, this milestone
simply waits — an acceptable limitation since membership changes are
short and serialized one at a time, and `CreateSnapshot` can always be
retried once the transition completes (`TestCreateSnapshotBlockedDuringJointThenAllowedAfter`).

Any log truncation or InstallSnapshot boundary change triggers
`rebuildMembershipLocked` — no stale Joint state is ever left in memory
after either.

## 9. The `AddVoter`/`RemoveVoter` API

```go
func (n *Node) AddVoter(ctx context.Context, id NodeID, addr string) error
func (n *Node) RemoveVoter(ctx context.Context, id NodeID) error
func (n *Node) MembershipStatus() MembershipStatus // read-only, defensively copied
```

Internal, Node-level, leader-only API (`ErrNotLeader` otherwise) —
deliberately not a client-protocol-level `ADD_NODE`/`REMOVE_NODE`
command, since there is no compelling implementation reason for one this
milestone. `AddVoter` rejects an already-present NodeID with
`ErrAlreadyVoter` — including reusing an existing NodeID with a
*different* address: an existing NodeID's address never changes through
a transition, and this is never silently reinterpreted as an address
change, always an error. `RemoveVoter` rejects an unknown NodeID
(`ErrNotAVoter`) and removing the last remaining voter
(`ErrInvalidConfiguration`, via `NewConfiguration`'s own non-empty
check).

Only one transition runs at a time. The guard is exactly
`membership.Mode == ModeJoint` — no separate lock: `Node.mu` already
serializes the check-then-append, so two concurrent calls deterministically
produce one success and one `ErrMembershipChangeInProgress`
(`TestConcurrentMembershipChangesOnlyOneSucceeds`, run under `-race`).

Both calls **block until the transition truly finishes**: the final
`Stable(C_new)` entry has committed *and* been applied locally — not
merely appended, and not merely committed. The wait
(`Node.waitForStableConfiguration`) does not poll; every place that can
change the outcome pings a buffered notification channel
(`membershipChanged`). If the caller's `ctx` expires first, the call
returns that error without aborting the transition — it may still commit
later, an intentionally ambiguous administrative outcome. The caller
should inspect `MembershipStatus()` before retrying rather than assuming
failure; this is deliberately different from Milestone 9's client-write
request-deduplication semantics (see
[docs/request-dedup.md](request-dedup.md)), which do not apply here.

A new peer's replication state (`nextIndex = leaderLastIndex+1`,
`matchIndex = 0`) is initialized and catch-up (including InstallSnapshot
if it is behind the leader's compacted prefix) begins immediately once
the transition begins — it does not wait for the final commit. During a
removal, the leader keeps replicating to the removed node (it is still in
`Old`, hence still in `Targets`) until the final Stable entry commits;
only then does it stop being a heartbeat/replication/election target.
`TestMembershipChangeOverRealTCP` is the mandatory real-socket proof,
including a brand-new node catching up entirely through real
InstallSnapshot because it joins behind an already-compacted leader log.

## 10. Leader-crash resilience

**A cluster must never get permanently stuck in Joint because the
initiating leader died.** `Node.maybeCompleteMembershipTransitionLocked`
runs on every commit-index advance and on becoming leader: if
`Mode == ModeJoint`, the preceding Joint entry has committed
(`membershipEntryIndex <= commitIndex`), and no completing Stable entry
is already pending (`pendingStableIndex == 0`), it appends
`Stable(C_new)` itself. Whichever node ends up leading next — the
original leader, or a successor elected after it crashed — finds this
same state and finishes the transition automatically.

Two crash points are explicitly tested
(`internal/raft/config_change_crash_test.go`, using the same
direct-state-injection technique
`TestRestartFinishesInterruptedCompaction` already established for a
Milestone 7 crash window, rather than trying to race a live system into
an exact window):

1. **After the Joint entry commits, before the Stable entry is ever
   appended.** A new leader notices and appends it.
2. **After the Stable entry is appended locally, before it ever
   replicates or commits anywhere else.** A new leader's log never has
   that entry at all (it never left the old leader); the new leader
   appends its own completing entry instead of waiting for one that will
   never exist anywhere else.

## 11. Self-removal

A leader may remove itself. The transition proceeds to completion
normally — the leader keeps leading long enough to finish it (its own
`RemoveVoter(ctx, self)` call is waiting on exactly that completion).
Once the final Stable entry — which excludes it — commits and applies,
`Node.stepDownIfNoLongerVoterLocked` converts it to a passive Follower
immediately, with **no higher term required first**: membership, not
term, is what disqualifies it from leading a cluster it is no longer a
member of (`TestSelfRemovalLeaderStepsDownAfterCompletion`,
`TestMembershipChangeOverRealTCP`).

## 12. `MembershipStatus`

```go
type MembershipStatus struct {
    Mode     MembershipMode
    Stable   Configuration // valid only when Mode == ModeStable
    Old, New Configuration // valid only when Mode == ModeJoint
}
```

Read-only, and every `Configuration` it carries is a defensive copy —
mutating a returned `MembershipStatus` can never reach back into `Node`
state (`TestMembershipStatusIsDefensiveCopy`).

## 13. Limitations

- Exactly one voter change at a time; no batched multi-node changes.
- No learner/observer promotion protocol as a public feature (internal
  passivity, described in §7, is sufficient for this milestone).
- No automatic rebalancing, discovery, or DNS-based address resolution.
- An existing NodeID's address can never change through a transition —
  `AddVoter` on an already-present NodeID always errors, never
  reinterpreted as an address update.
- No witness nodes, no multi-Raft, no sharding, no TLS/auth, no admin
  CLI. PreVote and leadership transfer exist since Milestone 11 (see
  [docs/raft-election.md](raft-election.md) and
  [docs/leadership-transfer.md](leadership-transfer.md)), but a
  leadership transfer is rejected outright while membership is Joint,
  and an active transfer likewise blocks `AddVoter`/`RemoveVoter` — the
  two administrative transitions never run concurrently, in either
  direction (see leadership-transfer.md §10).
- `CreateSnapshot` refuses during an active Joint transition (§8) — a
  short, serialized wait, not a permanent limitation.
- Prefer: *implemented*, *tested*, *observed* — not *fault tolerant*,
  *production-ready*, or *highly available*.

## 14. Test evidence

Pure quorum-math and codec unit tests in
`internal/raft/membership_test.go`/`membership_codec_test.go`: known
byte vectors for both Stable and Joint (hand-derived, not
encoder-generated), independently re-derived add/remove quorum math
(including a case the milestone's own worked example needed correcting
against), the no-union-majority-bug proof, `IsVoter`/`Targets`,
configuration validation, and equality.

Log-format and wire-format coverage in `internal/raft/log_test.go`,
`append_entries_test.go`, `install_snapshot_test.go`, `snapshot_test.go`:
v1/v2 backward-compatible decode, updated known byte vectors, unknown
`EntryKind` rejection, snapshot v1 legacy compatibility.

Node-integration coverage in `internal/raft/membership_rebuild_test.go`,
`config_change_test.go`, `config_change_crash_test.go`,
`config_change_tcp_test.go`: local-log-derived activation timing (Joint
immediate, final Stable commit-gated), truncation reverting an
uncommitted entry, the full `AddVoter`/`RemoveVoter` API contract
(validation, concurrency, defensive `MembershipStatus`), partition proofs
for commit/election/ReadIndex requiring both majorities, both mandatory
leader-crash-mid-transition windows, real-TCP add/remove/self-remove
with genuine InstallSnapshot catch-up for a far-behind new node, and
Milestone 8/9 regression checks (ReadIndex still works correctly during
an active Joint transition).
