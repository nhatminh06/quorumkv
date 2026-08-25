# Leadership transfer

Milestone 11 adds a controlled way to move leadership to a specific,
caught-up voter — intentional maintenance ("I need to restart A, please
move leadership to B first"), not a substitute for failure recovery.
This builds on [docs/raft-election.md](raft-election.md) (PreVote, the
real election flow) and [docs/membership.md](membership.md) (the
quorum/membership abstraction everything here reuses); read those
first.

## 1. Purpose

Without this, leadership only changes through failure/election: an
operator who wants to take A down for maintenance has to just stop it
and wait for the cluster to notice and recover — real disruption, and no
control over which node becomes the new leader. `Node.TransferLeadership`
lets the *current* leader hand off deliberately, to a *specific* target,
while it is still healthy.

`TransferLeadership` never simply declares a target the leader — there
is no `role[target] = Leader` anywhere in this implementation. Leadership
still changes only through a real Raft election; this milestone's
contribution is authorizing and orchestrating one, not bypassing the
protocol.

## 2. Target requirements

```go
func (n *Node) TransferLeadership(ctx context.Context, target NodeID) error
```

Leader-only (`ErrNotLeader` otherwise). `target` is rejected with:

- `ErrCannotTransferToSelf` if `target == n.id`.
- `ErrMembershipChangeInProgress` if membership is currently Joint — this
  milestone deliberately does not mix a leadership transfer with an
  in-flight membership transition; retry once it finishes (see §10).
- `ErrLeadershipTransferInProgress` if another transfer is already
  active — only one runs at a time, the same one-major-administrative-
  transition-at-a-time discipline Milestone 10's membership changes
  established. The guard is exactly `n.transfer != nil`, checked and set
  under `Node.mu` alongside every other check — no separate lock, and no
  race between two concurrent calls (`TestConcurrentTransferRequestsOnlyOneSucceeds`,
  run under `-race`).
- `ErrNotAVoter` if `target` is not a voter in the current effective
  membership — an unknown NodeID and a known-but-removed one are
  rejected identically, since both fail the same `Membership.IsVoter`
  check.

## 3. Catch-up phase

Once validated, the transfer enters `transferCatchingUp`. **Client
writes remain allowed** during this phase — freezing them here would be
needless disruption for what may take several replication rounds, and
the target keeps dynamically catching up regardless.

Catch-up itself does nothing new: `waitForTransferCatchUp` simply waits
(via a notification channel, never a polling sleep — see §"No busy
wait" below) for `matchIndex[target] >= LastIndex`. The existing
heartbeat loop already replicates to every voter, target included,
diverting to `InstallSnapshot` automatically if the target has fallen
behind the leader's compacted log prefix (see
[docs/snapshots.md](snapshots.md)) — there is no parallel/special
transfer data path. `TestLeadershipTransferToFarBehindSnapshotTarget` is
the proof this composes correctly: a brand-new node added via `AddVoter`,
still behind the leader's already-compacted log at the moment
`TransferLeadership` is called, is caught up through a real
`InstallSnapshot` transfer before ever being sent `TimeoutNow`.

If the target regresses again between catch-up completing and the final
pre-handoff check (§4) — e.g. a write lands in between — the transition
loops back to catch-up and tries again rather than proceeding on stale
information.

## 4. Handoff freeze

Once the target is (re-)confirmed caught up, the transfer enters
`transferHandoff`. From this point on, three things are rejected with
`ErrLeadershipTransferInProgress` (mapped to a transient client status —
see §12) until the transfer clears:

- New `Propose` calls — `PUT`/`DELETE`/any application command, and
  `ReadIndex`'s own internal current-term no-op barrier (since it also
  goes through `Propose`). Continuing to append writes after the target
  is declared caught up could make it stale again right as handoff
  begins.
- New `AddVoter`/`RemoveVoter` calls.
- New `ReadIndex` calls, checked explicitly at `ReadIndex`'s own entry
  point (not only via the barrier's `Propose` call, since a read that
  reuses an already-established barrier would otherwise slip through
  that path entirely) — this leader is intentionally giving up
  leadership and must not serve, or even begin establishing quorum for,
  a new read once that's underway.

Not frozen: `AppendEntries`/`RequestVote`/`PreVote` handling, `ReadIndex`
responses this node is answering as a *peer* for someone else's probe,
and replication required to finish delivering the leader's own existing
log (the target must still receive whatever it's missing).

The re-check that enters this phase reads `matchIndex`/`LastIndex` fresh
under `Node.mu` — never off a snapshot taken earlier in the call — so
there is no window where a write slips in between "confirmed caught up"
and actually sending `TimeoutNow`.

## 5. TimeoutNow

```go
type TimeoutNowRequest struct { Term Term; LeaderID NodeID }
type TimeoutNowResponse struct { Term Term; Accepted bool }
```

Deliberately minimal — no log data, since the leader has already ensured
the target is caught up. `Node.HandleTimeoutNow` accepts only from the
exact leader/term the target currently recognizes:

```text
if req.Term > currentTerm: persist, step down (ordinary higher-term evidence)
if req.Term != currentTerm
   or leaderID == nil or *leaderID != req.LeaderID
   or not an effective voter: reject, Accepted=false
otherwise: kick off a real election in the background, Accepted=true
```

A genuinely higher term in the request is still processed as ordinary
higher-term evidence (this is the same mechanism every other RPC handler
uses — no special-cased leadership-transfer branch), but stepping down
clears `leaderID`, so the identity check immediately after correctly
still fails for what turns out not to be a request from a currently-
recognized leader — an arbitrary voter cannot force an immediate election
just by presenting a higher term (`TestTimeoutNowHigherTermProcessedButNotAuthorized`).
A passive (non-voter) target also rejects.

## 6. Immediate real election, bypassing PreVote

Accepted `TimeoutNow` calls `startRealElection` directly — the same
function `StartElection` calls after its own PreVote round succeeds, but
here PreVote is skipped entirely. This is deliberate, not an oversight:
if the target ran ordinary PreVote here, other cluster members that
recently heard from the *current* leader would correctly reject it under
PreVote's own leader-contact safeguard (see docs/raft-election.md) —
exactly the disruption-prevention behavior that safeguard exists for,
which would defeat an *authorized* transfer. The current leader has
already decided to hand off; there is no disruption risk left to guard
against for this one election. Every other election path — including one
started by a node's own timer — still goes through PreVote unconditionally;
this is not a general "skip PreVote" switch.

`HandleTimeoutNow` runs the election in a background goroutine (tracked
by `Node.bgWG`, so `Close` still waits for it) rather than blocking the
RPC response on the election's own outcome.

## 7. Success condition

`Accepted=true` alone is not success (§5-6 kick off an *attempt*, not a
guarantee). `TransferLeadership` returns success only once it observes
real evidence the target actually became leader: `waitForTransferCompletion`
waits (again, no polling — see below) for `n.leaderID != nil &&
*n.leaderID == target && currentTerm > originalTerm` — valid AppendEntries
contact from the target at a higher term, exactly the same signal
`HandleAppendEntries` already produces for any leader, processed with no
transfer-specific special case (`docs/raft-election.md`'s "no
`if target == transferTarget then role=Follower`" rule).

## 8. Cancellation

- **Before `TimeoutNow` is sent** (still in catch-up, or the pre-handoff
  re-check found the target stale again): `ctx` canceling simply aborts
  the attempt. The leader remains leader, whatever freeze was in effect
  (possibly none, if cancellation happened during catch-up) is released,
  and normal operation resumes immediately
  (`TestTransferLeadershipContextCancellationDuringCatchUp`,
  `TestLeadershipTransferTargetPartitionedFails`).
- **After `TimeoutNow` is accepted**: cancellation cannot undo it. The
  target may still become leader regardless of what this call returns —
  an intentionally ambiguous administrative outcome. `TransferLeadership`
  returns `ctx`'s error, but the caller should inspect current cluster
  state (`Role`, `MembershipStatus`, or simply try a client request)
  rather than assume failure.

`Node.Close` unblocks a pending call the same way: `ErrNodeClosed`,
bounded, no goroutine leak (`TestTransferLeadershipReturnsBoundedErrorOnNodeClose`).

## 9. Failure after `TimeoutNow`

If the target crashes or gets partitioned away right after accepting
`TimeoutNow` (it may have already incremented its own term before
that), the old leader will very likely observe that higher term (via
the target's own `RequestVote` fan-out reaching it, or indirectly) and
step down anyway — via the same ordinary higher-term mechanics as
always, no rollback of term is ever attempted. The cluster is not stuck:
whichever node becomes reachable-and-eligible next recovers through
normal PreVote/election, exactly as it would after any other leader
loss.

## 10. Membership interaction

Two administrative transitions never run concurrently, checked from
both directions:

- `TransferLeadership` while membership is Joint →
  `ErrMembershipChangeInProgress` (§2) — no network side effects at all,
  checked before anything is sent.
- `AddVoter`/`RemoveVoter` while a transfer is active (either phase) →
  `ErrLeadershipTransferInProgress` (`TestMembershipChangeRejectedDuringTransfer`).

This milestone deliberately does not attempt a combined "transfer during
Joint" protocol — over-engineering two independent state machines
together for a scenario an operator can trivially avoid (finish one
transition, then start the other) was explicitly out of scope.

Milestone 10's leader self-removal (`RemoveVoter` on self) remains
independently valid and never requires a prior `TransferLeadership` call
— the two features are complementary, not sequenced
(`TestSelfRemovalDoesNotRequireLeadershipTransfer`).

## 11. Snapshot interaction

`CreateSnapshot` is never blocked by an active transfer — there is no
coupling between the two. A transfer target behind the leader's
compacted log prefix is caught up via the ordinary
InstallSnapshot-diversion path described in §3, with no special-casing.

## 12. Client behavior

- A write or read hitting the freeze (§4) surfaces to a client as
  `StatusTimeout` — the same transient, safely-retryable-with-the-same-
  request-identity bucket Milestone 9's dedup already treats context/
  quorum timeouts as, not `StatusInternalError` (which would look like a
  genuine bug). No client-side changes were needed: the existing retry
  logic already retries `StatusTimeout` with the same `ClientID`/
  `Sequence`.
- `LeaderHint` never speculatively names the transfer target before it
  has actually won — `n.leaderID` only ever changes via real
  AppendEntries contact (§7), so this falls out of the existing
  mechanism with no extra code.
- A write committed before a transfer is unaffected by it; its eventual
  commitment remains governed by ordinary Raft rules, never
  retroactively faked (`TestLeadershipTransferOverRealTCP`'s
  read-through-the-new-leader assertion).

## 13. Limitations

- No automatic leader selection, balancing, ranking, or periodic
  transfer — the caller specifies the exact target; nothing here ever
  picks one on its own.
- No leadership transfer while membership is Joint (§10) — a short,
  avoidable wait, not a permanent limitation.
- Transfer state (`target`, `originalTerm`, `phase`) is purely volatile,
  never persisted — a crash or restart simply forgets an in-flight
  transfer, which is correct: there is nothing meaningful to resume
  (the next leader, whoever it ends up being, was never told to
  continue one).
- No bandwidth prioritization or replication acceleration for a
  catching-up target — the same replication path every other voter uses,
  at the same pace.
- Prefer: *implemented*, *tested*, *observed*. Leadership transfer is a
  **best-effort, intentional handoff** to a specified up-to-date voter —
  not a guarantee it can never fail (a partitioned or crashed target
  fails it cleanly, per §8-9), and not zero-downtime maintenance in any
  formally verified sense.

## 14. Test evidence

Unit coverage in `internal/raft/leadership_transfer_test.go`: every
rejection (not leader, self, unknown target, non-voter target distinct
from unknown, active Joint membership, concurrent transfer), the
already-caught-up fast path, waiting for a genuinely behind target,
context cancellation during catch-up leaving the leader fully
functional afterward, `Close` unblocking a pending call, membership
changes rejected during an active transfer, and the self-removal
regression check.

`TimeoutNow` handler coverage in
`internal/raft/timeoutnow_election_test.go`: valid acceptance and
immediate (PreVote-bypassing) election, stale term, wrong claimed
leader identity, no recognized leader at all, a passive target, and the
higher-term-processed-but-not-authorized nuance.

Real-TCP integration coverage in
`internal/raft/leadership_transfer_tcp_test.go`: a healthy transfer with
client state (a pre-transfer write reads correctly through the new
leader via ReadIndex, a post-transfer write succeeds), a transfer to a
target behind a compacted log (real InstallSnapshot catch-up, composing
Milestones 7/10/11), a genuinely unreachable target failing cleanly via
context deadline with the leader unaffected, and a repeated
A→B→C→A cycle proving term monotonicity and eventual consistency across
multiple transfers.

Two real bugs this suite caught: `sendSnapshotToPeer` updated
`matchIndex` on an InstallSnapshot completion but never woke a waiting
transfer's catch-up check (fixed by pinging the same notification
channel there too); and the shared real-TCP test harness never recorded
each node's own dialable address in its bootstrap configuration, which
never mattered until a transfer target needed to dial *back* to a node
it only knew about through a replicated `Configuration` — the first
scenario in this codebase to exercise that direction.
