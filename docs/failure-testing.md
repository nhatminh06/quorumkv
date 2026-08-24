# Failure and recovery testing

Milestone 6 adds no new production behavior beyond two lifecycle fixes
(below) — it exists to prove, with executable evidence, that the Raft
implementation built in Milestones 3–5 actually behaves correctly under
controlled node and network failures.

This is not a formal proof and does not claim one. It is a set of
deterministic scenarios, each checking a specific Raft safety property
through real log/commitIndex/lastApplied/KV inspection — not merely "the
process is still running" or "the final value looks right."

## Failure model

Failures are modeled separately, since they exercise different
invariants:

```text
message drop / block   — a specific RPC never arrives
directional partition  — A→B blocked, B→A may still work
bidirectional partition — both directions blocked (partition/heal)
node stop               — Close() the Node and Transport; files untouched
node restart             — genuinely new Store/Log/CommitStore/Node/
                            StateMachine built over the same directory
leader isolation         — the leader is partitioned from every follower
minority isolation       — fewer than a majority remain connected
stale follower           — a follower missed some (or all) recent entries
```

No firewall/iptables/tc is used. No random chaos: every test is
`setup → failure → expected invariant → recovery → assertion`.

## Fault controller design

Two deterministic, directional fault controllers exist, both test-only:

- **`internal/raft` (`directedNetwork`, in `fault_test.go`)**: dispatches
  RequestVote/AppendEntries directly to a peer's real handler in process
  — no sockets — honoring per-`(from, to)` blocking. Used for the
  Raft-level scenarios below, where precise log/term/commitIndex
  inspection matters more than exercising real sockets (already proven
  separately by Milestone 2–5's TCP integration tests).
- **`internal/service` (`faultNet`, in `fault_test.go`)**: sits above
  `transport.Send` for the client-visible scenarios — a blocked link
  returns an error before ever calling `transport.Send`; an allowed one
  still goes over a real TCP socket. This needed two small additions to
  `raft.Node`: `SetVoteSend`/`SetAppendSend`, which let a test replace a
  node's outbound RPC sender. Production code never calls them — the
  defaults already go over real transport.

Both support `partition(a, b)` (block both directions) and `heal(a, b)`
(restore both), plus one-directional `block`/`allow` for asymmetric
faults.

A `faultCluster` (`internal/raft`) and `startCluster`+`wireFaultNet`
(`internal/service`) provide node lifecycle: every node gets its own
`t.TempDir()`; `stop` closes the `Node` (and `Transport`, at the service
layer) without touching its files; `restart` builds an entirely new
`Store`/`Log`/`CommitStore`/`Node` (and, where used, `StateMachine`) over
the same directory — never reusing old in-memory state.

## A genuine lifecycle bug this testing found

`Node.Close()` previously only canceled its background context
(`bgCancel()`) without waiting for the heartbeat/apply goroutines it had
started to actually exit. In production, over real TCP, this is nearly
invisible — real socket I/O aborts quickly on context cancellation. But a
repeated-failover test (`TestRepeatedFailoverCyclesRemainStable`) flaked
once in ~30 runs: a heartbeat round already in flight when a leader was
stopped could still land on a peer afterward, occasionally interfering
with an election started immediately after. Fixed by giving `Node` a
`bgWG sync.WaitGroup` that every background goroutine (heartbeat loop,
apply loop, `Propose`'s post-append replication) registers with; `Close`
now cancels and then waits, so a caller can rely on "no further
heartbeats or application" once `Close` returns, not just "cancellation
was requested." Verified with 100 repeat runs of the previously-flaky
test and full `-race` repeats of the whole fault suite afterward.

## Tested scenarios

| # | Scenario | Invariant under test | Test |
|---|----------|----------------------|------|
| 1 | Leader commits a write, then crashes | Leader completeness: the entry (exact term+command, not just final KV value) survives in whichever node the majority elects next | `TestCommittedWriteSurvivesLeaderCrash` |
| 2 | One follower down, quorum writes continue | A 3-node cluster tolerates one node being unavailable for quorum writes | `TestOneFollowerDownStillPermitsQuorumWrites` |
| 3 | Leader isolated from both followers | An isolated leader can append locally but its commitIndex/lastApplied never advance without quorum | `TestIsolatedLeaderCannotCommitButMajorityElectsNewLeader` |
| 4 | Majority partition elects a new leader while the old one is isolated | A majority partition (B+C) can make progress in a higher term without waiting for the isolated leader | (same test) |
| 5 | Partition heals; old leader had a divergent uncommitted entry | The old leader learns the higher term through the protocol itself (no manual role reset), and the divergent entry is repaired away — never committed or applied anywhere | `TestOldLeaderStepsDownAndDivergentEntryIsRepaired` |
| 6 | Follower partitioned away, then heals | Missed entries replicate and the follower's commitIndex/lastApplied/log converge | `TestStaleFollowerCatchesUpAfterPartitionHeal` |
| 7 | Follower stopped, restarted from disk, then reconnected | Persisted stale state + replication converges (not just an in-memory reconnect) | `TestStaleFollowerCatchesUpAfterRestart` |
| 8 | Follower has a matching committed prefix + divergent uncommitted suffix | Conflict repair replaces only the divergent suffix; the matching prefix is untouched; the repair persists across restart | `TestDivergentUncommittedSuffixIsRepairedPreservingPrefix` |
| 9 | Stale-log node tries to win an election against a majority with a newer log | RequestVote's real (persisted, not synthetic) log-freshness check rejects it; the up-to-date node wins | `TestStaleCandidateCannotWinAgainstMajorityWithNewerLog` |
| 10 | Restart a node with committed history | term/votedFor/log/commitIndex/KV/`lastApplied==commitIndex`/role=Follower all rebuild from disk before any new replication | `TestRestartRestoresCommittedNodeState` |
| 11 | Restart a node with an uncommitted suffix on disk | Log retains the suffix; KV/lastApplied reflect only the committed prefix | `TestRestartWithUncommittedSuffixAppliesOnlyCommittedPrefix` |
| 12 | Advance term, crash, restart, then send a stale-term request | currentTerm never regresses; the stale request is still rejected | `TestTermPersistsAcrossCrashAndRejectsStaleTerm` |
| 13 | Vote for A in term T, crash, restart, B requests a vote in the same term | One-vote-per-term survives a crash | `TestVotedForPersistsAcrossCrash` |
| 14 | Commit an entry, crash immediately, restart | Durable commit metadata (and the applied state it implies) survives the crash | `TestCommitMetaSurvivesCrash` |
| 15 | Stop both followers in a 3-node cluster | Majority stays 2 — a dead peer is unavailable, not removed from the quorum denominator; the sole survivor cannot commit alone | `TestQuorumDenominatorDoesNotShrinkWithDeadNodes` |
| 16 | Repeated elect/commit/crash/restart cycles across all three nodes | No lifecycle regression (goroutine leaks, stale timers, reused state) under `-race`; final logs agree on the shared prefix | `TestRepeatedFailoverCyclesRemainStable` |
| 17 | Client PUT to a leader isolated from the majority | No client OK; a majority-elected replacement commits a different write; the never-committed value is absent from authoritative state after healing | `TestClientWriteDuringPartitionNeverCommits` |
| 18 | Client PUT in flight; leader stopped before it can commit | Client gets a bounded error, never a false OK; no leaked waiter/goroutine | `TestClientReceivesNoFalseOKWhenLeaderCrashesMidWrite` |
| 19 | Client receives OK; leader stopped immediately after | The surviving majority still has the value | `TestSurvivingMajorityRecoversValueAfterClientOK` |
| 20 | Client's cached leader dies; retried against a known survivor | The stale-cache attempt returns a transport error (no blind retry, per Milestone 5); a fresh call against a live node succeeds | `TestClientRedirectsToNewLeaderAfterFailover` |

Every result above is from an actually-passing test at the time this
document was written — see "Verification" in the PR description for the
exact commands run.

## Isolated-old-leader GET is out of scope for this milestone's claims

An isolated old leader may still believe it is Leader (it has not yet
observed a higher term) and could answer a leader-local GET from stale
applied state during that window. This is **not** treated as a Raft
write-safety failure here — GET was never claimed linearizable (see
[docs/client-protocol.md](client-protocol.md)). This milestone does not
"fix" that by adding a heuristic (leader lease, heartbeat freshness) —
that belongs to a dedicated ReadIndex/quorum-read milestone.

## Current limitations

- No snapshots, `InstallSnapshot`, or log compaction — stale followers
  always catch up via the full retained log.
- No ReadIndex / quorum-confirmed linearizable reads.
- No request deduplication; Scenario 18 above is exactly why that
  matters for future work (a client that times out cannot safely
  distinguish "never committed" from "committed, response lost").
- No membership changes; the peer set is static throughout every
  scenario.
- The two-node/three-node partition scenarios are exercised directly; a
  5-node minority/majority split was not added, since the mandatory
  3-node cases already exercise the same commit-rule and quorum-count
  logic without a materially different code path.
