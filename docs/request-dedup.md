# Request identity and deduplication

Milestone 9 closes QuorumKV's last documented write-safety gap: a client
that sent a PUT/DELETE and then saw a transport failure, a timeout, or a
NOT_LEADER redirect had no way to know whether the write had actually
committed — so it was prohibited from retrying, since a blind retry
risked applying the same logical write twice. This builds on
[docs/state-machine.md](state-machine.md) (the KV state machine and
command codec) and [docs/client-protocol.md](client-protocol.md) (the
wire protocol); read those first.

## 1. The previous ambiguity

```text
Client → PUT x=1 → Leader → committed + applied → response lost → Client sees timeout
```

The client could not distinguish "the command never committed" from "the
command committed but its response was lost." Both looked identical from
the client's side: a transport error or a timeout. Retrying was therefore
unsafe and, before this milestone, never attempted for PUT/DELETE.

## 2. Request identity: ClientID + Sequence

`internal/reqid` defines the two values that together identify one
logical write:

- `ClientID [16]byte` — a per-`Client`-object session identifier,
  generated with `crypto/rand` (never `math/rand`, a timestamp, or a
  PID — identity collisions across independently created clients must be
  negligibly unlikely). The all-zero `ClientID` is reserved invalid.
- `Sequence uint64` — a monotonic per-`ClientID` write counter, starting
  at 1. 0 is reserved invalid/unassigned; `internal/client.Client` never
  emits it (see §9), and both the wire protocol and the replicated
  command codec reject it explicitly.

`(ClientID, Sequence)` is the entire request identity — no separate
random UUID per operation, since this pair already uniquely names one
logical write **for as long as the client that issued it doesn't reuse
Sequence for a different operation**, which is exactly what §3 exists to
catch.

A `ClientID` is a session identifier, not a credential. Without
authentication (out of scope for this project), another actor could in
principle claim the same `ClientID`; this milestone provides logical
request-identity deduplication, not security.

## 3. Fingerprint: detecting identity misuse

A request identity must never silently represent two different
operations. `kv.Fingerprint(cmd)` computes a SHA-256 digest of a
command's `Type`, `Key`, and `Value` only — deliberately never `ClientID`/
`Sequence` — over an explicit fixed byte layout (`type | keyLength | key
| valueLength | value`; never `fmt.Sprintf`, JSON, or map serialization,
so the result is stable and doesn't depend on incidental formatting).
Two requests with the same `(ClientID, Sequence)` but different
fingerprints mean the client reused a request identity for a different
operation — a client bug, not a legitimate retry — and the second one is
rejected as `RequestConflict` (see §5), never treated as a duplicate.
SHA-256 here is a compact conflict-detection digest, not
authentication.

## 4. Replicated command format (version 2)

`internal/kv`'s command codec gained a second wire/persistent shape
carrying request identity — see
[docs/state-machine.md](state-machine.md#command-format) for the exact
byte layout of both versions. `EncodeCommand` picks version 1 (identical
to every Milestone 1-8 command) for a `Command` with a zero `ClientID`,
and version 2 for a non-zero one; `DecodeCommand` reads both forever.
**No historical Raft log or snapshot is rewritten** — an old version-1
entry decodes exactly as it always did (no request identity, dedup
bypassed entirely for it), and no migration step runs on startup or
compaction.

## 5. Replicated dedup state and classification

`kv.StateMachine` keeps one `ClientRecord` per known `ClientID` — not a
history of every request ever seen:

```go
type ClientRecord struct {
    LastSequence    reqid.Sequence
    LastFingerprint reqid.Fingerprint
    LastResult      ApplyStatus // currently only ever ApplyStatusOK
}
```

This is sufficient because a `Client` serializes its own writes (§9): only
the *latest* sequence/fingerprint/result is ever needed. It also keeps
the table's size bounded by the number of distinct known `ClientID`s, not
by request volume (see §12's limitation on this).

Given an incoming `(sequence, fingerprint)` and a `ClientID`'s current
record (the zero record if unseen), classification is a single pure
comparison, shared by the authoritative apply path and the service-level
lookup optimization (§6):

```text
sequence == record.LastSequence:
    fingerprint matches  → AppliedDuplicate (no mutation; return cached OK)
    fingerprint differs  → RequestConflict  (no mutation; reject)
sequence == record.LastSequence + 1:
    → AppliedNew (mutate; record this as the new LastSequence/fingerprint)
otherwise (behind, or a gap ahead of, LastSequence):
    → StaleRequest (no mutation; reject)
```

This is Milestone 9's chosen policy — **exact next sequence**, not
merely "any sequence greater than the last" — because the client already
serializes its writes, so an exact-next requirement additionally catches
a lost/misused client sequence rather than silently accepting a gap. A
request that was rejected as a gap is not lost forever: once the missing
sequence actually applies, a later attempt at the previously-gapped
sequence becomes valid as a new entry — it is never retroactively applied
to the earlier rejected log position.

## 6. Apply-time dedup is authoritative; the service-level lookup is an optimization

`StateMachine.Apply` performs the classification above and is the
**single authoritative dedup point**: regardless of what happened above
it (a redundant proposal from a race, a duplicate entry surviving into
two different Raft terms — see §12), a command that reaches `Apply` twice
mutates state at most once. `lastApplied` still advances through a
duplicate, stale, or conflicting entry exactly like a genuinely-new one —
deduplication never stalls the apply pipeline (see
[docs/state-machine.md](state-machine.md) for the apply loop this feeds
into).

`StateMachine.LookupRequest(id, seq, fingerprint)` runs the identical
classification without mutating anything. `Service.write` (the PUT/
DELETE handler) calls it, after confirming this node is Leader and has
applied through its own current `commitIndex`, to avoid proposing an
already-completed duplicate's redundant Raft entry — but this is purely
an efficiency shortcut. If a duplicate proposal is appended anyway
(possible under races or a leader failover — see §11), `Apply`'s own
check still guarantees correctness.

## 7. In-flight coalescing

A retry can arrive while the original request has been proposed but not
yet applied — before any dedup record exists for it. `Service` keeps a
small leader-local, **volatile** map (`pendingWrite`, keyed by
`(ClientID, Sequence)`) of writes currently being proposed: a concurrent
request for the same identity and fingerprint waits on the same
in-progress completion instead of appending a second (harmless, but
redundant) Raft entry; a concurrent request for the same identity with a
*different* fingerprint is rejected immediately as `RequestConflict`,
without ever reaching Raft. This state is never persisted — after a
leader crash, the new leader has no memory of it and resolves any retry
purely from replicated state (§5/§6) and the Raft log, exactly as it
would for a request it had never seen coalesced before.

## 8. The service write flow

```text
decode + validate ClientID/Sequence (reject zero either way)
    ↓
fingerprint the operation
    ↓
in-flight coalescing check (§7): matching → wait on it; conflicting → reject
    ↓
local apply catch-up: WaitApplied(this node's own commitIndex)
    — never ReadIndex; write dedup is governed by Raft proposal/commit,
      not by GET's quorum-confirmed-read machinery (docs/read-index.md)
    ↓
replicated dedup lookup (§6):
    Duplicate → cached OK, no Raft entry
    Stale/Conflict → reject, no Raft entry
    Unseen → Propose, wait for commit+apply, inspect the *real* apply
             outcome (not merely "did WaitApplied return nil" — the
             entry that actually lands at this index could itself be a
             duplicate/conflict/stale outcome, e.g. after a failover;
             Service tracks this via a small per-index result-waiter
             map, mirroring raft.Node's own apply-waiter pattern)
    ↓
map outcome to a client status (§5's table) and respond
```

A follower never reaches any of this — `dispatch` rejects a non-leader
before touching Raft at all, even if that follower happens to already
have the request applied locally: writes go through the current leader
only, keeping the external protocol simple.

## 9. Client retry policy

`internal/client.Client` (`New`/`NewWithID`) generates or accepts a
`ClientID` and keeps an in-memory next-sequence counter, starting at 1.
**PUT/DELETE calls on one `Client` are serialized** (`writeMu`): a
request allocates its sequence once, retains it across every retry of
that logical operation, and only advances to the next sequence once the
operation reaches a *successful* terminal outcome. GET is unaffected —
no request identity, no serialization against writes, unchanged from
Milestone 8.

Retry policy for PUT/DELETE, all bounded by the caller's `ctx` (never
retried past it, no hidden background retries, no unconditional sleeps —
a context-aware small fixed delay between passes):

```text
OK / NOT_FOUND        → success, terminal
NOT_LEADER + hint      → retry the exact same request against the hint
transport failure      → retry the exact same request (rotating through
                          configured seeds — a small bounded policy, not
                          service discovery)
TIMEOUT                → retry the exact same request (now safe, unlike
                          Milestone 5-8's conservative behavior)
REQUEST_CONFLICT        → terminal error, no retry (identity misuse)
STALE_REQUEST            → terminal error, no retry (session state
                          disagreement)
BAD_REQUEST              → terminal error, no retry
```

The request identity is never changed mid-retry (`(ClientID, Sequence,
operation, key, value)` byte-for-byte identical on every attempt) — this
is the mechanism the whole milestone depends on; see §12 for what happens
if a proposal is lost entirely rather than merely un-acknowledged.

A default `Client` (`New`) preserves safe retry identity for its own
process lifetime — its `ClientID` and next-sequence counter live only in
memory. This package does not implement client-side session persistence;
a caller that needs safe retry across its own process restart must use
`NewWithID` with an externally persisted `ClientID` (and starting
sequence).

## 10. Snapshot persistence of dedup state

`kv.StateMachine.Snapshot`/`Restore` serialize **both** the KV contents
and the dedup table (client records, sorted by `ClientID` for
determinism, alongside the existing key-sorted KV entries) — see
[docs/snapshots.md](snapshots.md) for the exact byte layout (snapshot
version 2) and its version-1 backward compatibility (an old snapshot
restores with an empty dedup table). Without this, compacting a request's
log entry away would silently erase the ability to recognize its retry —
exactly the scenario Milestone 7 warned would need addressing once
request identity existed. KV contents plus request-dedup metadata
together are this package's entire replicated application state; a
snapshot is still not a backup (Milestone 7's qualification stands
unchanged).

## 11. Restart, compaction, and InstallSnapshot

Only a **committed and applied** request identity enters the
authoritative dedup table — never a merely-appended-locally or
merely-proposed one. A request whose entry never committed (overwritten
by conflict repair after a leader crash, for instance) is correctly
treated as unseen by whichever node next handles a retry for it, and may
execute exactly once there. Restart replay (log-only, snapshot-only, or
snapshot-plus-suffix) reconstructs the dedup table identically to KV
state, through the same apply pipeline. A stale follower that receives a
compacted request's identity only via `InstallSnapshot` (never having
seen the original log entry at all) recognizes a later retry from its
installed table exactly as if it had applied the entry directly — proven
with a real three-node, real-`InstallSnapshot` scenario (§14).

Deduplication is deliberately layered **above** Raft: it never
special-cases request identity inside log replication/matching (Raft
still treats commands as opaque bytes), never invents a commit outside
Raft's normal majority/term rules, and never skips `lastApplied`
advancement for a duplicate/stale/conflicting entry.

## 12. Correctness invariants under duplication

- **Same identity committed twice** (e.g. two proposals for one retried
  request landing at two different log indexes, possibly across
  different terms): the first applies; the second's `Apply` call
  classifies as `AppliedDuplicate`, does not mutate, and the apply loop
  continues normally.
- **Same identity, different payload, both committed**: the first
  applies; the second classifies as `RequestConflict`, does not mutate,
  and the apply loop continues — a conflict is a deterministic outcome
  for that entry, not a fatal application error.
- **A stale/gapped sequence reaching a committed log entry** (e.g. a very
  late retry from before a since-superseded session): classifies as
  `StaleRequest`, no mutation, apply loop continues.
- None of the above ever halts `applyLoop` (see
  [docs/state-machine.md](state-machine.md)) — only a genuinely malformed
  command (decode failure) does that, and that distinction is
  deliberate: a valid command with an identity Raft/the state machine
  simply disagrees with is a normal, deterministic outcome, not
  corruption.

## 13. Limitations

- The dedup table's size grows with the number of *distinct* `ClientID`s
  a node has ever seen, not with request volume per client — but it is
  still unbounded in that dimension. No garbage collection, quota, or
  session-expiration mechanism exists yet; an adversarial or buggy client
  flooding many `ClientID`s is a known, accepted limitation (not
  mitigated in this milestone).
- No client-side session persistence — see §9.
- Two independent, concurrently-writing `Client` objects sharing the
  same `ClientID` are unsupported unless the caller externally serializes
  them; this package does not implement distributed client-side sequence
  coordination.
- The correct claim after verification is: **replicated request
  identity provides at-most-once state-machine effects for retried
  PUT/DELETE operations that reuse the same ClientID and sequence
  number.** This is *not* a claim of exactly-once networking, exactly-once
  delivery, or exactly-once execution under arbitrary client misuse — the
  transport remains at-most/unreliable request-response, and a client
  that never retries a lost request simply never completes it (this
  milestone makes retrying *safe*, not automatic-forever — retries are
  still bounded by the caller's `ctx`).

## 14. Test evidence

Unit coverage: `internal/kv`'s `dedup_test.go` (exact duplicate, conflict,
stale, sequence-gap-then-fill, independent clients, `LookupRequest`
agreement, apply-loop continuation after a conflict), `codec_test.go`
(version-2 known byte vector, legacy version-1 decode, identity
validation), `snapshot_test.go` (version-2 known byte vector including a
client record, version-1 legacy decode, ClientID-sorted determinism,
restore-then-retry-recognized). `internal/clientproto`'s
`protocol_test.go` (version-2 request byte vectors for PUT/DELETE/GET,
identity validation both directions, `REQUEST_CONFLICT`/`STALE_REQUEST`
round trips). `internal/client`'s `identity_test.go` (unique/stable
`ClientID`s, sequence advances only after success, 20 concurrent writes
from one `Client` produce unique ordered sequences, two `Client`s don't
collide, sequence exhaustion reports an explicit error).

Real-TCP service-level coverage (`internal/service`): the mandatory
response-lost-after-commit proof for both PUT and DELETE (retry succeeds,
exactly one mutation); the mandatory strongest-proof scenario — a leader
commits+applies a write, its response is lost, the leader crashes before
any further retry can reach it, a new leader is elected, and the client's
retry against the new leader is recognized from *replicated* state, never
a leader-local cache, with no per-replica double mutation; a request
never committed before its leader crashed is correctly treated as unseen
by the new leader and applied exactly once there; `REQUEST_CONFLICT` and
`STALE_REQUEST` over real TCP; the mandatory `InstallSnapshot` scenario —
a stale follower isolated during several writes (including one tracked
request) whose log entry gets compacted away entirely, catches up via a
real `InstallSnapshot` transfer, is legitimately elected leader, and
recognizes a retry of the tracked request from its installed dedup table
without any further mutation; snapshot-compaction and full-restart dedup
survival.
