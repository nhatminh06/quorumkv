# Raft leader election

`internal/raft` implements persistent Raft term/vote state and
RequestVote-based leader election. Since Milestone 4 it uses each node's
real replicated Raft log for vote freshness, and leader heartbeats
(AppendEntries with no entries) now keep an elected leader stable — see
[docs/raft-log-replication.md](raft-log-replication.md) for the log,
AppendEntries, and commit model. "Known limitations" below still applies
to what this document alone covers.

## Persistent vs. volatile state

Persistent (survives restart, in `PersistentState`):

```text
currentTerm
votedFor  (nil, or the NodeID voted for in currentTerm)
```

Volatile (reset on restart, in `Node`):

```text
role       — always starts Follower, loaded term/vote from disk
votes      — vote tally, valid only while Candidate in the current election
```

Role is deliberately never persisted.

## Persistent state format

Fixed 26-byte file, big-endian, rewritten atomically on every `Save`:

```
+------------------------+
| magic           (4B)   |  "RFT1"
+------------------------+
| version         (1B)   |  1
+------------------------+
| currentTerm     (8B)   |
+------------------------+
| hasVotedFor     (1B)   |  0 or 1
+------------------------+
| votedFor        (8B)   |  meaningful only if hasVotedFor == 1
+------------------------+
| checksum        (4B)   |  CRC32C over version..votedFor
+------------------------+
```

`Save` writes to a temp file in the same directory, `fsync`s it, renames
it over the target path (atomic on POSIX), then `fsync`s the directory so
the rename itself survives a crash. A reader always sees either the
complete previous state or the complete new state, never a partial write.

A missing file means a brand-new node: `Load` returns `currentTerm=0,
votedFor=nil` with no error. An *existing* file that fails validation
(bad magic, unsupported version, wrong size, invalid hasVotedFor byte, or
checksum mismatch) returns `ErrCorruptState` instead — it is never
silently treated as a fresh/zero state, since that could let a node
violate the vote-once-per-term safety property after partial disk
corruption.

## RequestVote wire format

Both payloads travel inside a `transport.Message`, whose frame already
carries a CRC32C over the whole payload (see docs/transport.md), so
neither RPC encoding duplicates that checksum. All integers big-endian.

`RequestVoteRequest` (32 bytes):

```
term (8B) | candidateID (8B) | lastLogIndex (8B) | lastLogTerm (8B)
```

`lastLogIndex`/`lastLogTerm` were added to the RPC ahead of log
replication so the wire format wouldn't need an incompatible change
later. Since Milestone 4, both are populated from each node's actual
`Log` (`LastIndex`/`LastTerm`) rather than a hardcoded `(0, 0)` — a fresh
node with an empty log still naturally produces `(0, 0)` via the log's
own sentinel convention (see docs/raft-log-replication.md), so behavior
for an empty-log cluster is unchanged.

`RequestVoteResponse` (9 bytes):

```
term (8B) | voteGranted (1B, 0 or 1)
```

## Vote eligibility

`Node.HandleRequestVote` applies these rules in order:

1. `req.Term < currentTerm` → deny, don't touch persistent state.
2. `req.Term > currentTerm` → step down first: persist the new term with
   `votedFor` cleared, become Follower — *before* evaluating the vote.
3. Grant only if `votedFor` is nil or already equals `req.CandidateID`,
   **and** the candidate's log is at least as up to date as this node's
   (see below). Granting to the same candidate twice in the same term is
   idempotent.
4. A vote is persisted before the response reports it granted; if
   persisting fails, `HandleRequestVote` returns an error and the
   response is never sent — this node never claims to have granted a
   vote it didn't actually persist.

Log freshness (`LogUpToDate`, a pure function so it's usable once real
log state exists):

```text
candidate wins if candidateLastLogTerm > localLastLogTerm
candidate wins if terms are equal and candidateLastLogIndex >= localLastLogIndex
candidate loses otherwise
```

## Election flow

`Node.StartElection`:

```text
increment currentTerm
vote for self
persist                          (must succeed before anything below)
  ↓
if self-vote is already a majority → become Leader, done (no RPCs sent)
  ↓
otherwise send RequestVote to every peer concurrently
  ↓
each response is validated (applyVoteResponse) before counting:
  - higher term in the response  → step down to Follower, persist, stop
  - stale term/no-longer-Candidate → response ignored
  - duplicate grant from a peer   → not double-counted
  - majority of granted votes reached → become Leader
```

`Majority(clusterSize)` is `clusterSize/2 + 1`.

## Concurrency model

A single mutex protects all of `Node`'s state. Network I/O (dialing a
peer, waiting for a RequestVote response) never happens while that mutex
is held: `StartElection` snapshots what it needs (term, peer addresses,
last-log info) under the lock, unlocks, performs the RPCs concurrently,
and re-acquires the lock only to validate and apply each response.

## Timer model

Production election timeout is randomized in the 150–300ms range
(`randomElectionTimeout`), so followers don't all time out together. Tests
never depend on that real range — `Node.timeoutFunc` is an injectable
`func() time.Duration`, overridden with short fixed values for
deterministic/fast tests, and most election tests call `StartElection`
directly rather than going through the timer at all.

`Node.Run` drives the production timer loop: if the timeout fires and the
node isn't already Leader, it starts an election. The timer restarts
whenever the timeout fires, an election attempt finishes, or `resetTimer`
is called. Since Milestone 4, `resetTimer` fires on two signals: granting
a vote (`HandleRequestVote`), and any valid current-term (or higher-term)
AppendEntries contact from a leader (`HandleAppendEntries`) — including a
heartbeat. That second signal is now the primary mechanism keeping a
healthy cluster's followers from starting unnecessary elections; see
docs/raft-log-replication.md for the heartbeat interval and exact reset
rules.

## Known limitations

- No client-facing writes through Raft yet — only internal code can
  `Propose`.
- No snapshots, no membership changes.
- `internal/kv` and `internal/wal` are untouched — the Milestone 1 WAL is
  a state-machine command log, not the Raft log; they are separate
  concerns by design. See docs/raft-log-replication.md for further
  limitations specific to log replication and commit.
