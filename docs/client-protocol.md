# Client protocol

`internal/clientproto` defines the bounded binary wire format for client
PUT/GET/DELETE requests; `internal/service` implements the leader-aware
server side on top of it; `internal/client` is a small reusable Go client.
Payloads travel inside a `transport.Message`
(`MessageClientRequest`/`MessageClientResponse`), whose frame already
carries a CRC32C over the whole payload, so this format does not
duplicate that checksum. See [docs/state-machine.md](state-machine.md)
for what "committed and applied" means underneath this.

There is no TLS and no authentication in this protocol.

## Request format

```
version      1B   = 1
operation    1B   = PUT (1) | GET (2) | DELETE (3)
keyLength    4B
valueLength  4B
key          N bytes
value        M bytes
```

Big-endian. GET and DELETE must not carry a value (`valueLength` must be
0). Bounds: `MaxKeySize` = 64 KiB, `MaxValueSize` = 200 KiB, validated
before any allocation based on the declared lengths.

## Response format

```
version            1B   = 1
status             1B
leaderHintLength   2B
valueLength        4B
leaderHint         N bytes
value              M bytes
```

`MaxLeaderHintSize` = 256 bytes (a `"host:port"` string). `LeaderHint` is
only meaningful when `status` is `NOT_LEADER`; `Value` is only meaningful
for a successful GET.

## Status codes

```
OK               — request succeeded
NOT_FOUND        — GET found no value for the key
NOT_LEADER       — this node is not the leader; see LeaderHint
TIMEOUT          — this node gave up waiting for commit+apply; outcome uncertain
INTERNAL_ERROR   — an unexpected local failure
BAD_REQUEST      — the request was malformed/oversized before reaching Raft
```

Internal Go error strings are never sent over the wire — only this fixed
set of codes.

## Operation semantics

### PUT / DELETE

```text
decode request
  ↓
encode kv.Command
  ↓
Propose (append to leader's local log, persist, start replicating)
  ↓
WaitApplied (block until this exact entry is committed AND applied)
  ↓
respond OK
```

`OK` for a PUT/DELETE means: **the entry was committed by Raft and
applied to the leader's KV state machine** — never merely appended
locally, and never merely replicated to a majority without also being
applied. Deleting a key that doesn't exist is a deterministic no-op and
still returns `OK` (matching `kv.StateMachine`'s existing semantics).

A follower rejects PUT/DELETE (and GET) with `NOT_LEADER` before ever
touching Raft — it never proposes on a client's behalf.

### GET

```text
verify this node's role is Leader
  ↓
wait until lastApplied >= this node's own current commitIndex
  ↓
read the local KV state machine
  ↓
respond OK + value, or NOT_FOUND
```

GET is **not replicated** — it never becomes a Raft log entry. It is
served only by the node's current leader role and only from applied
state. **This is not yet quorum-confirmed linearizable**: a partitioned
former leader could in principle still believe it is Leader long enough
to answer a GET from stale local state, since a role check can be
invalidated the instant after it's read. Closing that gap needs
ReadIndex or an equivalent quorum-confirmed read, which is a later
milestone. Do not read this project as claiming linearizable reads.

## Leader tracking and redirect

Each node tracks a volatile `leaderID`: itself once it becomes Leader, or
the sender of the last valid `AppendEntries` it accepted; unknown (no
hint) once it becomes Candidate or steps up to a higher term with no
leader contact yet. Never persisted.

A `NOT_LEADER` response's `LeaderHint` is that node's current belief,
resolved to an address via the static peer table — never fabricated: if
the leader is unknown, `LeaderHint` is empty.

`internal/client.Client` follows up to 3 `NOT_LEADER` redirects per call
(`maxRedirects`), tracking which addresses it has already tried within
that call so a misbehaving or flapping cluster cannot cause an infinite
loop. It remembers the last address that actually answered as a hint for
its *next* call, so a healthy cluster typically needs no redirect at all
after the first request.

## Timeout and failure semantics

A `TIMEOUT` status or a transport-level failure (connection reset, EOF,
context deadline) means **the client did not observe completion** — it
does *not* mean the command was never committed. Without request
deduplication (deliberately out of scope this milestone), the client
never automatically retries a write after either kind of failure: a
blind retry could duplicate a logical operation once commands stop being
naturally idempotent. `internal/client` returns the failure to the
caller instead of guessing.

PUT and DELETE happen to be idempotent for identical arguments today, but
the protocol itself makes no exactly-once promise.

## Security limitation

No TLS, no authentication. Anyone able to reach a node's listening
address can send it client requests. This is a local educational
transport, not a hardened one.
