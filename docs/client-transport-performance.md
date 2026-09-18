# Client transport performance

Milestone 19 replaces one TCP connection per external client request with a
bounded pool owned by each long-lived `client.Client`. This follows the M18
profile, where fresh external connection send/dial work was the largest
observed application-level CPU cost in the concurrent-write benchmark.

## Architecture

`transport.PoolClient` maintains at most eight sequential sessions per
address and tracks at most 64 addresses. Connections open lazily. Each
physical connection carries exactly one request/response exchange at a time;
there are no wire request IDs or multiplexing. A caller checks out an idle
session, creates one below the bound, or waits with its context.

The pool performs one transport exchange per `Send` and never retries.
Write/read errors, EOF, malformed frames, invalid decoded responses,
cancellation during an exchange, and shutdown discard the affected session.
A correctly decoded application response remains reusable, including
`NOT_LEADER`, `BUSY`, `TIMEOUT`, `NOT_FOUND`, `BAD_REQUEST`,
`REQUEST_CONFLICT`, and `STALE_REQUEST`.

Pool metadata is bounded at 64 addresses. Once full, a new address is rejected
with `transport.ErrAddressLimit`, avoiding unbounded retention from hints.

## Ownership and shutdown

Every `client.Client` owns one pool, and construction performs no network
I/O. `Client.Close` is idempotent and safe with active operations and
concurrent Close calls. It rejects future work, cancels dialing and I/O, wakes
waiters, joins registered sends, and closes idle connections. No housekeeping
goroutine exists.

One-shot `qkv` commands close their client, but sockets cannot be reused
between separate processes. Reuse benefits long-lived Go clients and multiple
attempts within one client lifetime.

## Retry and identity safety

A TCP session is a transport resource, distinct from the deduplication
identity `ClientID + Sequence`. If a PUT reaches the server and its response
is lost, the pool returns the transport error without retrying. The existing
client retry loop resends the same operation, ClientID, and Sequence. Changing
or reconnecting a TCP session never allocates a new write sequence.

The leader cache is unchanged. Calls prefer the cached leader, follow bounded
hints, and fall back through seeds. A valid `NOT_LEADER` does not poison the
old node's connection. Broken connections are discarded; the discovered
leader gets its own bounded sessions.

## Pool-width experiment

This used 32-way load, 5,000 real loopback exchanges, and three runs per width.

| Width | Mean ns/op | Typical p50 | Typical p95 | Typical p99 | Connections |
|---:|---:|---:|---:|---:|---:|
| 1 | 12,078 | 9–10 µs | 17–18 µs | 27–28 µs | 1 |
| 2 | 7,710 | 14 µs | 23–25 µs | 2.6–3.3 ms | 2 |
| 4 | 5,696 | 19–23 µs | 325–370 µs | 828–1,029 µs | 4 |
| 8 | 3,670 | 26–28 µs | 114–127 µs | 217–247 µs | 8 |

Width one reproduces M17's same-peer serialization tradeoff. Width two has
severe p99 queueing, and width four still has a large p95 under 32-way load.
Width eight is the smallest tested width that materially reduces aggregate
time and queued tail latency, so it is the default.

## Focused client benchmarks

Same M18 host: Linux/amd64, Intel i5-11400H, Go 1.26.5, 12 logical CPUs.
Values are means of three 1,000-operation runs.

| Workload | M18 ns/op | M19 ns/op | Change | M18 allocs/op | M19 allocs/op |
|---|---:|---:|---:|---:|---:|
| Sequential GET | 41,982 | 9,433 | -77.5% | 38 | 32 |
| Sequential PUT | 43,448 | 9,310 | -78.6% | 37 | 30 |
| Concurrent GET | 10,592 | 3,145 | -70.3% | 38 | 32 |
| Mixed 80/20 | 45,217 | 9,344 | -79.3% | 37 | 31 |
| One-shot redirect | 86,110 | 94,728 | +10.0% | 85 | 143 |

The redirect benchmark creates and closes a Client for every operation, so it
measures pool construction without reuse. That is a real regression for
one-shot redirect-heavy use. Long-lived clients pay the redirect once.

| Workload | M18 p50/p95/p99 | M19 p50/p95/p99 |
|---|---|---|
| Sequential GET | 39–40 / 53–56 / 67–78 µs | 8 / 10–12 / 17–18 µs |
| Sequential PUT | 39–42 / 52–63 / 72–83 µs | 8 / 11–14 / 14–21 µs |
| Concurrent GET | 223–266 / 300–442 / 447–565 µs | 90–91 / 118–150 / 149–296 µs |
| Mixed | 39–48 / 54–75 / 71–94 µs | 8 / 10–11 / 14–18 µs |

For 1,000 sequential requests, connections changed from 1,000 to 1: one dial
and 999 reuses. The 32-client service stress performed 6,400 PUT/GET
operations with 32 dials and 6,368 reuses, then closed all 32.

## Established service benchmarks

Means of three unchanged runs:

| Workload | M18 ns/op | M19 ns/op | Change |
|---|---:|---:|---:|
| Sequential PUT, 16 B | 193,473 | 146,409 | -24.3% |
| Sequential PUT, 1024 B | 344,556 | 329,939 | -4.2% |
| Concurrent PUT, 16 B | 65,992 | 54,793 | -17.0% |
| Concurrent PUT, 1024 B | 166,226 | 141,166 | -15.1% |
| Concurrent GET | 28,906 | 25,075 | -13.3% |
| Mixed 80/20 | 31,753 | 28,076 | -11.6% |

The first M18 and M19 concurrent 16-byte PUT runs both had high tail outliers.
They remain included in these means.

## CPU profile

The M18 profile attributed 250 ms cumulative (27.8%) to fresh
`transport.Send` and 160 ms (17.8%) to `DialContext`. The longer M19
profile sampled `PoolClient.Send` at 70 ms cumulative (11.1%);
`DialContext` was absent from the top 50 cumulative nodes. Persistence/log
rewrite was 80–90 ms, while allocation/GC remained prominent. These profiles
are sparse, so they support removal of the dial hotspot without establishing
precise CPU percentages.

## Tests and limitations

Tests cover sequential reuse, pool bounds and wait cancellation, malformed
responses, active and queued Close, concurrent Close, shared-client response
matching, follower responses, ambiguous write identity, 32-client sustained
load, leader failover, and same-address restart.

There is no TLS, authentication, multiplexing, idle timeout, background health
check, cross-process CLI reuse, or pooling for admin commands. Calls above the
eight-session address bound wait. The profile now points more strongly at
persistence/log rewrite and allocation pressure; measure persistence with
larger logs and follower catch-up before selecting a storage redesign.
