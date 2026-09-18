# Allocation efficiency and byte ownership

Milestone 21 follows the segmented-log work with a measure-first review of
temporary byte representations and ownership boundaries. Raw focused results
are in `allocation-baseline-m20.txt` and `allocation-after-m21.txt`; unchanged
service results are in `service-benchmark-m21.txt`, with longer GET/mixed runs
in `service-benchmark-m21-long.txt`.

## Baseline and profiles

The M20 baseline was measured from merge commit `b3dfe96` in an isolated
source export using the same benchmark files and Go 1.26.5 environment as the
M21 run. Fixture construction is outside every timed region.

| Path | M20 B/op | M20 allocs/op |
| --- | ---: | ---: |
| Fingerprint, 1 KiB | 1,152 | 1 |
| Fingerprint, 16 KiB | 18,432 | 1 |
| Entry record, 1 KiB | 2,304 | 2 |
| Entry record, 16 KiB | 36,864 | 2 |
| Entry record, 256 KiB | 540,672 | 2 |
| 64-entry append to 50k log | 99,286 | 42 |
| 25k follower persistence | 11,439,171 | 15,802 |
| Open 50k-entry log | about 121.1 MB | about 50,379 |

The pre-change concurrent 1 KiB PUT allocation profile allocated 116.5 MB.
The leading sites were segmented append (40.9 MB cumulative), KV command
cloning (14.5 MB), Raft command cloning (13.5 MB), record encoding (11.5 MB),
fingerprint assembly (9.0 MB), and transport payload cloning (6.1 MB). The
25k catch-up profile allocated 140.5 MB; record encoding accounted for 34.5
MB and 125,644 objects. CPU profiles were short and noisy, but the pre-change
catch-up profile sampled only GC marking/sweeping work.

## Ownership and copy map

| Stage | Input owner | Copy | Reason and disposition |
| --- | --- | ---: | --- |
| Client request encoding | client request | yes | Produces independent wire bytes. |
| `NewOwnedMessage` | fresh encoder | no | Encoder transfers the new payload; message owns it. |
| Frame encoding | message | yes | Produces a contiguous frame for transport write. |
| Frame/request decode | connection/frame | yes | Request fields must outlive the reusable/read frame. |
| Internal command construction | decoded request | no | Service transfers its exclusively owned decoded slices. |
| Fingerprint | command | no proportional copy | SHA-256 consumes canonical fields incrementally. |
| Command encoding | command | yes | Creates the durable/wire command representation. |
| `ProposeOwned` | fresh command encoding | no | Ownership transfers to the asynchronous proposal queue. |
| Public `Node.Propose` | caller | yes | Caller may mutate immediately after return. |
| Segmented record encoding | queued/log command | yes | One exact-size temporary buffer exists through write+fsync only. |
| Internal log retention | owned proposal | no | Ownership transfers after successful fsync. |
| Public `Log.Append` | caller | yes | Prevents mutable aliases into retained log state. |
| `EntriesRange` | log | yes | RPC data must remain valid after `Node.mu` is released for network I/O. |
| AppendEntries encoding | range result | yes | Produces an independent RPC payload. |
| Follower RPC decode | transport payload | yes | Entries must outlive request-buffer handling and persistence. |
| State-machine apply | decoded/log command | yes | KV state remains isolated from retained Raft history. |
| State-machine GET | KV state | yes | Callers cannot mutate stored database values. |

Network I/O remains outside `Node.mu`. `EntriesRange` therefore still deep
copies commands; removing that copy would require encoding a complete RPC
payload under the lock or a more explicit immutable snapshot mechanism. That
larger synchronization change was not justified by the current profiles.

## Changes

Segment records now append directly into a caller-provided destination. Batch
encoding validates sizes, computes exact capacity, allocates one bounded
buffer, and writes entry and batch-commit records into it. The M20 byte layout,
CRC32C input, batch commit marker, segment headers, and manifest are unchanged.
With sufficient capacity the record appender performs zero allocations.

Fingerprinting now streams type, key length, key, value length, and value into
SHA-256. Five hundred deterministic randomized compatibility cases compare it
with the M20 canonical-buffer implementation. The digest algorithm, field
order, length encoding, and exclusion of client identity are unchanged.

Freshly allocated data uses narrowly scoped ownership-transfer APIs:
`NewOwnedIdentifiedPutCommand`, `NewOwnedIdentifiedDeleteCommand`,
`Node.ProposeOwned`, the internal owned log append, and
`transport.NewOwnedMessage`. Their names and comments require exclusive
ownership. Existing safe APIs retain their defensive-copy behavior.

No buffer pool was added. Direct encoding removed the dominant temporary
objects, and pooling would add lifetime complexity and large-buffer retention
without evidence of enough additional benefit.

## Allocation and latency results

| Path | M20 B/op, allocs | M21 B/op, allocs | Change |
| --- | ---: | ---: | ---: |
| Fingerprint, 1 KiB | 1,152, 1 | 0, 0 | -100% bytes |
| Fingerprint, 16 KiB | 18,432, 1 | 0, 0 | -100% bytes |
| Entry record, 1 KiB | 2,304, 2 | 1,152, 1 | -50% bytes |
| Entry record, 16 KiB | 36,864, 2 | 18,432, 1 | -50% bytes |
| Entry record, 256 KiB | 540,672, 2 | 270,336, 1 | -50% bytes |
| 64-entry append to 50k log | 99,286, 42 | 28,440, 13 | -71% bytes |
| 25k follower persistence | 11,439,171, 15,802 | 4,156,054, 5,215 | -64% bytes |
| Profiled concurrent 1 KiB PUT | 116.5 MB total | 70.7 MB total | -39% |

The 25k follower workload contains 6.93 MB of logical record data. Allocation
fell from 1.65 to 0.60 allocated bytes per logical byte. Runtime improved from
2.54-2.81 ms to 1.62-1.72 ms in the five-iteration focused runs, while physical
write amplification remained 1.001x. The 5k and 10k cases likewise retained
1.001x amplification and reduced allocations by roughly two thirds.

Large-log append remains flat with retained history. The 50k-entry 64-entry
batch used 8.0-13.1 microseconds in the short post-change runs versus
12.6-14.2 microseconds at M20, with unchanged physical bytes. Small timing
differences on this cached filesystem are not treated as storage claims.

## Service and tail latency

Means of the three required unchanged runs:

| Workload | M20 ns/op | M21 ns/op | Change |
| --- | ---: | ---: | ---: |
| Sequential PUT, 16 B | 158,519 | 115,736 | -27.0% |
| Sequential PUT, 1 KiB | 193,534 | 182,419 | -5.7% |
| Concurrent PUT, 16 B | 47,137 | 32,181 | -31.7% |
| Concurrent PUT, 1 KiB | 51,113 | 51,555 | +0.9% |
| Concurrent GET | 44,709 | 45,524 | +1.8% |
| Mixed 80/20 | 50,427 | 40,456 | -19.8% |

The 300-operation GET runs remained noisy (23.6-57.4 microseconds/op). Five
longer 1,000-operation runs averaged 27.1 microseconds/op for GET and 29.6
microseconds/op for mixed traffic. Their p50 values were 619-640 microseconds
and 576-697 microseconds respectively; isolated p99 outliers remained. No GET
improvement is attributed to allocation work because GET does not use the
optimized write path.

## CPU, GC, and startup findings

The post-change concurrent PUT profile allocated 70.7 MB. Record encoding and
fingerprint assembly disappeared from its leading allocation sites; retained
costs include state-machine/log ownership clones, transport framing and reads,
AppendEntries encoding/decoding, and command encoding. Its sparse CPU profile
was led by syscalls; SHA-256 had one 10 ms sample and GC/runtime work remained
distributed rather than dominant.

The post-change catch-up profile allocated 50.1 MB and 65,559 objects, down
from 140.5 MB and 329,926 objects. Its short CPU profile sampled only the write
syscall, whereas the M20 sample contained GC marking and sweeping. This is
consistent with lower allocation pressure, but the profiles are too short to
claim a precise GC CPU percentage or collection-count reduction.

OpenLog was deliberately unchanged. Results remained about 0.5 ms/2.24 MB at
1,000 entries, 5-6 ms/23.2 MB at 10,000, and 33-34 ms/121.1 MB at 50,000,
with roughly one allocation per retained command. Avoiding those command
copies would require retaining whole encoded segment buffers or a lazy
disk-backed representation, both outside M21's ownership and architecture
scope.

## Copies deliberately retained and next work

Public command constructors, public proposal and log APIs, state-machine
storage, GET results, follower RPC decode, and replication range snapshots all
still copy for distinct lifetime or mutation-isolation reasons. M21 is not a
zero-copy design.

The next evidence-backed target is replication snapshot construction:
`EntriesRange` alone allocates 68,224 bytes and 65 objects for a 64-entry 1 KiB
batch, followed by a 73,728-byte RPC encoding. A future milestone should
evaluate encoding a complete immutable RPC payload under the Raft lock, then
performing network I/O after unlock, while preserving test sender interfaces
and retry bookkeeping.
