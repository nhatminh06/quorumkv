# Observability

QuorumKV exposes Prometheus metrics, health probes, structured JSON logs,
and the existing `qkv status` diagnostic view. Instrumentation is
observational: scrapes read snapshots of counters and Raft state and never
participate in elections, quorum decisions, replication, persistence, or
request handling.

## Enable the HTTP endpoint

Metrics are disabled by default. Give each node a separate HTTP listener:

```bash
./bin/quorumkv node \
  --id 1 \
  --listen 127.0.0.1:7001 \
  --metrics-listen 127.0.0.1:9101 \
  --data ./data/node1 \
  --peer 2=127.0.0.1:7002 \
  --peer 3=127.0.0.1:7003
```

The listener serves:

- `/metrics`: Prometheus text exposition.
- `/healthz`: process liveness. It returns 200 while the process is running.
- `/readyz`: serving readiness. Followers are ready; a node becomes unready
  only after a permanent state-machine apply error.

Keep this listener on a trusted network. It has no TLS or authentication.
The Raft/client binary protocol never carries HTTP metrics traffic.

Set `--log-level` to `debug`, `info`, `warn`, or `error`. Logs are JSON on
standard error and include stable fields such as `event`, `node_id`, `term`,
`role`, `peer`, `operation`, `duration_seconds`, and `error` when relevant.
Lifecycle, role/term transitions, overload, membership changes, leadership
transfer, and snapshot operations emit structured events. Healthy idle nodes
do not log per-heartbeat traffic.

## Prometheus setup

[`examples/prometheus.yml`](../examples/prometheus.yml) scrapes a local
three-node cluster. [`examples/alerts.yml`](../examples/alerts.yml) contains
starter alerts for repeated elections, apply lag, transport reconnects and
queue pressure, BUSY responses, persistence latency, and an absent metrics
target. Import
[`examples/grafana-dashboard.json`](../examples/grafana-dashboard.json) for a
compact cluster dashboard.

```bash
prometheus --config.file=examples/prometheus.yml
```

The local cluster script enables ports 9101 through 9103, so these examples
work with `./scripts/start-local-cluster.sh`.

## Metric reference

All names start with `quorumkv_`. Durations use seconds and sizes use bytes.
Histograms use fixed bucket boundaries, and counters ending in `_total` are
monotonic for the process lifetime.

| Area | Metrics |
|---|---|
| Node | `raft_term`, `raft_role`, `raft_leader`, `raft_commit_index`, `raft_last_applied`, `raft_last_log_index`, `raft_apply_lag`, `raft_elections_total`, `raft_leadership_changes_total`, `raft_term_changes_total` |
| Replication | `raft_peer_match_index`, `raft_peer_next_index`, `raft_peer_replication_lag`, `raft_replication_rpcs_total`, `raft_replication_bytes_total`, `raft_replication_failures_total`, `raft_replication_stale_responses_total` |
| Transport | `transport_connections_dialed_total`, `transport_connections_reused_total`, `transport_connections_closed_total`, `transport_send_failures_total`, `transport_active_connections`, `transport_waiters`, `transport_rpc_duration_seconds` |
| Service | `requests_total`, `request_duration_seconds`, `requests_inflight`, `requests_busy_total` |
| Proposals | `proposal_queue_depth`, `proposal_batches_total`, `proposal_batch_entries_total`, `proposals_admitted_total`, `proposals_busy_total` |
| Persistence | `persistence_writes_total`, `persistence_bytes_total`, `persistence_duration_seconds`, `persistence_fsync_duration_seconds` |
| Snapshots | `snapshots_created_total`, `snapshot_install_total`, `snapshot_install_failures_total`, `snapshot_install_bytes_total`, `snapshot_size_bytes`, `snapshot_duration_seconds`, `snapshot_last_index` |
| Reads | `readindex_total`, `readindex_failures_total`, `readindex_duration_seconds` |

`raft_role` emits exactly the `leader`, `follower`, and `candidate` series.
`raft_apply_lag` is `max(commit_index - last_applied, 0)`. On leaders,
`raft_peer_lag` is `max(last_log_index - peer_match_index, 0)`.

Request metrics use bounded `operation` and `status` values defined by the
wire protocol. Admission failures occur before request decoding, so
`requests_busy_total` is the authoritative overload counter. Persistence
metrics use a fixed operation set covering Raft state, log, commit index, and
snapshot writes.

## Cardinality policy

| Label | Allowed values and bound |
|---|---|
| `node` | The configured node ID; one value per process |
| `role` | `leader`, `follower`, `candidate` |
| `peer` | Raft peer IDs from cluster membership |
| `operation` | Fixed client, transport, and persistence operation enums |
| `status` | Fixed client response status enum |

Keys, values, client IDs, request sequences, addresses, and error messages
never appear as metric labels. Peer series are retained after a peer is
removed until the process restarts; their total is therefore bounded by the
membership IDs observed during that process lifetime. Histograms and
per-peer storage are preallocated or fixed-size, and scrape work is bounded
by the number of configured peers and metric series.

## Reading a cluster

Use the dashboard or PromQL together with `qkv status --all`:

```promql
# Current leader candidates
quorumkv_raft_role{role="leader"} == 1

# Nodes applying more slowly than they commit
quorumkv_raft_apply_lag > 0

# Replication failures by peer over five minutes
sum by (node, peer) (rate(quorumkv_raft_replication_failures_total[5m]))

# Proposal admission rejections
rate(quorumkv_proposals_busy_total[5m])

# p99-equivalent persistence histogram quantile
histogram_quantile(0.99,
  sum by (le, node) (rate(quorumkv_persistence_duration_seconds_bucket[5m])))
```

Metrics are node-local observations, not a linearizable cluster snapshot.
Scrapes can cross a term or role transition. Compare several nodes and use
the counters and logs to understand the transition rather than expecting all
samples to represent one instant.

## Instrumentation overhead

Milestone 18 was measured against its immediate pre-instrumentation commit on
the same Linux/amd64 host (Intel i5-11400H, Go 1.26.5, 12 logical CPUs,
`GOMAXPROCS=12`). Each result is the mean of three unchanged benchmark runs.
Positive percentages are higher latency.

| Workload | Before ns/op | Instrumented ns/op | Change |
|---|---:|---:|---:|
| Sequential PUT, 16 B | 188,470 | 210,199 | +11.5% |
| Sequential PUT, 1024 B | 323,477 | 367,494 | +13.6% |
| Concurrent PUT, 16 B | 67,874 | 78,388 | +15.5% |
| Concurrent PUT, 1024 B | 162,821 | 177,770 | +9.2% |
| Concurrent GET | 28,878 | 30,936 | +7.1% |
| Mixed 80% GET / 20% PUT | 44,822 | 33,959 | -24.2% |

The mixed baseline included a 71,797 ns/op outlier. Its median comparison is
31,820 before and 34,075 after (+7.1%), which is the more representative
interpretation. The largest repeatable cost is on write paths, where multiple
fixed histogram and counter observations accompany proposal, persistence,
replication, and service work.

An idle `/metrics` scrape averaged about 98 microseconds; scraping while
metrics were updated concurrently averaged about 105 microseconds. The
response is approximately 17 KiB for a three-node configuration. Scrapes do
not acquire the Raft mutex while formatting output: the node first copies a
small state snapshot, then exposition uses atomics and bounded peer snapshots.

Reproduce the service comparison with:

```bash
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeSequentialPut' -benchtime=50x -count=3
go test ./internal/service -run '^$' -bench 'BenchmarkThreeNodeConcurrentPut|BenchmarkThreeNodeConcurrentGet|BenchmarkThreeNodeMixedReadWrite' -benchtime=300x -count=3
go test ./internal/observability -run '^$' -bench 'BenchmarkMetricsScrape' -count=3
```

The concurrent 16-byte PUT CPU profile showed fresh client connection send
and dial work as the largest application cost; measured persistence was also
visible but smaller in this workload. The next optimization should evaluate
reusing external client connections, while separately remeasuring persistence
with larger logs and follower catch-up before changing storage behavior.

## Limits

- Metrics and health endpoints have no TLS or authentication.
- Metrics live in memory and reset when the process restarts.
- Histograms use fixed buckets and do not retain individual observations.
- There is no distributed trace context or trace exporter.
- Readiness deliberately reports followers as ready because they can serve
  protocol requests, including redirects and diagnostics.
- No automatic snapshot policy exists; snapshot metrics describe only
  operator-triggered creation and Raft-driven installation.
