// Package observability provides bounded, in-memory instrumentation.
// Metrics are observational: recording has no error path and snapshots do
// not perform I/O, persistence, Raft RPCs, or consensus state transitions.
package observability

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var durationBounds = [...]float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}

type histogram struct {
	count   atomic.Uint64
	sumNS   atomic.Uint64
	buckets [len(durationBounds)]atomic.Uint64
}

func (h *histogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.count.Add(1)
	h.sumNS.Add(uint64(d))
	seconds := d.Seconds()
	for i, bound := range durationBounds {
		if seconds <= bound {
			h.buckets[i].Add(1)
		}
	}
}

type histSnapshot struct {
	count, sumNS uint64
	buckets      [len(durationBounds)]uint64
}

func (h *histogram) snapshot() (s histSnapshot) {
	s.count, s.sumNS = h.count.Load(), h.sumNS.Load()
	for i := range s.buckets {
		s.buckets[i] = h.buckets[i].Load()
	}
	return s
}

const (
	opGet = iota
	opPut
	opDelete
	operationCount
)
const (
	statusOK = iota
	statusNotFound
	statusBusy
	statusTimeout
	statusNotLeader
	statusBadRequest
	statusInternal
	statusCount
)
const (
	rpcAppend = iota
	rpcVote
	rpcPreVote
	rpcSnapshot
	rpcTimeoutNow
	rpcCount
)
const (
	domainLog = iota
	domainStable
	domainCommit
	domainSnapshot
	domainCount
)

var operationNames = [...]string{"get", "put", "delete"}
var statusNames = [...]string{"ok", "not_found", "busy", "timeout", "not_leader", "bad_request", "internal"}
var rpcNames = [...]string{"append_entries", "request_vote", "pre_vote", "install_snapshot", "timeout_now"}
var domainNames = [...]string{"log", "stable", "commit", "snapshot"}

func operationIndex(s string) int {
	for i, v := range operationNames {
		if s == v {
			return i
		}
	}
	return -1
}
func statusIndex(s string) int {
	for i, v := range statusNames {
		if s == v {
			return i
		}
	}
	return statusInternal
}
func rpcIndex(s string) int {
	for i, v := range rpcNames {
		if s == v {
			return i
		}
	}
	return -1
}
func domainIndex(s string) int {
	for i, v := range domainNames {
		if s == v {
			return i
		}
	}
	return -1
}

type peerMetrics struct {
	matchIndex, nextIndex, replicationBytes              atomic.Uint64
	replicationRPCs, replicationFailures, staleResponses atomic.Uint64
}

// Metrics is owned by one node. Every non-peer label has a fixed vocabulary;
// peer entries are keyed only by stable configured NodeID and therefore have
// cluster-bounded cardinality.
type Metrics struct {
	requests                                                                          [operationCount][statusCount]atomic.Uint64
	requestDuration                                                                   [operationCount]histogram
	requestsInflight                                                                  atomic.Int64
	requestsBusy                                                                      atomic.Uint64
	rpcDuration                                                                       [rpcCount]histogram
	readIndexTotal, readIndexFailures                                                 atomic.Uint64
	readIndexDuration                                                                 histogram
	elections, leadershipChanges, termChanges                                         atomic.Uint64
	persistenceWrites, persistenceBytes                                               [domainCount]atomic.Uint64
	persistenceDuration, fsyncDuration                                                [domainCount]histogram
	snapshotsCreated, snapshotInstalls, snapshotInstallFailures, snapshotInstallBytes atomic.Uint64
	snapshotDuration                                                                  histogram
	snapshotSize, snapshotLastIndex                                                   atomic.Uint64
	peersMu                                                                           sync.RWMutex
	peers                                                                             map[uint64]*peerMetrics
}

func New() *Metrics { return &Metrics{peers: make(map[uint64]*peerMetrics)} }

func (m *Metrics) RecordRequest(operation, status string, d time.Duration) {
	op := operationIndex(operation)
	if op < 0 {
		return
	}
	m.requests[op][statusIndex(status)].Add(1)
	m.requestDuration[op].observe(d)
	if status == "busy" {
		m.requestsBusy.Add(1)
	}
}
func (m *Metrics) RequestStarted()      { m.requestsInflight.Add(1) }
func (m *Metrics) RequestFinished()     { m.requestsInflight.Add(-1) }
func (m *Metrics) RequestRejectedBusy() { m.requestsBusy.Add(1) }
func (m *Metrics) RecordRPC(rpc string, d time.Duration) {
	if i := rpcIndex(rpc); i >= 0 {
		m.rpcDuration[i].observe(d)
	}
}
func (m *Metrics) RecordReadIndex(d time.Duration, failed bool) {
	m.readIndexTotal.Add(1)
	if failed {
		m.readIndexFailures.Add(1)
	}
	m.readIndexDuration.observe(d)
}
func (m *Metrics) ElectionStarted()   { m.elections.Add(1) }
func (m *Metrics) LeadershipChanged() { m.leadershipChanges.Add(1) }
func (m *Metrics) TermChanged()       { m.termChanges.Add(1) }
func (m *Metrics) RecordPersistence(domain string, bytes int, d, fsync time.Duration) {
	i := domainIndex(domain)
	if i < 0 {
		return
	}
	m.persistenceWrites[i].Add(1)
	if bytes > 0 {
		m.persistenceBytes[i].Add(uint64(bytes))
	}
	m.persistenceDuration[i].observe(d)
	if fsync > 0 {
		m.fsyncDuration[i].observe(fsync)
	}
}
func (m *Metrics) SnapshotCreated(d time.Duration, size int, index uint64) {
	m.snapshotsCreated.Add(1)
	m.snapshotDuration.observe(d)
	if size >= 0 {
		m.snapshotSize.Store(uint64(size))
	}
	m.snapshotLastIndex.Store(index)
}
func (m *Metrics) SnapshotInstalled(bytes int, failed bool) {
	m.snapshotInstalls.Add(1)
	if failed {
		m.snapshotInstallFailures.Add(1)
	}
	if bytes > 0 {
		m.snapshotInstallBytes.Add(uint64(bytes))
	}
}

func (m *Metrics) peer(id uint64) *peerMetrics {
	m.peersMu.RLock()
	p := m.peers[id]
	m.peersMu.RUnlock()
	if p != nil {
		return p
	}
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if p = m.peers[id]; p == nil {
		p = &peerMetrics{}
		m.peers[id] = p
	}
	return p
}
func (m *Metrics) RecordReplication(id, match, next uint64, bytes int, failed, stale bool) {
	p := m.peer(id)
	p.replicationRPCs.Add(1)
	p.matchIndex.Store(match)
	p.nextIndex.Store(next)
	if bytes > 0 {
		p.replicationBytes.Add(uint64(bytes))
	}
	if failed {
		p.replicationFailures.Add(1)
	}
	if stale {
		p.staleResponses.Add(1)
	}
}
func (m *Metrics) RecordStaleReplication(id uint64) { m.peer(id).staleResponses.Add(1) }

// NodeSnapshot is gathered by the owner before encoding. ApplyLag is clamped
// to zero so inconsistent concurrent observations can never export a negative
// value; Node should provide its indexes from one locked snapshot.
type NodeSnapshot struct {
	NodeID, Term, CommitIndex, LastApplied, LastLogIndex                  uint64
	Role                                                                  string
	Leader                                                                bool
	ProposalAdmitted, ProposalBusy, ProposalBatches, ProposalBatchEntries uint64
	ProposalQueueDepth                                                    int64
	ConnectionsDialed, ConnectionsReused, ConnectionsClosed, SendFailures uint64
	ActiveConnections, TransportWaiters                                   int64
}

type peerSnapshot struct {
	id                                        uint64
	match, next, rpcs, bytes, failures, stale uint64
}

// WritePrometheus writes a point-in-time Prometheus text exposition. It only
// reads atomics and copied snapshots and deliberately has no error impact on
// the running node beyond returning the writer's error to the HTTP handler.
func (m *Metrics) WritePrometheus(w io.Writer, n NodeSnapshot) error {
	write := func(format string, args ...any) error { _, err := fmt.Fprintf(w, format, args...); return err }
	if err := write("# TYPE quorumkv_raft_term gauge\nquorumkv_raft_term %d\n", n.Term); err != nil {
		return err
	}
	roles := []string{"leader", "follower", "candidate"}
	for _, role := range roles {
		value := 0
		if n.Role == role {
			value = 1
		}
		if err := write("quorumkv_raft_role{role=%q} %d\n", role, value); err != nil {
			return err
		}
	}
	leader := 0
	if n.Leader {
		leader = 1
	}
	applyLag := uint64(0)
	if n.CommitIndex > n.LastApplied {
		applyLag = n.CommitIndex - n.LastApplied
	}
	gauges := []struct {
		name  string
		value any
	}{
		{"quorumkv_raft_leader", leader}, {"quorumkv_raft_commit_index", n.CommitIndex}, {"quorumkv_raft_last_applied", n.LastApplied}, {"quorumkv_raft_last_log_index", n.LastLogIndex}, {"quorumkv_raft_apply_lag", applyLag},
		{"quorumkv_proposal_queue_depth", n.ProposalQueueDepth}, {"quorumkv_requests_inflight", m.requestsInflight.Load()}, {"quorumkv_transport_active_connections", n.ActiveConnections}, {"quorumkv_transport_waiters", n.TransportWaiters},
	}
	for _, g := range gauges {
		if err := write("# TYPE %s gauge\n%s %v\n", g.name, g.name, g.value); err != nil {
			return err
		}
	}
	counters := []struct {
		name  string
		value uint64
	}{
		{"quorumkv_raft_elections_total", m.elections.Load()}, {"quorumkv_raft_leadership_changes_total", m.leadershipChanges.Load()}, {"quorumkv_raft_term_changes_total", m.termChanges.Load()},
		{"quorumkv_proposals_admitted_total", n.ProposalAdmitted}, {"quorumkv_proposals_busy_total", n.ProposalBusy}, {"quorumkv_proposal_batches_total", n.ProposalBatches}, {"quorumkv_proposal_batch_entries_total", n.ProposalBatchEntries}, {"quorumkv_requests_busy_total", m.requestsBusy.Load()},
		{"quorumkv_transport_connections_dialed_total", n.ConnectionsDialed}, {"quorumkv_transport_connections_reused_total", n.ConnectionsReused}, {"quorumkv_transport_connections_closed_total", n.ConnectionsClosed}, {"quorumkv_transport_send_failures_total", n.SendFailures},
		{"quorumkv_readindex_total", m.readIndexTotal.Load()}, {"quorumkv_readindex_failures_total", m.readIndexFailures.Load()}, {"quorumkv_snapshots_created_total", m.snapshotsCreated.Load()}, {"quorumkv_snapshot_install_total", m.snapshotInstalls.Load()}, {"quorumkv_snapshot_install_failures_total", m.snapshotInstallFailures.Load()}, {"quorumkv_snapshot_install_bytes_total", m.snapshotInstallBytes.Load()},
	}
	for _, c := range counters {
		if err := write("# TYPE %s counter\n%s %d\n", c.name, c.name, c.value); err != nil {
			return err
		}
	}
	if err := write("quorumkv_snapshot_size_bytes %d\nquorumkv_snapshot_last_index %d\n", m.snapshotSize.Load(), m.snapshotLastIndex.Load()); err != nil {
		return err
	}
	for op := range operationNames {
		for status := range statusNames {
			if err := write("quorumkv_requests_total{operation=%q,status=%q} %d\n", operationNames[op], statusNames[status], m.requests[op][status].Load()); err != nil {
				return err
			}
		}
		if err := writeHistogram(w, "quorumkv_request_duration_seconds", "operation", operationNames[op], m.requestDuration[op].snapshot()); err != nil {
			return err
		}
	}
	for rpc := range rpcNames {
		if err := writeHistogram(w, "quorumkv_transport_rpc_duration_seconds", "rpc", rpcNames[rpc], m.rpcDuration[rpc].snapshot()); err != nil {
			return err
		}
	}
	if err := writeHistogram(w, "quorumkv_readindex_duration_seconds", "", "", m.readIndexDuration.snapshot()); err != nil {
		return err
	}
	for domain := range domainNames {
		if err := write("quorumkv_persistence_writes_total{domain=%q} %d\nquorumkv_persistence_bytes_total{domain=%q} %d\n", domainNames[domain], m.persistenceWrites[domain].Load(), domainNames[domain], m.persistenceBytes[domain].Load()); err != nil {
			return err
		}
		if err := writeHistogram(w, "quorumkv_persistence_duration_seconds", "domain", domainNames[domain], m.persistenceDuration[domain].snapshot()); err != nil {
			return err
		}
		if err := writeHistogram(w, "quorumkv_persistence_fsync_duration_seconds", "domain", domainNames[domain], m.fsyncDuration[domain].snapshot()); err != nil {
			return err
		}
	}
	if err := writeHistogram(w, "quorumkv_snapshot_duration_seconds", "", "", m.snapshotDuration.snapshot()); err != nil {
		return err
	}
	m.peersMu.RLock()
	peers := make([]peerSnapshot, 0, len(m.peers))
	for id, p := range m.peers {
		peers = append(peers, peerSnapshot{id, p.matchIndex.Load(), p.nextIndex.Load(), p.replicationRPCs.Load(), p.replicationBytes.Load(), p.replicationFailures.Load(), p.staleResponses.Load()})
	}
	m.peersMu.RUnlock()
	sort.Slice(peers, func(i, j int) bool { return peers[i].id < peers[j].id })
	for _, p := range peers {
		label := strconv.FormatUint(p.id, 10)
		lag := uint64(0)
		if n.LastLogIndex > p.match {
			lag = n.LastLogIndex - p.match
		}
		if err := write("quorumkv_raft_peer_match_index{peer=%q} %d\nquorumkv_raft_peer_next_index{peer=%q} %d\nquorumkv_raft_peer_replication_lag{peer=%q} %d\nquorumkv_raft_replication_rpcs_total{peer=%q} %d\nquorumkv_raft_replication_bytes_total{peer=%q} %d\nquorumkv_raft_replication_failures_total{peer=%q} %d\nquorumkv_raft_replication_stale_responses_total{peer=%q} %d\n", label, p.match, label, p.next, label, lag, label, p.rpcs, label, p.bytes, label, p.failures, label, p.stale); err != nil {
			return err
		}
	}
	return nil
}

func writeHistogram(w io.Writer, name, label, value string, h histSnapshot) error {
	labels := ""
	if label != "" {
		labels = label + "=\"" + value + "\","
	}
	for i, bound := range durationBounds {
		if _, err := fmt.Fprintf(w, "%s_bucket{%sle=\"%g\"} %d\n", name, labels, bound, h.buckets[i]); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "%s_bucket{%sle=\"+Inf\"} %d\n", name, labels, h.count); err != nil {
		return err
	}
	suffix := ""
	if label != "" {
		suffix = "{" + label + "=\"" + value + "\"}"
	}
	_, err := fmt.Fprintf(w, "%s_sum%s %.9g\n%s_count%s %d\n", name, suffix, float64(h.sumNS)/float64(time.Second), name, suffix, h.count)
	return err
}
