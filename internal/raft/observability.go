package raft

import "quorumkv/internal/observability"

// ObservabilitySnapshot copies local state without network or disk I/O. All
// Raft fields come from one short lock hold; proposal and transport snapshots
// are gathered afterward so a scrape never holds the Raft lock while waiting
// for another subsystem.
func (n *Node) ObservabilitySnapshot() observability.NodeSnapshot {
	n.mu.Lock()
	s := observability.NodeSnapshot{
		NodeID: uint64(n.id), Term: uint64(n.persistent.CurrentTerm),
		CommitIndex: uint64(n.commitIndex), LastApplied: uint64(n.lastApplied),
		LastLogIndex: uint64(n.log.LastIndex()), Role: roleMetric(n.role),
		Leader: n.role == Leader,
	}
	n.mu.Unlock()
	proposals := n.Stats()
	s.ProposalAdmitted = uint64(maxInt64(proposals.ProposalsAdmitted))
	s.ProposalBusy = uint64(maxInt64(proposals.ProposalsRejectedBusy))
	s.ProposalBatches = uint64(maxInt64(proposals.ProposalBatches))
	s.ProposalBatchEntries = uint64(maxInt64(proposals.ProposalBatchEntries))
	s.ProposalQueueDepth = proposals.QueueDepth
	transport := n.TransportStats()
	s.ConnectionsDialed, s.ConnectionsReused = transport.ConnectionsDialed, transport.ConnectionsReused
	s.ConnectionsClosed, s.SendFailures = transport.ConnectionsClosed, transport.SendFailures
	s.ActiveConnections, s.TransportWaiters = transport.ActiveConnections, transport.Waiters
	return s
}

func roleMetric(r Role) string {
	switch r {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

func maxInt64(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
