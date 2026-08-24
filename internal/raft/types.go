// Package raft implements Raft leader election from first principles:
// persistent term/vote state, the RequestVote RPC, and majority-based
// transition to leader for an empty-log cluster. It does not yet
// implement AppendEntries, heartbeats, or log replication — see
// docs/raft-election.md for exactly what is and is not implemented.
package raft

// Term is a Raft election term. Terms are totally ordered and must never
// decrease for a given node's persistent state.
type Term uint64

// LogIndex identifies a position in the (not yet implemented) replicated
// Raft log. Until log replication exists, every node behaves as though
// its log is empty: LogIndex 0 and Term 0 are the fixed "empty log"
// convention used throughout this package.
type LogIndex uint64

// NodeID is a stable identifier for a Raft node, used both as the wire
// representation of a candidate/voter identity and as the key peers are
// addressed by.
type NodeID uint64

// Role is a node's current, volatile Raft role. Role is never persisted:
// a restarted node always starts as Follower, using whatever term/vote it
// last persisted.
type Role uint8

const (
	Follower Role = iota + 1
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Majority returns the number of votes required for a majority of a
// cluster with clusterSize members (including self): floor(n/2) + 1.
func Majority(clusterSize int) int {
	return clusterSize/2 + 1
}
