// Package adminproto defines the bounded binary wire protocol for
// QuorumKV's operational admin commands: status inspection, snapshot
// creation, leadership transfer, and voter add/remove. It knows nothing
// about Raft or the KV state machine — like clientproto, it only defines
// request/response bytes; package service decodes/dispatches them and is
// the only place that calls into the real raft.Node operations this
// protocol is a thin wire wrapper over.
//
// This protocol is unauthenticated, exactly like the client protocol it
// sits beside on the same TCP connection (see transport.MessageAdminRequest/
// MessageAdminResponse) — it is intended for local/trusted-network
// operation and demos, not a public administrative surface. See
// docs/operations.md.
package adminproto

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const protocolVersion = 1

// MaxAddrLen bounds a single voter address in this protocol — generous
// for any realistic "host:port" string.
const MaxAddrLen = 256

// MaxVoters bounds how many voters a StatusInfo's voter lists may
// declare, matching raft's own Configuration limit (adminproto does not
// import package raft, so this is not literally the same constant, but
// is chosen to match it — see package raft's MaxVoters).
const MaxVoters = 31

// Operation identifies the requested admin command.
type Operation uint8

const (
	OpStatus Operation = iota + 1
	OpSnapshot
	OpTransferLeadership
	OpAddVoter
	OpRemoveVoter
)

// Status identifies the outcome of an admin request. Internal Go error
// strings are never sent over the wire — only this small fixed set of
// codes.
type Status uint8

const (
	StatusOK Status = iota + 1
	StatusNotLeader
	StatusBadRequest
	StatusInternalError
	// StatusMembershipChangeInProgress means a Joint transition is
	// already active — mirrors raft.ErrMembershipChangeInProgress.
	StatusMembershipChangeInProgress
	// StatusLeadershipTransferInProgress means a transfer is already
	// active, or (for OpAddVoter/OpRemoveVoter/OpSnapshot) blocks a new
	// one from starting — mirrors raft.ErrLeadershipTransferInProgress.
	StatusLeadershipTransferInProgress
	// StatusNotAVoter means the request named a NodeID that is not (or
	// no longer) a voter — mirrors raft.ErrNotAVoter.
	StatusNotAVoter
	// StatusInvalidConfiguration mirrors raft.ErrInvalidConfiguration
	// (e.g. removing the last voter, a duplicate NodeID).
	StatusInvalidConfiguration
	// StatusTimeout means the operation's context expired before this
	// node could confirm a definite outcome — the underlying operation
	// (a membership change, a leadership transfer) may or may not have
	// actually completed. See docs/runbook-membership.md and
	// docs/runbook-leadership-transfer.md: do not blindly retry.
	StatusTimeout
	// StatusCannotTransferToSelf mirrors raft.ErrCannotTransferToSelf.
	StatusCannotTransferToSelf
	// StatusTransferRejected mirrors raft.ErrLeadershipTransferRejected:
	// the target explicitly declined TimeoutNow (as opposed to a
	// network/context failure reaching it at all, which is StatusTimeout).
	StatusTransferRejected
)

// Request is one admin command.
//
//	OpStatus:             no fields used.
//	OpSnapshot:           no fields used.
//	OpTransferLeadership: TransferTarget set (non-zero).
//	OpAddVoter:           VoterID and VoterAddr both set.
//	OpRemoveVoter:        VoterID set.
type Request struct {
	Operation      Operation
	TransferTarget uint64
	VoterID        uint64
	VoterAddr      []byte
}

// Voter is one entry in a StatusInfo voter list.
type Voter struct {
	ID   uint64
	Addr []byte
}

// MembershipMode mirrors raft.MembershipMode's two values.
type MembershipMode uint8

const (
	MembershipStable MembershipMode = iota + 1
	MembershipJoint
)

// Role mirrors raft.Role's three values.
type Role uint8

const (
	RoleFollower Role = iota + 1
	RoleCandidate
	RoleLeader
)

// StatusInfo is the payload of a successful OpStatus response. LeaderID
// is 0 when this node does not currently know the leader (mirrors
// raft.Node.LeaderHint's ok==false). OldVoters/NewVoters are only
// meaningful when Mode == MembershipJoint; StableVoters is only
// meaningful when Mode == MembershipStable.
//
// This is observational metadata read directly from the node's own
// in-memory state — not a linearizable read (see docs/operations.md):
// it never runs ReadIndex, never blocks on quorum confirmation, and
// never mutates term, log, membership, commit index, or any timer.
type StatusInfo struct {
	NodeID        uint64
	Role          Role
	Term          uint64
	LeaderID      uint64
	LastLogIndex  uint64
	CommitIndex   uint64
	LastApplied   uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Mode          MembershipMode
	StableVoters  []Voter
	OldVoters     []Voter
	NewVoters     []Voter
}

// Response is an admin request's result.
//
//	LeaderHint is only meaningful (and may be non-empty) when Status is
//	StatusNotLeader.
//	Info is only meaningful when Status is StatusOK and the request was
//	OpStatus.
//	SnapshotIndex/SnapshotTerm are only meaningful when Status is
//	StatusOK and the request was OpSnapshot.
type Response struct {
	Status        Status
	LeaderHint    []byte
	Info          StatusInfo
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

var (
	ErrMalformedRequest  = errors.New("adminproto: malformed request")
	ErrMalformedResponse = errors.New("adminproto: malformed response")
)

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func validOperation(op Operation) bool {
	switch op {
	case OpStatus, OpSnapshot, OpTransferLeadership, OpAddVoter, OpRemoveVoter:
		return true
	default:
		return false
	}
}

// requestFixedSize: version(1) + operation(1) + transferTarget(8) +
// voterID(8) + voterAddrLength(2).
const requestFixedSize = 1 + 1 + 8 + 8 + 2

// EncodeRequest produces the exact wire bytes for r.
func EncodeRequest(r Request) ([]byte, error) {
	if !validOperation(r.Operation) {
		return nil, fmt.Errorf("adminproto: unknown operation %d", r.Operation)
	}
	if len(r.VoterAddr) > MaxAddrLen {
		return nil, fmt.Errorf("adminproto: voter address length %d exceeds max %d", len(r.VoterAddr), MaxAddrLen)
	}
	buf := make([]byte, requestFixedSize+len(r.VoterAddr))
	buf[0] = protocolVersion
	buf[1] = byte(r.Operation)
	binary.BigEndian.PutUint64(buf[2:10], r.TransferTarget)
	binary.BigEndian.PutUint64(buf[10:18], r.VoterID)
	binary.BigEndian.PutUint16(buf[18:20], uint16(len(r.VoterAddr)))
	copy(buf[requestFixedSize:], r.VoterAddr)
	return buf, nil
}

// DecodeRequest validates and decodes a request payload.
func DecodeRequest(b []byte) (Request, error) {
	if len(b) < requestFixedSize {
		return Request{}, fmt.Errorf("%w: too short", ErrMalformedRequest)
	}
	if b[0] != protocolVersion {
		return Request{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedRequest, b[0])
	}
	op := Operation(b[1])
	if !validOperation(op) {
		return Request{}, fmt.Errorf("%w: unknown operation %d", ErrMalformedRequest, op)
	}
	target := binary.BigEndian.Uint64(b[2:10])
	voterID := binary.BigEndian.Uint64(b[10:18])
	addrLen := binary.BigEndian.Uint16(b[18:20])
	if int(addrLen) > MaxAddrLen {
		return Request{}, fmt.Errorf("%w: voter address length %d exceeds max %d", ErrMalformedRequest, addrLen, MaxAddrLen)
	}
	if len(b) != requestFixedSize+int(addrLen) {
		return Request{}, fmt.Errorf("%w: declared address length does not match payload size", ErrMalformedRequest)
	}
	return Request{
		Operation:      op,
		TransferTarget: target,
		VoterID:        voterID,
		VoterAddr:      cloneBytes(b[requestFixedSize:]),
	}, nil
}
