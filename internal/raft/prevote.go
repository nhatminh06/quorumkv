package raft

import (
	"encoding/binary"
	"fmt"
)

// PreVoteRequest asks "would you vote for me in term ProspectiveTerm?"
// without committing to anything: neither side's persistent state changes
// as a result of a PreVote round, only of the real election that may
// follow it. ProspectiveTerm is normally CurrentTerm+1 at the moment the
// round starts — it is never written to persistent state merely to
// construct this request. See docs/raft-election.md for the full
// PreVote phase.
type PreVoteRequest struct {
	ProspectiveTerm Term
	CandidateID     NodeID
	LastLogIndex    LogIndex
	LastLogTerm     Term
}

// PreVoteResponse reports the responder's actual current term (never the
// prospective one it was asked about — a responder that has not entered
// ProspectiveTerm must not pretend it has) and whether it would grant a
// real vote for that prospective election.
type PreVoteResponse struct {
	Term        Term
	VoteGranted bool
}

// preVoteSize is the fixed wire size of a PreVoteRequest: identical
// layout to RequestVoteRequest (prospectiveTerm(8) + candidateID(8) +
// lastLogIndex(8) + lastLogTerm(8)), all big-endian. This payload
// travels inside a transport.Message, whose frame already carries a
// CRC32C checksum over the whole payload.
const preVoteSize = 8 + 8 + 8 + 8

// preVoteResponseSize is the fixed wire size of a PreVoteResponse:
// term(8) + voteGranted(1), big-endian — identical layout to
// RequestVoteResponse.
const preVoteResponseSize = 8 + 1

// EncodePreVote produces the exact wire bytes for req.
func EncodePreVote(req PreVoteRequest) []byte {
	buf := make([]byte, preVoteSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(req.ProspectiveTerm))
	binary.BigEndian.PutUint64(buf[8:16], uint64(req.CandidateID))
	binary.BigEndian.PutUint64(buf[16:24], uint64(req.LastLogIndex))
	binary.BigEndian.PutUint64(buf[24:32], uint64(req.LastLogTerm))
	return buf
}

// DecodePreVote validates and decodes a PreVoteRequest payload.
func DecodePreVote(b []byte) (PreVoteRequest, error) {
	if len(b) != preVoteSize {
		return PreVoteRequest{}, fmt.Errorf("%w: PreVote length %d, want %d", ErrMalformedRPC, len(b), preVoteSize)
	}
	return PreVoteRequest{
		ProspectiveTerm: Term(binary.BigEndian.Uint64(b[0:8])),
		CandidateID:     NodeID(binary.BigEndian.Uint64(b[8:16])),
		LastLogIndex:    LogIndex(binary.BigEndian.Uint64(b[16:24])),
		LastLogTerm:     Term(binary.BigEndian.Uint64(b[24:32])),
	}, nil
}

// EncodePreVoteResponse produces the exact wire bytes for resp.
func EncodePreVoteResponse(resp PreVoteResponse) []byte {
	buf := make([]byte, preVoteResponseSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(resp.Term))
	if resp.VoteGranted {
		buf[8] = 1
	}
	return buf
}

// DecodePreVoteResponse validates and decodes a PreVoteResponse payload.
func DecodePreVoteResponse(b []byte) (PreVoteResponse, error) {
	if len(b) != preVoteResponseSize {
		return PreVoteResponse{}, fmt.Errorf("%w: PreVoteResponse length %d, want %d", ErrMalformedRPC, len(b), preVoteResponseSize)
	}
	granted := b[8]
	if granted > 1 {
		return PreVoteResponse{}, fmt.Errorf("%w: invalid voteGranted encoding %d", ErrMalformedRPC, granted)
	}
	return PreVoteResponse{
		Term:        Term(binary.BigEndian.Uint64(b[0:8])),
		VoteGranted: granted == 1,
	}, nil
}
