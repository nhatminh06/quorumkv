package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// RequestVoteRequest is the Raft RequestVote RPC. LastLogIndex and
// LastLogTerm are part of the RPC now, ahead of log replication, so the
// wire format does not need to change incompatibly later; until log
// replication exists every node uses the fixed "empty log" convention of
// LastLogIndex 0, LastLogTerm 0.
type RequestVoteRequest struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

// RequestVoteResponse is the Raft RequestVote RPC response.
type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

// requestVoteSize is the fixed wire size of a RequestVoteRequest: term(8)
// + candidateID(8) + lastLogIndex(8) + lastLogTerm(8), all big-endian.
// This payload travels inside a transport.Message, whose frame already
// carries a CRC32C checksum over the whole payload, so the RPC encoding
// itself does not duplicate that integrity check.
const requestVoteSize = 8 + 8 + 8 + 8

// requestVoteResponseSize is the fixed wire size of a
// RequestVoteResponse: term(8) + voteGranted(1), big-endian.
const requestVoteResponseSize = 8 + 1

var ErrMalformedRPC = errors.New("raft: malformed RPC payload")

// EncodeRequestVote produces the exact wire bytes for req.
func EncodeRequestVote(req RequestVoteRequest) []byte {
	buf := make([]byte, requestVoteSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(req.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(req.CandidateID))
	binary.BigEndian.PutUint64(buf[16:24], uint64(req.LastLogIndex))
	binary.BigEndian.PutUint64(buf[24:32], uint64(req.LastLogTerm))
	return buf
}

// DecodeRequestVote validates and decodes a RequestVoteRequest payload.
func DecodeRequestVote(b []byte) (RequestVoteRequest, error) {
	if len(b) != requestVoteSize {
		return RequestVoteRequest{}, fmt.Errorf("%w: RequestVote length %d, want %d", ErrMalformedRPC, len(b), requestVoteSize)
	}
	return RequestVoteRequest{
		Term:         Term(binary.BigEndian.Uint64(b[0:8])),
		CandidateID:  NodeID(binary.BigEndian.Uint64(b[8:16])),
		LastLogIndex: LogIndex(binary.BigEndian.Uint64(b[16:24])),
		LastLogTerm:  Term(binary.BigEndian.Uint64(b[24:32])),
	}, nil
}

// EncodeRequestVoteResponse produces the exact wire bytes for resp.
func EncodeRequestVoteResponse(resp RequestVoteResponse) []byte {
	buf := make([]byte, requestVoteResponseSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(resp.Term))
	if resp.VoteGranted {
		buf[8] = 1
	}
	return buf
}

// DecodeRequestVoteResponse validates and decodes a RequestVoteResponse
// payload.
func DecodeRequestVoteResponse(b []byte) (RequestVoteResponse, error) {
	if len(b) != requestVoteResponseSize {
		return RequestVoteResponse{}, fmt.Errorf("%w: RequestVoteResponse length %d, want %d", ErrMalformedRPC, len(b), requestVoteResponseSize)
	}
	granted := b[8]
	if granted > 1 {
		return RequestVoteResponse{}, fmt.Errorf("%w: invalid voteGranted encoding %d", ErrMalformedRPC, granted)
	}
	return RequestVoteResponse{
		Term:        Term(binary.BigEndian.Uint64(b[0:8])),
		VoteGranted: granted == 1,
	}, nil
}

// LogUpToDate reports whether a candidate's log (candidateLastLogTerm,
// candidateLastLogIndex) is at least as up to date as a voter's own log
// (localLastLogTerm, localLastLogIndex), per the Raft RequestVote
// eligibility rule: the log with the later term is more up to date; if
// the terms are equal, the longer log (higher index) is more up to date.
func LogUpToDate(candidateLastLogTerm Term, candidateLastLogIndex LogIndex, localLastLogTerm Term, localLastLogIndex LogIndex) bool {
	if candidateLastLogTerm != localLastLogTerm {
		return candidateLastLogTerm > localLastLogTerm
	}
	return candidateLastLogIndex >= localLastLogIndex
}
