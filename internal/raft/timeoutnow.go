package raft

import (
	"encoding/binary"
	"fmt"
)

// TimeoutNowRequest tells a fully caught-up follower to start an
// election immediately, bypassing its normal election timeout — used
// only for an authorized leadership transfer (see leadership_transfer.go
// and docs/leadership-transfer.md). It deliberately carries no log data:
// the leader has already ensured the target is caught up before ever
// sending this.
type TimeoutNowRequest struct {
	Term     Term
	LeaderID NodeID
}

// TimeoutNowResponse acknowledges that TimeoutNow was accepted (and an
// immediate election attempt started) or rejected. Accepted does not by
// itself mean the target went on to become leader — TransferLeadership
// waits for separate, real evidence of that (see waitForTransferCompletion).
type TimeoutNowResponse struct {
	Term     Term
	Accepted bool
}

// timeoutNowSize is the fixed wire size of a TimeoutNowRequest: term(8)
// + leaderID(8), big-endian.
const timeoutNowSize = 8 + 8

// timeoutNowResponseSize is the fixed wire size of a TimeoutNowResponse:
// term(8) + accepted(1), big-endian.
const timeoutNowResponseSize = 8 + 1

// EncodeTimeoutNow produces the exact wire bytes for req.
func EncodeTimeoutNow(req TimeoutNowRequest) []byte {
	buf := make([]byte, timeoutNowSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(req.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(req.LeaderID))
	return buf
}

// DecodeTimeoutNow validates and decodes a TimeoutNowRequest payload.
func DecodeTimeoutNow(b []byte) (TimeoutNowRequest, error) {
	if len(b) != timeoutNowSize {
		return TimeoutNowRequest{}, fmt.Errorf("%w: TimeoutNow length %d, want %d", ErrMalformedRPC, len(b), timeoutNowSize)
	}
	return TimeoutNowRequest{
		Term:     Term(binary.BigEndian.Uint64(b[0:8])),
		LeaderID: NodeID(binary.BigEndian.Uint64(b[8:16])),
	}, nil
}

// EncodeTimeoutNowResponse produces the exact wire bytes for resp.
func EncodeTimeoutNowResponse(resp TimeoutNowResponse) []byte {
	buf := make([]byte, timeoutNowResponseSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(resp.Term))
	if resp.Accepted {
		buf[8] = 1
	}
	return buf
}

// DecodeTimeoutNowResponse validates and decodes a TimeoutNowResponse
// payload.
func DecodeTimeoutNowResponse(b []byte) (TimeoutNowResponse, error) {
	if len(b) != timeoutNowResponseSize {
		return TimeoutNowResponse{}, fmt.Errorf("%w: TimeoutNowResponse length %d, want %d", ErrMalformedRPC, len(b), timeoutNowResponseSize)
	}
	accepted := b[8]
	if accepted > 1 {
		return TimeoutNowResponse{}, fmt.Errorf("%w: invalid accepted encoding %d", ErrMalformedRPC, accepted)
	}
	return TimeoutNowResponse{
		Term:     Term(binary.BigEndian.Uint64(b[0:8])),
		Accepted: accepted == 1,
	}, nil
}
