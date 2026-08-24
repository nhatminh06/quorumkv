// Package clientproto defines the bounded binary wire protocol clients use
// to send PUT/GET/DELETE requests to a QuorumKV node and receive
// responses. It knows nothing about Raft or the KV state machine — it
// only defines request/response bytes; package service decodes/dispatches
// them. Payloads travel inside a transport.Message (MessageClientRequest/
// MessageClientResponse), whose frame already carries a CRC32C over the
// whole payload, so this format does not duplicate that checksum.
//
// There is no TLS and no authentication in this protocol.
//
// Since Milestone 9, a PUT/DELETE request carries a request identity
// (ClientID, Sequence) so a leader can recognize and safely suppress a
// retried write's second effect — see docs/request-dedup.md. This is a
// protocol version bump (protocolVersion 1 -> 2), not an additive/
// optional field: an old Milestone 1-8 client cannot talk to this
// server, and vice versa. That compatibility was never promised.
package clientproto

import (
	"encoding/binary"
	"errors"
	"fmt"

	"quorumkv/internal/reqid"
)

const protocolVersion = 2

// MaxKeySize/MaxValueSize bound a single request/response's key/value.
// MaxValueSize matches kv.MaxValueSize so a PUT that fits the client
// protocol always fits the resulting Raft log entry.
const (
	MaxKeySize   = 64 * 1024
	MaxValueSize = 200 * 1024
	// MaxLeaderHintSize bounds a NOT_LEADER response's leader address hint
	// (a "host:port" string) — generous for any realistic address.
	MaxLeaderHintSize = 256
)

// Operation identifies the requested KV operation.
type Operation uint8

const (
	OpPut Operation = iota + 1
	OpGet
	OpDelete
)

// Status identifies the outcome of a request. Internal Go error strings
// are never sent over the wire — only this small fixed set of codes.
type Status uint8

const (
	StatusOK Status = iota + 1
	StatusNotFound
	StatusNotLeader
	StatusTimeout
	StatusInternalError
	StatusBadRequest
	// StatusStaleRequest (since Milestone 9) means a PUT/DELETE's
	// Sequence did not match the expected next sequence for its
	// ClientID (either behind the last applied sequence, or ahead of it
	// by more than one) — a client/session state disagreement. Terminal:
	// a client must not automatically retry this with a new sequence.
	StatusStaleRequest
	// StatusRequestConflict (since Milestone 9) means a PUT/DELETE
	// reused a (ClientID, Sequence) already used for a *different*
	// operation — invalid client behavior, not a legitimate retry.
	// Terminal.
	StatusRequestConflict
)

// Request is a client PUT/GET/DELETE request.
//
//	PUT:    ClientID/Sequence both set (non-zero); Key and Value both set.
//	GET:    ClientID/Sequence both zero (reads carry no request identity —
//	        see docs/request-dedup.md); Key set, Value must be empty.
//	DELETE: ClientID/Sequence both set (non-zero); Key set, Value must be
//	        empty.
type Request struct {
	Operation Operation
	ClientID  reqid.ClientID
	Sequence  reqid.Sequence
	Key       []byte
	Value     []byte
}

// Response is a client request's result.
//
//	LeaderHint is only meaningful (and may be non-empty) when Status is
//	StatusNotLeader; it is empty if this node does not currently know the
//	leader.
//	Value is only meaningful when Status is StatusOK for a GET.
type Response struct {
	Status     Status
	LeaderHint []byte
	Value      []byte
}

var (
	ErrMalformedRequest  = errors.New("clientproto: malformed request")
	ErrMalformedResponse = errors.New("clientproto: malformed response")
)

// requestFixedHeaderSize: version(1) + operation(1) + clientID(16) +
// sequence(8) + keyLength(4) + valueLength(4).
const requestFixedHeaderSize = 1 + 1 + 16 + 8 + 4 + 4

// EncodeRequest produces the exact wire bytes for r. All integers
// big-endian. GET/DELETE requests must not carry a value. A PUT/DELETE
// must carry a non-zero ClientID and non-zero Sequence (see
// internal/reqid); a GET must carry neither — one clear, uniform rule
// rather than treating the identity fields as optional.
func EncodeRequest(r Request) ([]byte, error) {
	if len(r.Key) > MaxKeySize {
		return nil, fmt.Errorf("clientproto: key length %d exceeds max %d", len(r.Key), MaxKeySize)
	}
	if len(r.Value) > MaxValueSize {
		return nil, fmt.Errorf("clientproto: value length %d exceeds max %d", len(r.Value), MaxValueSize)
	}
	switch r.Operation {
	case OpPut:
		if err := requireIdentity(r); err != nil {
			return nil, err
		}
	case OpDelete:
		if len(r.Value) != 0 {
			return nil, fmt.Errorf("clientproto: operation %d must not carry a value", r.Operation)
		}
		if err := requireIdentity(r); err != nil {
			return nil, err
		}
	case OpGet:
		if len(r.Value) != 0 {
			return nil, fmt.Errorf("clientproto: operation %d must not carry a value", r.Operation)
		}
		if err := requireNoIdentity(r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("clientproto: unknown operation %d", r.Operation)
	}

	buf := make([]byte, requestFixedHeaderSize+len(r.Key)+len(r.Value))
	buf[0] = protocolVersion
	buf[1] = byte(r.Operation)
	off := 2
	off += copy(buf[off:], r.ClientID[:])
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(r.Sequence))
	off += 8
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.Key)))
	off += 4
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.Value)))
	off += 4
	off += copy(buf[off:], r.Key)
	copy(buf[off:], r.Value)
	return buf, nil
}

func requireIdentity(r Request) error {
	if r.ClientID.IsZero() {
		return fmt.Errorf("clientproto: operation %d requires a non-zero ClientID", r.Operation)
	}
	if r.Sequence == 0 {
		return fmt.Errorf("clientproto: operation %d requires a non-zero Sequence", r.Operation)
	}
	return nil
}

func requireNoIdentity(r Request) error {
	if !r.ClientID.IsZero() || r.Sequence != 0 {
		return fmt.Errorf("clientproto: operation %d must not carry a request identity", r.Operation)
	}
	return nil
}

// DecodeRequest validates and decodes a request payload. Declared key and
// value lengths are validated before any allocation based on them.
func DecodeRequest(b []byte) (Request, error) {
	if len(b) < requestFixedHeaderSize {
		return Request{}, fmt.Errorf("%w: too short", ErrMalformedRequest)
	}
	if b[0] != protocolVersion {
		return Request{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedRequest, b[0])
	}
	op := Operation(b[1])
	switch op {
	case OpPut, OpGet, OpDelete:
	default:
		return Request{}, fmt.Errorf("%w: unknown operation %d", ErrMalformedRequest, op)
	}
	var id reqid.ClientID
	off := 2
	copy(id[:], b[off:off+16])
	off += 16
	seq := reqid.Sequence(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	keyLen := binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	valLen := binary.BigEndian.Uint32(b[off : off+4])
	off += 4

	switch op {
	case OpPut, OpDelete:
		if id.IsZero() {
			return Request{}, fmt.Errorf("%w: operation %d requires a non-zero ClientID", ErrMalformedRequest, op)
		}
		if seq == 0 {
			return Request{}, fmt.Errorf("%w: operation %d requires a non-zero Sequence", ErrMalformedRequest, op)
		}
	case OpGet:
		if !id.IsZero() || seq != 0 {
			return Request{}, fmt.Errorf("%w: operation %d must not carry a request identity", ErrMalformedRequest, op)
		}
	}
	if keyLen > MaxKeySize {
		return Request{}, fmt.Errorf("%w: key length %d exceeds max %d", ErrMalformedRequest, keyLen, MaxKeySize)
	}
	if valLen > MaxValueSize {
		return Request{}, fmt.Errorf("%w: value length %d exceeds max %d", ErrMalformedRequest, valLen, MaxValueSize)
	}
	if (op == OpGet || op == OpDelete) && valLen != 0 {
		return Request{}, fmt.Errorf("%w: operation %d must not carry a value", ErrMalformedRequest, op)
	}
	want := requestFixedHeaderSize + int(keyLen) + int(valLen)
	if len(b) != want {
		return Request{}, fmt.Errorf("%w: length mismatch (declared %d, got %d bytes)", ErrMalformedRequest, want, len(b))
	}

	key := cloneBytes(b[off : off+int(keyLen)])
	value := cloneBytes(b[off+int(keyLen):])
	return Request{Operation: op, ClientID: id, Sequence: seq, Key: key, Value: value}, nil
}

// responseFixedHeaderSize: version(1) + status(1) + leaderHintLength(2) +
// valueLength(4).
const responseFixedHeaderSize = 1 + 1 + 2 + 4

// EncodeResponse produces the exact wire bytes for r.
func EncodeResponse(r Response) ([]byte, error) {
	if len(r.LeaderHint) > MaxLeaderHintSize {
		return nil, fmt.Errorf("clientproto: leader hint length %d exceeds max %d", len(r.LeaderHint), MaxLeaderHintSize)
	}
	if len(r.Value) > MaxValueSize {
		return nil, fmt.Errorf("clientproto: value length %d exceeds max %d", len(r.Value), MaxValueSize)
	}
	switch r.Status {
	case StatusOK, StatusNotFound, StatusNotLeader, StatusTimeout, StatusInternalError, StatusBadRequest, StatusStaleRequest, StatusRequestConflict:
	default:
		return nil, fmt.Errorf("clientproto: unknown status %d", r.Status)
	}

	buf := make([]byte, responseFixedHeaderSize+len(r.LeaderHint)+len(r.Value))
	buf[0] = protocolVersion
	buf[1] = byte(r.Status)
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(r.LeaderHint)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(r.Value)))
	off := responseFixedHeaderSize
	off += copy(buf[off:], r.LeaderHint)
	copy(buf[off:], r.Value)
	return buf, nil
}

// DecodeResponse validates and decodes a response payload. Declared
// leader-hint and value lengths are validated before any allocation based
// on them.
func DecodeResponse(b []byte) (Response, error) {
	if len(b) < responseFixedHeaderSize {
		return Response{}, fmt.Errorf("%w: too short", ErrMalformedResponse)
	}
	if b[0] != protocolVersion {
		return Response{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedResponse, b[0])
	}
	status := Status(b[1])
	switch status {
	case StatusOK, StatusNotFound, StatusNotLeader, StatusTimeout, StatusInternalError, StatusBadRequest, StatusStaleRequest, StatusRequestConflict:
	default:
		return Response{}, fmt.Errorf("%w: unknown status %d", ErrMalformedResponse, status)
	}
	hintLen := binary.BigEndian.Uint16(b[2:4])
	if int(hintLen) > MaxLeaderHintSize {
		return Response{}, fmt.Errorf("%w: leader hint length %d exceeds max %d", ErrMalformedResponse, hintLen, MaxLeaderHintSize)
	}
	valLen := binary.BigEndian.Uint32(b[4:8])
	if valLen > MaxValueSize {
		return Response{}, fmt.Errorf("%w: value length %d exceeds max %d", ErrMalformedResponse, valLen, MaxValueSize)
	}
	want := responseFixedHeaderSize + int(hintLen) + int(valLen)
	if len(b) != want {
		return Response{}, fmt.Errorf("%w: length mismatch (declared %d, got %d bytes)", ErrMalformedResponse, want, len(b))
	}

	hint := cloneBytes(b[responseFixedHeaderSize : responseFixedHeaderSize+int(hintLen)])
	value := cloneBytes(b[responseFixedHeaderSize+int(hintLen):])
	return Response{Status: status, LeaderHint: hint, Value: value}, nil
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
