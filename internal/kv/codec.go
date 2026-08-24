package kv

import (
	"encoding/binary"
	"errors"
	"fmt"

	"quorumkv/internal/reqid"
)

// EncodeCommand/DecodeCommand define the deterministic wire format used to
// carry a Command inside an opaque Raft log entry. This package has no
// knowledge of Raft; it only guarantees that encoding is deterministic and
// bounded so the result always fits in one legal Raft log entry.
//
// GET is never encoded here — it is a read, not a replicated command.
//
// Since Milestone 9 there are two on-disk/wire shapes, both readable
// forever (existing committed logs/snapshots must never become
// unreadable — see docs/request-dedup.md):
//
//   - version 1 (commandVersion1): the original Milestone 1 shape, no
//     request identity. EncodeCommand still produces this for any
//     Command with a zero ClientID (the shape NewPutCommand/
//     NewDeleteCommand build) — byte-for-byte identical to every
//     Milestone 1-8 command.
//   - version 2 (commandVersion2): adds ClientID/Sequence for
//     deduplication. EncodeCommand produces this for any Command with a
//     non-zero ClientID (the shape NewIdentifiedPutCommand/
//     NewIdentifiedDeleteCommand build).
//
// No log rewrite/migration is needed or performed: which version a given
// entry decodes as is exactly which version originally encoded it.

const (
	commandVersion1 = 1
	commandVersion2 = 2
)

// MaxKeySize/MaxValueSize bound a single command, chosen to comfortably
// fit within Raft's per-entry command limit (see internal/raft's
// maxCommandSize) once this format's fixed overhead is included.
const (
	MaxKeySize   = 64 * 1024
	MaxValueSize = 200 * 1024
)

// commandV1FixedHeaderSize: version(1) + operation(1) + keyLength(4) +
// valueLength(4).
const commandV1FixedHeaderSize = 1 + 1 + 4 + 4

// commandV2FixedHeaderSize: version(1) + operation(1) + clientID(16) +
// sequence(8) + keyLength(4) + valueLength(4).
const commandV2FixedHeaderSize = 1 + 1 + 16 + 8 + 4 + 4

// ErrMalformedCommand indicates a command payload failed validation on
// decode (wrong version, unknown operation, an oversized/inconsistent
// declared length, a DELETE carrying a value, or — for version 2 — an
// invalid request identity).
var ErrMalformedCommand = errors.New("kv: malformed command")

// EncodeCommand produces the exact wire bytes for cmd. A Command with a
// zero ClientID encodes as version 1:
//
//	version(1B) | operation(1B) | keyLength(4B) | valueLength(4B) | key | value
//
// A Command with a non-zero ClientID encodes as version 2:
//
//	version(1B) | operation(1B) | clientID(16B) | sequence(8B) | keyLength(4B) | valueLength(4B) | key | value
//
// All integers big-endian. A DELETE command must not carry a value. An
// identified command (non-zero ClientID) must have a non-zero Sequence —
// 0 is reserved as invalid/unassigned (see internal/reqid).
func EncodeCommand(cmd Command) ([]byte, error) {
	if cmd.Type != CommandPut && cmd.Type != CommandDelete {
		return nil, fmt.Errorf("kv: unknown command type %d", cmd.Type)
	}
	if len(cmd.Key) > MaxKeySize {
		return nil, fmt.Errorf("kv: key length %d exceeds max %d", len(cmd.Key), MaxKeySize)
	}
	if len(cmd.Value) > MaxValueSize {
		return nil, fmt.Errorf("kv: value length %d exceeds max %d", len(cmd.Value), MaxValueSize)
	}
	if cmd.Type == CommandDelete && len(cmd.Value) != 0 {
		return nil, fmt.Errorf("kv: DELETE command must not carry a value")
	}

	if cmd.ClientID.IsZero() {
		if cmd.Sequence != 0 {
			return nil, fmt.Errorf("kv: a zero ClientID must not carry a non-zero Sequence")
		}
		return encodeCommandV1(cmd), nil
	}
	if cmd.Sequence == 0 {
		return nil, fmt.Errorf("kv: identified command (ClientID %s) has Sequence 0, which is reserved invalid", cmd.ClientID)
	}
	return encodeCommandV2(cmd), nil
}

func encodeCommandV1(cmd Command) []byte {
	buf := make([]byte, commandV1FixedHeaderSize+len(cmd.Key)+len(cmd.Value))
	buf[0] = commandVersion1
	buf[1] = byte(cmd.Type)
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(cmd.Key)))
	binary.BigEndian.PutUint32(buf[6:10], uint32(len(cmd.Value)))
	off := commandV1FixedHeaderSize
	off += copy(buf[off:], cmd.Key)
	copy(buf[off:], cmd.Value)
	return buf
}

func encodeCommandV2(cmd Command) []byte {
	buf := make([]byte, commandV2FixedHeaderSize+len(cmd.Key)+len(cmd.Value))
	buf[0] = commandVersion2
	buf[1] = byte(cmd.Type)
	off := 2
	off += copy(buf[off:], cmd.ClientID[:])
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(cmd.Sequence))
	off += 8
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(cmd.Key)))
	off += 4
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(cmd.Value)))
	off += 4
	off += copy(buf[off:], cmd.Key)
	copy(buf[off:], cmd.Value)
	return buf
}

// DecodeCommand validates and decodes a command payload of either
// version. Declared key and value lengths are validated against
// MaxKeySize/MaxValueSize before any allocation based on them, so a
// corrupt or hostile payload cannot force an oversized allocation.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 1 {
		return Command{}, fmt.Errorf("%w: too short", ErrMalformedCommand)
	}
	switch b[0] {
	case commandVersion1:
		return decodeCommandV1(b)
	case commandVersion2:
		return decodeCommandV2(b)
	default:
		return Command{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedCommand, b[0])
	}
}

func decodeCommandV1(b []byte) (Command, error) {
	if len(b) < commandV1FixedHeaderSize {
		return Command{}, fmt.Errorf("%w: too short", ErrMalformedCommand)
	}
	typ := CommandType(b[1])
	if typ != CommandPut && typ != CommandDelete {
		return Command{}, fmt.Errorf("%w: unknown operation %d", ErrMalformedCommand, typ)
	}
	keyLen := binary.BigEndian.Uint32(b[2:6])
	valLen := binary.BigEndian.Uint32(b[6:10])
	if keyLen > MaxKeySize {
		return Command{}, fmt.Errorf("%w: key length %d exceeds max %d", ErrMalformedCommand, keyLen, MaxKeySize)
	}
	if valLen > MaxValueSize {
		return Command{}, fmt.Errorf("%w: value length %d exceeds max %d", ErrMalformedCommand, valLen, MaxValueSize)
	}
	if typ == CommandDelete && valLen != 0 {
		return Command{}, fmt.Errorf("%w: DELETE command must not carry a value", ErrMalformedCommand)
	}
	want := commandV1FixedHeaderSize + int(keyLen) + int(valLen)
	if len(b) != want {
		return Command{}, fmt.Errorf("%w: length mismatch (declared %d, got %d bytes)", ErrMalformedCommand, want, len(b))
	}

	key := cloneBytes(b[commandV1FixedHeaderSize : commandV1FixedHeaderSize+int(keyLen)])
	value := cloneBytes(b[commandV1FixedHeaderSize+int(keyLen):])
	return Command{Type: typ, Key: key, Value: value}, nil
}

func decodeCommandV2(b []byte) (Command, error) {
	if len(b) < commandV2FixedHeaderSize {
		return Command{}, fmt.Errorf("%w: too short", ErrMalformedCommand)
	}
	typ := CommandType(b[1])
	if typ != CommandPut && typ != CommandDelete {
		return Command{}, fmt.Errorf("%w: unknown operation %d", ErrMalformedCommand, typ)
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

	if id.IsZero() {
		return Command{}, fmt.Errorf("%w: version 2 command has a zero ClientID, which is reserved invalid", ErrMalformedCommand)
	}
	if seq == 0 {
		return Command{}, fmt.Errorf("%w: version 2 command has Sequence 0, which is reserved invalid", ErrMalformedCommand)
	}
	if keyLen > MaxKeySize {
		return Command{}, fmt.Errorf("%w: key length %d exceeds max %d", ErrMalformedCommand, keyLen, MaxKeySize)
	}
	if valLen > MaxValueSize {
		return Command{}, fmt.Errorf("%w: value length %d exceeds max %d", ErrMalformedCommand, valLen, MaxValueSize)
	}
	if typ == CommandDelete && valLen != 0 {
		return Command{}, fmt.Errorf("%w: DELETE command must not carry a value", ErrMalformedCommand)
	}
	want := commandV2FixedHeaderSize + int(keyLen) + int(valLen)
	if len(b) != want {
		return Command{}, fmt.Errorf("%w: length mismatch (declared %d, got %d bytes)", ErrMalformedCommand, want, len(b))
	}

	key := cloneBytes(b[off : off+int(keyLen)])
	value := cloneBytes(b[off+int(keyLen):])
	return Command{Type: typ, ClientID: id, Sequence: seq, Key: key, Value: value}, nil
}
