package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// EncodeCommand/DecodeCommand define the deterministic wire format used to
// carry a Command inside an opaque Raft log entry. This package has no
// knowledge of Raft; it only guarantees that encoding is deterministic and
// bounded so the result always fits in one legal Raft log entry.
//
// GET is never encoded here — it is a read, not a replicated command.

const commandVersion = 1

// MaxKeySize/MaxValueSize bound a single command, chosen to comfortably
// fit within Raft's per-entry command limit (see internal/raft's
// maxCommandSize) once this format's fixed overhead is included.
const (
	MaxKeySize   = 64 * 1024
	MaxValueSize = 200 * 1024
)

// commandFixedHeaderSize: version(1) + operation(1) + keyLength(4) +
// valueLength(4).
const commandFixedHeaderSize = 1 + 1 + 4 + 4

// ErrMalformedCommand indicates a command payload failed validation on
// decode (wrong version, unknown operation, an oversized/inconsistent
// declared length, or a DELETE carrying a value).
var ErrMalformedCommand = errors.New("kv: malformed command")

// EncodeCommand produces the exact wire bytes for cmd:
//
//	version(1B) | operation(1B) | keyLength(4B) | valueLength(4B) | key | value
//
// All integers big-endian. A DELETE command must not carry a value.
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

	buf := make([]byte, commandFixedHeaderSize+len(cmd.Key)+len(cmd.Value))
	buf[0] = commandVersion
	buf[1] = byte(cmd.Type)
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(cmd.Key)))
	binary.BigEndian.PutUint32(buf[6:10], uint32(len(cmd.Value)))
	off := commandFixedHeaderSize
	off += copy(buf[off:], cmd.Key)
	copy(buf[off:], cmd.Value)
	return buf, nil
}

// DecodeCommand validates and decodes a command payload. Declared key and
// value lengths are validated against MaxKeySize/MaxValueSize before any
// allocation based on them, so a corrupt or hostile payload cannot force
// an oversized allocation.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < commandFixedHeaderSize {
		return Command{}, fmt.Errorf("%w: too short", ErrMalformedCommand)
	}
	if b[0] != commandVersion {
		return Command{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedCommand, b[0])
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
	want := commandFixedHeaderSize + int(keyLen) + int(valLen)
	if len(b) != want {
		return Command{}, fmt.Errorf("%w: length mismatch (declared %d, got %d bytes)", ErrMalformedCommand, want, len(b))
	}

	key := cloneBytes(b[commandFixedHeaderSize : commandFixedHeaderSize+int(keyLen)])
	value := cloneBytes(b[commandFixedHeaderSize+int(keyLen):])
	return Command{Type: typ, Key: key, Value: value}, nil
}
