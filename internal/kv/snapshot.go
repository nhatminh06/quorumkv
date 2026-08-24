package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// Snapshot/Restore define a deterministic serialization of the entire KV
// state machine, used by Raft to compact its committed log prefix (see
// internal/raft). The state machine's internal map has no defined
// iteration order, so encoding sorts keys lexicographically by raw bytes
// first — two calls to Snapshot on machines with identical state, built
// via different insertion orders, always produce byte-identical output.

const snapshotVersion = 1

// MaxSnapshotSize bounds the total encoded snapshot payload.
// MaxSnapshotEntries bounds the number of key/value pairs. Both are
// checked before allocation; MaxSnapshotSize is the primary protection,
// MaxSnapshotEntries a simple defensive secondary bound.
const (
	MaxSnapshotSize    = 64 * 1024 * 1024 // 64 MiB
	MaxSnapshotEntries = 4 * 1024 * 1024
)

// snapshotFixedHeaderSize: version(1) + entryCount(4).
const snapshotFixedHeaderSize = 1 + 4

// snapshotEntryHeaderSize: keyLength(4) + valueLength(4), per entry.
const snapshotEntryHeaderSize = 4 + 4

var ErrMalformedSnapshot = errors.New("kv: malformed snapshot")

// Snapshot returns a deterministic encoding of the current state:
//
//	version(1B) | entryCount(4B) | repeated{keyLength(4B) valueLength(4B) key value}
//
// All integers big-endian, entries sorted by key. The returned bytes do
// not alias internal state.
func (m *StateMachine) Snapshot() ([]byte, error) {
	keys := make([]string, 0, len(m.state))
	for k := range m.state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) > MaxSnapshotEntries {
		return nil, fmt.Errorf("kv: %d entries exceeds max %d", len(keys), MaxSnapshotEntries)
	}

	size := snapshotFixedHeaderSize
	for _, k := range keys {
		size += snapshotEntryHeaderSize + len(k) + len(m.state[k])
	}
	if size > MaxSnapshotSize {
		return nil, fmt.Errorf("kv: snapshot size %d exceeds max %d", size, MaxSnapshotSize)
	}

	buf := make([]byte, size)
	buf[0] = snapshotVersion
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(keys)))
	off := snapshotFixedHeaderSize
	for _, k := range keys {
		v := m.state[k]
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(k)))
		binary.BigEndian.PutUint32(buf[off+4:off+8], uint32(len(v)))
		off += snapshotEntryHeaderSize
		off += copy(buf[off:], k)
		off += copy(buf[off:], v)
	}
	return buf, nil
}

// Restore replaces the current state with the state encoded in data. It
// decodes and validates the entire payload into a fresh map first and
// only swaps it in once decoding fully succeeds — a malformed payload
// never partially mutates existing state. Declared lengths are validated
// against MaxKeySize/MaxValueSize/MaxSnapshotEntries before allocation.
func (m *StateMachine) Restore(data []byte) error {
	next, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	m.state = next
	return nil
}

func decodeSnapshot(data []byte) (map[string][]byte, error) {
	if len(data) < snapshotFixedHeaderSize {
		return nil, fmt.Errorf("%w: too short", ErrMalformedSnapshot)
	}
	if data[0] != snapshotVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrMalformedSnapshot, data[0])
	}
	count := binary.BigEndian.Uint32(data[1:5])
	if count > MaxSnapshotEntries {
		return nil, fmt.Errorf("%w: entry count %d exceeds max %d", ErrMalformedSnapshot, count, MaxSnapshotEntries)
	}

	state := make(map[string][]byte, count)
	off := snapshotFixedHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+snapshotEntryHeaderSize > len(data) {
			return nil, fmt.Errorf("%w: truncated entry header", ErrMalformedSnapshot)
		}
		keyLen := binary.BigEndian.Uint32(data[off : off+4])
		valLen := binary.BigEndian.Uint32(data[off+4 : off+8])
		if keyLen > MaxKeySize {
			return nil, fmt.Errorf("%w: key length %d exceeds max %d", ErrMalformedSnapshot, keyLen, MaxKeySize)
		}
		if valLen > MaxValueSize {
			return nil, fmt.Errorf("%w: value length %d exceeds max %d", ErrMalformedSnapshot, valLen, MaxValueSize)
		}
		off += snapshotEntryHeaderSize
		need := int(keyLen) + int(valLen)
		if need < 0 || off+need > len(data) { // need<0 guards a 32-bit overflow on a 32-bit int platform
			return nil, fmt.Errorf("%w: truncated entry data", ErrMalformedSnapshot)
		}
		key := string(data[off : off+int(keyLen)])
		off += int(keyLen)
		value := cloneBytes(data[off : off+int(valLen)])
		off += int(valLen)
		state[key] = value
	}
	if off != len(data) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrMalformedSnapshot)
	}
	return state, nil
}
