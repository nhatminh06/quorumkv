package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"quorumkv/internal/reqid"
)

// Snapshot/Restore define a deterministic serialization of the entire KV
// state machine, used by Raft to compact its committed log prefix (see
// internal/raft). The state machine's internal maps have no defined
// iteration order, so encoding sorts KV keys lexicographically by raw
// bytes, and (since Milestone 9) client dedup records lexicographically
// by ClientID — two calls to Snapshot on machines with identical state,
// built via different insertion orders, always produce byte-identical
// output.
//
// Since Milestone 9, a snapshot also carries the replicated request-
// dedup table (see docs/request-dedup.md): KV contents + dedup metadata
// together form this package's replicated application state. Without
// this, log compaction would silently erase a leader's ability to
// recognize a retried request whose original commit had already been
// compacted away.
//
//   - version 1 (snapshotVersion1): the original Milestone 7 shape, KV
//     entries only. Restore still reads this correctly — an old snapshot
//     simply starts with an empty dedup table.
//   - version 2 (snapshotVersion2): KV entries, then the dedup table.
//     Snapshot always produces this now (even with an empty dedup
//     table), the same way the command codec always encodes the current
//     version going forward while still decoding the old one.
const (
	snapshotVersion1 = 1
	snapshotVersion2 = 2
)

// MaxSnapshotSize bounds the total encoded snapshot payload.
// MaxSnapshotEntries bounds the number of key/value pairs, and (since
// Milestone 9) MaxSnapshotClients separately bounds the number of client
// dedup records — the two grow independently (see docs/request-dedup.md
// on the dedup table's own size being bounded by distinct known
// ClientIDs, not by request volume). All three are checked before
// allocation; MaxSnapshotSize is the primary protection, the entry/client
// counts a simple defensive secondary bound.
const (
	MaxSnapshotSize    = 64 * 1024 * 1024 // 64 MiB
	MaxSnapshotEntries = 4 * 1024 * 1024
	MaxSnapshotClients = 4 * 1024 * 1024
)

// snapshotFixedHeaderSize: version(1) + kvEntryCount(4).
const snapshotFixedHeaderSize = 1 + 4

// snapshotEntryHeaderSize: keyLength(4) + valueLength(4), per KV entry.
const snapshotEntryHeaderSize = 4 + 4

// snapshotClientCountSize: clientCount(4), the header for the section
// following the KV entries in a version-2 snapshot.
const snapshotClientCountSize = 4

// snapshotClientRecordSize: clientID(16) + lastSequence(8) +
// fingerprint(32) + result(1), per client dedup record — a fixed size,
// unlike a KV entry, since none of its fields are variable-length.
const snapshotClientRecordSize = 16 + 8 + 32 + 1

var ErrMalformedSnapshot = errors.New("kv: malformed snapshot")

// Snapshot returns a deterministic encoding of the current state,
// version 2:
//
//	version(1B) | kvEntryCount(4B) | repeated{keyLength(4B) valueLength(4B) key value}
//	  | clientCount(4B) | repeated{clientID(16B) lastSequence(8B) fingerprint(32B) result(1B)}
//
// All integers big-endian, KV entries sorted by key, client records
// sorted by ClientID. The returned bytes do not alias internal state.
func (m *StateMachine) Snapshot() ([]byte, error) {
	keys := make([]string, 0, len(m.state))
	for k := range m.state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > MaxSnapshotEntries {
		return nil, fmt.Errorf("kv: %d entries exceeds max %d", len(keys), MaxSnapshotEntries)
	}

	clientIDs := make([]reqid.ClientID, 0, len(m.clients))
	for id := range m.clients {
		clientIDs = append(clientIDs, id)
	}
	sort.Slice(clientIDs, func(i, j int) bool { return bytes.Compare(clientIDs[i][:], clientIDs[j][:]) < 0 })
	if len(clientIDs) > MaxSnapshotClients {
		return nil, fmt.Errorf("kv: %d client records exceeds max %d", len(clientIDs), MaxSnapshotClients)
	}

	size := snapshotFixedHeaderSize
	for _, k := range keys {
		size += snapshotEntryHeaderSize + len(k) + len(m.state[k])
	}
	size += snapshotClientCountSize + len(clientIDs)*snapshotClientRecordSize
	if size > MaxSnapshotSize {
		return nil, fmt.Errorf("kv: snapshot size %d exceeds max %d", size, MaxSnapshotSize)
	}

	buf := make([]byte, size)
	buf[0] = snapshotVersion2
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

	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(clientIDs)))
	off += snapshotClientCountSize
	for _, id := range clientIDs {
		rec := m.clients[id]
		off += copy(buf[off:], id[:])
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(rec.LastSequence))
		off += 8
		off += copy(buf[off:], rec.LastFingerprint[:])
		buf[off] = byte(rec.LastResult)
		off++
	}
	return buf, nil
}

// Restore replaces the current state (KV contents and, for a version-2
// snapshot, the dedup table) with the state encoded in data. It decodes
// and validates the entire payload into fresh structures first and only
// swaps them in once decoding fully succeeds — a malformed payload never
// partially mutates existing state. Declared lengths/counts are
// validated against their respective bounds before allocation.
func (m *StateMachine) Restore(data []byte) error {
	state, clients, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	m.state = state
	m.clients = clients
	return nil
}

func decodeSnapshot(data []byte) (map[string][]byte, map[reqid.ClientID]ClientRecord, error) {
	if len(data) < 1 {
		return nil, nil, fmt.Errorf("%w: too short", ErrMalformedSnapshot)
	}
	switch data[0] {
	case snapshotVersion1, snapshotVersion2:
	default:
		return nil, nil, fmt.Errorf("%w: unsupported version %d", ErrMalformedSnapshot, data[0])
	}

	if len(data) < snapshotFixedHeaderSize {
		return nil, nil, fmt.Errorf("%w: too short", ErrMalformedSnapshot)
	}
	count := binary.BigEndian.Uint32(data[1:5])
	if count > MaxSnapshotEntries {
		return nil, nil, fmt.Errorf("%w: entry count %d exceeds max %d", ErrMalformedSnapshot, count, MaxSnapshotEntries)
	}

	state := make(map[string][]byte, count)
	off := snapshotFixedHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+snapshotEntryHeaderSize > len(data) {
			return nil, nil, fmt.Errorf("%w: truncated entry header", ErrMalformedSnapshot)
		}
		keyLen := binary.BigEndian.Uint32(data[off : off+4])
		valLen := binary.BigEndian.Uint32(data[off+4 : off+8])
		if keyLen > MaxKeySize {
			return nil, nil, fmt.Errorf("%w: key length %d exceeds max %d", ErrMalformedSnapshot, keyLen, MaxKeySize)
		}
		if valLen > MaxValueSize {
			return nil, nil, fmt.Errorf("%w: value length %d exceeds max %d", ErrMalformedSnapshot, valLen, MaxValueSize)
		}
		off += snapshotEntryHeaderSize
		need := int(keyLen) + int(valLen)
		if need < 0 || off+need > len(data) { // need<0 guards a 32-bit overflow on a 32-bit int platform
			return nil, nil, fmt.Errorf("%w: truncated entry data", ErrMalformedSnapshot)
		}
		key := string(data[off : off+int(keyLen)])
		off += int(keyLen)
		value := cloneBytes(data[off : off+int(valLen)])
		off += int(valLen)
		state[key] = value
	}

	if data[0] == snapshotVersion1 {
		if off != len(data) {
			return nil, nil, fmt.Errorf("%w: trailing bytes", ErrMalformedSnapshot)
		}
		return state, make(map[reqid.ClientID]ClientRecord), nil
	}

	if off+snapshotClientCountSize > len(data) {
		return nil, nil, fmt.Errorf("%w: truncated client count", ErrMalformedSnapshot)
	}
	clientCount := binary.BigEndian.Uint32(data[off : off+4])
	off += snapshotClientCountSize
	if clientCount > MaxSnapshotClients {
		return nil, nil, fmt.Errorf("%w: client count %d exceeds max %d", ErrMalformedSnapshot, clientCount, MaxSnapshotClients)
	}
	clients := make(map[reqid.ClientID]ClientRecord, clientCount)
	for i := uint32(0); i < clientCount; i++ {
		if off+snapshotClientRecordSize > len(data) {
			return nil, nil, fmt.Errorf("%w: truncated client record", ErrMalformedSnapshot)
		}
		var id reqid.ClientID
		copy(id[:], data[off:off+16])
		off += 16
		seq := reqid.Sequence(binary.BigEndian.Uint64(data[off : off+8]))
		off += 8
		var fp reqid.Fingerprint
		copy(fp[:], data[off:off+32])
		off += 32
		result := ApplyStatus(data[off])
		off++
		clients[id] = ClientRecord{LastSequence: seq, LastFingerprint: fp, LastResult: result}
	}
	if off != len(data) {
		return nil, nil, fmt.Errorf("%w: trailing bytes", ErrMalformedSnapshot)
	}
	return state, clients, nil
}
