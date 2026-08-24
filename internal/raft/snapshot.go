package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// Snapshot is a durable Raft snapshot: the application state machine's
// serialized bytes (opaque to this package — see internal/kv's Snapshot/
// Restore for what they actually mean) plus the Raft boundary it
// represents. lastIncludedIndex/lastIncludedTerm become part of the Raft
// log's logical history even after the physical entries they cover are
// compacted away (see Log's baseIndex/baseTerm).
type Snapshot struct {
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Data              []byte
}

var snapshotFileMagic = [4]byte{'S', 'N', 'P', '1'}

const snapshotFileVersion = 1

// maxSnapshotPayloadSize bounds the application payload this package will
// accept, matching kv.MaxSnapshotSize (raft does not import kv — the
// application snapshot format is opaque here — but the two bounds must
// stay in sync so a legal kv snapshot always fits a legal raft one).
const maxSnapshotPayloadSize = 64 * 1024 * 1024 // 64 MiB

// snapshotFixedHeaderSize: magic(4) + version(1).
const snapshotFixedHeaderSize = 4 + 1

// snapshotMetaSize: lastIncludedIndex(8) + lastIncludedTerm(8) + payloadLength(8).
const snapshotMetaSize = 8 + 8 + 8

const snapshotChecksumSize = 4

// ErrCorruptSnapshot indicates a snapshot file exists but failed
// validation. A missing file is not corruption — it means no snapshot
// has been taken yet.
var ErrCorruptSnapshot = errors.New("raft: corrupt snapshot")

// SnapshotStore persists a single canonical Snapshot to path, rewritten
// atomically on every Save. SnapshotStore is not safe for concurrent use;
// Node serializes access to it, the same convention as Store/Log/
// CommitStore.
type SnapshotStore struct {
	path string
}

func NewSnapshotStore(path string) *SnapshotStore {
	return &SnapshotStore{path: path}
}

// Load reads the canonical snapshot. A missing file returns (nil, nil) —
// "no snapshot yet" (lastIncludedIndex/Term are 0), not an error.
func (s *SnapshotStore) Load() (*Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return decodeSnapshotFile(data)
}

// Save atomically replaces the canonical snapshot file with snap, using
// the same temp-file/fsync/rename/directory-fsync sequence as the rest of
// this package's persistent files. It never publishes a partial file.
//
//	magic(4B "SNP1") | version(1B) | lastIncludedIndex(8B) | lastIncludedTerm(8B) | payloadLength(8B) | payload | checksum(4B CRC32C)
//
// The checksum covers version..payload (not magic, not itself).
func (s *SnapshotStore) Save(snap Snapshot) error {
	if len(snap.Data) > maxSnapshotPayloadSize {
		return fmt.Errorf("raft: snapshot payload %d exceeds max %d", len(snap.Data), maxSnapshotPayloadSize)
	}
	size := snapshotFixedHeaderSize + snapshotMetaSize + len(snap.Data) + snapshotChecksumSize
	buf := make([]byte, size)
	copy(buf[0:4], snapshotFileMagic[:])
	buf[4] = snapshotFileVersion
	off := snapshotFixedHeaderSize
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(snap.LastIncludedIndex))
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(snap.LastIncludedTerm))
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(len(snap.Data)))
	off += 8
	off += copy(buf[off:], snap.Data)

	checksum := crc32.Checksum(buf[4:off], crc32cTable)
	binary.BigEndian.PutUint32(buf[off:off+snapshotChecksumSize], checksum)

	return atomicWriteFile(s.path, buf)
}

func decodeSnapshotFile(data []byte) (*Snapshot, error) {
	if len(data) < snapshotFixedHeaderSize {
		return nil, fmt.Errorf("%w: too short", ErrCorruptSnapshot)
	}
	if [4]byte(data[0:4]) != snapshotFileMagic {
		return nil, fmt.Errorf("%w: invalid magic", ErrCorruptSnapshot)
	}
	if data[4] != snapshotFileVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrCorruptSnapshot, data[4])
	}
	if len(data) < snapshotFixedHeaderSize+snapshotMetaSize {
		return nil, fmt.Errorf("%w: truncated metadata", ErrCorruptSnapshot)
	}

	off := snapshotFixedHeaderSize
	lastIndex := LogIndex(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	lastTerm := Term(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	payloadLen := binary.BigEndian.Uint64(data[off : off+8])
	off += 8
	if payloadLen > maxSnapshotPayloadSize {
		return nil, fmt.Errorf("%w: declared payload length %d exceeds max %d", ErrCorruptSnapshot, payloadLen, maxSnapshotPayloadSize)
	}
	if uint64(len(data)-off) < payloadLen {
		return nil, fmt.Errorf("%w: truncated payload", ErrCorruptSnapshot)
	}
	payloadEnd := off + int(payloadLen)
	payload := data[off:payloadEnd]

	checksumStart := payloadEnd
	if len(data) < checksumStart+snapshotChecksumSize {
		return nil, fmt.Errorf("%w: truncated checksum", ErrCorruptSnapshot)
	}
	if len(data) != checksumStart+snapshotChecksumSize {
		return nil, fmt.Errorf("%w: trailing bytes", ErrCorruptSnapshot)
	}
	wantChecksum := binary.BigEndian.Uint32(data[checksumStart : checksumStart+snapshotChecksumSize])
	gotChecksum := crc32.Checksum(data[4:checksumStart], crc32cTable)
	if gotChecksum != wantChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptSnapshot)
	}

	return &Snapshot{
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              cloneBytes(payload),
	}, nil
}
