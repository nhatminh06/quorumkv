package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// PersistentState is the Raft state that must survive a restart:
// currentTerm and votedFor. Role is deliberately not part of this type —
// role is volatile and always starts as Follower on restart.
type PersistentState struct {
	CurrentTerm Term
	// VotedFor is nil when this node has not voted in CurrentTerm.
	VotedFor *NodeID
}

// stateFileMagic identifies bytes on disk as a QuorumKV Raft persistent
// state file.
var stateFileMagic = [4]byte{'R', 'F', 'T', '1'}

const stateFileVersion = 1

// stateFileSize is the exact, fixed size of a persistent state file:
// magic(4) + version(1) + currentTerm(8) + hasVotedFor(1) + votedFor(8) + checksum(4).
const stateFileSize = 4 + 1 + 8 + 1 + 8 + 4

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorruptState indicates a persistent state file exists but failed
// validation (bad magic, unsupported version, wrong size, invalid
// hasVotedFor encoding, or a checksum mismatch). This is distinct from a
// missing file: a missing file is a new node with no prior state, while a
// corrupt file must never be silently treated as term 0/no vote, since
// doing so could let a node violate Raft's vote-once-per-term safety
// property after a partial disk failure.
var ErrCorruptState = errors.New("raft: corrupt persistent state")

// Store persists PersistentState to a single file at path, rewriting it
// atomically on every Save.
//
// Store is not safe for concurrent use; Node serializes access to it.
type Store struct {
	path string
}

func NewStore(path string) *Store {
	return &Store{path: path}
}

// Load reads the persistent state from disk. A missing file is a new
// node and returns the zero state (CurrentTerm 0, VotedFor nil) with no
// error. Any existing-but-invalid file returns ErrCorruptState.
func (s *Store) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return PersistentState{}, nil
		}
		return PersistentState{}, err
	}

	if len(data) != stateFileSize {
		return PersistentState{}, fmt.Errorf("%w: wrong file size %d", ErrCorruptState, len(data))
	}
	if [4]byte(data[0:4]) != stateFileMagic {
		return PersistentState{}, fmt.Errorf("%w: invalid magic", ErrCorruptState)
	}
	if data[4] != stateFileVersion {
		return PersistentState{}, fmt.Errorf("%w: unsupported version %d", ErrCorruptState, data[4])
	}

	checksumStart := stateFileSize - 4
	wantChecksum := binary.BigEndian.Uint32(data[checksumStart:])
	gotChecksum := crc32.Checksum(data[4:checksumStart], crc32cTable)
	if gotChecksum != wantChecksum {
		return PersistentState{}, fmt.Errorf("%w: checksum mismatch", ErrCorruptState)
	}

	term := Term(binary.BigEndian.Uint64(data[5:13]))
	hasVotedFor := data[13]
	if hasVotedFor > 1 {
		return PersistentState{}, fmt.Errorf("%w: invalid hasVotedFor encoding %d", ErrCorruptState, hasVotedFor)
	}
	votedForRaw := NodeID(binary.BigEndian.Uint64(data[14:22]))

	state := PersistentState{CurrentTerm: term}
	if hasVotedFor == 1 {
		v := votedForRaw
		state.VotedFor = &v
	}
	return state, nil
}

// Save atomically replaces the persistent state file with state: it
// writes to a temporary file in the same directory, fsyncs it, renames it
// into place (an atomic replace on POSIX filesystems), then fsyncs the
// containing directory so the rename itself survives a crash. A reader
// therefore always observes either the previous complete state or the
// new complete state, never a partial write.
//
// Save does not return until the write is durable on disk; it is safe to
// treat a successful Save as a completed persist step for Raft's
// persist-before-respond ordering.
func (s *Store) Save(state PersistentState) error {
	data := make([]byte, stateFileSize)
	copy(data[0:4], stateFileMagic[:])
	data[4] = stateFileVersion
	binary.BigEndian.PutUint64(data[5:13], uint64(state.CurrentTerm))
	if state.VotedFor != nil {
		data[13] = 1
		binary.BigEndian.PutUint64(data[14:22], uint64(*state.VotedFor))
	}
	checksumStart := stateFileSize - 4
	checksum := crc32.Checksum(data[4:checksumStart], crc32cTable)
	binary.BigEndian.PutUint32(data[checksumStart:], checksum)

	return atomicWriteFile("stable", s.path, data)
}
