package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// CommitStore persists commitIndex separately from currentTerm/votedFor
// (Store) and the Raft log itself. Before Milestone 5, commitIndex was
// purely volatile — acceptable while nothing replayed committed entries
// into an application. Now restart must know which durable log entries
// are safe to replay, so commitIndex needs its own durable record.
//
// CommitStore is not safe for concurrent use; Node serializes access to
// it under its own mutex, the same convention as Store and Log.
type CommitStore struct {
	path string
}

func NewCommitStore(path string) *CommitStore {
	return &CommitStore{path: path}
}

var commitMetaMagic = [4]byte{'C', 'M', 'T', '1'}

const commitMetaVersion = 1

// commitMetaFileSize: magic(4) + version(1) + commitIndex(8) + checksum(4).
const commitMetaFileSize = 4 + 1 + 8 + 4

// ErrCorruptCommitMeta indicates a commit-metadata file exists but failed
// validation (bad magic, unsupported version, wrong size, or a checksum
// mismatch). A missing file is a fresh node (commitIndex 0), which is not
// the same as a corrupt one — corruption is never silently treated as 0,
// since that could make a node replay less of its committed history than
// it durably recorded, or (worse, for a future leader) mis-evaluate what
// is safely committed.
var ErrCorruptCommitMeta = errors.New("raft: corrupt commit metadata")

// Load reads the persisted commitIndex. A missing file means a fresh node
// and returns 0 with no error.
func (s *CommitStore) Load() (LogIndex, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) != commitMetaFileSize {
		return 0, fmt.Errorf("%w: wrong file size %d", ErrCorruptCommitMeta, len(data))
	}
	if [4]byte(data[0:4]) != commitMetaMagic {
		return 0, fmt.Errorf("%w: invalid magic", ErrCorruptCommitMeta)
	}
	if data[4] != commitMetaVersion {
		return 0, fmt.Errorf("%w: unsupported version %d", ErrCorruptCommitMeta, data[4])
	}
	checksumStart := commitMetaFileSize - 4
	wantChecksum := binary.BigEndian.Uint32(data[checksumStart:])
	gotChecksum := crc32.Checksum(data[4:checksumStart], crc32cTable)
	if gotChecksum != wantChecksum {
		return 0, fmt.Errorf("%w: checksum mismatch", ErrCorruptCommitMeta)
	}
	return LogIndex(binary.BigEndian.Uint64(data[5:13])), nil
}

// Save atomically replaces the commit-metadata file with index, using the
// same temp-file/fsync/rename/directory-fsync sequence as Store and Log.
func (s *CommitStore) Save(index LogIndex) error {
	data := make([]byte, commitMetaFileSize)
	copy(data[0:4], commitMetaMagic[:])
	data[4] = commitMetaVersion
	binary.BigEndian.PutUint64(data[5:13], uint64(index))
	checksumStart := commitMetaFileSize - 4
	checksum := crc32.Checksum(data[4:checksumStart], crc32cTable)
	binary.BigEndian.PutUint32(data[checksumStart:], checksum)
	return atomicWriteFile(s.path, data)
}
