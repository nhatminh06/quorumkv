package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// LogEntry is one entry in a node's replicated Raft log: the term it was
// created in, and an opaque application command. The log storage layer
// never interprets Command — that's the state machine's job, once one is
// wired up to committed entries.
type LogEntry struct {
	Term    Term
	Command []byte
}

// Logical indexing follows Raft convention: index 0 is a sentinel meaning
// "no entry" (the position before the first real entry), and real entries
// start at index 1. This makes the empty-log convention already used by
// RequestVote (lastLogIndex=0, lastLogTerm=0) fall out naturally: Term(0)
// is defined to be 0. Log stores entries in a 0-based Go slice internally
// (entries[i] is logical index i+1) and converts at the API boundary.

var logFileMagic = [4]byte{'R', 'L', 'G', '1'}

const logFileVersion = 1

// maxCommandSize bounds a single log entry's command, comfortably below
// both an AppendEntries batch and the transport's 1 MiB frame limit.
const maxCommandSize = 256 * 1024 // 256 KiB

// Per-entry on-disk record: term(8) + commandLength(4) + command + checksum(4).
const (
	logEntryHeaderSize  = 8 + 4
	logChecksumSize     = 4
	logLengthPrefixSize = 4
	maxLogRecordSize    = logEntryHeaderSize + maxCommandSize + logChecksumSize
)

// ErrCorruptLog indicates the log file exists but failed validation. The
// file is always rewritten atomically as a whole (see atomicWriteFile), so
// unlike Milestone 1's append-only WAL there is no legitimate "torn tail"
// case to tolerate here: any structural problem — short read, bad
// checksum, an inconsistent or oversized declared length — means the file
// was corrupted, and is rejected outright rather than silently repaired
// or reset to empty.
var ErrCorruptLog = errors.New("raft: corrupt log")

// Log is a node's persistent Raft log, rewritten atomically on every
// mutation (Append or TruncateAndAppend). Log is not safe for concurrent
// use; Node serializes access to it under its own mutex, the same
// convention used for Store.
type Log struct {
	path    string
	entries []LogEntry
}

// OpenLog loads the log at path. A missing file means a brand-new node's
// empty log. An existing-but-invalid file returns ErrCorruptLog.
func OpenLog(path string) (*Log, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Log{path: path}, nil
		}
		return nil, err
	}
	entries, err := decodeLogFile(data)
	if err != nil {
		return nil, err
	}
	return &Log{path: path, entries: entries}, nil
}

func decodeLogFile(data []byte) ([]LogEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 5 {
		return nil, fmt.Errorf("%w: file too short", ErrCorruptLog)
	}
	if [4]byte(data[0:4]) != logFileMagic {
		return nil, fmt.Errorf("%w: invalid magic", ErrCorruptLog)
	}
	if data[4] != logFileVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrCorruptLog, data[4])
	}

	var entries []LogEntry
	pos := 5
	for pos < len(data) {
		if pos+logLengthPrefixSize > len(data) {
			return nil, fmt.Errorf("%w: truncated record length", ErrCorruptLog)
		}
		recLen := binary.BigEndian.Uint32(data[pos : pos+logLengthPrefixSize])
		pos += logLengthPrefixSize
		if recLen < logEntryHeaderSize+logChecksumSize || recLen > maxLogRecordSize {
			return nil, fmt.Errorf("%w: declared record length %d out of bounds", ErrCorruptLog, recLen)
		}
		if pos+int(recLen) > len(data) {
			return nil, fmt.Errorf("%w: truncated record body", ErrCorruptLog)
		}
		body := data[pos : pos+int(recLen)]
		pos += int(recLen)

		term := Term(binary.BigEndian.Uint64(body[0:8]))
		cmdLen := binary.BigEndian.Uint32(body[8:12])
		wantRecLen := uint32(logEntryHeaderSize) + cmdLen + logChecksumSize
		if wantRecLen != recLen {
			return nil, fmt.Errorf("%w: inconsistent record length", ErrCorruptLog)
		}
		checksumStart := logEntryHeaderSize + int(cmdLen)
		command := body[logEntryHeaderSize:checksumStart]
		wantChecksum := binary.BigEndian.Uint32(body[checksumStart : checksumStart+logChecksumSize])
		gotChecksum := crc32.Checksum(body[:checksumStart], crc32cTable)
		if gotChecksum != wantChecksum {
			return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptLog)
		}

		entries = append(entries, LogEntry{Term: term, Command: cloneBytes(command)})
	}
	return entries, nil
}

func encodeLogFile(entries []LogEntry) ([]byte, error) {
	buf := make([]byte, 0, 5+len(entries)*32)
	buf = append(buf, logFileMagic[:]...)
	buf = append(buf, logFileVersion)

	for _, e := range entries {
		if len(e.Command) > maxCommandSize {
			return nil, fmt.Errorf("raft: command length %d exceeds max %d", len(e.Command), maxCommandSize)
		}
		body := make([]byte, logEntryHeaderSize+len(e.Command)+logChecksumSize)
		binary.BigEndian.PutUint64(body[0:8], uint64(e.Term))
		binary.BigEndian.PutUint32(body[8:12], uint32(len(e.Command)))
		copy(body[logEntryHeaderSize:], e.Command)
		checksumStart := logEntryHeaderSize + len(e.Command)
		checksum := crc32.Checksum(body[:checksumStart], crc32cTable)
		binary.BigEndian.PutUint32(body[checksumStart:], checksum)

		var lenPrefix [logLengthPrefixSize]byte
		binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(body)))
		buf = append(buf, lenPrefix[:]...)
		buf = append(buf, body...)
	}
	return buf, nil
}

func (l *Log) rewrite() error {
	data, err := encodeLogFile(l.entries)
	if err != nil {
		return err
	}
	return atomicWriteFile(l.path, data)
}

// LastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *Log) LastIndex() LogIndex {
	return LogIndex(len(l.entries))
}

// LastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *Log) LastTerm() Term {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// Term returns the term stored at index, or 0 for the sentinel index 0.
// ok is false if index refers to an entry that doesn't exist.
func (l *Log) Term(index LogIndex) (term Term, ok bool) {
	if index == 0 {
		return 0, true
	}
	if index < 1 || int(index) > len(l.entries) {
		return 0, false
	}
	return l.entries[index-1].Term, true
}

// Entry returns the entry at index. ok is false if it doesn't exist.
func (l *Log) Entry(index LogIndex) (LogEntry, bool) {
	if index < 1 || int(index) > len(l.entries) {
		return LogEntry{}, false
	}
	return l.entries[index-1], true
}

// EntriesFrom returns a copy of every entry at index >= from (from < 1 is
// treated as 1). Returns nil if from is past the end of the log.
func (l *Log) EntriesFrom(from LogIndex) []LogEntry {
	if from < 1 {
		from = 1
	}
	if int(from) > len(l.entries) {
		return nil
	}
	return cloneEntries(l.entries[from-1:])
}

func cloneEntries(entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{Term: e.Term, Command: cloneBytes(e.Command)}
	}
	return out
}

// Append adds entries to the tail of the log and persists the result
// before returning. On failure the log is left exactly as it was.
func (l *Log) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	prev := l.entries
	l.entries = append(append([]LogEntry{}, l.entries...), cloneEntries(entries)...)
	if err := l.rewrite(); err != nil {
		l.entries = prev
		return err
	}
	return nil
}

// TruncateAndAppend removes every entry at index >= fromIndex (fromIndex <
// 1 is treated as 1), then appends entries starting at fromIndex,
// persisting the result before returning. This is Raft's conflict-repair
// primitive: passing an empty entries slice performs a pure truncation.
// On failure the log is left exactly as it was.
func (l *Log) TruncateAndAppend(fromIndex LogIndex, entries []LogEntry) error {
	if fromIndex < 1 {
		fromIndex = 1
	}
	prev := l.entries
	var kept []LogEntry
	if int(fromIndex)-1 <= len(l.entries) {
		kept = append([]LogEntry{}, l.entries[:fromIndex-1]...)
	} else {
		kept = append([]LogEntry{}, l.entries...)
	}
	l.entries = append(kept, cloneEntries(entries)...)
	if err := l.rewrite(); err != nil {
		l.entries = prev
		return err
	}
	return nil
}
