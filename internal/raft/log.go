package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"quorumkv/internal/observability"
)

// EntryKind distinguishes what a LogEntry actually is, since Milestone 10
// introduced a third kind (configuration) and it is no longer safe to
// infer meaning purely from Command's byte content. EntryApplication is
// deliberately the zero value: every LogEntry{...} literal anywhere in
// this codebase (an enormous, pre-existing test surface) that never
// mentions Kind continues to mean exactly what it always meant — an
// ordinary application command — with no mechanical rewrite required.
type EntryKind uint8

const (
	// EntryApplication is an ordinary opaque application command, handed
	// to ApplyFunc once committed. The zero value — see the type doc.
	EntryApplication EntryKind = iota
	// EntryNoop is Raft's internal current-term commit barrier (see
	// docs/read-index.md): committed and advances lastApplied, but never
	// reaches ApplyFunc. Command is conventionally empty for a
	// newly-written EntryNoop, but Kind — not Command's length — is what
	// decides this for any entry written under the current (version 3)
	// log format; only legacy-format decoding still infers Noop from an
	// empty Command (see decodeLogFile).
	EntryNoop
	// EntryConfiguration carries a deterministically encoded Membership
	// (see membership_codec.go) describing a joint or stable
	// configuration change (docs/membership.md). Never reaches
	// ApplyFunc; advances lastApplied and updates Node's effective
	// membership once committed.
	EntryConfiguration
)

func (k EntryKind) String() string {
	switch k {
	case EntryApplication:
		return "Application"
	case EntryNoop:
		return "Noop"
	case EntryConfiguration:
		return "Configuration"
	default:
		return "Unknown"
	}
}

// LogEntry is one entry in a node's replicated Raft log: the term it was
// created in, its kind, and an opaque payload. The log storage layer
// never interprets Command — that's the application/Node layer's job,
// once committed entries are applied.
type LogEntry struct {
	Term    Term
	Kind    EntryKind
	Command []byte
}

// Logical indexing follows Raft convention: index 0 is a sentinel meaning
// "no entry" (the position before the first real entry), and real entries
// start at index 1. This makes the empty-log convention already used by
// RequestVote (lastLogIndex=0, lastLogTerm=0) fall out naturally.
//
// Since Milestone 7, a Log may be compacted by a snapshot: baseIndex/
// baseTerm ("the log's own boundary, before any suffix truncation") is
// the (lastIncludedIndex, lastIncludedTerm) of the most recent
// compaction — 0/0 if the log has never been compacted, which folds back
// into the original index-0 sentinel case exactly. Log stores physical
// entries 0-based internally; entries[i] is logical index baseIndex+i+1.
// The command bytes for index baseIndex itself are never retained after
// compaction — only its index/term remain, which is all Raft needs.

var logFileMagic = [4]byte{'R', 'L', 'G', '1'}

// logFileVersion1 is the pre-Milestone-7 format: no base index/term
// fields, always equivalent to baseIndex=0, baseTerm=0. logFileVersion2
// (Milestone 7) added those fields but has no per-entry Kind byte —
// every entry decodes as EntryNoop if Command is empty, EntryApplication
// otherwise (the only two kinds that existed before Milestone 10).
// logFileVersion3 (Milestone 10) adds an explicit per-entry Kind byte,
// needed once EntryConfiguration exists (a configuration entry's payload
// can't be told apart from an application command by content alone).
// All three versions remain readable so existing repositories load
// correctly; the first subsequent mutation migrates it to segmented
// storage.
const (
	logFileVersion1 = 1
	logFileVersion2 = 2
	logFileVersion3 = 3
)

const currentLogFileVersion = logFileVersion3

// maxCommandSize bounds a single log entry's command, comfortably below
// both an AppendEntries batch and the transport's 1 MiB frame limit.
const maxCommandSize = 256 * 1024 // 256 KiB

// Per-entry on-disk record (version 3): term(8) + kind(1) +
// commandLength(4) + command + checksum(4). Versions 1/2 have no kind
// byte — see decodeLogFile.
const (
	logEntryHeaderSizeV2 = 8 + 4     // term + commandLength (versions 1/2)
	logEntryHeaderSizeV3 = 8 + 1 + 4 // term + kind + commandLength (version 3+)
	logChecksumSize      = 4
	logLengthPrefixSize  = 4
	maxLogRecordSize     = logEntryHeaderSizeV3 + maxCommandSize + logChecksumSize

	logV1HeaderSize = 4 + 1         // magic + version
	logV2HeaderSize = 4 + 1 + 8 + 8 // magic + version + baseIndex + baseTerm
	logV3HeaderSize = logV2HeaderSize
)

// ErrCorruptLog indicates that persistent log data failed validation.
// Segmented storage recovers an incomplete record only at the active
// segment tail. Invalid complete records, sealed-segment truncation, gaps,
// overlaps, and invalid metadata remain hard errors.
var ErrCorruptLog = errors.New("raft: corrupt log")

// Log is a node's persistent segmented Raft log. Ordinary appends extend
// the active segment; truncation and compaction atomically publish a new
// generation that reuses unaffected segments. Log is not safe for
// concurrent use; Node serializes access under its own mutex.
type Log struct {
	path      string
	baseIndex LogIndex
	baseTerm  Term
	entries   []LogEntry
	// segmentBackings owns the immutable raw bytes read for retained
	// segmented generations. Entries loaded from those segments may point
	// into these buffers; mutation paths materialize owned commands before
	// replacing or rewriting the logical log.
	segmentBackings [][]byte
	segmented       bool
	generation      uint64
	activeSegment   string
	activeSize      int64
	segmentFiles    int
	appendFsync     time.Duration
	appendBytes     int
	rewriteBytes    int
	observer        atomic.Pointer[observability.Metrics]
	logger          atomic.Pointer[slog.Logger]
	recoveredTail   bool
}

func (l *Log) setObserver(m *observability.Metrics) { l.observer.Store(m) }
func (l *Log) setLogger(logger *slog.Logger) {
	l.logger.Store(logger)
	if logger != nil && l.recoveredTail {
		logger.Info("raft_log_recovered_torn_tail", "event", "raft_log_recovered_torn_tail")
		l.recoveredTail = false
	}
}
func (l *Log) logInfo(event string, args ...any) {
	if logger := l.logger.Load(); logger != nil {
		logger.Info(event, append([]any{"event", event}, args...)...)
	}
}

// OpenLog loads the log at path. A missing file means a brand-new node's
// empty log (baseIndex 0, baseTerm 0, no entries). An existing-but-invalid
// file returns ErrCorruptLog.
func OpenLog(path string) (*Log, error) {
	if log, found, err := openSegmentedLog(path); found {
		return log, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Log{path: path}, nil
		}
		return nil, err
	}
	baseIndex, baseTerm, entries, err := decodeLogFile(data)
	if err != nil {
		return nil, err
	}
	return &Log{path: path, baseIndex: baseIndex, baseTerm: baseTerm, entries: entries}, nil
}

func decodeLogFile(data []byte) (baseIndex LogIndex, baseTerm Term, entries []LogEntry, err error) {
	if len(data) == 0 {
		return 0, 0, nil, nil
	}
	if len(data) < 5 {
		return 0, 0, nil, fmt.Errorf("%w: file too short", ErrCorruptLog)
	}
	if [4]byte(data[0:4]) != logFileMagic {
		return 0, 0, nil, fmt.Errorf("%w: invalid magic", ErrCorruptLog)
	}

	var pos int
	legacy := false // versions 1/2: no per-entry Kind byte on disk
	switch data[4] {
	case logFileVersion1:
		pos = logV1HeaderSize
		legacy = true
	case logFileVersion2:
		if len(data) < logV2HeaderSize {
			return 0, 0, nil, fmt.Errorf("%w: truncated header", ErrCorruptLog)
		}
		baseIndex = LogIndex(binary.BigEndian.Uint64(data[5:13]))
		baseTerm = Term(binary.BigEndian.Uint64(data[13:21]))
		pos = logV2HeaderSize
		legacy = true
	case logFileVersion3:
		if len(data) < logV3HeaderSize {
			return 0, 0, nil, fmt.Errorf("%w: truncated header", ErrCorruptLog)
		}
		baseIndex = LogIndex(binary.BigEndian.Uint64(data[5:13]))
		baseTerm = Term(binary.BigEndian.Uint64(data[13:21]))
		pos = logV3HeaderSize
	default:
		return 0, 0, nil, fmt.Errorf("%w: unsupported version %d", ErrCorruptLog, data[4])
	}

	entryHeaderSize := logEntryHeaderSizeV3
	if legacy {
		entryHeaderSize = logEntryHeaderSizeV2
	}

	for pos < len(data) {
		if pos+logLengthPrefixSize > len(data) {
			return 0, 0, nil, fmt.Errorf("%w: truncated record length", ErrCorruptLog)
		}
		recLen := binary.BigEndian.Uint32(data[pos : pos+logLengthPrefixSize])
		pos += logLengthPrefixSize
		if recLen < uint32(entryHeaderSize)+logChecksumSize || recLen > maxLogRecordSize {
			return 0, 0, nil, fmt.Errorf("%w: declared record length %d out of bounds", ErrCorruptLog, recLen)
		}
		if pos+int(recLen) > len(data) {
			return 0, 0, nil, fmt.Errorf("%w: truncated record body", ErrCorruptLog)
		}
		body := data[pos : pos+int(recLen)]
		pos += int(recLen)

		term := Term(binary.BigEndian.Uint64(body[0:8]))
		var kind EntryKind
		var cmdLen uint32
		if legacy {
			cmdLen = binary.BigEndian.Uint32(body[8:12])
		} else {
			kind = EntryKind(body[8])
			cmdLen = binary.BigEndian.Uint32(body[9:13])
		}
		wantRecLen := uint32(entryHeaderSize) + cmdLen + logChecksumSize
		if wantRecLen != recLen {
			return 0, 0, nil, fmt.Errorf("%w: inconsistent record length", ErrCorruptLog)
		}
		checksumStart := entryHeaderSize + int(cmdLen)
		command := body[entryHeaderSize:checksumStart]
		wantChecksum := binary.BigEndian.Uint32(body[checksumStart : checksumStart+logChecksumSize])
		gotChecksum := crc32.Checksum(body[:checksumStart], crc32cTable)
		if gotChecksum != wantChecksum {
			return 0, 0, nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptLog)
		}

		if legacy {
			// Versions 1/2 predate EntryKind entirely: the only two
			// kinds that existed then were an ordinary application
			// command and Milestone 8's reserved-empty-command no-op —
			// exactly the same rule apply.go used to use directly.
			if cmdLen == 0 {
				kind = EntryNoop
			} else {
				kind = EntryApplication
			}
		} else if kind != EntryApplication && kind != EntryNoop && kind != EntryConfiguration {
			return 0, 0, nil, fmt.Errorf("%w: unknown entry kind %d", ErrCorruptLog, kind)
		}

		entries = append(entries, LogEntry{Term: term, Kind: kind, Command: cloneBytes(command)})
	}
	return baseIndex, baseTerm, entries, nil
}

func encodeLogFile(baseIndex LogIndex, baseTerm Term, entries []LogEntry) ([]byte, error) {
	buf := make([]byte, 0, logV2HeaderSize+len(entries)*32)
	buf = append(buf, logFileMagic[:]...)
	buf = append(buf, currentLogFileVersion)
	var idxBuf, termBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], uint64(baseIndex))
	binary.BigEndian.PutUint64(termBuf[:], uint64(baseTerm))
	buf = append(buf, idxBuf[:]...)
	buf = append(buf, termBuf[:]...)

	for _, e := range entries {
		if len(e.Command) > maxCommandSize {
			return nil, fmt.Errorf("raft: command length %d exceeds max %d", len(e.Command), maxCommandSize)
		}
		body := make([]byte, logEntryHeaderSizeV3+len(e.Command)+logChecksumSize)
		binary.BigEndian.PutUint64(body[0:8], uint64(e.Term))
		body[8] = byte(e.Kind)
		binary.BigEndian.PutUint32(body[9:13], uint32(len(e.Command)))
		copy(body[logEntryHeaderSizeV3:], e.Command)
		checksumStart := logEntryHeaderSizeV3 + len(e.Command)
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
	start := time.Now()
	err := l.rewriteSegmented()
	if m := l.observer.Load(); m != nil {
		m.RecordPersistence("log", l.rewriteBytes, time.Since(start), 0)
	}
	return err
}

// BaseIndex returns the index of this log's compaction boundary
// (lastIncludedIndex of the most recent snapshot), or 0 if the log has
// never been compacted.
func (l *Log) BaseIndex() LogIndex { return l.baseIndex }

// BaseTerm returns the term of BaseIndex.
func (l *Log) BaseTerm() Term { return l.baseTerm }

// LastIndex returns the index of the last entry, or BaseIndex() if there
// is no retained suffix.
func (l *Log) LastIndex() LogIndex {
	return l.baseIndex + LogIndex(len(l.entries))
}

// LastTerm returns the term of the last entry, or BaseTerm() if there is
// no retained suffix.
func (l *Log) LastTerm() Term {
	if len(l.entries) == 0 {
		return l.baseTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// Term returns the term stored at index. ok is false if index is outside
// what this log can answer for: before the compaction boundary (its
// history was discarded — "compacted/unavailable", not fabricated as 0),
// or past the last retained entry. Term(BaseIndex()) always succeeds,
// returning BaseTerm(), even though no physical entry backs it anymore.
func (l *Log) Term(index LogIndex) (term Term, ok bool) {
	if index == l.baseIndex {
		return l.baseTerm, true
	}
	if index < l.baseIndex || index > l.LastIndex() {
		return 0, false
	}
	return l.entries[index-l.baseIndex-1].Term, true
}

// Entry returns a defensive copy of the entry at index. ok is false if it
// doesn't exist or index is at or before the compaction boundary — the
// command bytes at BaseIndex() are not retained after compaction; only its
// index/term are (via Term).
func (l *Log) Entry(index LogIndex) (LogEntry, bool) {
	if index <= l.baseIndex || index > l.LastIndex() {
		return LogEntry{}, false
	}
	return cloneEntry(l.entries[index-l.baseIndex-1]), true
}

// entryView returns a borrowed entry for internal callers that already hold
// the log's synchronization. Its Command must not be mutated or retained
// past that synchronization boundary.
func (l *Log) entryView(index LogIndex) (LogEntry, bool) {
	if index <= l.baseIndex || index > l.LastIndex() {
		return LogEntry{}, false
	}
	return l.entries[index-l.baseIndex-1], true
}

// EntriesFrom returns a copy of every entry at index >= from (from at or
// before the compaction boundary is treated as BaseIndex()+1). Returns
// nil if from is past the end of the log.
func (l *Log) EntriesFrom(from LogIndex) []LogEntry {
	if from <= l.baseIndex {
		from = l.baseIndex + 1
	}
	physIdx := from - l.baseIndex - 1
	if physIdx < 0 || int(physIdx) > len(l.entries) {
		return nil
	}
	return cloneEntries(l.entries[physIdx:])
}

// EntriesRange returns up to maxEntries entries starting at from (the
// same snapshot-boundary clamping as EntriesFrom: from at or before the
// compaction boundary is treated as BaseIndex()+1), stopping before
// including an entry that would push the running encoded-wire-size total
// (see encodedEntrySize) beyond maxEncodedBytes. The first entry is
// always included regardless of its own size — a single entry larger
// than maxEncodedBytes is still returned alone rather than becoming
// unsendable, matching the caller's normal-batch-target-vs-single-large-
// entry distinction (see MaxAppendEntriesBytes). Returns nil if from is
// past the end of the log or maxEntries <= 0.
//
// Unlike EntriesFrom, this never copies more of the retained log than
// the returned result actually needs — EntriesFrom, unbounded, would
// clone an entire multi-thousand-entry retained suffix just to form one
// small replication batch.
func (l *Log) EntriesRange(from LogIndex, maxEntries int, maxEncodedBytes int) []LogEntry {
	view := l.entriesRangeView(from, maxEntries, maxEncodedBytes)
	if len(view) == 0 {
		return nil
	}
	return cloneEntries(view)
}

// entriesRangeView borrows Log-owned entries for immediate synchronous encoding.
// The caller must hold the enclosing Node.mu (or provide exclusive access),
// must not mutate entries or commands, and must not retain the view beyond that
// synchronization boundary. Only an independently encoded payload may escape.
func (l *Log) entriesRangeView(from LogIndex, maxEntries int, maxEncodedBytes int) []LogEntry {
	if from <= l.baseIndex {
		from = l.baseIndex + 1
	}
	physIdx := from - l.baseIndex - 1
	if physIdx < 0 || int(physIdx) > len(l.entries) {
		return nil
	}
	avail := l.entries[physIdx:]
	if maxEntries <= 0 || len(avail) == 0 {
		return nil
	}
	if maxEntries > len(avail) {
		maxEntries = len(avail)
	}
	count := 1
	total := encodedEntrySize(avail[0])
	for count < maxEntries {
		next := encodedEntrySize(avail[count])
		if total+next > maxEncodedBytes {
			break
		}
		total += next
		count++
	}
	return avail[:count:count]
}

func cloneEntries(entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = cloneEntry(e)
	}
	return out
}

func cloneEntry(e LogEntry) LogEntry {
	return LogEntry{Term: e.Term, Kind: e.Kind, Command: cloneBytes(e.Command)}
}

func equalLogEntries(a, b []LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || a[i].Kind != b[i].Kind || !bytes.Equal(a[i].Command, b[i].Command) {
			return false
		}
	}
	return true
}

// Append adds entries to the tail of the log and persists the result
// before returning. On failure the log is left exactly as it was.
func (l *Log) Append(entries []LogEntry) error {
	return l.append(entries, false)
}

// appendOwned persists entries and takes ownership of their Command slices on
// success. Callers must not mutate them after the call. On failure ownership
// remains with the caller.
func (l *Log) appendOwned(entries []LogEntry) error {
	return l.append(entries, true)
}

func (l *Log) append(entries []LogEntry, owned bool) error {
	if len(entries) == 0 {
		return nil
	}
	migrated := !l.segmented
	if migrated {
		if err := l.rewriteSegmented(); err != nil {
			return err
		}
		l.logInfo("raft_log_migrated")
	}
	previousSegment := l.activeSegment
	start := time.Now()
	if err := l.appendSegmented(entries, owned); err != nil {
		return err
	}
	if m := l.observer.Load(); m != nil {
		var bytes int
		for _, entry := range entries {
			bytes += logLengthPrefixSize + logEntryHeaderSizeV3 + len(entry.Command) + logChecksumSize
		}
		m.RecordPersistence("log", bytes, time.Since(start), l.appendFsync)
		m.RecordRaftLogWrite(bytes, l.appendBytes)
		if previousSegment != l.activeSegment {
			m.RaftLogRotated()
		}
		m.SetRaftLogSegments(int64(l.segmentCount()))
	}
	if previousSegment != l.activeSegment {
		l.logInfo("raft_log_segment_rotated", "segments", l.segmentCount())
	}
	return nil
}

// TruncateAndAppend removes every entry at index >= fromIndex (fromIndex
// at or before the compaction boundary is treated as BaseIndex()+1),
// then appends entries starting at fromIndex, persisting the result
// before returning. This is Raft's conflict-repair primitive: passing an
// empty entries slice performs a pure truncation. On failure the log is
// left exactly as it was.
func (l *Log) TruncateAndAppend(fromIndex LogIndex, entries []LogEntry) error {
	if fromIndex <= l.baseIndex {
		fromIndex = l.baseIndex + 1
	}
	if fromIndex == l.LastIndex()+1 {
		return l.Append(entries)
	}
	prev := l.entries
	physIdx := int(fromIndex - l.baseIndex - 1)
	var kept []LogEntry
	if physIdx <= len(l.entries) {
		kept = append([]LogEntry{}, l.entries[:physIdx]...)
	} else {
		kept = append([]LogEntry{}, l.entries...)
	}
	l.entries = append(kept, cloneEntries(entries)...)
	if err := l.rewrite(); err != nil {
		l.entries = prev
		return err
	}
	if m := l.observer.Load(); m != nil {
		m.RaftLogTruncated()
		m.SetRaftLogSegments(int64(l.segmentCount()))
	}
	l.logInfo("raft_log_truncated", "from_index", uint64(fromIndex), "last_index", uint64(l.LastIndex()))
	return nil
}

// Compact discards physical entries with index <= newBaseIndex, replacing
// them with the snapshot boundary (newBaseIndex, newBaseTerm), and
// persists the result. It is the caller's responsibility to have already
// durably persisted a snapshot covering newBaseIndex before calling
// Compact — this method only removes physical log records; it neither
// creates nor validates a snapshot itself.
//
// Compact is a no-op if newBaseIndex <= the log's current base (it never
// regresses the boundary or re-does redundant compaction) and fails if
// newBaseIndex exceeds LastIndex() (an index that does not exist yet
// cannot be compacted through).
func (l *Log) Compact(newBaseIndex LogIndex, newBaseTerm Term) error {
	if newBaseIndex <= l.baseIndex {
		return nil
	}
	if newBaseIndex > l.LastIndex() {
		return fmt.Errorf("raft: cannot compact through index %d beyond last index %d", newBaseIndex, l.LastIndex())
	}
	prevBaseIndex, prevBaseTerm, prevEntries := l.baseIndex, l.baseTerm, l.entries
	keepFrom := int(newBaseIndex - l.baseIndex)
	l.baseIndex = newBaseIndex
	l.baseTerm = newBaseTerm
	l.entries = append([]LogEntry{}, l.entries[keepFrom:]...)
	if err := l.rewrite(); err != nil {
		l.baseIndex, l.baseTerm, l.entries = prevBaseIndex, prevBaseTerm, prevEntries
		return err
	}
	if m := l.observer.Load(); m != nil {
		m.RaftLogTruncated()
		m.SetRaftLogSegments(int64(l.segmentCount()))
	}
	l.logInfo("raft_log_compacted", "base_index", uint64(newBaseIndex), "segments", l.segmentCount())
	return nil
}

// InstallSnapshotBoundary resets the log's compaction boundary to
// (newBaseIndex, newBaseTerm), used when installing a snapshot received
// from a leader rather than compacting the node's own already-consistent
// history. Unlike Compact — which only ever shrinks a prefix the log
// already agrees with — this may need to discard the log's ENTIRE
// retained suffix: if the local log doesn't reach newBaseIndex, or the
// entry it has there has a different term, local history cannot be
// trusted to lead into this snapshot and is discarded wholesale. If the
// local log already has a matching entry at newBaseIndex, only the
// prefix through it is discarded and the verified-consistent suffix
// beyond it is retained (exactly like Compact). Never regresses an
// already-equal-or-later boundary.
func (l *Log) InstallSnapshotBoundary(newBaseIndex LogIndex, newBaseTerm Term) error {
	if newBaseIndex <= l.baseIndex {
		return nil
	}
	localTerm, matches := l.Term(newBaseIndex)
	prevBaseIndex, prevBaseTerm, prevEntries := l.baseIndex, l.baseTerm, l.entries
	if matches && localTerm == newBaseTerm {
		keepFrom := int(newBaseIndex - l.baseIndex)
		l.entries = append([]LogEntry{}, l.entries[keepFrom:]...)
	} else {
		l.entries = nil
	}
	l.baseIndex = newBaseIndex
	l.baseTerm = newBaseTerm
	if err := l.rewrite(); err != nil {
		l.baseIndex, l.baseTerm, l.entries = prevBaseIndex, prevBaseTerm, prevEntries
		return err
	}
	return nil
}
