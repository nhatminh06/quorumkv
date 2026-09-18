package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const raftLogSegmentBytes = 4 << 20

var segmentMagic = [4]byte{'R', 'S', 'G', '1'}
var manifestMagic = [4]byte{'R', 'L', 'M', '1'}

const segmentHeaderSize = 4 + 1 + 8
const manifestSize = 4 + 1 + 8 + 8 + 8 + 4
const segmentBatchCommitKind EntryKind = 0xff

func entryRecordSize(e LogEntry) int {
	return logLengthPrefixSize + logEntryHeaderSizeV3 + len(e.Command) + logChecksumSize
}

// appendEntryRecord appends the exact version-1 segment record for e directly
// to dst. It does not retain dst or alias e.Command.
func appendEntryRecord(dst []byte, e LogEntry) ([]byte, error) {
	if len(e.Command) > maxCommandSize {
		return nil, fmt.Errorf("raft: command length %d exceeds max %d", len(e.Command), maxCommandSize)
	}
	start := len(dst)
	recordSize := entryRecordSize(e)
	dst = append(dst, make([]byte, recordSize)...)
	bodyStart := start + logLengthPrefixSize
	bodyEnd := bodyStart + logEntryHeaderSizeV3 + len(e.Command)
	binary.BigEndian.PutUint32(dst[start:bodyStart], uint32(recordSize-logLengthPrefixSize))
	binary.BigEndian.PutUint64(dst[bodyStart:bodyStart+8], uint64(e.Term))
	dst[bodyStart+8] = byte(e.Kind)
	binary.BigEndian.PutUint32(dst[bodyStart+9:bodyStart+13], uint32(len(e.Command)))
	copy(dst[bodyStart+logEntryHeaderSizeV3:bodyEnd], e.Command)
	binary.BigEndian.PutUint32(dst[bodyEnd:], crc32.Checksum(dst[bodyStart:bodyEnd], crc32cTable))
	return dst, nil
}

func encodeEntryRecord(e LogEntry) ([]byte, error) {
	return appendEntryRecord(make([]byte, 0, entryRecordSize(e)), e)
}

func encodeBatchCommit(count int) ([]byte, error) {
	var command [4]byte
	binary.BigEndian.PutUint32(command[:], uint32(count))
	return encodeEntryRecord(LogEntry{Kind: segmentBatchCommitKind, Command: command[:]})
}

func appendBatchCommit(dst []byte, count int) ([]byte, error) {
	var command [4]byte
	binary.BigEndian.PutUint32(command[:], uint32(count))
	return appendEntryRecord(dst, LogEntry{Kind: segmentBatchCommitKind, Command: command[:]})
}

// decodeSegmentRecords validates a segment and returns entries whose command
// slices borrow data. The caller must retain data unchanged for as long as
// those entries remain live.
func decodeSegmentRecords(data []byte, allowTornTail bool) ([]LogEntry, int, error) {
	var entries []LogEntry
	var pending []LogEntry
	pendingStart := 0
	pos := 0
	for pos < len(data) {
		recordStart := pos
		if pos+4 > len(data) {
			if allowTornTail {
				if len(pending) != 0 {
					return entries, pendingStart, nil
				}
				return entries, recordStart, nil
			}
			return nil, 0, fmt.Errorf("%w: truncated record length", ErrCorruptLog)
		}
		recLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if recLen < logEntryHeaderSizeV3+logChecksumSize || recLen > maxLogRecordSize {
			return nil, 0, fmt.Errorf("%w: segment record length %d out of bounds", ErrCorruptLog, recLen)
		}
		if pos+recLen > len(data) {
			if allowTornTail {
				if len(pending) != 0 {
					return entries, pendingStart, nil
				}
				return entries, recordStart, nil
			}
			return nil, 0, fmt.Errorf("%w: truncated record body", ErrCorruptLog)
		}
		body := data[pos : pos+recLen]
		pos += recLen
		cmdLen := int(binary.BigEndian.Uint32(body[9:13]))
		if logEntryHeaderSizeV3+cmdLen+logChecksumSize != recLen {
			return nil, 0, fmt.Errorf("%w: inconsistent segment record length", ErrCorruptLog)
		}
		kind := EntryKind(body[8])
		if kind != EntryApplication && kind != EntryNoop && kind != EntryConfiguration && kind != segmentBatchCommitKind {
			return nil, 0, fmt.Errorf("%w: unknown entry kind %d", ErrCorruptLog, kind)
		}
		checksumAt := logEntryHeaderSizeV3 + cmdLen
		if crc32.Checksum(body[:checksumAt], crc32cTable) != binary.BigEndian.Uint32(body[checksumAt:]) {
			return nil, 0, fmt.Errorf("%w: segment checksum mismatch", ErrCorruptLog)
		}
		if kind == segmentBatchCommitKind {
			if cmdLen != 4 || int(binary.BigEndian.Uint32(body[logEntryHeaderSizeV3:checksumAt])) != len(pending) || len(pending) == 0 {
				return nil, 0, fmt.Errorf("%w: invalid segment batch commit", ErrCorruptLog)
			}
			entries = append(entries, pending...)
			pending = pending[:0]
			pendingStart = pos
			continue
		}
		if len(pending) == 0 {
			pendingStart = recordStart
		}
		pending = append(pending, LogEntry{
			Term: Term(binary.BigEndian.Uint64(body[:8])), Kind: kind,
			Command: body[logEntryHeaderSizeV3:checksumAt],
		})
	}
	if len(pending) != 0 {
		if allowTornTail {
			return entries, pendingStart, nil
		}
		return nil, 0, fmt.Errorf("%w: segment ends before batch commit", ErrCorruptLog)
	}
	return entries, pos, nil
}

func manifestPath(path string) string { return path + ".manifest" }
func segmentRoot(path string) string  { return path + ".segments" }
func generationDir(path string, generation uint64) string {
	return filepath.Join(segmentRoot(path), fmt.Sprintf("g%020d", generation))
}

func encodeManifest(generation uint64, baseIndex LogIndex, baseTerm Term) []byte {
	data := make([]byte, manifestSize)
	copy(data[:4], manifestMagic[:])
	data[4] = 1
	binary.BigEndian.PutUint64(data[5:13], generation)
	binary.BigEndian.PutUint64(data[13:21], uint64(baseIndex))
	binary.BigEndian.PutUint64(data[21:29], uint64(baseTerm))
	binary.BigEndian.PutUint32(data[29:], crc32.Checksum(data[:29], crc32cTable))
	return data
}

func decodeManifest(data []byte) (uint64, LogIndex, Term, error) {
	if len(data) != manifestSize || [4]byte(data[:4]) != manifestMagic || data[4] != 1 {
		return 0, 0, 0, fmt.Errorf("%w: invalid segmented log manifest", ErrCorruptLog)
	}
	if crc32.Checksum(data[:29], crc32cTable) != binary.BigEndian.Uint32(data[29:]) {
		return 0, 0, 0, fmt.Errorf("%w: manifest checksum mismatch", ErrCorruptLog)
	}
	return binary.BigEndian.Uint64(data[5:13]), LogIndex(binary.BigEndian.Uint64(data[13:21])), Term(binary.BigEndian.Uint64(data[21:29])), nil
}

func openSegmentedLog(path string) (*Log, bool, error) {
	data, err := os.ReadFile(manifestPath(path))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	generation, baseIndex, baseTerm, err := decodeManifest(data)
	if err != nil {
		return nil, true, err
	}
	dir := generationDir(path, generation)
	names, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		return nil, true, err
	}
	sort.Strings(names)
	log := &Log{path: path, baseIndex: baseIndex, baseTerm: baseTerm, segmented: true, generation: generation, segmentFiles: len(names)}
	expected := baseIndex + 1
	for i, name := range names {
		raw, readErr := os.ReadFile(name)
		if readErr != nil {
			return nil, true, readErr
		}
		if len(raw) < segmentHeaderSize || [4]byte(raw[:4]) != segmentMagic || raw[4] != 1 {
			return nil, true, fmt.Errorf("%w: invalid segment header", ErrCorruptLog)
		}
		start := LogIndex(binary.BigEndian.Uint64(raw[5:13]))
		if filepath.Base(name) != fmt.Sprintf("%020d.seg", start) {
			return nil, true, fmt.Errorf("%w: segment name does not match start index %d", ErrCorruptLog, start)
		}
		if start != expected {
			return nil, true, fmt.Errorf("%w: segment gap or overlap: got %d want %d", ErrCorruptLog, start, expected)
		}
		entries, valid, decErr := decodeSegmentRecords(raw[segmentHeaderSize:], i == len(names)-1)
		if decErr != nil {
			return nil, true, decErr
		}
		if valid != len(raw)-segmentHeaderSize {
			if err := os.Truncate(name, int64(segmentHeaderSize+valid)); err != nil {
				return nil, true, err
			}
			f, err := os.OpenFile(name, os.O_WRONLY, 0)
			if err != nil {
				return nil, true, err
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return nil, true, err
			}
			if err := f.Close(); err != nil {
				return nil, true, err
			}
			raw = raw[:segmentHeaderSize+valid]
			log.recoveredTail = true
		}
		log.segmentBackings = append(log.segmentBackings, raw)
		log.entries = append(log.entries, entries...)
		expected += LogIndex(len(entries))
		log.activeSegment = name
		log.activeSize = int64(len(raw))
	}
	return log, true, nil
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (l *Log) writeGeneration(baseIndex LogIndex, baseTerm Term, entries []LogEntry) (uint64, string, int64, error) {
	generation := l.generation + 1
	root := segmentRoot(l.path)
	if err := os.Mkdir(root, 0o700); err != nil && !os.IsExist(err) {
		return 0, "", 0, err
	}
	if children, err := os.ReadDir(root); err == nil {
		for _, child := range children {
			if candidate, ok := parseGenerationName(child.Name()); ok && candidate >= generation {
				generation = candidate + 1
			}
		}
	}
	dir := generationDir(l.path, generation)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return 0, "", 0, err
	}
	type reusableSegment struct {
		path  string
		count int
		size  int64
	}
	reusable := make(map[LogIndex]reusableSegment)
	if l.segmented && l.generation != 0 {
		names, _ := filepath.Glob(filepath.Join(generationDir(l.path, l.generation), "*.seg"))
		sort.Strings(names)
		for _, name := range names {
			raw, readErr := os.ReadFile(name)
			if readErr != nil || len(raw) < segmentHeaderSize {
				continue
			}
			start := LogIndex(binary.BigEndian.Uint64(raw[5:13]))
			decoded, valid, decodeErr := decodeSegmentRecords(raw[segmentHeaderSize:], false)
			if decodeErr != nil || valid != len(raw)-segmentHeaderSize || start <= baseIndex {
				continue
			}
			offset := int(start - baseIndex - 1)
			if offset < 0 || offset+len(decoded) > len(entries) || !equalLogEntries(decoded, entries[offset:offset+len(decoded)]) {
				continue
			}
			reusable[start] = reusableSegment{path: name, count: len(decoded), size: int64(len(raw))}
		}
	}
	var active string
	var size int64
	physicalWritten := 0
	index := baseIndex + 1
	for len(entries) > 0 {
		start := index
		name := filepath.Join(dir, fmt.Sprintf("%020d.seg", start))
		if segment, ok := reusable[start]; ok && segment.count > 0 {
			if err := os.Link(segment.path, name); err != nil {
				return 0, "", 0, err
			}
			entries = entries[segment.count:]
			index += LogIndex(segment.count)
			active, size = name, segment.size
			continue
		}
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return 0, "", 0, err
		}
		header := make([]byte, segmentHeaderSize)
		copy(header[:4], segmentMagic[:])
		header[4] = 1
		binary.BigEndian.PutUint64(header[5:], uint64(start))
		if err := writeFull(f, header); err != nil {
			f.Close()
			return 0, "", 0, err
		}
		physicalWritten += len(header)
		size = segmentHeaderSize
		for len(entries) > 0 {
			if _, ok := reusable[index]; ok && size > segmentHeaderSize {
				break
			}
			record, err := encodeEntryRecord(entries[0])
			if err != nil {
				f.Close()
				return 0, "", 0, err
			}
			commit, err := encodeBatchCommit(1)
			if err != nil {
				f.Close()
				return 0, "", 0, err
			}
			if size > segmentHeaderSize && size+int64(len(record)+len(commit)) > raftLogSegmentBytes {
				break
			}
			if err := writeFull(f, record); err != nil {
				f.Close()
				return 0, "", 0, err
			}
			physicalWritten += len(record)
			if err := writeFull(f, commit); err != nil {
				f.Close()
				return 0, "", 0, err
			}
			physicalWritten += len(commit)
			size += int64(len(record) + len(commit))
			entries = entries[1:]
			index++
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return 0, "", 0, err
		}
		if err := f.Close(); err != nil {
			return 0, "", 0, err
		}
		active = name
	}
	if err := syncDir(dir); err != nil {
		return 0, "", 0, err
	}
	if err := syncDir(root); err != nil {
		return 0, "", 0, err
	}
	if err := atomicWriteFile("log", manifestPath(l.path), encodeManifest(generation, baseIndex, baseTerm)); err != nil {
		return 0, "", 0, err
	}
	l.rewriteBytes = physicalWritten + manifestSize
	return generation, active, size, nil
}

func (l *Log) rewriteSegmented() error {
	generation, active, size, err := l.writeGeneration(l.baseIndex, l.baseTerm, l.entries)
	if err != nil {
		return err
	}
	old := l.generation
	if len(l.segmentBackings) != 0 {
		// The new generation is independently durable. Re-own the logical
		// entries before releasing old segment buffers; this also preserves
		// any entries that survived a truncate or compaction boundary.
		l.entries = cloneEntries(l.entries)
		l.segmentBackings = nil
	}
	l.segmented, l.generation, l.activeSegment, l.activeSize = true, generation, active, size
	files, _ := filepath.Glob(filepath.Join(generationDir(l.path, generation), "*.seg"))
	l.segmentFiles = len(files)
	if m := l.observer.Load(); m != nil {
		m.RecordRaftLogWrite(0, l.rewriteBytes)
	}
	if old != 0 {
		_ = os.RemoveAll(generationDir(l.path, old))
	}
	return nil
}

func (l *Log) segmentCount() int {
	return l.segmentFiles
}

func (l *Log) appendSegmented(entries []LogEntry, owned bool) error {
	if !l.segmented {
		return fmt.Errorf("raft: segmented append without layout")
	}
	l.appendFsync = 0
	l.appendBytes = 0
	encodedSize := logLengthPrefixSize + logEntryHeaderSizeV3 + 4 + logChecksumSize
	for _, entry := range entries {
		if len(entry.Command) > maxCommandSize {
			return fmt.Errorf("raft: command length %d exceeds max %d", len(entry.Command), maxCommandSize)
		}
		encodedSize += entryRecordSize(entry)
	}
	encoded := make([]byte, 0, encodedSize)
	for _, entry := range entries {
		var err error
		encoded, err = appendEntryRecord(encoded, entry)
		if err != nil {
			return err
		}
	}
	var err error
	encoded, err = appendBatchCommit(encoded, len(entries))
	if err != nil {
		return err
	}
	l.appendBytes = len(encoded)
	if int64(len(encoded))+segmentHeaderSize > raftLogSegmentBytes {
		combined := append(cloneEntries(l.entries), cloneEntries(entries)...)
		generation, active, size, err := l.writeGeneration(l.baseIndex, l.baseTerm, combined)
		if err != nil {
			return err
		}
		old := l.generation
		l.generation, l.activeSegment, l.activeSize = generation, active, size
		files, _ := filepath.Glob(filepath.Join(generationDir(l.path, generation), "*.seg"))
		l.segmentFiles = len(files)
		l.entries = combined
		l.segmentBackings = nil
		_ = os.RemoveAll(generationDir(l.path, old))
		return nil
	}
	if l.activeSegment == "" || (l.activeSize > segmentHeaderSize && l.activeSize+int64(len(encoded)) > raftLogSegmentBytes) {
		dir := generationDir(l.path, l.generation)
		start := l.LastIndex() + 1
		name := filepath.Join(dir, fmt.Sprintf("%020d.seg", start))
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		header := make([]byte, segmentHeaderSize)
		copy(header[:4], segmentMagic[:])
		header[4] = 1
		binary.BigEndian.PutUint64(header[5:], uint64(start))
		if err := writeFull(f, header); err != nil {
			f.Close()
			return err
		}
		syncStart := time.Now()
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		l.appendFsync += time.Since(syncStart)
		if err := f.Close(); err != nil {
			return err
		}
		if err := syncDir(dir); err != nil {
			return err
		}
		l.activeSegment, l.activeSize = name, segmentHeaderSize
		l.segmentFiles++
	}
	f, err := os.OpenFile(l.activeSegment, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	oldSize := l.activeSize
	if err := checkFailpoint("log", "before-temp-write"); err != nil {
		f.Close()
		return err
	}
	if err := writeFull(f, encoded); err != nil {
		f.Close()
		return err
	}
	if err := checkFailpoint("log", "after-temp-write"); err != nil {
		f.Close()
		_ = os.Truncate(l.activeSegment, oldSize)
		return err
	}
	syncStart := time.Now()
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Truncate(l.activeSegment, oldSize)
		return err
	}
	l.appendFsync += time.Since(syncStart)
	if err := checkFailpoint("log", "after-temp-fsync"); err != nil {
		f.Close()
		_ = os.Truncate(l.activeSegment, oldSize)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Truncate(l.activeSegment, oldSize)
		return err
	}
	if err := checkFailpoint("log", "after-rename"); err != nil {
		_ = os.Truncate(l.activeSegment, oldSize)
		return err
	}
	if err := checkFailpoint("log", "after-dir-fsync"); err != nil {
		_ = os.Truncate(l.activeSegment, oldSize)
		return err
	}
	l.activeSize += int64(len(encoded))
	if owned {
		l.entries = append(l.entries, entries...)
	} else {
		l.entries = append(l.entries, cloneEntries(entries)...)
	}
	return nil
}

func parseGenerationName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "g") {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(name, "g"), 10, 64)
	return n, err == nil
}
