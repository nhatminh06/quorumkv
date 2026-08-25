package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func tempLogPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "raft-log")
}

func TestEmptyLogOpensWithNoEntries(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if l.LastIndex() != 0 || l.LastTerm() != 0 {
		t.Fatalf("LastIndex/LastTerm = %d/%d, want 0/0", l.LastIndex(), l.LastTerm())
	}
}

func TestLogAppendAndReopen(t *testing.T) {
	path := tempLogPath(t)
	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	entries := []LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 2, Command: []byte("c")},
	}
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	l2, err := OpenLog(path)
	if err != nil {
		t.Fatalf("reopen OpenLog: %v", err)
	}
	if l2.LastIndex() != 3 || l2.LastTerm() != 2 {
		t.Fatalf("LastIndex/LastTerm = %d/%d, want 3/2", l2.LastIndex(), l2.LastTerm())
	}
	for i, want := range entries {
		got, ok := l2.Entry(LogIndex(i + 1))
		if !ok || got.Term != want.Term || !bytes.Equal(got.Command, want.Command) {
			t.Fatalf("Entry(%d) = %+v, want %+v", i+1, got, want)
		}
	}
}

func TestLogSentinelIndexZero(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	term, ok := l.Term(0)
	if !ok || term != 0 {
		t.Fatalf("Term(0) = %d, %v; want 0, true", term, ok)
	}
}

func TestLogTruncateAndAppendPreservesPrefix(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{
		{Term: 1, Command: []byte("A")},
		{Term: 2, Command: []byte("B")},
		{Term: 3, Command: []byte("X")},
		{Term: 3, Command: []byte("Y")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := l.TruncateAndAppend(3, []LogEntry{
		{Term: 4, Command: []byte("C")},
		{Term: 4, Command: []byte("D")},
	}); err != nil {
		t.Fatalf("TruncateAndAppend: %v", err)
	}

	want := []LogEntry{
		{Term: 1, Command: []byte("A")},
		{Term: 2, Command: []byte("B")},
		{Term: 4, Command: []byte("C")},
		{Term: 4, Command: []byte("D")},
	}
	if l.LastIndex() != 4 {
		t.Fatalf("LastIndex() = %d, want 4", l.LastIndex())
	}
	for i, w := range want {
		got, ok := l.Entry(LogIndex(i + 1))
		if !ok || got.Term != w.Term || !bytes.Equal(got.Command, w.Command) {
			t.Fatalf("Entry(%d) = %+v, want %+v", i+1, got, w)
		}
	}
}

func TestLogTruncateAndAppendPersistsAcrossReopen(t *testing.T) {
	path := tempLogPath(t)
	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	l.Append([]LogEntry{{Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}})
	if err := l.TruncateAndAppend(2, []LogEntry{{Term: 2, Command: []byte("B2")}}); err != nil {
		t.Fatalf("TruncateAndAppend: %v", err)
	}

	l2, err := OpenLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if l2.LastIndex() != 2 {
		t.Fatalf("LastIndex() = %d, want 2", l2.LastIndex())
	}
	e, _ := l2.Entry(2)
	if e.Term != 2 || string(e.Command) != "B2" {
		t.Fatalf("Entry(2) = %+v, want {2 B2}", e)
	}
}

func TestEntriesFromCopiesOwnership(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	l.Append([]LogEntry{{Term: 1, Command: []byte("hello")}})
	got := l.EntriesFrom(1)
	got[0].Command[0] = 'H'

	e, _ := l.Entry(1)
	if string(e.Command) != "hello" {
		t.Fatalf("internal entry mutated via EntriesFrom result: got %q", e.Command)
	}
}

// --- EntriesRange (see append_entries.go's encodedEntrySize) ---

func TestEntriesRangeRespectsEntryLimit(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	entries := make([]LogEntry, 10)
	for i := range entries {
		entries[i] = LogEntry{Term: 1, Command: []byte("x")}
	}
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got := l.EntriesRange(1, 4, 1<<20)
	if len(got) != 4 {
		t.Fatalf("len(EntriesRange) = %d, want 4 (entry limit)", len(got))
	}
}

func TestEntriesRangeRespectsByteLimit(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	// Each entry costs perEntryHeaderSize + len(command) = 13 + 10 = 23
	// encoded bytes. A budget of 50 fits exactly 2 (23+23=46 <= 50, a
	// third would push it to 69 > 50).
	entries := make([]LogEntry, 5)
	for i := range entries {
		entries[i] = LogEntry{Term: 1, Command: []byte("0123456789")}
	}
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got := l.EntriesRange(1, 100, 50)
	if len(got) != 2 {
		t.Fatalf("len(EntriesRange) = %d, want 2 (byte limit)", len(got))
	}
}

func TestEntriesRangeReturnsLargeEntryAlone(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	large := make([]byte, 1000)
	entries := []LogEntry{
		{Term: 1, Command: large},
		{Term: 1, Command: []byte("small")},
	}
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// A byte budget far smaller than the first entry alone: it must
	// still be returned, by itself, rather than making it unsendable.
	got := l.EntriesRange(1, 100, 10)
	if len(got) != 1 {
		t.Fatalf("len(EntriesRange) = %d, want 1 (oversized first entry sent alone)", len(got))
	}
	if len(got[0].Command) != 1000 {
		t.Fatalf("returned entry command length = %d, want 1000", len(got[0].Command))
	}
}

func TestEntriesRangeDoesNotExposeInternalState(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 1, Command: []byte("hello")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got := l.EntriesRange(1, 10, 1<<20)
	got[0].Command[0] = 'H'

	e, _ := l.Entry(1)
	if string(e.Command) != "hello" {
		t.Fatalf("internal entry mutated via EntriesRange result: got %q", e.Command)
	}
}

func TestEntriesRangeAtSnapshotBoundary(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 1, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Requesting from the (now-compacted) boundary index itself, or
	// anything at/before it, must start at BaseIndex()+1 — the same
	// clamping EntriesFrom already does — never fabricate a compacted
	// entry.
	for _, from := range []LogIndex{0, 1} {
		got := l.EntriesRange(from, 10, 1<<20)
		if len(got) != 2 || string(got[0].Command) != "b" || string(got[1].Command) != "c" {
			t.Fatalf("EntriesRange(%d) = %v, want [b c]", from, got)
		}
	}
}

func TestEntriesRangePastLastIndex(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 1, Command: []byte("a")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := l.EntriesRange(l.LastIndex()+1, 10, 1<<20); got != nil {
		t.Fatalf("EntriesRange(LastIndex()+1) = %v, want nil", got)
	}
	if got := l.EntriesRange(5, 10, 1<<20); got != nil {
		t.Fatalf("EntriesRange(far past LastIndex()) = %v, want nil", got)
	}
}

func TestEntriesRangeEmptyLog(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if got := l.EntriesRange(1, 10, 1<<20); got != nil {
		t.Fatalf("EntriesRange on empty log = %v, want nil", got)
	}
}

// TestKnownLogByteVector independently derives the expected on-disk bytes
// for a single entry (term=5, kind=EntryApplication, command="abc")
// rather than round-tripping Append->reopen. The checksum was computed
// with the standard hash/crc32 Castagnoli table outside this package (a
// standalone script, not this package's own encoder).
func TestKnownLogByteVector(t *testing.T) {
	path := tempLogPath(t)
	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 5, Kind: EntryApplication, Command: []byte("abc")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	want := []byte{
		'R', 'L', 'G', '1', // magic
		0x03,                   // version
		0, 0, 0, 0, 0, 0, 0, 0, // baseIndex = 0 (never compacted)
		0, 0, 0, 0, 0, 0, 0, 0, // baseTerm = 0
		0x00, 0x00, 0x00, 0x14, // record length = 20
		0, 0, 0, 0, 0, 0, 0, 5, // term = 5
		0x00,       // kind = EntryApplication (0)
		0, 0, 0, 3, // command length = 3
		'a', 'b', 'c', // command
		0x5c, 0x8c, 0x9b, 0x65, // CRC32C(term|kind|commandLength|command)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("log bytes:\n got  % x\n want % x", got, want)
	}
}

func writeRawLog(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func validLogBytes(t *testing.T) []byte {
	t.Helper()
	path := tempLogPath(t)
	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 1, Command: []byte("a")}, {Term: 2, Command: []byte("b")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return b
}

func TestLogBadMagicRejected(t *testing.T) {
	full := validLogBytes(t)
	corrupt := append([]byte(nil), full...)
	corrupt[0] ^= 0xFF
	path := tempLogPath(t)
	writeRawLog(t, path, corrupt)

	_, err := OpenLog(path)
	if !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("OpenLog error = %v, want ErrCorruptLog", err)
	}
}

func TestLogBadChecksumRejected(t *testing.T) {
	full := validLogBytes(t)
	corrupt := append([]byte(nil), full...)
	corrupt[len(corrupt)-1] ^= 0xFF
	path := tempLogPath(t)
	writeRawLog(t, path, corrupt)

	_, err := OpenLog(path)
	if !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("OpenLog error = %v, want ErrCorruptLog", err)
	}
}

func TestLogTruncatedEntryRejected(t *testing.T) {
	full := validLogBytes(t)
	path := tempLogPath(t)
	writeRawLog(t, path, full[:len(full)-3]) // cut into the last entry

	_, err := OpenLog(path)
	if !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("OpenLog error = %v, want ErrCorruptLog", err)
	}
}

func TestLogInvalidDeclaredLengthRejected(t *testing.T) {
	full := validLogBytes(t)
	corrupt := append([]byte(nil), full...)
	// First record's length prefix, right after the v2 header (magic +
	// version + baseIndex + baseTerm); corrupt it to something wildly
	// inconsistent with the actual body that follows.
	off := logV2HeaderSize
	corrupt[off], corrupt[off+1], corrupt[off+2], corrupt[off+3] = 0, 0, 0, 200
	path := tempLogPath(t)
	writeRawLog(t, path, corrupt)

	_, err := OpenLog(path)
	if !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("OpenLog error = %v, want ErrCorruptLog", err)
	}
}

func TestLogOversizedCommandRejected(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	oversized := make([]byte, maxCommandSize+1)
	err = l.Append([]LogEntry{{Term: 1, Command: oversized}})
	if err == nil {
		t.Fatalf("Append succeeded with oversized command, want error")
	}
	if l.LastIndex() != 0 {
		t.Fatalf("LastIndex() = %d, want 0 (append must not partially apply)", l.LastIndex())
	}
}

// TestLogV1FileStillLoads proves a pre-Milestone-7 log file (version 1,
// no baseIndex/baseTerm fields) still loads correctly, equivalent to
// baseIndex=0, baseTerm=0 — existing repositories must not be
// invalidated by the format upgrade.
func TestLogV1FileStillLoads(t *testing.T) {
	path := tempLogPath(t)
	v1 := []byte{
		'R', 'L', 'G', '1', // magic
		0x01,                   // version 1
		0x00, 0x00, 0x00, 0x13, // record length = 19
		0, 0, 0, 0, 0, 0, 0, 5, // term = 5
		0, 0, 0, 3, // command length = 3
		'a', 'b', 'c', // command
		0x0f, 0x22, 0x3e, 0xae, // CRC32C(term|commandLength|command)
	}
	writeRawLog(t, path, v1)

	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if l.BaseIndex() != 0 || l.BaseTerm() != 0 {
		t.Fatalf("BaseIndex/BaseTerm = %d/%d, want 0/0", l.BaseIndex(), l.BaseTerm())
	}
	if l.LastIndex() != 1 {
		t.Fatalf("LastIndex() = %d, want 1", l.LastIndex())
	}
	e, ok := l.Entry(1)
	if !ok || e.Term != 5 || string(e.Command) != "abc" {
		t.Fatalf("Entry(1) = %+v, ok=%v, want {5 abc}, true", e, ok)
	}

	// A subsequent mutation silently upgrades the file to v3.
	if err := l.Append([]LogEntry{{Term: 5, Command: []byte("d")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if data[4] != logFileVersion3 {
		t.Fatalf("version after rewrite = %d, want %d", data[4], logFileVersion3)
	}
}

func TestLogCompactRemovesCoveredPrefixKeepsSuffix(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	entries := make([]LogEntry, 10)
	for i := range entries {
		entries[i] = LogEntry{Term: 1, Command: []byte{byte('a' + i)}}
	}
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := l.Compact(7, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if l.BaseIndex() != 7 || l.BaseTerm() != 1 {
		t.Fatalf("BaseIndex/BaseTerm = %d/%d, want 7/1", l.BaseIndex(), l.BaseTerm())
	}
	if l.LastIndex() != 10 {
		t.Fatalf("LastIndex() = %d, want 10 (unchanged by compaction)", l.LastIndex())
	}
	// Term at the boundary is answerable without a physical entry.
	term, ok := l.Term(7)
	if !ok || term != 1 {
		t.Fatalf("Term(7) = %d, %v, want 1, true", term, ok)
	}
	// The command at the boundary itself is gone.
	if _, ok := l.Entry(7); ok {
		t.Fatalf("Entry(7) should be unavailable after compaction")
	}
	// Before the boundary is unavailable, not fabricated.
	if _, ok := l.Term(6); ok {
		t.Fatalf("Term(6) should be unavailable (compacted) after compaction")
	}
	// Retained suffix (8, 9, 10) is untouched.
	for i := LogIndex(8); i <= 10; i++ {
		e, ok := l.Entry(i)
		want := string(rune('a' + int(i) - 1))
		if !ok || e.Term != 1 || string(e.Command) != want {
			t.Fatalf("Entry(%d) = %+v, ok=%v, want {1 %s}, true", i, e, ok, want)
		}
	}
}

func TestLogCompactPersistsAcrossReopen(t *testing.T) {
	path := tempLogPath(t)
	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 2, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Compact(2, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	reopened, err := OpenLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.BaseIndex() != 2 || reopened.BaseTerm() != 1 {
		t.Fatalf("BaseIndex/BaseTerm = %d/%d, want 2/1", reopened.BaseIndex(), reopened.BaseTerm())
	}
	if reopened.LastIndex() != 3 {
		t.Fatalf("LastIndex() = %d, want 3", reopened.LastIndex())
	}
	e, ok := reopened.Entry(3)
	if !ok || e.Term != 2 || string(e.Command) != "c" {
		t.Fatalf("Entry(3) = %+v, ok=%v, want {2 c}, true", e, ok)
	}
}

func TestLogCompactNeverRegresses(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Compact(2, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := l.Compact(1, 1); err != nil {
		t.Fatalf("Compact (no-op): %v", err)
	}
	if l.BaseIndex() != 2 {
		t.Fatalf("BaseIndex() = %d, want unchanged 2 (compaction must never regress)", l.BaseIndex())
	}
}

func TestLogCompactBeyondLastIndexRejected(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 1, Command: []byte("a")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Compact(5, 1); err == nil {
		t.Fatalf("Compact succeeded beyond LastIndex(), want error")
	}
	if l.BaseIndex() != 0 {
		t.Fatalf("BaseIndex() = %d, want unchanged 0 after rejected compaction", l.BaseIndex())
	}
}

func TestLogEntriesFromAfterCompactionStartsAtBoundaryPlusOne(t *testing.T) {
	l, err := OpenLog(tempLogPath(t))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 1, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Compact(2, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got := l.EntriesFrom(1) // before/at boundary clamps to boundary+1
	if len(got) != 1 || string(got[0].Command) != "c" {
		t.Fatalf("EntriesFrom(1) = %+v, want just [c]", got)
	}
	if got := l.EntriesFrom(2); len(got) != 1 || string(got[0].Command) != "c" {
		t.Fatalf("EntriesFrom(2) = %+v, want just [c]", got)
	}
}
