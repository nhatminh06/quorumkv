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

// TestKnownLogByteVector independently derives the expected on-disk bytes
// for a single entry (term=5, command="abc") rather than round-tripping
// Append->reopen. The checksum was computed with the standard hash/crc32
// Castagnoli table outside this package.
func TestKnownLogByteVector(t *testing.T) {
	path := tempLogPath(t)
	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	if err := l.Append([]LogEntry{{Term: 5, Command: []byte("abc")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	want := []byte{
		'R', 'L', 'G', '1', // magic
		0x01,                   // version
		0x00, 0x00, 0x00, 0x13, // record length = 19
		0, 0, 0, 0, 0, 0, 0, 5, // term = 5
		0, 0, 0, 3, // command length = 3
		'a', 'b', 'c', // command
		0x0f, 0x22, 0x3e, 0xae, // CRC32C(term|commandLength|command)
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
	// First record's length prefix (offset 5..9); corrupt it to something
	// wildly inconsistent with the actual body that follows.
	corrupt[5], corrupt[6], corrupt[7], corrupt[8] = 0, 0, 0, 200
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
