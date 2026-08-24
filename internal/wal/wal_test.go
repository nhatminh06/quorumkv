package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"quorumkv/internal/kv"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func TestEmptyWALReplaysToEmptyState(t *testing.T) {
	path := tempWALPath(t)

	w, cmds, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	if len(cmds) != 0 {
		t.Fatalf("expected no commands from empty WAL, got %v", cmds)
	}
}

func TestAppendAndReopenReplaysInOrder(t *testing.T) {
	path := tempWALPath(t)

	w, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cmds := []kv.Command{
		kv.NewPutCommand([]byte("x"), []byte("1")),
		kv.NewPutCommand([]byte("x"), []byte("2")),
		kv.NewDeleteCommand([]byte("x")),
		kv.NewPutCommand([]byte("x"), []byte("3")),
	}
	for _, c := range cmds {
		if err := w.Append(c); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen as a separate WAL value; do not just re-read within the same
	// object, to genuinely exercise persistence across process lifetimes.
	w2, replayed, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	if len(replayed) != len(cmds) {
		t.Fatalf("replayed %d commands, want %d", len(replayed), len(cmds))
	}
	for i, c := range cmds {
		if replayed[i].Type != c.Type || !bytes.Equal(replayed[i].Key, c.Key) || !bytes.Equal(replayed[i].Value, c.Value) {
			t.Fatalf("record %d = %+v, want %+v", i, replayed[i], c)
		}
	}

	m := kv.NewStateMachine()
	for _, c := range replayed {
		m.Apply(c)
	}
	v, ok := m.Get([]byte("x"))
	if !ok || string(v) != "3" {
		t.Fatalf("Get(x) = %q, %v; want 3, true", v, ok)
	}
}

func TestMultipleKeysPreserveOrderAcrossReopen(t *testing.T) {
	path := tempWALPath(t)

	w, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writes := []kv.Command{
		kv.NewPutCommand([]byte("a"), []byte("1")),
		kv.NewPutCommand([]byte("b"), []byte("2")),
		kv.NewPutCommand([]byte("c"), []byte("3")),
		kv.NewPutCommand([]byte("b"), []byte("22")),
		kv.NewDeleteCommand([]byte("a")),
	}
	for _, c := range writes {
		if err := w.Append(c); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, replayed, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	m := kv.NewStateMachine()
	for _, c := range replayed {
		m.Apply(c)
	}
	if _, ok := m.Get([]byte("a")); ok {
		t.Fatalf("a should have been deleted")
	}
	if v, ok := m.Get([]byte("b")); !ok || string(v) != "22" {
		t.Fatalf("b = %q, %v; want 22, true", v, ok)
	}
	if v, ok := m.Get([]byte("c")); !ok || string(v) != "3" {
		t.Fatalf("c = %q, %v; want 3, true", v, ok)
	}
}

// TestKnownByteVector independently derives the expected on-disk bytes for
// a single PUT record (key="a", value="1") rather than round-tripping
// encode->decode, so an encoder/decoder that share a bug cannot pass this
// test. The checksum was independently computed with the standard
// hash/crc32 Castagnoli table outside this package.
func TestKnownByteVector(t *testing.T) {
	path := tempWALPath(t)
	w, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Append(kv.NewPutCommand([]byte("a"), []byte("1"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 0x0f, // record length = 15
		0x01,                   // type = CommandPut
		0x00, 0x00, 0x00, 0x01, // key length = 1
		0x00, 0x00, 0x00, 0x01, // value length = 1
		0x61,                   // key = "a"
		0x31,                   // value = "1"
		0xa1, 0x54, 0x88, 0x4c, // CRC32C(type|keyLen|valLen|key|value)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("wal bytes:\n got  % x\n want % x", got, want)
	}
}

func writeRawFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func validRecordBytes(t *testing.T, key, value []byte) []byte {
	t.Helper()
	path := tempWALPath(t)
	w, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Append(kv.NewPutCommand(key, value)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return b
}

func TestTruncatedLengthPrefixIsTornTail(t *testing.T) {
	full := validRecordBytes(t, []byte("a"), []byte("1"))
	path := tempWALPath(t)
	writeRawFile(t, path, full[:2]) // only 2 of 4 length-prefix bytes

	w, cmds, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	if len(cmds) != 0 {
		t.Fatalf("expected no commands, got %v", cmds)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected torn tail truncated to 0 bytes, got %d", info.Size())
	}
}

func TestTruncatedBodyIsTornTail(t *testing.T) {
	full := validRecordBytes(t, []byte("a"), []byte("1"))
	path := tempWALPath(t)
	writeRawFile(t, path, full[:10]) // length prefix intact, body cut short

	_, cmds, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("expected no commands, got %v", cmds)
	}
}

func TestTruncatedChecksumIsTornTail(t *testing.T) {
	full := validRecordBytes(t, []byte("a"), []byte("1"))
	path := tempWALPath(t)
	writeRawFile(t, path, full[:len(full)-2]) // checksum cut short

	_, cmds, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("expected no commands, got %v", cmds)
	}
}

func TestValidRecordsFollowedByTornFinalRecord(t *testing.T) {
	path := tempWALPath(t)
	w, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Append(kv.NewPutCommand([]byte("a"), []byte("1"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(kv.NewPutCommand([]byte("b"), []byte("2"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	torn := append(full, []byte{0x00, 0x00, 0x00, 0x20, 0x01, 0x02, 0x03}...)
	writeRawFile(t, path, torn)

	_, cmds, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(cmds) != 2 {
		t.Fatalf("expected 2 valid commands preserved, got %d: %v", len(cmds), cmds)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != int64(len(full)) {
		t.Fatalf("expected torn tail truncated back to %d bytes, got %d", len(full), info.Size())
	}
}

func TestChecksumMismatchIsCorruption(t *testing.T) {
	full := validRecordBytes(t, []byte("a"), []byte("1"))
	corrupt := append([]byte(nil), full...)
	corrupt[len(corrupt)-1] ^= 0xFF // flip a byte in the checksum
	path := tempWALPath(t)
	writeRawFile(t, path, corrupt)

	_, _, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestInvalidCommandTypeIsCorruption(t *testing.T) {
	full := validRecordBytes(t, []byte("a"), []byte("1"))
	corrupt := append([]byte(nil), full...)
	corrupt[4] = 0xFF // type byte, right after the length prefix; also
	// breaks the checksum, but either way this must surface as ErrCorrupt.
	path := tempWALPath(t)
	writeRawFile(t, path, corrupt)

	_, _, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestOversizedDeclaredLengthIsRejected(t *testing.T) {
	path := tempWALPath(t)
	buf := make([]byte, 4)
	// Declare a record length far beyond maxRecordBodySize.
	huge := uint32(maxRecordBodySize) + 1
	buf[0] = byte(huge >> 24)
	buf[1] = byte(huge >> 16)
	buf[2] = byte(huge >> 8)
	buf[3] = byte(huge)
	writeRawFile(t, path, buf)

	_, _, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestMidLogCorruptionStopsReplay(t *testing.T) {
	path := tempWALPath(t)
	w, _, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	firstRecord, err := encodeRecord(kv.NewPutCommand([]byte("a"), []byte("1")))
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	secondRecord, err := encodeRecord(kv.NewPutCommand([]byte("b"), []byte("2")))
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	thirdRecord, err := encodeRecord(kv.NewPutCommand([]byte("c"), []byte("3")))
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	if err := w.Append(kv.NewPutCommand([]byte("a"), []byte("1"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(kv.NewPutCommand([]byte("b"), []byte("2"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(kv.NewPutCommand([]byte("c"), []byte("3"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Corrupt the last byte (checksum) of the middle record only; the
	// first and third records remain byte-for-byte valid.
	middleChecksumEnd := len(firstRecord) + len(secondRecord)
	full[middleChecksumEnd-1] ^= 0xFF
	writeRawFile(t, path, full)
	_ = thirdRecord

	_, _, err = Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}
