package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func tempSnapshotPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "snapshot")
}

func sampleSnapshotConfiguration(t *testing.T) Configuration {
	t.Helper()
	c, err := NewConfiguration(map[NodeID]string{1: "A"})
	if err != nil {
		t.Fatalf("NewConfiguration: %v", err)
	}
	return c
}

func TestSnapshotStoreMissingFileIsNil(t *testing.T) {
	s := NewSnapshotStore(tempSnapshotPath(t))
	snap, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap != nil {
		t.Fatalf("snap = %+v, want nil (no snapshot yet)", snap)
	}
}

func TestSnapshotStoreSurvivesReopen(t *testing.T) {
	path := tempSnapshotPath(t)
	want := Snapshot{LastIncludedIndex: 100, LastIncludedTerm: 6, Data: []byte("abc"), Configuration: sampleSnapshotConfiguration(t)}
	if err := NewSnapshotStore(path).Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := NewSnapshotStore(path).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil || got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm ||
		!bytes.Equal(got.Data, want.Data) || !got.ConfigurationPresent || !got.Configuration.Equal(want.Configuration) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// TestSnapshotKnownByteVector independently derives the expected on-disk
// bytes for lastIncludedIndex=100, lastIncludedTerm=6, payload="abc",
// configuration=Stable{1:"A"}. The checksum was computed with the
// standard hash/crc32 Castagnoli table outside this package. The
// membership section is EncodeMembership(StableMembership({1:"A"})): see
// TestInstallSnapshotKnownByteVector for its byte-by-byte derivation
// (identical here — same Configuration).
func TestSnapshotKnownByteVector(t *testing.T) {
	path := tempSnapshotPath(t)
	if err := NewSnapshotStore(path).Save(Snapshot{LastIncludedIndex: 100, LastIncludedTerm: 6, Data: []byte("abc"), Configuration: sampleSnapshotConfiguration(t)}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := []byte{
		'S', 'N', 'P', '1', // magic
		0x02,                     // version
		0, 0, 0, 0, 0, 0, 0, 100, // lastIncludedIndex = 100
		0, 0, 0, 0, 0, 0, 0, 6, // lastIncludedTerm = 6
		0, 0, 0, 0, 0, 0, 0, 3, // payload length = 3
		'a', 'b', 'c', // payload
		0, 0, 0, 0, 0, 0, 0, 17, // membership length = 17
		0x01,       // membership version
		0x01,       // mode = Stable
		0, 0, 0, 1, // voterCount = 1
		0, 0, 0, 0, 0, 0, 0, 1, // nodeID = 1
		0, 1, // addrLen = 1
		'A',
	}
	if len(want) != snapshotFixedHeaderSize+snapshotMetaSize+3+8+17 {
		t.Fatalf("test bug: want length mismatch")
	}
	// Checksum covers version..membership (data[4:len(data)-4]).
	checksum := crc32.Checksum(want[4:], crc32cTable)
	want = binary.BigEndian.AppendUint32(want, checksum)
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func writeRawSnapshot(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func validSnapshotBytes(t *testing.T) []byte {
	t.Helper()
	path := tempSnapshotPath(t)
	if err := NewSnapshotStore(path).Save(Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 2, Data: []byte("hello"), Configuration: sampleSnapshotConfiguration(t)}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return b
}

// TestSnapshotStoreLegacyV1FileStillLoads proves a pre-Milestone-10
// snapshot file (version 1, no membership metadata) still loads
// correctly, with ConfigurationPresent=false telling the caller to fall
// back to its own bootstrap configuration rather than treating the
// missing metadata as corruption.
func TestSnapshotStoreLegacyV1FileStillLoads(t *testing.T) {
	path := tempSnapshotPath(t)
	v1 := []byte{
		'S', 'N', 'P', '1', // magic
		0x01,                     // version 1
		0, 0, 0, 0, 0, 0, 0, 100, // lastIncludedIndex = 100
		0, 0, 0, 0, 0, 0, 0, 6, // lastIncludedTerm = 6
		0, 0, 0, 0, 0, 0, 0, 3, // payload length = 3
		'a', 'b', 'c', // payload
		0x02, 0x2e, 0x4b, 0x86, // CRC32C(version|lastIncludedIndex|lastIncludedTerm|payloadLength|payload)
	}
	writeRawSnapshot(t, path, v1)

	got, err := NewSnapshotStore(path).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ConfigurationPresent {
		t.Fatalf("ConfigurationPresent = true for a legacy v1 file, want false")
	}
	if got.LastIncludedIndex != 100 || got.LastIncludedTerm != 6 || string(got.Data) != "abc" {
		t.Fatalf("got %+v, want index=100 term=6 data=abc", got)
	}
}

func TestSnapshotStoreBadMagicRejected(t *testing.T) {
	full := validSnapshotBytes(t)
	corrupt := append([]byte(nil), full...)
	corrupt[0] ^= 0xFF
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, corrupt)
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestSnapshotStoreBadVersionRejected(t *testing.T) {
	full := validSnapshotBytes(t)
	corrupt := append([]byte(nil), full...)
	corrupt[4] = 99
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, corrupt)
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestSnapshotStoreTruncatedMetadataRejected(t *testing.T) {
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, []byte{'S', 'N', 'P', '1', 0x01, 0, 0})
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestSnapshotStoreOversizedPayloadDeclarationRejected(t *testing.T) {
	full := validSnapshotBytes(t)
	corrupt := append([]byte(nil), full...)
	// payload length field: bytes [13:21) of the fixed metadata.
	off := snapshotFixedHeaderSize + 16
	corrupt[off], corrupt[off+1] = 0xFF, 0xFF // absurdly large declared length
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, corrupt)
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestSnapshotStoreTruncatedPayloadRejected(t *testing.T) {
	full := validSnapshotBytes(t)
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, full[:len(full)-6]) // cut into payload+checksum
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestSnapshotStoreChecksumMismatchRejected(t *testing.T) {
	full := validSnapshotBytes(t)
	corrupt := append([]byte(nil), full...)
	corrupt[len(corrupt)-1] ^= 0xFF
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, corrupt)
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestSnapshotStoreTrailingBytesRejected(t *testing.T) {
	full := validSnapshotBytes(t)
	corrupt := append(append([]byte(nil), full...), 0xFF)
	path := tempSnapshotPath(t)
	writeRawSnapshot(t, path, corrupt)
	_, err := NewSnapshotStore(path).Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("err = %v, want ErrCorruptSnapshot", err)
	}
}
