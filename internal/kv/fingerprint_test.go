package kv

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"testing"
)

func legacyFingerprint(cmd Command) [32]byte {
	buf := make([]byte, 0, 1+4+len(cmd.Key)+4+len(cmd.Value))
	buf = append(buf, byte(cmd.Type))
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(cmd.Key)))
	buf = append(buf, length[:]...)
	buf = append(buf, cmd.Key...)
	binary.BigEndian.PutUint32(length[:], uint32(len(cmd.Value)))
	buf = append(buf, length[:]...)
	buf = append(buf, cmd.Value...)
	return sha256.Sum256(buf)
}

func TestFingerprintSameOperationSameFingerprint(t *testing.T) {
	a := Fingerprint(NewPutCommand([]byte("x"), []byte("1")))
	b := Fingerprint(NewPutCommand([]byte("x"), []byte("1")))
	if a != b {
		t.Fatalf("identical PUT operations produced different fingerprints: %x vs %x", a, b)
	}
}

func TestFingerprintIgnoresClientIdentity(t *testing.T) {
	id := clientIDOf(1)
	a := Fingerprint(NewPutCommand([]byte("x"), []byte("1")))
	b := Fingerprint(NewIdentifiedPutCommand(id, 7, []byte("x"), []byte("1")))
	if a != b {
		t.Fatalf("fingerprint must depend only on Type/Key/Value, not ClientID/Sequence: %x vs %x", a, b)
	}
}

func TestFingerprintDistinguishesDifferentOperations(t *testing.T) {
	put1 := Fingerprint(NewPutCommand([]byte("x"), []byte("1")))
	put2 := Fingerprint(NewPutCommand([]byte("x"), []byte("2")))
	del := Fingerprint(NewDeleteCommand([]byte("x")))
	putOtherKey := Fingerprint(NewPutCommand([]byte("y"), []byte("1")))

	all := map[string][32]byte{"put1": put1, "put2": put2, "del": del, "putOtherKey": putOtherKey}
	seen := map[[32]byte]string{}
	for name, fp := range all {
		if other, dup := seen[fp]; dup {
			t.Fatalf("%q and %q produced the same fingerprint, want distinct", name, other)
		}
		seen[fp] = name
	}
}

func TestFingerprintMatchesLegacyCanonicalEncoding(t *testing.T) {
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < 500; i++ {
		key := make([]byte, rng.Intn(128))
		value := make([]byte, rng.Intn(32*1024))
		_, _ = rng.Read(key)
		_, _ = rng.Read(value)
		cmd := Command{Type: CommandPut, Key: key, Value: value}
		if got, want := Fingerprint(cmd), legacyFingerprint(cmd); got != want {
			t.Fatalf("case %d fingerprint = %x, want %x", i, got, want)
		}
	}
}

func TestFingerprintAllocationDoesNotScaleWithValue(t *testing.T) {
	cmd := Command{Type: CommandPut, Key: []byte("key"), Value: make([]byte, 16*1024)}
	if got := testing.AllocsPerRun(100, func() { _ = Fingerprint(cmd) }); got > 1 {
		t.Fatalf("Fingerprint allocations = %.1f, want <= 1", got)
	}
}

func clientIDOf(b byte) (id [16]byte) {
	id[0] = b
	return id
}
