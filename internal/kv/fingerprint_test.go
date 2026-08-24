package kv

import "testing"

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

func clientIDOf(b byte) (id [16]byte) {
	id[0] = b
	return id
}
