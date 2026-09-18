package kv

import (
	"crypto/sha256"
	"encoding/binary"

	"quorumkv/internal/reqid"
)

// Fingerprint returns a deterministic digest of cmd's logical operation
// (Type, Key, Value only — never ClientID/Sequence, which identify the
// request, not the operation it names). Two commands with the same
// fingerprint are, for dedup purposes, "the same operation"; a client
// reusing a (ClientID, Sequence) pair for two different fingerprints is
// misusing its request identity — see docs/request-dedup.md.
//
// This is a compact conflict-detection digest, not a security/
// authentication mechanism. The canonical input is a fixed, explicit
// byte layout (type | keyLength | key | valueLength | value) — never
// fmt.Sprintf, JSON, or map serialization, so the result is stable
// across Go versions and independent of any struct/field ordering.
func Fingerprint(cmd Command) reqid.Fingerprint {
	h := sha256.New()
	typeByte := [1]byte{byte(cmd.Type)}
	_, _ = h.Write(typeByte[:])
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(cmd.Key)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(cmd.Key)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(cmd.Value)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(cmd.Value)
	var fingerprint reqid.Fingerprint
	h.Sum(fingerprint[:0])
	return fingerprint
}
