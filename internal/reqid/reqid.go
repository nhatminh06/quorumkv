// Package reqid defines the client request identity types shared by the
// KV command codec (internal/kv), the client wire protocol
// (internal/clientproto), and the Go client (internal/client): a stable
// per-client ClientID plus a monotonic per-client Sequence identify one
// logical write, so a retried PUT/DELETE that reuses the same
// (ClientID, Sequence) can be recognized and suppressed rather than
// applied twice. See docs/request-dedup.md.
//
// ClientID is a session identifier, not a credential: without
// authentication, another actor could in principle claim the same
// ClientID. reqid provides logical request-identity conflict detection,
// not security.
package reqid

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// ClientID identifies one logical writer. Fixed-size (128 bits) rather
// than an arbitrary-length string: cheap to compare, hash, and embed in a
// fixed-layout wire/persistent format.
type ClientID [16]byte

// Sequence is a per-ClientID monotonic write counter. 0 is reserved as
// "invalid/unassigned" — never a legal sequence for an identified write.
// The first real write from a ClientID uses Sequence 1.
type Sequence uint64

// Fingerprint is a deterministic digest of one logical operation (see
// internal/kv's Fingerprint function), used to detect a client reusing
// the same (ClientID, Sequence) for two different operations — a client
// bug, not a legitimate retry.
type Fingerprint [32]byte

// ErrSequenceExhausted means a client's Sequence counter has used every
// value up to the maximum representable Sequence. Practically
// unreachable, but left explicit rather than silently wrapping back to
// the reserved 0.
var ErrSequenceExhausted = errors.New("reqid: client sequence space exhausted")

// NewClientID generates a fresh, cryptographically random ClientID.
// crypto/rand (not math/rand, not a timestamp, not a PID) is used because
// identity collisions across independently created clients must be
// negligibly unlikely — not because ClientID is a secret.
func NewClientID() (ClientID, error) {
	var id ClientID
	if _, err := rand.Read(id[:]); err != nil {
		return ClientID{}, fmt.Errorf("reqid: generating ClientID: %w", err)
	}
	return id, nil
}

// IsZero reports whether id is the all-zero ClientID, which is reserved
// as invalid/uninitialized — never a legal identity for an identified
// write (see internal/kv's command codec and docs/request-dedup.md).
func (id ClientID) IsZero() bool {
	return id == ClientID{}
}

// String renders id as lowercase hex, for logging/debugging only — never
// parsed back, never part of any wire/persistent format.
func (id ClientID) String() string {
	return hex.EncodeToString(id[:])
}
