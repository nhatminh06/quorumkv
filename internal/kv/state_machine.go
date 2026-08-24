package kv

import "quorumkv/internal/reqid"

// ApplyOutcome classifies the effect an Apply call had, distinguishing a
// genuinely new mutation from the dedup outcomes a retried identified
// command can produce (see docs/request-dedup.md). An unidentified
// command (zero ClientID — every pre-Milestone-9 command, and any
// internal command with no client session) is always AppliedNew: dedup
// only exists for identified commands.
type ApplyOutcome uint8

const (
	// AppliedNew means the command mutated state for the first time —
	// either it carries no request identity, or it is the exact next
	// sequence for a known/new ClientID.
	AppliedNew ApplyOutcome = iota + 1
	// AppliedDuplicate means this exact (ClientID, Sequence, fingerprint)
	// already applied; state was NOT mutated again, and the original
	// result (always OK for PUT/DELETE) should be returned.
	AppliedDuplicate
	// StaleRequest means Sequence does not match this ClientID's expected
	// next sequence (either behind the last applied sequence, or ahead of
	// it by more than one — a gap). State was not mutated.
	StaleRequest
	// RequestConflict means Sequence matches the last applied sequence
	// for this ClientID, but the fingerprint differs: the same request
	// identity was reused for a different operation, which is invalid
	// client behavior (see docs/request-dedup.md). State was not
	// mutated.
	RequestConflict
)

// ApplyStatus records the outcome of the write a ClientRecord
// remembers. Currently PUT/DELETE only ever succeed as OK, so this has a
// single defined value today; it exists as an explicit, bounded field
// (see the snapshot format) rather than assuming OK forever.
type ApplyStatus uint8

// ApplyStatusOK is the only currently-defined ApplyStatus: PUT/DELETE
// have no other successful terminal outcome to remember.
const ApplyStatusOK ApplyStatus = 1

// ClientRecord is the durable, replicated dedup state kept for one
// ClientID: only the LATEST request's sequence/fingerprint/result, not a
// history of every request ever seen — sufficient because a client
// serializes its own writes (see docs/request-dedup.md), and it keeps
// this table's size bounded by the number of distinct known ClientIDs
// rather than the number of requests ever made.
type ClientRecord struct {
	LastSequence    reqid.Sequence
	LastFingerprint reqid.Fingerprint
	LastResult      ApplyStatus
}

// StateMachine is a deterministic in-memory key-value store, plus (since
// Milestone 9) per-client request-dedup state. Applying the same ordered
// sequence of commands to two StateMachine values always produces the
// same resulting state.
//
// StateMachine is not safe for concurrent use. Callers needing concurrent
// access must synchronize externally.
type StateMachine struct {
	state   map[string][]byte
	clients map[reqid.ClientID]ClientRecord
}

func NewStateMachine() *StateMachine {
	return &StateMachine{state: make(map[string][]byte), clients: make(map[reqid.ClientID]ClientRecord)}
}

// Apply executes cmd against the state machine and returns what actually
// happened. An unidentified command (zero ClientID) is always applied
// unconditionally and returns AppliedNew — this is the entire
// pre-Milestone-9 behavior, unchanged.
//
// An identified command (non-zero ClientID) is checked against that
// ClientID's ClientRecord first (see classifyRequest): only a genuinely
// new next-sequence request actually mutates state and updates the
// record; a duplicate, stale, or conflicting request never does. This
// makes Apply itself the single authoritative dedup point — regardless
// of what a leader-local shortcut above it decided, a command that
// somehow reaches Apply twice can never mutate state twice.
func (m *StateMachine) Apply(cmd Command) ApplyOutcome {
	if cmd.ClientID.IsZero() {
		m.applyRaw(cmd)
		return AppliedNew
	}

	fp := Fingerprint(cmd)
	rec := m.clients[cmd.ClientID] // zero value (LastSequence 0) if unseen
	switch classifyRequest(rec, cmd.Sequence, fp) {
	case AppliedDuplicate:
		return AppliedDuplicate
	case RequestConflict:
		return RequestConflict
	case StaleRequest:
		return StaleRequest
	default: // AppliedNew: cmd.Sequence == rec.LastSequence+1
		m.applyRaw(cmd)
		m.clients[cmd.ClientID] = ClientRecord{LastSequence: cmd.Sequence, LastFingerprint: fp, LastResult: ApplyStatusOK}
		return AppliedNew
	}
}

// classifyRequest implements the exact-next-sequence dedup policy shared
// by Apply and LookupRequest: given what's on record for a ClientID (the
// zero ClientRecord if none), decide what an incoming (sequence,
// fingerprint) means. It never mutates anything — callers act on the
// result.
func classifyRequest(rec ClientRecord, seq reqid.Sequence, fp reqid.Fingerprint) ApplyOutcome {
	switch {
	case seq == rec.LastSequence: // only possible if rec exists: seq is never 0 for an identified command
		if fp == rec.LastFingerprint {
			return AppliedDuplicate
		}
		return RequestConflict
	case seq == rec.LastSequence+1:
		return AppliedNew
	default:
		return StaleRequest
	}
}

// LookupRequest reports what Apply would do for an identified command
// with the given ClientID/Sequence/fingerprint, without applying
// anything. This is an optimization the service layer uses to avoid
// proposing a Raft entry for a request it can already answer from
// replicated state — it is never itself authoritative; Apply's own
// dedup check is (see docs/request-dedup.md item 34).
func (m *StateMachine) LookupRequest(id reqid.ClientID, seq reqid.Sequence, fp reqid.Fingerprint) ApplyOutcome {
	return classifyRequest(m.clients[id], seq, fp)
}

func (m *StateMachine) applyRaw(cmd Command) {
	switch cmd.Type {
	case CommandPut:
		m.state[string(cmd.Key)] = cloneBytes(cmd.Value)
	case CommandDelete:
		delete(m.state, string(cmd.Key))
	}
}

// Put is a convenience wrapper that applies an unidentified CommandPut.
func (m *StateMachine) Put(key, value []byte) {
	m.Apply(NewPutCommand(key, value))
}

// Delete is a convenience wrapper that applies an unidentified
// CommandDelete. Deleting a key that does not exist is a deterministic
// no-op.
func (m *StateMachine) Delete(key []byte) {
	m.Apply(NewDeleteCommand(key))
}

// Get returns a copy of the stored value so callers cannot mutate internal
// state through the returned slice.
func (m *StateMachine) Get(key []byte) ([]byte, bool) {
	v, ok := m.state[string(key)]
	if !ok {
		return nil, false
	}
	return cloneBytes(v), true
}
