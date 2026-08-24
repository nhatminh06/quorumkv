package kv

import "quorumkv/internal/reqid"

// CommandType identifies the kind of state-changing operation a Command
// represents. GET is a read and is not represented as a Command since it
// does not change state and therefore has nothing to persist or replay.
type CommandType uint8

const (
	CommandPut CommandType = iota + 1
	CommandDelete
)

// Command is a deterministic, replayable description of a state-changing
// operation. Key and Value are owned by the Command once constructed; use
// NewPutCommand/NewDeleteCommand/NewIdentifiedPutCommand/
// NewIdentifiedDeleteCommand rather than building a Command literal
// directly, so callers cannot retain a mutable alias into stored data.
//
// ClientID/Sequence (since Milestone 9) identify one logical write for
// deduplication — see docs/request-dedup.md. A zero ClientID (with a
// necessarily zero Sequence) means this Command carries no request
// identity: it is applied unconditionally, exactly like every Command
// before Milestone 9, and never touches the dedup table. This is the
// shape every pre-Milestone-9 committed log entry has, and is also valid
// for any future internal, non-client-driven command that has no
// meaningful client session to deduplicate against.
type Command struct {
	Type     CommandType
	ClientID reqid.ClientID
	Sequence reqid.Sequence
	Key      []byte
	Value    []byte
}

// NewPutCommand copies key and value so later mutation of the caller's
// slices cannot change the command after construction. The resulting
// Command carries no request identity (ClientID/Sequence are zero) — see
// NewIdentifiedPutCommand for a deduplicated client write.
func NewPutCommand(key, value []byte) Command {
	return Command{Type: CommandPut, Key: cloneBytes(key), Value: cloneBytes(value)}
}

// NewDeleteCommand copies key for the same reason as NewPutCommand, and
// likewise carries no request identity.
func NewDeleteCommand(key []byte) Command {
	return Command{Type: CommandDelete, Key: cloneBytes(key)}
}

// NewIdentifiedPutCommand builds a PUT command carrying request identity
// for deduplication. id must be non-zero and seq must be non-zero —
// EncodeCommand rejects an identified command that violates either.
func NewIdentifiedPutCommand(id reqid.ClientID, seq reqid.Sequence, key, value []byte) Command {
	return Command{Type: CommandPut, ClientID: id, Sequence: seq, Key: cloneBytes(key), Value: cloneBytes(value)}
}

// NewIdentifiedDeleteCommand builds a DELETE command carrying request
// identity, with the same id/seq requirements as
// NewIdentifiedPutCommand.
func NewIdentifiedDeleteCommand(id reqid.ClientID, seq reqid.Sequence, key []byte) Command {
	return Command{Type: CommandDelete, ClientID: id, Sequence: seq, Key: cloneBytes(key)}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
