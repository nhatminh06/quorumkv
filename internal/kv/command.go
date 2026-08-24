package kv

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
// NewPutCommand/NewDeleteCommand rather than building a Command literal
// directly, so callers cannot retain a mutable alias into stored data.
type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte
}

// NewPutCommand copies key and value so later mutation of the caller's
// slices cannot change the command after construction.
func NewPutCommand(key, value []byte) Command {
	return Command{Type: CommandPut, Key: cloneBytes(key), Value: cloneBytes(value)}
}

// NewDeleteCommand copies key for the same reason as NewPutCommand.
func NewDeleteCommand(key []byte) Command {
	return Command{Type: CommandDelete, Key: cloneBytes(key)}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
