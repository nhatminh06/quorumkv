package kv

// StateMachine is a deterministic in-memory key-value store. Applying the
// same ordered sequence of commands to two StateMachine values always
// produces the same resulting state.
//
// StateMachine is not safe for concurrent use. Callers needing concurrent
// access must synchronize externally.
type StateMachine struct {
	state map[string][]byte
}

func NewStateMachine() *StateMachine {
	return &StateMachine{state: make(map[string][]byte)}
}

// Apply executes a command against the state machine.
func (m *StateMachine) Apply(cmd Command) {
	switch cmd.Type {
	case CommandPut:
		m.state[string(cmd.Key)] = cloneBytes(cmd.Value)
	case CommandDelete:
		delete(m.state, string(cmd.Key))
	}
}

// Put is a convenience wrapper that applies a CommandPut.
func (m *StateMachine) Put(key, value []byte) {
	m.Apply(NewPutCommand(key, value))
}

// Delete is a convenience wrapper that applies a CommandDelete. Deleting a
// key that does not exist is a deterministic no-op.
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
