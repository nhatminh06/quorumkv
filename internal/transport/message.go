// Package transport implements bounded framing and TCP request/response
// delivery for messages exchanged between QuorumKV nodes. It knows nothing
// about Raft (terms, elections, replication) — it only answers "how do I
// safely send a bounded message from one node to another." See
// docs/transport.md for the wire format and delivery guarantees.
package transport

// MessageType identifies the kind of payload a Message carries. It is a
// transport-level tag only — transport treats every payload as opaque
// bytes regardless of type. MessagePing/MessagePong/MessageTest exist so
// this package's own tests can exercise framing and request/response
// delivery without depending on the raft package (transport must not
// import raft). MessageRequestVote/MessageRequestVoteResponse carry the
// Raft RequestVote RPC and MessageAppendEntries/
// MessageAppendEntriesResponse carry the Raft AppendEntries RPC; their
// payload encoding lives in package raft.
type MessageType uint8

const (
	MessagePing MessageType = iota + 1
	MessagePong
	MessageTest
	MessageRequestVote
	MessageRequestVoteResponse
	MessageAppendEntries
	MessageAppendEntriesResponse
)

// Message is a generic envelope transport carries between nodes.
//
// Message owns its Payload once constructed: NewMessage copies the input
// slice so a caller mutating its original buffer afterward cannot change
// the Message, and decoding never aliases the underlying connection's read
// buffer.
type Message struct {
	Type    MessageType
	Payload []byte
}

// NewMessage copies payload so the returned Message does not alias the
// caller's slice.
func NewMessage(typ MessageType, payload []byte) Message {
	return Message{Type: typ, Payload: cloneBytes(payload)}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
