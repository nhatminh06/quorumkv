package raft

import (
	"context"
	"fmt"
	"time"

	"quorumkv/internal/observability"
	"quorumkv/internal/transport"
)

// peerRPC adapts bounded transport frames to typed Raft RPCs. Nodes own the
// client lifetime, but all socket state stays in transport. Existing typed
// sender overrides continue to support deterministic fault injection.
type peerRPC struct {
	client   transport.Client
	observer func() *observability.Metrics
}

func sendPeerRPC[T any](ctx context.Context, c transport.Client, addr string, msg transport.Message, want transport.MessageType, decode func([]byte) (T, error)) (result T, err error) {
	_, err = c.Send(ctx, addr, msg, func(resp transport.Message) error {
		if resp.Type != want {
			return fmt.Errorf("raft: unexpected response message type %d", resp.Type)
		}
		var decodeErr error
		result, decodeErr = decode(resp.Payload)
		return decodeErr
	})
	return result, err
}

func (p *peerRPC) observe(start time.Time, rpc string) {
	if p.observer == nil {
		return
	}
	if m := p.observer(); m != nil {
		m.RecordRPC(rpc, time.Since(start))
	}
}

// TransportStats reports outgoing peer connection observations only. It does
// not affect elections, replication, quorum decisions, or client behavior.
func (n *Node) TransportStats() transport.ClientStats { return n.peerClient.Stats() }
