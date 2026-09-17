package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"quorumkv/internal/transport"
)

func TestPeerRPCInvalidResponseDiscardsConnection(t *testing.T) {
	for _, kind := range []transport.MessageType{transport.MessageTest, transport.MessageRequestVoteResponse} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			tr, err := transport.Listen("127.0.0.1:0", func(context.Context, transport.Message) (transport.Message, error) {
				// Wrong message type or correctly typed but truncated Raft payload.
				return transport.Message{Type: kind}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Close()
			c := transport.NewPeerClient()
			defer c.Close()
			rpc := &peerRPC{client: c}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for i := 0; i < 2; i++ {
				if _, err := rpc.sendOverTransport(ctx, tr.Addr(), RequestVoteRequest{}); err == nil {
					t.Fatal("invalid response accepted")
				}
			}
			if s := c.Stats(); s.ConnectionsDialed != 2 || s.ConnectionsClosed != 2 {
				t.Fatalf("unsafe reuse: %+v", s)
			}
		})
	}
}
