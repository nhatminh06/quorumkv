// Package service wires a raft.Node to a kv.StateMachine and serves the
// client protocol (package clientproto): PUT/DELETE go through Raft
// (Propose + WaitApplied) and only succeed once committed and applied;
// GET is served leader-only from locally applied state.
//
// GET is deliberately narrower than PUT/DELETE: it is not replicated, and
// it is not yet quorum-confirmed linearizable. A partitioned former
// leader could in principle still believe it is Leader long enough to
// answer a GET from stale local state; that gap is not closed until a
// future milestone adds ReadIndex or an equivalent quorum-confirmed read.
package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"quorumkv/internal/clientproto"
	"quorumkv/internal/kv"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// writeTimeout bounds how long this node waits for its own commit+apply
// before giving up and reporting StatusTimeout, independent of whatever
// deadline the remote client used for the transport round trip. It is
// derived from the ctx transport.Handler is called with — which is the
// serving Transport's own lifecycle context (canceled on that Transport's
// Close), not something carrying the client's actual per-request
// deadline — so this also bounds ordinary requests to a sane duration
// rather than only reacting to server shutdown.
const writeTimeout = 5 * time.Second

// Service owns the KV state machine and dispatches the client protocol on
// top of a raft.Node. kv.StateMachine is not safe for concurrent use, so
// every access to it (the apply callback and GET) is serialized through
// mu — a service-level lock distinct from raft.Node's own internal lock.
type Service struct {
	node  *raft.Node
	peers map[raft.NodeID]string // for resolving a leader hint to an address

	mu sync.Mutex
	sm *kv.StateMachine
}

// New constructs a Service. peers is used only to resolve a NOT_LEADER
// response's leader hint to an address; it should be the same peer table
// the eventual node is constructed with.
//
// New deliberately does not take a *raft.Node: raft.NewNode itself needs
// this Service's Apply method as its applyFunc, so the node cannot exist
// yet when the Service is constructed. Call Attach once the node exists:
//
//	svc := service.New(peers)
//	node, err := raft.NewNode(id, store, log, commitStore, peers, svc.Apply)
//	svc.Attach(node)
func New(peers map[raft.NodeID]string) *Service {
	return &Service{peers: peers, sm: kv.NewStateMachine()}
}

// Attach completes construction by giving the Service the raft.Node it
// serves. Must be called once, before Handler/dispatch are used.
func (s *Service) Attach(node *raft.Node) {
	s.node = node
}

// Apply is a raft.ApplyFunc: decode the committed command and apply it to
// the KV state machine. raft.Node calls this strictly in log order,
// exactly once per index per process run, and never while holding its
// own internal lock. Pass this to raft.NewNode's applyFunc parameter.
//
// A malformed committed command is a serious local failure, not a
// skippable one — restart recovery must not trust disk merely because
// the data originated locally (a previous run could have written a
// command this build's codec no longer accepts, for instance). Returning
// an error here permanently halts this node's application pipeline for
// this process run (see raft.Node.ApplyError).
func (s *Service) Apply(index raft.LogIndex, command []byte) error {
	cmd, err := kv.DecodeCommand(command)
	if err != nil {
		return fmt.Errorf("service: malformed committed command at index %d: %w", index, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sm.Apply(cmd)
	return nil
}

// Handler returns the transport.Handler that serves both this node's Raft
// RPCs and the client protocol on the same listener.
func (s *Service) Handler() transport.Handler {
	raftHandler := s.node.Handler()
	return func(ctx context.Context, m transport.Message) (transport.Message, error) {
		if m.Type == transport.MessageClientRequest {
			return s.handleClient(ctx, m)
		}
		return raftHandler(ctx, m)
	}
}

func (s *Service) handleClient(ctx context.Context, m transport.Message) (transport.Message, error) {
	req, err := clientproto.DecodeRequest(m.Payload)
	if err != nil {
		return s.respond(clientproto.Response{Status: clientproto.StatusBadRequest})
	}
	return s.respond(s.dispatch(ctx, req))
}

func (s *Service) respond(r clientproto.Response) (transport.Message, error) {
	payload, err := clientproto.EncodeResponse(r)
	if err != nil {
		return transport.Message{}, err
	}
	return transport.NewMessage(transport.MessageClientResponse, payload), nil
}

// dispatch rejects a follower's request before touching Raft at all —
// followers never propose commands.
func (s *Service) dispatch(ctx context.Context, req clientproto.Request) clientproto.Response {
	if s.node.Role() != raft.Leader {
		return s.notLeaderResponse()
	}
	switch req.Operation {
	case clientproto.OpPut:
		return s.put(ctx, req.Key, req.Value)
	case clientproto.OpDelete:
		return s.delete(ctx, req.Key)
	case clientproto.OpGet:
		return s.get(ctx, req.Key)
	default:
		return clientproto.Response{Status: clientproto.StatusBadRequest}
	}
}

func (s *Service) notLeaderResponse() clientproto.Response {
	var hint []byte
	if id, ok := s.node.LeaderHint(); ok {
		if addr, ok := s.peers[id]; ok {
			hint = []byte(addr)
		}
	}
	return clientproto.Response{Status: clientproto.StatusNotLeader, LeaderHint: hint}
}

func (s *Service) put(ctx context.Context, key, value []byte) clientproto.Response {
	cmd, err := kv.EncodeCommand(kv.NewPutCommand(key, value))
	if err != nil {
		return clientproto.Response{Status: clientproto.StatusBadRequest}
	}
	return s.proposeAndWait(ctx, cmd)
}

func (s *Service) delete(ctx context.Context, key []byte) clientproto.Response {
	cmd, err := kv.EncodeCommand(kv.NewDeleteCommand(key))
	if err != nil {
		return clientproto.Response{Status: clientproto.StatusBadRequest}
	}
	return s.proposeAndWait(ctx, cmd)
}

// proposeAndWait implements the required write ordering: encode -> Propose
// -> WaitApplied -> respond OK. It never returns OK before WaitApplied
// confirms the entry was committed and applied.
func (s *Service) proposeAndWait(ctx context.Context, encodedCmd []byte) clientproto.Response {
	index, term, err := s.node.Propose(encodedCmd)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return s.notLeaderResponse()
		}
		return clientproto.Response{Status: clientproto.StatusInternalError}
	}

	waitCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := s.node.WaitApplied(waitCtx, index, term); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// The write's outcome is uncertain here, not negative: the
			// entry may still commit later if it was legitimately
			// proposed. This response only means this node did not
			// observe completion within the bound.
			return clientproto.Response{Status: clientproto.StatusTimeout}
		}
		return clientproto.Response{Status: clientproto.StatusInternalError}
	}
	return clientproto.Response{Status: clientproto.StatusOK}
}

func (s *Service) get(ctx context.Context, key []byte) clientproto.Response {
	waitCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	// Make sure locally known committed entries are applied before
	// reading — this is a leader-local guarantee, not a quorum-confirmed
	// read: see the package doc comment.
	if err := s.node.WaitApplied(waitCtx, s.node.CommitIndex(), 0); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return clientproto.Response{Status: clientproto.StatusTimeout}
		}
		return clientproto.Response{Status: clientproto.StatusInternalError}
	}

	s.mu.Lock()
	v, ok := s.sm.Get(key)
	s.mu.Unlock()
	if !ok {
		return clientproto.Response{Status: clientproto.StatusNotFound}
	}
	return clientproto.Response{Status: clientproto.StatusOK, Value: v}
}
