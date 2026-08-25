// Package service wires a raft.Node to a kv.StateMachine and serves the
// client protocol (package clientproto): PUT/DELETE go through Raft
// (Propose + WaitApplied) and only succeed once committed and applied;
// GET goes through raft.Node.ReadIndex (a quorum-confirmed ReadIndex
// probe, establishing a current-term commit barrier first if needed) and
// then WaitApplied on the returned index before reading local state — see
// docs/read-index.md.
//
// Since Milestone 9, PUT/DELETE carry a request identity (ClientID,
// Sequence — see internal/reqid) that the replicated kv.StateMachine
// deduplicates: a retried write that reaches Apply twice mutates state
// at most once, and the leader answers a recognized retry with the
// original cached result rather than proposing (and waiting on) a
// redundant Raft entry. This is what makes it safe for
// internal/client to automatically retry an ambiguous PUT/DELETE (a
// transport failure, TIMEOUT, or NOT_LEADER redirect no longer means the
// client must give up) — see docs/request-dedup.md.
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
	"quorumkv/internal/reqid"
	"quorumkv/internal/transport"
)

// DefaultMaxConcurrentRequests bounds how many client requests (PUT,
// DELETE, and GET alike) this Service will admit into dispatch at once.
// A request beyond the bound is rejected immediately with StatusBusy
// rather than queued: item 53 — an overloaded server should answer fast
// and let the client's own bounded retry (see internal/client) pace
// itself, not accumulate thousands of goroutines blocked waiting for a
// semaphore slot. 256 is generous for ordinary load while still bounding
// worst-case concurrent work (goroutines, in-flight Raft proposals,
// state-machine lock contention) under a flood.
const DefaultMaxConcurrentRequests = 256

// requestTimeout bounds how long this node waits for its own request
// processing — commit+apply for PUT/DELETE, ReadIndex quorum
// confirmation + WaitApplied for GET — before giving up and reporting
// StatusTimeout, independent of whatever deadline the remote client used
// for the transport round trip. It is derived from the ctx
// transport.Handler is called with — which is the serving Transport's own
// lifecycle context (canceled on that Transport's Close), not something
// carrying the client's actual per-request deadline — so this also bounds
// ordinary requests to a sane duration rather than only reacting to
// server shutdown.
const requestTimeout = 5 * time.Second

// pendingKey identifies one in-flight identified write by request
// identity — see pending below.
type pendingKey struct {
	id  reqid.ClientID
	seq reqid.Sequence
}

// pendingWrite coalesces concurrent callers on this leader for the same
// in-flight (ClientID, Sequence): the first caller proposes and waits;
// any concurrent retry for the exact same request identity and
// fingerprint waits on the same completion instead of appending a
// second (harmless but redundant) Raft entry. This is leader-local,
// volatile optimization state — never persisted, never authoritative
// (see docs/request-dedup.md) — cleared once the write reaches a
// terminal outcome, so it never grows unbounded.
type pendingWrite struct {
	fingerprint reqid.Fingerprint
	done        chan struct{}
	resp        clientproto.Response
}

// Service owns the KV state machine and dispatches the client protocol on
// top of a raft.Node. kv.StateMachine is not safe for concurrent use, so
// every access to it (the apply callback, dedup lookups, and GET) is
// serialized through mu — a service-level lock distinct from raft.Node's
// own internal lock.
type Service struct {
	node  *raft.Node
	peers map[raft.NodeID]string // for resolving a leader hint to an address

	mu sync.Mutex
	sm *kv.StateMachine

	// resMu/results/completed let proposeAndWaitIdentified learn the
	// ApplyOutcome Apply produced for the specific index it proposed,
	// without polling or re-deriving it. The caller only registers a
	// waiter AFTER Propose has already returned the index — so Apply can
	// legitimately run first if commit+apply happens to win that race
	// (increasingly likely now that Propose can hand off to a batching
	// worker goroutine — see raft/proposal.go — instead of returning
	// synchronously in the same call). completed holds an outcome Apply
	// produced with no waiter registered yet, so a registration arriving
	// afterward still finds it instead of waiting on a channel nothing
	// will ever write to. Every index that ever reaches either map
	// always gets claimed by the one registerResultWaiter call
	// proposeAndWaitIdentified makes right after its own Propose
	// succeeds, so neither map accumulates entries for indexes no one is
	// coming back for.
	resMu     sync.Mutex
	results   map[raft.LogIndex]chan kv.ApplyOutcome
	completed map[raft.LogIndex]kv.ApplyOutcome

	// pendMu/pending implement in-flight coalescing (see pendingWrite).
	pendMu  sync.Mutex
	pending map[pendingKey]*pendingWrite

	// admission bounds concurrent in-flight client requests — see
	// DefaultMaxConcurrentRequests/handleClient. A buffered channel used
	// purely as a counting semaphore: acquire is a non-blocking send,
	// release a receive, capacity is the concurrency bound.
	admission chan struct{}
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
//	node, err := raft.NewNode(id, store, log, commitStore, snapshotStore, peers, svc.Apply, svc.Snapshot, svc.Restore)
//	svc.Attach(node)
func New(peers map[raft.NodeID]string) *Service {
	return &Service{
		peers:     peers,
		sm:        kv.NewStateMachine(),
		results:   make(map[raft.LogIndex]chan kv.ApplyOutcome),
		completed: make(map[raft.LogIndex]kv.ApplyOutcome),
		pending:   make(map[pendingKey]*pendingWrite),
		admission: make(chan struct{}, DefaultMaxConcurrentRequests),
	}
}

// SetMaxConcurrentRequests overrides the concurrency bound handleClient
// admits under — tests use this to construct a tiny bound (e.g. 1 or 2)
// so overload/BUSY behavior is deterministic rather than needing real
// load to trigger. Call before serving any traffic; not meant to be
// tuned live in production.
func (s *Service) SetMaxConcurrentRequests(n int) {
	s.admission = make(chan struct{}, n)
}

// Attach completes construction by giving the Service the raft.Node it
// serves. Must be called once, before Handler/dispatch are used.
func (s *Service) Attach(node *raft.Node) {
	s.node = node
}

// Apply is a raft.ApplyFunc: decode the committed command and apply it to
// the KV state machine (including request dedup — see kv.StateMachine.
// Apply). raft.Node calls this strictly in log order, exactly once per
// index per process run, and never while holding its own internal lock.
// Pass this to raft.NewNode's applyFunc parameter.
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
	outcome := s.sm.Apply(cmd)
	s.mu.Unlock()

	s.resMu.Lock()
	if ch, ok := s.results[index]; ok {
		ch <- outcome
		delete(s.results, index)
	} else {
		// No one has registered for this index yet (see the Service
		// struct's doc comment) — remember the outcome so the
		// registration that is still coming finds it instead of
		// blocking on a channel Apply will never write to again (Apply
		// runs each index exactly once).
		s.completed[index] = outcome
	}
	s.resMu.Unlock()
	return nil
}

// Snapshot is a raft.SnapshotFunc: serialize the current KV+dedup state
// (see kv.StateMachine.Snapshot). Pass to raft.NewNode's snapshotFn
// parameter.
func (s *Service) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sm.Snapshot()
}

// Restore is a raft.RestoreFunc: replace KV+dedup state wholesale from a
// snapshot (see kv.StateMachine.Restore). Pass to raft.NewNode's
// restoreFn parameter.
func (s *Service) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sm.Restore(data)
}

// registerResultWaiter records interest in index's eventual ApplyOutcome,
// mirroring raft.Node's own applyWaiter pattern: register before/around
// waiting so a fast Apply can never "complete before anyone was
// listening." Callers must always eventually pair this with either a
// successful read from the returned channel (which Apply also cleans up
// after) or removeResultWaiter (see the call sites) — never both, never
// neither, so map entries cannot accumulate for indexes that will never
// be applied on this node (a superseded proposal, a timed-out caller).
func (s *Service) registerResultWaiter(index raft.LogIndex) chan kv.ApplyOutcome {
	ch := make(chan kv.ApplyOutcome, 1)
	s.resMu.Lock()
	if outcome, ok := s.completed[index]; ok {
		delete(s.completed, index)
		s.resMu.Unlock()
		ch <- outcome
		return ch
	}
	s.results[index] = ch
	s.resMu.Unlock()
	return ch
}

func (s *Service) removeResultWaiter(index raft.LogIndex) {
	s.resMu.Lock()
	delete(s.results, index)
	s.resMu.Unlock()
}

// Handler returns the transport.Handler that serves both this node's Raft
// RPCs and the client protocol on the same listener.
func (s *Service) Handler() transport.Handler {
	raftHandler := s.node.Handler()
	return func(ctx context.Context, m transport.Message) (transport.Message, error) {
		switch m.Type {
		case transport.MessageClientRequest:
			return s.handleClient(ctx, m)
		case transport.MessageAdminRequest:
			return s.handleAdmin(ctx, m)
		default:
			return raftHandler(ctx, m)
		}
	}
}

func (s *Service) handleClient(ctx context.Context, m transport.Message) (transport.Message, error) {
	// Bound concurrent in-flight work before doing anything else — a
	// non-blocking acquire, never a wait: a request beyond capacity gets
	// an immediate BUSY instead of piling up a blocked goroutine (item
	// 53). ch is captured locally so release always targets the exact
	// channel this request acquired from, even if SetMaxConcurrentRequests
	// were (against its own documented precondition) called concurrently.
	ch := s.admission
	select {
	case ch <- struct{}{}:
		defer func() { <-ch }()
	default:
		return s.respond(clientproto.Response{Status: clientproto.StatusBusy})
	}

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
// followers never propose commands, and never answer a write from local
// state as if authoritative even if they happen to already have it
// applied (see docs/request-dedup.md).
func (s *Service) dispatch(ctx context.Context, req clientproto.Request) clientproto.Response {
	if s.node.Role() != raft.Leader {
		return s.notLeaderResponse()
	}
	switch req.Operation {
	case clientproto.OpPut:
		return s.write(ctx, kv.NewIdentifiedPutCommand(req.ClientID, req.Sequence, req.Key, req.Value))
	case clientproto.OpDelete:
		return s.write(ctx, kv.NewIdentifiedDeleteCommand(req.ClientID, req.Sequence, req.Key))
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

// write implements the identified PUT/DELETE flow (see
// docs/request-dedup.md item 35):
//
//	in-flight coalescing check
//	     v
//	local apply catch-up (WaitApplied to this node's own commitIndex —
//	   never ReadIndex: write dedup is governed by Raft proposal/commit,
//	   not quorum-confirmed reads)
//	     v
//	replicated dedup lookup: duplicate/stale/conflict short-circuit,
//	   or unseen -> propose + wait + inspect the real apply outcome
//
// cmd's ClientID/Sequence are assumed already validated non-zero by
// clientproto.DecodeRequest — dispatch only reaches here for PUT/DELETE,
// which DecodeRequest already requires identity for.
func (s *Service) write(ctx context.Context, cmd kv.Command) clientproto.Response {
	key := pendingKey{id: cmd.ClientID, seq: cmd.Sequence}
	fp := kv.Fingerprint(cmd)

	if resp, ok := s.joinPending(ctx, key, fp); ok {
		return resp
	}

	waitCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err := s.node.WaitApplied(waitCtx, s.node.CommitIndex(), 0); err != nil {
		return s.readFailureResponse(err)
	}

	s.mu.Lock()
	lookup := s.sm.LookupRequest(cmd.ClientID, cmd.Sequence, fp)
	s.mu.Unlock()
	switch lookup {
	case kv.AppliedDuplicate:
		return clientproto.Response{Status: clientproto.StatusOK}
	case kv.RequestConflict:
		return clientproto.Response{Status: clientproto.StatusRequestConflict}
	case kv.StaleRequest:
		return clientproto.Response{Status: clientproto.StatusStaleRequest}
	}

	pw := s.beginPending(key, fp)
	resp := s.proposeAndWaitIdentified(waitCtx, cmd)
	s.finishPending(key, pw, resp)
	return resp
}

// joinPending checks for an in-flight write with the same request
// identity: a matching fingerprint waits on its completion (coalescing,
// per docs/request-dedup.md item 39); a mismatched one is an immediate
// RequestConflict (item 40) without ever touching Raft.
func (s *Service) joinPending(ctx context.Context, key pendingKey, fp reqid.Fingerprint) (clientproto.Response, bool) {
	s.pendMu.Lock()
	pw, ok := s.pending[key]
	if !ok {
		s.pendMu.Unlock()
		return clientproto.Response{}, false
	}
	if pw.fingerprint != fp {
		s.pendMu.Unlock()
		return clientproto.Response{Status: clientproto.StatusRequestConflict}, true
	}
	s.pendMu.Unlock()

	select {
	case <-pw.done:
		return pw.resp, true
	case <-ctx.Done():
		return clientproto.Response{Status: clientproto.StatusTimeout}, true
	}
}

func (s *Service) beginPending(key pendingKey, fp reqid.Fingerprint) *pendingWrite {
	pw := &pendingWrite{fingerprint: fp, done: make(chan struct{})}
	s.pendMu.Lock()
	s.pending[key] = pw
	s.pendMu.Unlock()
	return pw
}

func (s *Service) finishPending(key pendingKey, pw *pendingWrite, resp clientproto.Response) {
	pw.resp = resp
	close(pw.done)
	s.pendMu.Lock()
	if s.pending[key] == pw {
		delete(s.pending, key)
	}
	s.pendMu.Unlock()
}

// proposeAndWaitIdentified encodes cmd, proposes it, waits for it to
// commit and apply, and inspects the real ApplyOutcome (via a
// registered result waiter — see registerResultWaiter) rather than
// assuming success means AppliedNew: a duplicate or conflicting entry
// can just as validly reach this exact code path (e.g. two proposals for
// the same request racing across a failover) and must be reported
// accurately, not treated as a fresh success.
func (s *Service) proposeAndWaitIdentified(ctx context.Context, cmd kv.Command) clientproto.Response {
	encoded, err := kv.EncodeCommand(cmd)
	if err != nil {
		return clientproto.Response{Status: clientproto.StatusBadRequest}
	}
	index, term, err := s.node.Propose(encoded)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return s.notLeaderResponse()
		}
		if errors.Is(err, raft.ErrLeadershipTransferInProgress) {
			// This leader is intentionally handing off; treat it as the
			// same kind of transient, safely-retryable-with-the-same-
			// request-identity outcome as a context/quorum timeout (see
			// docs/request-dedup.md) — not an internal error.
			return clientproto.Response{Status: clientproto.StatusTimeout}
		}
		if errors.Is(err, raft.ErrBackpressure) {
			// The proposal queue is full: nothing was appended, nothing
			// applied — safe to retry with the same request identity,
			// same as any other BUSY rejection (see docs/request-dedup.md).
			return clientproto.Response{Status: clientproto.StatusBusy}
		}
		return clientproto.Response{Status: clientproto.StatusInternalError}
	}

	resCh := s.registerResultWaiter(index)
	if err := s.node.WaitApplied(ctx, index, term); err != nil {
		s.removeResultWaiter(index)
		return s.readFailureResponse(err)
	}
	// WaitApplied succeeded, meaning lastApplied reached index for THIS
	// exact (term-verified) entry — Apply already ran for it and already
	// delivered to resCh before that became observable, so this receive
	// is non-blocking.
	outcome := <-resCh

	switch outcome {
	case kv.AppliedNew, kv.AppliedDuplicate:
		return clientproto.Response{Status: clientproto.StatusOK}
	case kv.RequestConflict:
		return clientproto.Response{Status: clientproto.StatusRequestConflict}
	case kv.StaleRequest:
		return clientproto.Response{Status: clientproto.StatusStaleRequest}
	default:
		return clientproto.Response{Status: clientproto.StatusInternalError}
	}
}

// get implements the quorum-confirmed linearizable read path: ReadIndex
// establishes (or reuses) a current-term commit barrier and confirms this
// node still holds leadership over a quorum, returning a committed index
// safe to read through; this node then waits until its own application
// of the log has caught up to at least that index before consulting local
// state. The read is linearized at the successful quorum confirmation
// inside ReadIndex, not at this function's role check — see
// docs/read-index.md. GET carries no request identity and is not
// deduplicated (see docs/request-dedup.md item 124).
func (s *Service) get(ctx context.Context, key []byte) clientproto.Response {
	waitCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	readIndex, err := s.node.ReadIndex(waitCtx)
	if err != nil {
		return s.readFailureResponse(err)
	}
	if err := s.node.WaitApplied(waitCtx, readIndex, 0); err != nil {
		return s.readFailureResponse(err)
	}

	s.mu.Lock()
	v, ok := s.sm.Get(key)
	s.mu.Unlock()
	if !ok {
		return clientproto.Response{Status: clientproto.StatusNotFound}
	}
	return clientproto.Response{Status: clientproto.StatusOK, Value: v}
}

// readFailureResponse maps a ReadIndex/WaitApplied failure to a client
// status. A node that actually stepped down (or was never leader) is
// NOT_LEADER, with a hint if this node has one; any bounded quorum/context
// failure is TIMEOUT — never a stale value, never NOT_FOUND standing in
// for "could not confirm." Despite the name, it is also used for the
// PUT/DELETE apply-wait path, whose failures map identically.
func (s *Service) readFailureResponse(err error) clientproto.Response {
	if errors.Is(err, raft.ErrNotLeader) {
		return s.notLeaderResponse()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, raft.ErrReadIndexUnavailable) || errors.Is(err, raft.ErrNodeClosed) ||
		errors.Is(err, raft.ErrLeadershipTransferInProgress) {
		return clientproto.Response{Status: clientproto.StatusTimeout}
	}
	return clientproto.Response{Status: clientproto.StatusInternalError}
}
