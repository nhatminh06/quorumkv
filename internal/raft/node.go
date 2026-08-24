package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"quorumkv/internal/transport"
)

// Default election timeout range. Randomization within this range is
// what keeps followers from all timing out together; the exact bounds
// are not load-bearing for correctness.
const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	span := maxElectionTimeout - minElectionTimeout
	return minElectionTimeout + time.Duration(rand.Int63n(int64(span)))
}

// sender issues a RequestVote RPC to addr and returns the decoded
// response. The default implementation goes over real TCP via package
// transport; tests substitute an in-process implementation that calls a
// peer Node's HandleRequestVote directly, so the exact same election
// logic in StartElection is exercised by both deterministic pure tests
// and real-socket integration tests.
type sender func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error)

func sendOverTransport(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	msg := transport.NewMessage(transport.MessageRequestVote, EncodeRequestVote(req))
	resp, err := transport.Send(ctx, addr, msg)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	if resp.Type != transport.MessageRequestVoteResponse {
		return RequestVoteResponse{}, fmt.Errorf("raft: unexpected response message type %d", resp.Type)
	}
	return DecodeRequestVoteResponse(resp.Payload)
}

// Node is a single Raft participant's election state: persistent
// term/vote, volatile role, and in-progress vote counting. It does not
// implement AppendEntries, heartbeats, or log replication — see
// docs/raft-election.md.
//
// A single mutex protects all of Node's state. Network I/O (RequestVote
// RPCs sent to peers) never happens while that mutex is held: StartElection
// snapshots what it needs, unlocks, performs I/O, and re-locks only to
// apply each response.
type Node struct {
	id    NodeID
	store *Store
	peers map[NodeID]string // NodeID -> address, fixed for this milestone
	send  sender

	timeoutFunc func() time.Duration
	resetCh     chan struct{}

	mu         sync.Mutex
	persistent PersistentState
	role       Role
	votes      map[NodeID]bool // valid only while role == Candidate
}

// NewNode loads persistent state from store and constructs a Node that
// starts, as every Raft node must on restart, as a Follower.
func NewNode(id NodeID, store *Store, peers map[NodeID]string) (*Node, error) {
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	return &Node{
		id:          id,
		store:       store,
		peers:       peers,
		send:        sendOverTransport,
		timeoutFunc: randomElectionTimeout,
		resetCh:     make(chan struct{}, 1),
		persistent:  state,
		role:        Follower,
	}, nil
}

// Handler returns the transport.Handler that dispatches inbound Raft RPC
// messages to this node. Wire it up with transport.Listen.
func (n *Node) Handler() transport.Handler {
	return n.handleMessage
}

// SetPeers replaces this node's peer address table. It exists for
// initial cluster bootstrap, where a node must start listening (to learn
// its own OS-assigned address) before every peer's address is known —
// it is not a dynamic membership-change API.
func (n *Node) SetPeers(peers map[NodeID]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers = peers
}

func (n *Node) handleMessage(_ context.Context, m transport.Message) (transport.Message, error) {
	switch m.Type {
	case transport.MessageRequestVote:
		req, err := DecodeRequestVote(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		resp, err := n.HandleRequestVote(req)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.NewMessage(transport.MessageRequestVoteResponse, EncodeRequestVoteResponse(resp)), nil
	default:
		return transport.Message{}, fmt.Errorf("raft: unexpected message type %d", m.Type)
	}
}

// Role returns the node's current volatile role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// CurrentTerm returns the node's current persistent term.
func (n *Node) CurrentTerm() Term {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.persistent.CurrentTerm
}

// VotedFor returns a copy of who this node voted for in CurrentTerm, or
// nil if it has not voted yet.
func (n *Node) VotedFor() *NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.persistent.VotedFor == nil {
		return nil
	}
	v := *n.persistent.VotedFor
	return &v
}

// lastLogInfo returns this node's last log index/term. Until log
// replication exists, every node behaves as though its log is empty.
// Must be called with n.mu held.
func (n *Node) lastLogInfo() (LogIndex, Term) {
	return 0, 0
}

// stepDownLocked updates currentTerm to newTerm, clears votedFor, and
// becomes Follower — persisting before any of that is externally visible.
// If persistence fails, no in-memory state is changed and the error is
// returned; callers must not treat the step-down as having happened.
// Must be called with n.mu held.
func (n *Node) stepDownLocked(newTerm Term) error {
	prev := n.persistent
	next := PersistentState{CurrentTerm: newTerm, VotedFor: nil}
	if err := n.store.Save(next); err != nil {
		n.persistent = prev
		return err
	}
	n.persistent = next
	n.role = Follower
	n.votes = nil
	return nil
}

// resetTimer requests that Run restart its election timeout. It is
// called only when this node grants a vote in HandleRequestVote — there
// is no heartbeat-based reset because AppendEntries does not exist yet,
// so a granted vote is the only signal available this milestone that the
// cluster is making election progress.
func (n *Node) resetTimer() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

// HandleRequestVote implements the Raft RequestVote RPC handler: reject
// stale terms, step down (persisting first) on a newer term, then grant
// the vote if this node hasn't voted for someone else this term and the
// candidate's log is at least as up to date. A vote is never reported as
// granted unless persisting it first succeeded.
func (n *Node) HandleRequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.persistent.CurrentTerm {
		return RequestVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: false}, nil
	}
	if req.Term > n.persistent.CurrentTerm {
		if err := n.stepDownLocked(req.Term); err != nil {
			return RequestVoteResponse{}, err
		}
	}

	grant := false
	if n.persistent.VotedFor == nil || *n.persistent.VotedFor == req.CandidateID {
		lastIndex, lastTerm := n.lastLogInfo()
		if LogUpToDate(req.LastLogTerm, req.LastLogIndex, lastTerm, lastIndex) {
			grant = true
		}
	}

	if grant {
		if n.persistent.VotedFor == nil {
			prev := n.persistent
			v := req.CandidateID
			next := prev
			next.VotedFor = &v
			if err := n.store.Save(next); err != nil {
				return RequestVoteResponse{}, err
			}
			n.persistent = next
		}
		n.resetTimer()
	}

	return RequestVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: grant}, nil
}

// StartElection transitions this node to Candidate and runs one election
// attempt: increment currentTerm, vote for self, persist that before
// sending anything, then request votes from every peer concurrently. If
// self-vote alone is already a majority (a single-node cluster), it
// becomes Leader with no network requests at all.
//
// StartElection does not retry and does not loop waiting for further
// votes after this attempt's responses come back; a subsequent election
// (whether from Run's timer or another explicit call) is a new attempt in
// a higher term.
func (n *Node) StartElection(ctx context.Context) error {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return nil
	}

	prev := n.persistent
	newTerm := prev.CurrentTerm + 1
	self := n.id
	next := PersistentState{CurrentTerm: newTerm, VotedFor: &self}
	if err := n.store.Save(next); err != nil {
		n.mu.Unlock()
		return err
	}
	n.persistent = next
	n.role = Candidate
	n.votes = map[NodeID]bool{n.id: true}
	lastIndex, lastTerm := n.lastLogInfo()

	clusterSize := len(n.peers) + 1
	if len(n.votes) >= Majority(clusterSize) {
		n.role = Leader
		n.mu.Unlock()
		return nil
	}

	peers := make(map[NodeID]string, len(n.peers))
	for id, addr := range n.peers {
		peers[id] = addr
	}
	n.mu.Unlock()

	req := RequestVoteRequest{
		Term:         newTerm,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	var wg sync.WaitGroup
	for id, addr := range peers {
		wg.Add(1)
		go func(id NodeID, addr string) {
			defer wg.Done()
			resp, err := n.send(ctx, addr, req)
			if err != nil {
				return
			}
			n.applyVoteResponse(newTerm, id, resp)
		}(id, addr)
	}
	wg.Wait()
	return nil
}

// applyVoteResponse validates a RequestVote response against the current
// state before counting it: a higher term forces step-down; a response
// for a term/role this node has already moved on from is stale and
// ignored; a duplicate grant from the same peer is not double-counted.
func (n *Node) applyVoteResponse(electionTerm Term, from NodeID, resp RequestVoteResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.persistent.CurrentTerm {
		_ = n.stepDownLocked(resp.Term) // best-effort; on failure state is unchanged
		return
	}
	if n.role != Candidate || n.persistent.CurrentTerm != electionTerm {
		return
	}
	if !resp.VoteGranted {
		return
	}
	if n.votes[from] {
		return
	}
	n.votes[from] = true

	if len(n.votes) >= Majority(len(n.peers)+1) {
		n.role = Leader
	}
}

// Run drives this node's election timer until ctx is canceled: whenever
// the timeout fires and the node is not already Leader, it starts an
// election. The timer restarts whenever the timeout fires, an election
// attempt completes, or resetTimer is called (a granted vote). Run is the
// production timer path; tests generally call StartElection directly for
// deterministic control and use Run only to prove the timer path itself
// works, with an injected short timeoutFunc.
func (n *Node) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.resetCh:
			continue
		case <-time.After(n.timeoutFunc()):
			if n.Role() != Leader {
				n.StartElection(ctx)
			}
		}
	}
}
