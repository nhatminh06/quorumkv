package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"quorumkv/internal/transport"
)

// Default election timeout range. Randomization within this range is
// what keeps followers from all timing out together; the exact bounds
// are not load-bearing for correctness. defaultHeartbeatInterval is kept
// well below minElectionTimeout so a healthy leader's heartbeats reliably
// beat a follower's election timeout.
const (
	minElectionTimeout       = 150 * time.Millisecond
	maxElectionTimeout       = 300 * time.Millisecond
	defaultHeartbeatInterval = 50 * time.Millisecond
)

// ErrNotLeader is returned by Propose when this node is not currently
// Leader.
var ErrNotLeader = errors.New("raft: not leader")

// ErrReservedCommand is returned by Propose for an empty command: a
// zero-length LogEntry.Command is reserved for Raft's own internal
// current-term barrier no-op (see read_index.go) and may never be
// manufactured by an application-level caller.
var ErrReservedCommand = errors.New("raft: empty command is reserved for an internal Raft no-op")

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

// appendSender issues an AppendEntries RPC to addr and returns the
// decoded response, mirroring sender's real-transport/fake-network
// substitutability.
type appendSender func(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)

func sendAppendOverTransport(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	payload, err := EncodeAppendEntries(req)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	msg := transport.NewMessage(transport.MessageAppendEntries, payload)
	resp, err := transport.Send(ctx, addr, msg)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	if resp.Type != transport.MessageAppendEntriesResponse {
		return AppendEntriesResponse{}, fmt.Errorf("raft: unexpected response message type %d", resp.Type)
	}
	return DecodeAppendEntriesResponse(resp.Payload)
}

// Node is a single Raft participant: persistent term/vote/log, volatile
// role, in-progress vote counting, and (while Leader) replication state.
// See docs/raft-election.md and docs/raft-log-replication.md.
//
// A single mutex protects all of Node's state. Network I/O (RequestVote
// and AppendEntries RPCs sent to peers) never happens while that mutex is
// held: StartElection and replicateToAllPeers each snapshot what they
// need, unlock, perform I/O, and re-lock only to apply each response.
type Node struct {
	id                  NodeID
	store               *Store
	log                 *Log
	commitStore         *CommitStore
	snapshotStore       *SnapshotStore
	peers               map[NodeID]string // NodeID -> address, excluding self
	selfAddr            string            // this node's own dialable address, for Configuration entries other nodes need to resolve it by
	send                sender
	sendAppend          appendSender
	sendInstallSnapshot installSnapshotSender

	timeoutFunc       func() time.Duration
	heartbeatInterval time.Duration
	resetCh           chan struct{}

	// bgCtx/bgCancel bound this Node's own background work (heartbeat and
	// apply loops), independent of whatever short-lived ctx a particular
	// StartElection/HandleRequestVote/HandleAppendEntries call happens to
	// receive. Close cancels it and waits (via bgWG) for every background
	// goroutine spawned under it to actually exit before returning, so a
	// caller that has called Close can rely on no further heartbeats or
	// application happening — not just that cancellation was requested.
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWG     sync.WaitGroup

	mu         sync.Mutex
	persistent PersistentState
	role       Role
	votes      map[NodeID]bool // valid only while role == Candidate
	// leaderID is this node's current belief about who leads the current
	// term: itself once it becomes Leader, the sender of the last valid
	// AppendEntries it accepted, or nil ("unknown") once it becomes
	// Candidate or steps down to a higher term with no leader contact yet.
	// Never persisted.
	leaderID *NodeID

	// Leader-only volatile replication state; re-initialized every time
	// this node becomes Leader and never persisted.
	nextIndex  map[NodeID]LogIndex
	matchIndex map[NodeID]LogIndex
	// snapshotSending guards against starting a second concurrent
	// InstallSnapshot transfer to a peer while one is already in flight.
	snapshotSending map[NodeID]bool
	// leaderCancel stops this leadership term's heartbeat loop; nil
	// unless role == Leader.
	leaderCancel context.CancelFunc

	// commitIndex is durably recorded via commitStore whenever it
	// advances (persisted before becoming visible in memory), so restart
	// knows exactly which prefix of the log is safe to replay.
	commitIndex LogIndex

	// Application pipeline: see apply.go. lastApplied/applying/applyErr/
	// waiters are all volatile — reconstructed by replaying the log up to
	// the restored commitIndex on every startup, never persisted directly.
	// applyMu (distinct from mu, the Raft state lock) serializes ApplyFunc
	// against SnapshotFunc/RestoreFunc so a snapshot always corresponds to
	// exactly the lastApplied index it claims — see CreateSnapshot.
	applyFunc   ApplyFunc
	snapshotFn  SnapshotFunc
	restoreFn   RestoreFunc
	applyMu     sync.Mutex
	lastApplied LogIndex
	applying    bool
	applyErr    error
	waiters     []*applyWaiter

	// incoming is this node's in-progress InstallSnapshot transfer
	// session as a follower, if any (see snapshot_node.go).
	incoming *incomingSnapshot

	// Read-path state (see read_index.go), all volatile and never
	// persisted: readContextCounter generates unique ReadContext values
	// for this process's active ReadIndex probes, and pendingBarrier
	// tracks (at most one) in-flight current-term commit barrier per
	// term so concurrent first reads in a new term single-flight onto
	// one no-op rather than each appending their own.
	readContextCounter uint64
	pendingBarrier     *pendingBarrier

	// membership is this node's effective configuration — see
	// membership.go. membershipFromLog is false until a Configuration log
	// entry has ever been applied to it; while false, membership tracks
	// peers/selfAddr directly (bootstrap). Once true, persisted
	// configuration history is authoritative forever and SetPeers/
	// SetSelfAddr no longer overwrite it.
	membership        Membership
	membershipFromLog bool
}

// NewNode loads persistent state, the Raft log, the durably recorded
// commitIndex, and the canonical snapshot (if any), and constructs a
// Node that starts, as every Raft node must on restart, as a Follower.
//
// If a snapshot exists, its state is restored (via restoreFn) and
// lastApplied/commitIndex start from at least its lastIncludedIndex
// before anything else happens — never regressing either. If the log was
// not yet compacted through the snapshot's boundary (a crash between
// Milestone 7's mandatory "persist snapshot, then compact log" ordering
// left that step unfinished), NewNode finishes it here rather than
// treating it as corruption: Log.Compact is idempotent and safe to
// re-apply.
//
// NewNode then begins (in the background) replaying any
// committed-but-unapplied prefix of the log through applyFunc — call
// WaitApplied(ctx, node's initial CommitIndex, 0) to block until that
// replay completes if the caller needs the state machine ready before
// serving anything. applyFunc/snapshotFn/restoreFn may be nil: committed
// entries are then counted as applied without any actual application
// work, useful for tests that only exercise Raft itself. A nil
// snapshotFn makes CreateSnapshot fail; a nil restoreFn is a no-op.
//
// NewNode returns an error if the durably recorded commitIndex, or the
// snapshot's lastIncludedIndex, exceeds the log's last index — states
// that should never occur from this package's own persistence ordering,
// and are treated as corruption rather than silently clamped.
func NewNode(id NodeID, store *Store, log *Log, commitStore *CommitStore, snapshotStore *SnapshotStore, peers map[NodeID]string, applyFunc ApplyFunc, snapshotFn SnapshotFunc, restoreFn RestoreFunc) (*Node, error) {
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	commitIndex, err := commitStore.Load()
	if err != nil {
		return nil, err
	}
	snap, err := snapshotStore.Load()
	if err != nil {
		return nil, err
	}
	if snap != nil {
		if snap.LastIncludedIndex > log.LastIndex() {
			return nil, fmt.Errorf("raft: snapshot index %d exceeds log length %d", snap.LastIncludedIndex, log.LastIndex())
		}
		if snap.LastIncludedIndex > log.BaseIndex() {
			if err := log.Compact(snap.LastIncludedIndex, snap.LastIncludedTerm); err != nil {
				return nil, err
			}
		}
		if commitIndex < snap.LastIncludedIndex {
			commitIndex = snap.LastIncludedIndex
			if err := commitStore.Save(commitIndex); err != nil {
				return nil, err
			}
		}
	}
	if commitIndex > log.LastIndex() {
		return nil, fmt.Errorf("raft: persisted commitIndex %d exceeds log length %d", commitIndex, log.LastIndex())
	}
	if applyFunc == nil {
		applyFunc = func(LogIndex, []byte) error { return nil }
	}
	if restoreFn == nil {
		restoreFn = func([]byte) error { return nil }
	}
	bgCtx, bgCancel := context.WithCancel(context.Background())
	n := &Node{
		id:                  id,
		store:               store,
		log:                 log,
		commitStore:         commitStore,
		snapshotStore:       snapshotStore,
		peers:               peers,
		send:                sendOverTransport,
		sendAppend:          sendAppendOverTransport,
		sendInstallSnapshot: sendInstallSnapshotOverTransport,
		timeoutFunc:         randomElectionTimeout,
		heartbeatInterval:   defaultHeartbeatInterval,
		resetCh:             make(chan struct{}, 1),
		bgCtx:               bgCtx,
		bgCancel:            bgCancel,
		persistent:          state,
		role:                Follower,
		nextIndex:           make(map[NodeID]LogIndex),
		matchIndex:          make(map[NodeID]LogIndex),
		snapshotSending:     make(map[NodeID]bool),
		commitIndex:         commitIndex,
		applyFunc:           applyFunc,
		snapshotFn:          snapshotFn,
		restoreFn:           restoreFn,
	}
	if snap != nil {
		if err := restoreFn(snap.Data); err != nil {
			return nil, fmt.Errorf("raft: restoring snapshot at startup: %w", err)
		}
		n.lastApplied = snap.LastIncludedIndex
	}
	n.mu.Lock()
	n.recomputeMembershipLocked()
	n.kickApplyLocked()
	n.mu.Unlock()
	return n, nil
}

// Close stops this node's background heartbeat/replication/apply work
// and waits for it to actually finish before returning. Safe to call
// whether or not this node is currently Leader.
func (n *Node) Close() {
	n.bgCancel()
	n.bgWG.Wait()
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
	n.recomputeMembershipLocked()
}

// SetSelfAddr records this node's own dialable address. It exists for
// initial cluster bootstrap, alongside SetPeers, so this node's bootstrap
// Configuration (used until any real membership change is ever applied)
// has a real address for itself — needed so a newly-added future peer,
// or a snapshot's stored stable configuration, can resolve it. Before
// this is ever called, a non-empty placeholder is used; since Targets
// always excludes self, correctness of replication/election/quorum never
// depends on this value being real, only its later use in a Configuration
// handed to another node does.
func (n *Node) SetSelfAddr(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.selfAddr = addr
	n.recomputeMembershipLocked()
}

// recomputeMembershipLocked refreshes n.membership from n.peers/n.selfAddr
// (the bootstrap configuration) unless persisted configuration-entry
// history has ever been applied, in which case that history is
// authoritative forever and bootstrap inputs must never overwrite it. Must
// be called with n.mu held.
func (n *Node) recomputeMembershipLocked() {
	if n.membershipFromLog {
		return
	}
	n.membership = StableMembership(n.bootstrapConfigurationLocked())
}

// bootstrapConfigurationLocked builds the Configuration this node starts
// with when no persisted membership-change history exists: itself plus
// every currently known peer. Must be called with n.mu held.
func (n *Node) bootstrapConfigurationLocked() Configuration {
	selfAddr := n.selfAddr
	if selfAddr == "" {
		selfAddr = fmt.Sprintf("unresolved-self-%d", n.id)
	}
	voters := make(map[NodeID]string, len(n.peers)+1)
	voters[n.id] = selfAddr
	for id, addr := range n.peers {
		voters[id] = addr
	}
	cfg, err := NewConfiguration(voters)
	if err != nil {
		// peers/selfAddr are always validated non-empty by NewConfiguration
		// itself at every call site that supplies real addresses; an empty
		// peers map still yields a valid single-voter (self-only)
		// Configuration, so this should be unreachable.
		panic(fmt.Sprintf("raft: invalid bootstrap configuration: %v", err))
	}
	return cfg
}

// SetVoteSend and SetAppendSend replace the functions this node uses to
// send RequestVote/AppendEntries RPCs. Production code never calls
// these — the defaults (sendOverTransport/sendAppendOverTransport)
// already go over the real transport. They exist so deterministic
// fault-injection tests outside this package can wrap the real sender
// with an allow/block decision while still delegating to real TCP for
// anything allowed through, rather than replacing the network path
// entirely.
func (n *Node) SetVoteSend(fn func(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.send = fn
}

func (n *Node) SetAppendSend(fn func(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sendAppend = fn
}

// SetInstallSnapshotSend replaces the function this node uses to send
// InstallSnapshot RPCs, for the same deterministic fault-injection reasons
// as SetVoteSend/SetAppendSend.
func (n *Node) SetInstallSnapshotSend(fn func(ctx context.Context, addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sendInstallSnapshot = fn
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
	case transport.MessageAppendEntries:
		req, err := DecodeAppendEntries(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		resp, err := n.HandleAppendEntries(req)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.NewMessage(transport.MessageAppendEntriesResponse, EncodeAppendEntriesResponse(resp)), nil
	case transport.MessageInstallSnapshot:
		req, err := DecodeInstallSnapshot(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		resp, err := n.HandleInstallSnapshot(req)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.NewMessage(transport.MessageInstallSnapshotResponse, EncodeInstallSnapshotResponse(resp)), nil
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

// LeaderHint returns this node's current belief about who leads the
// current term, if known: itself if it is Leader, or the sender of the
// last valid AppendEntries it accepted. ok is false if unknown (e.g. this
// node is Candidate, or recently stepped up to a higher term with no
// leader contact yet) — callers must not fabricate a destination in that
// case.
func (n *Node) LeaderHint() (id NodeID, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == nil {
		return 0, false
	}
	return *n.leaderID, true
}

// lastLogInfo returns this node's last log index/term. Must be called
// with n.mu held.
func (n *Node) lastLogInfo() (LogIndex, Term) {
	return n.log.LastIndex(), n.log.LastTerm()
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
	n.stepToFollowerLocked()
	n.leaderID = nil // a higher term alone doesn't tell us who leads it
	return nil
}

// stepToFollowerLocked converts a Candidate or Leader to Follower without
// touching persistent term/vote state — used when valid same-term leader
// contact proves another leader already exists for this term (Raft
// requires stepping down here, but not a term change). It also stops this
// node's own heartbeat loop, if it had one. Must be called with n.mu held.
func (n *Node) stepToFollowerLocked() {
	n.role = Follower
	n.votes = nil
	if n.leaderCancel != nil {
		n.leaderCancel()
		n.leaderCancel = nil
	}
}

// becomeLeaderLocked transitions to Leader: initializes nextIndex/
// matchIndex for every peer and starts this leadership term's heartbeat
// loop, bound to n.bgCtx (not to whatever short-lived ctx triggered the
// transition) so it keeps running until this node steps down or Close is
// called. Must be called with n.mu held.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	self := n.id
	n.leaderID = &self
	last := n.log.LastIndex()
	targets := n.membership.Targets(n.id)
	n.nextIndex = make(map[NodeID]LogIndex, len(targets))
	n.matchIndex = make(map[NodeID]LogIndex, len(targets))
	n.snapshotSending = make(map[NodeID]bool, len(targets))
	for id := range targets {
		n.nextIndex[id] = last + 1
		n.matchIndex[id] = 0
	}
	leaderCtx, cancel := context.WithCancel(n.bgCtx)
	n.leaderCancel = cancel
	n.bgWG.Add(1)
	go n.heartbeatLoop(leaderCtx)
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
	if n.membership.IsVoter(n.id) && (n.persistent.VotedFor == nil || *n.persistent.VotedFor == req.CandidateID) {
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
	if !n.membership.IsVoter(n.id) {
		// A node that is not an effective voter (e.g. removed, or not yet
		// activated as a new joiner) must not campaign.
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
	n.leaderID = nil // becoming a candidate means we no longer trust the old leader
	n.votes = map[NodeID]bool{n.id: true}
	lastIndex, lastTerm := n.lastLogInfo()

	if n.membership.HasQuorum(n.votes) {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		return nil
	}

	peers := n.membership.Targets(n.id)
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

	if n.membership.HasQuorum(n.votes) {
		n.becomeLeaderLocked()
	}
}

// HandleAppendEntries implements the Raft AppendEntries RPC handler,
// including heartbeats (Entries == nil/empty). Order of operations:
// reject stale terms; step down (persisting first) on a newer term, or
// convert to Follower without a term change if a Candidate/Leader sees
// valid same-term leader contact; reset the election timer for any
// accepted current-term leader contact regardless of what the log
// consistency check below finds; check prevLogIndex/prevLogTerm; then
// repair/append entries, preserving any already-matching prefix and
// persisting before reporting success.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.persistent.CurrentTerm {
		return AppendEntriesResponse{Term: n.persistent.CurrentTerm, Success: false, ReadContext: req.ReadContext}, nil
	}
	if req.Term > n.persistent.CurrentTerm {
		if err := n.stepDownLocked(req.Term); err != nil {
			return AppendEntriesResponse{}, err
		}
	} else if n.role != Follower {
		// Same term, valid leader contact: another leader already exists
		// for this term, so a Candidate or (in principle) a Leader must
		// step down — without changing term/vote.
		n.stepToFollowerLocked()
	}

	// This is valid contact from the current-term leader: reset the
	// election timer even if the log consistency check below fails,
	// since the sender is still that leader and a rejection here isn't a
	// reason for this follower to start its own election. Track who it
	// is, too.
	n.resetTimer()
	leader := req.LeaderID
	n.leaderID = &leader

	localPrevTerm, ok := n.log.Term(req.PrevLogIndex)
	if !ok || localPrevTerm != req.PrevLogTerm {
		return AppendEntriesResponse{Term: n.persistent.CurrentTerm, Success: false, ReadContext: req.ReadContext}, nil
	}

	if len(req.Entries) > 0 {
		conflictAt := LogIndex(0)
		for i, e := range req.Entries {
			idx := req.PrevLogIndex + LogIndex(i) + 1
			localTerm, ok := n.log.Term(idx)
			if !ok || localTerm != e.Term {
				conflictAt = idx
				break
			}
		}
		if conflictAt != 0 {
			newEntries := req.Entries[conflictAt-req.PrevLogIndex-1:]
			if err := n.log.TruncateAndAppend(conflictAt, newEntries); err != nil {
				return AppendEntriesResponse{}, err
			}
			// Truncation may have superseded an entry some waiter (e.g.
			// this node's own now-abandoned leadership attempt) was
			// waiting on; re-check them now rather than leaving them
			// blocked until their ctx times out.
			n.notifyWaitersLocked()
		}
		// If conflictAt stays 0, every incoming entry already matched the
		// local log — an idempotent retransmission — so no write happens.
	}

	lastNewIndex := req.PrevLogIndex + LogIndex(len(req.Entries))
	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if last := n.log.LastIndex(); last < newCommit {
			newCommit = last
		}
		if newCommit > n.commitIndex {
			// Persist before this follower's own recovery state treats
			// newCommit as durable; if persistence fails, silently retry
			// on the next AppendEntries/heartbeat rather than advancing
			// an unrecorded commitIndex.
			if err := n.commitStore.Save(newCommit); err == nil {
				n.commitIndex = newCommit
				n.kickApplyLocked()
			}
		}
	}

	return AppendEntriesResponse{Term: n.persistent.CurrentTerm, Success: true, MatchIndex: lastNewIndex, ReadContext: req.ReadContext}, nil
}

// replicationRequest bundles one peer's outbound AppendEntries with the
// addressing needed to send it, computed while the lock is held.
type replicationRequest struct {
	id   NodeID
	addr string
	req  AppendEntriesRequest
}

// replicateToAllPeers sends one round of AppendEntries (a heartbeat if a
// peer has nothing new) to every peer concurrently. It is called both by
// the periodic heartbeat loop and immediately after a successful Propose.
//
// A peer whose nextIndex has fallen at or behind the log's compacted base
// (this leader no longer retains the entries that peer would need) is
// diverted to InstallSnapshot instead: the transfer is started in the
// background (it may take many rounds' worth of wall-clock time to send
// all chunks) rather than joined into this round's AppendEntries wait, and
// snapshotSending guards against starting a second concurrent transfer to
// the same peer while one is already in flight.
func (n *Node) replicateToAllPeers(ctx context.Context) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.persistent.CurrentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	baseIndex := n.log.BaseIndex()

	targets := n.membership.Targets(n.id)
	reqs := make([]replicationRequest, 0, len(targets))
	var snapshotPeers []replicationRequest // reused only for id/addr
	for id, addr := range targets {
		next := n.nextIndex[id]
		if next < 1 {
			next = 1
		}
		if baseIndex > 0 && next <= baseIndex {
			if !n.snapshotSending[id] {
				n.snapshotSending[id] = true
				snapshotPeers = append(snapshotPeers, replicationRequest{id: id, addr: addr})
			}
			continue
		}
		prevIndex := next - 1
		prevTerm, _ := n.log.Term(prevIndex)
		entries := n.log.EntriesFrom(next)
		if len(entries) > maxEntriesPerAppend {
			entries = entries[:maxEntriesPerAppend]
		}
		reqs = append(reqs, replicationRequest{id: id, addr: addr, req: AppendEntriesRequest{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}})
	}
	n.mu.Unlock()

	for _, r := range snapshotPeers {
		n.bgWG.Add(1)
		go func(id NodeID, addr string) {
			defer n.bgWG.Done()
			// Bound to n.bgCtx, not the ctx this replication round was
			// called with: a snapshot transfer spans many chunks and must
			// keep running after this round's (possibly short-lived,
			// e.g. Propose's) ctx has already returned.
			n.sendSnapshotToPeer(n.bgCtx, term, id, addr)
		}(r.id, r.addr)
	}

	var wg sync.WaitGroup
	for _, r := range reqs {
		wg.Add(1)
		go func(r replicationRequest) {
			defer wg.Done()
			resp, err := n.sendAppend(ctx, r.addr, r.req)
			if err != nil {
				return
			}
			n.applyAppendEntriesResponse(term, r.id, r.req, resp)
		}(r)
	}
	wg.Wait()
}

// applyAppendEntriesResponse validates an AppendEntries response against
// current state before applying it: a higher term forces step-down; a
// response for a term/role this node has already moved on from is stale
// and ignored. On success, matchIndex/nextIndex are advanced but
// matchIndex never regresses (guards against a stale-but-successful
// older response). On failure, nextIndex backs off by one (never below
// 1) for a retry on the next round.
func (n *Node) applyAppendEntriesResponse(sentTerm Term, from NodeID, req AppendEntriesRequest, resp AppendEntriesResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.persistent.CurrentTerm {
		_ = n.stepDownLocked(resp.Term) // best-effort; on failure state is unchanged
		return
	}
	if n.role != Leader || n.persistent.CurrentTerm != sentTerm {
		return
	}

	if resp.Success {
		newMatch := req.PrevLogIndex + LogIndex(len(req.Entries))
		if newMatch > n.matchIndex[from] {
			n.matchIndex[from] = newMatch
			n.nextIndex[from] = newMatch + 1
		}
		n.maybeAdvanceCommitIndexLocked()
		return
	}
	if n.nextIndex[from] > 1 {
		n.nextIndex[from]--
	}
}

// maybeAdvanceCommitIndexLocked implements Raft's commit rule: commitIndex
// may advance to N only if a majority (including self) has matchIndex >=
// N AND log[N].term == currentTerm. An entry from an older term is never
// committed by majority replication alone — it can only become committed
// as a side effect of committing a later current-term entry. Must be
// called with n.mu held.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	last := n.log.LastIndex()
	for N := last; N > n.commitIndex; N-- {
		term, ok := n.log.Term(N)
		if !ok || term != n.persistent.CurrentTerm {
			continue
		}
		acked := map[NodeID]bool{n.id: true}
		for id, match := range n.matchIndex {
			if match >= N {
				acked[id] = true
			}
		}
		if n.membership.HasQuorum(acked) {
			// N is already logically committed cluster-wide (a majority
			// has it) regardless of what happens next. But this node must
			// not treat that as *durably recorded* — and so must not let
			// application/client-visible success advance past it — until
			// persisting the new commitIndex here succeeds. On failure,
			// leave commitIndex unchanged and let the next trigger retry;
			// do not claim N became uncommitted.
			if err := n.commitStore.Save(N); err != nil {
				return
			}
			n.commitIndex = N
			n.kickApplyLocked()
			return
		}
	}
}

// heartbeatLoop sends AppendEntries to every peer immediately upon
// becoming Leader, then every heartbeatInterval, until ctx is canceled
// (step-down, term change, or Close). One heartbeatLoop runs per
// leadership term; becomeLeaderLocked/stepToFollowerLocked/stepDownLocked
// ensure the previous one is always stopped before a new one can start.
func (n *Node) heartbeatLoop(ctx context.Context) {
	defer n.bgWG.Done()
	n.replicateToAllPeers(ctx)
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.replicateToAllPeers(ctx)
		}
	}
}

// Propose appends command as a new entry in the current term to this
// node's local log, persisting it before returning, and kicks off
// immediate replication to peers. It does not wait for commitment or
// application — pass the returned (index, term) to WaitApplied for that.
//
// Propose fails with ErrNotLeader if this node is not currently Leader,
// and with ErrReservedCommand if command is empty — a zero-length command
// is reserved for Raft's own internal current-term barrier no-op (see
// read_index.go) and can only be appended through that internal path,
// never by an application-level caller. If local persistence fails, the
// log is left unchanged and the error is returned; the entry is never
// treated as proposed. The returned term is the exact term the entry was
// created with (read atomically with the append, avoiding a
// check-then-act race with CurrentTerm changing between two separate
// calls), needed by WaitApplied to detect if this entry is later
// superseded by conflict repair before ever committing.
func (n *Node) Propose(command []byte) (LogIndex, Term, error) {
	if len(command) == 0 {
		return 0, 0, ErrReservedCommand
	}
	return n.proposeLocked(command)
}

// proposeLocked appends command (which may legally be empty only when
// called internally, e.g. by ensureCurrentTermCommitted) as a new
// current-term entry and kicks off replication. See Propose for the
// externally-visible contract; this is the shared implementation both it
// and the internal no-op barrier path use.
func (n *Node) proposeLocked(command []byte) (LogIndex, Term, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	term := n.persistent.CurrentTerm
	entry := LogEntry{Term: term, Command: cloneBytes(command)}
	if err := n.log.Append([]LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return 0, 0, err
	}
	index := n.log.LastIndex()
	n.maybeAdvanceCommitIndexLocked() // handles the single-node-cluster case
	n.mu.Unlock()

	n.bgWG.Add(1)
	go func() {
		defer n.bgWG.Done()
		n.replicateToAllPeers(n.bgCtx)
	}()
	return index, term, nil
}

// CommitIndex returns the highest log index this node currently
// considers committed.
func (n *Node) CommitIndex() LogIndex {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LogEntry returns the entry at index, if any.
func (n *Node) LogEntry(index LogIndex) (LogEntry, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.Entry(index)
}

// LastLogIndex returns this node's last local log index.
func (n *Node) LastLogIndex() LogIndex {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.LastIndex()
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
