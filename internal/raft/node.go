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

// preVoteSender issues a PreVote RPC to addr and returns the decoded
// response, mirroring sender's real-transport/fake-network
// substitutability.
type preVoteSender func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error)

func sendPreVoteOverTransport(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error) {
	msg := transport.NewMessage(transport.MessagePreVote, EncodePreVote(req))
	resp, err := transport.Send(ctx, addr, msg)
	if err != nil {
		return PreVoteResponse{}, err
	}
	if resp.Type != transport.MessagePreVoteResponse {
		return PreVoteResponse{}, fmt.Errorf("raft: unexpected response message type %d", resp.Type)
	}
	return DecodePreVoteResponse(resp.Payload)
}

// timeoutNowSender issues a TimeoutNow RPC to addr and returns the
// decoded response, mirroring sender's real-transport/fake-network
// substitutability.
type timeoutNowSender func(ctx context.Context, addr string, req TimeoutNowRequest) (TimeoutNowResponse, error)

func sendTimeoutNowOverTransport(ctx context.Context, addr string, req TimeoutNowRequest) (TimeoutNowResponse, error) {
	msg := transport.NewMessage(transport.MessageTimeoutNow, EncodeTimeoutNow(req))
	resp, err := transport.Send(ctx, addr, msg)
	if err != nil {
		return TimeoutNowResponse{}, err
	}
	if resp.Type != transport.MessageTimeoutNowResponse {
		return TimeoutNowResponse{}, fmt.Errorf("raft: unexpected response message type %d", resp.Type)
	}
	return DecodeTimeoutNowResponse(resp.Payload)
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
	sendPreVote         preVoteSender
	sendTimeoutNow      timeoutNowSender

	timeoutFunc       func() time.Duration
	heartbeatInterval time.Duration
	resetCh           chan struct{}
	// nowFunc is time.Now by default, injectable so tests can control
	// leader-contact recency (see hasRecentLeaderContactLocked)
	// deterministically instead of depending on real sleeps.
	nowFunc func() time.Time
	// lastLeaderContact is when this node last accepted valid AppendEntries
	// contact from a current/higher-term leader — the one signal PreVote's
	// leader-contact safeguard is built on (see docs/raft-election.md).
	// Zero value means "never observed" this process run.
	lastLeaderContact time.Time

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

	// membership is this node's effective configuration, always rebuilt
	// (never incrementally patched) from baseConfiguration plus every
	// EntryConfiguration entry surviving in the log — see
	// rebuildMembershipLocked. baseConfiguration is the stable
	// configuration as of the log's current BaseIndex: the most recent
	// snapshot's stored Configuration (or, for a legacy snapshot with no
	// stored Configuration, this node's own bootstrap configuration) if
	// one has ever been loaded/created/installed, or the bootstrap
	// configuration itself if not. Once any real Configuration entry
	// exists at or after BaseIndex+1, it is found by the rebuild walk and
	// wins over baseConfiguration regardless of what SetPeers/SetSelfAddr
	// are called with afterward — so persisted configuration history is
	// authoritative forever without needing a separate sticky flag.
	membership           Membership
	baseConfiguration    Configuration
	hasBaseConfiguration bool
	// membershipEntryIndex is the log index of the Configuration entry
	// that produced the current n.membership (0 if membership is still
	// just baseConfiguration, with no entry ever walked into it).
	// pendingStableIndex is the log index of an appended-but-not-yet-
	// committed final Stable entry following the current Joint
	// membership, or 0 if none — see rebuildMembershipLocked and
	// maybeCompleteMembershipTransitionLocked (config_change.go).
	// membershipChanged is pinged (non-blocking, buffered 1) every time a
	// rebuild produces a possibly-different membership, so AddVoter/
	// RemoveVoter can block on it instead of polling.
	membershipEntryIndex LogIndex
	pendingStableIndex   LogIndex
	membershipChanged    chan struct{}

	// transfer is this node's in-progress leadership transfer, if any (see
	// leadership_transfer.go) — nil when idle. Never persisted: a crash or
	// restart simply forgets it, which is correct (no transfer state
	// survives a process boundary). transferChanged is pinged (same
	// non-blocking, buffered-1 pattern as membershipChanged) whenever
	// something a transfer's waiters care about changes: matchIndex
	// (catch-up progress), LastIndex (new catch-up target), or
	// role/leaderID/currentTerm (transfer completion evidence).
	transfer        *transferState
	transferChanged chan struct{}
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
		sendPreVote:         sendPreVoteOverTransport,
		sendTimeoutNow:      sendTimeoutNowOverTransport,
		timeoutFunc:         randomElectionTimeout,
		heartbeatInterval:   defaultHeartbeatInterval,
		resetCh:             make(chan struct{}, 1),
		nowFunc:             time.Now,
		membershipChanged:   make(chan struct{}, 1),
		transferChanged:     make(chan struct{}, 1),
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
	if snap != nil {
		if snap.ConfigurationPresent {
			n.baseConfiguration = snap.Configuration
		} else {
			// Legacy (pre-Milestone-10) snapshot: fall back to this node's
			// own bootstrap configuration as the historical stable config.
			n.baseConfiguration = n.bootstrapConfigurationLocked()
		}
		n.hasBaseConfiguration = true
	}
	n.rebuildMembershipLocked()
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
	n.rebuildMembershipLocked()
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
	n.rebuildMembershipLocked()
}

// rebuildMembershipLocked recomputes n.membership from scratch: starting
// at baseConfiguration (the most recent snapshot's stored stable config,
// or this node's bootstrap configuration if none exists yet), walk every
// surviving log entry from BaseIndex+1 onward and apply each
// EntryConfiguration entry found in order:
//
//   - A Joint entry activates immediately, before it ever commits — a
//     node's effective membership is derived from its own local log, not
//     from a globally agreed "committed" source (see docs/membership.md).
//   - A Stable entry (completing a transition) activates only once it is
//     itself committed (index <= n.commitIndex); until then, effective
//     membership deliberately stays at whatever Joint state preceded it,
//     so quorum still requires both old and new majorities right up to
//     the moment the transition is truly final. This is a conservative
//     choice the spec calls out explicitly, not an oversight.
//
// Because this always re-derives from persisted history rather than
// patching in place, it is safe to call after anything that can change
// what that history means: log truncation (conflict repair), a newly
// appended/committed entry, or a snapshot install boundary change. Must
// be called with n.mu held.
func (n *Node) rebuildMembershipLocked() {
	base := n.bootstrapConfigurationLocked()
	if n.hasBaseConfiguration {
		base = n.baseConfiguration
	}
	effective := StableMembership(base)
	var entryIndex, pendingStableIndex LogIndex

	for idx := n.log.BaseIndex() + 1; idx <= n.log.LastIndex(); idx++ {
		e, ok := n.log.Entry(idx)
		if !ok || e.Kind != EntryConfiguration {
			continue
		}
		m, err := DecodeMembership(e.Command)
		if err != nil {
			continue // defensive: this package's own encoder never produces this
		}
		switch m.Mode {
		case ModeJoint:
			effective = m
			entryIndex = idx
			pendingStableIndex = 0
		case ModeStable:
			if idx <= n.commitIndex {
				effective = m
				entryIndex = idx
				pendingStableIndex = 0
			} else {
				// An uncommitted final Stable entry — leave effective as
				// the Joint state that preceded it, but remember this so a
				// leader doesn't append a second completing entry.
				pendingStableIndex = idx
			}
		}
	}
	n.membership = effective
	n.membershipEntryIndex = entryIndex
	n.pendingStableIndex = pendingStableIndex
	select {
	case n.membershipChanged <- struct{}{}:
	default:
	}
}

// resolveTargetsLocked returns the address every other effective voter
// should be dialed at: the ID set comes from n.membership.Targets (so
// joint-quorum-aware targeting, including a newly added not-yet-committed
// peer, is always correct), but the address for any ID this node
// currently has an entry for in n.peers wins over whatever address the
// (possibly stale, snapshot/log-derived) Membership itself has recorded —
// n.peers is this node's own freshest operational knowledge (kept current
// by SetPeers) and must not regress to a historical address just because
// a Configuration entry or snapshot boundary happens to embed one. A peer
// this node has never been separately told about via SetPeers (e.g. a
// brand-new joiner known only through a just-replicated Configuration
// entry) falls back to the Membership's own address. Must be called with
// n.mu held.
func (n *Node) resolveTargetsLocked() map[NodeID]string {
	targets := n.membership.Targets(n.id)
	for id := range targets {
		if addr, ok := n.peers[id]; ok {
			targets[id] = addr
		}
	}
	return targets
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

// SetPreVoteSend and SetTimeoutNowSend replace the functions this node
// uses to send PreVote/TimeoutNow RPCs, for the same deterministic
// fault-injection reasons as SetVoteSend/SetAppendSend.
func (n *Node) SetPreVoteSend(fn func(ctx context.Context, addr string, req PreVoteRequest) (PreVoteResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sendPreVote = fn
}

func (n *Node) SetTimeoutNowSend(fn func(ctx context.Context, addr string, req TimeoutNowRequest) (TimeoutNowResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sendTimeoutNow = fn
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
	case transport.MessagePreVote:
		req, err := DecodePreVote(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		resp, err := n.HandlePreVote(req)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.NewMessage(transport.MessagePreVoteResponse, EncodePreVoteResponse(resp)), nil
	case transport.MessageTimeoutNow:
		req, err := DecodeTimeoutNow(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		resp, err := n.HandleTimeoutNow(req)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.NewMessage(transport.MessageTimeoutNowResponse, EncodeTimeoutNowResponse(resp)), nil
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
	// A leadership-transfer catch-up/handoff waiter on this node needs to
	// notice losing leadership promptly, not only when its ctx eventually
	// expires (see leadership_transfer.go).
	n.pingTransferChanged()
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

	// A newly elected leader may be taking over mid-transition (the prior
	// leader died after a Joint entry committed but before appending the
	// completing Stable entry, or after appending it but before it
	// committed): resume/finalize automatically rather than leaving the
	// cluster stuck in Joint.
	n.maybeCompleteMembershipTransitionLocked()
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

// pingTransferChanged wakes any leadership-transfer waiter (catch-up or
// completion — see leadership_transfer.go) blocked on transferChanged, so
// it can re-check its condition instead of polling. Must be called with
// n.mu held (it only touches a channel, so this is really about keeping
// every call site obviously paired with the state change it announces).
func (n *Node) pingTransferChanged() {
	select {
	case n.transferChanged <- struct{}{}:
	default:
	}
}

// hasRecentLeaderContactLocked reports whether this node accepted valid
// AppendEntries contact from a current/higher-term leader within the last
// minElectionTimeout — PreVote's leader-contact safeguard (see
// docs/raft-election.md): a follower that has recently heard from a
// healthy leader must not grant a PreVote merely because some other node
// (e.g. one that is isolated, not the leader) timed out. Deliberately
// reuses minElectionTimeout, the same constant already governing the
// shortest legitimate election timeout, rather than a second
// independently-tuned window — "one clear election/timer model." Must be
// called with n.mu held.
func (n *Node) hasRecentLeaderContactLocked() bool {
	if n.lastLeaderContact.IsZero() {
		return false
	}
	return n.nowFunc().Sub(n.lastLeaderContact) < minElectionTimeout
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

// StartElection runs one PreVote-gated election attempt: first ask every
// peer, hypothetically, "would you vote for me in currentTerm+1?"
// (PreVote — see docs/raft-election.md) without touching any persistent
// state; only if that round reaches quorum does it proceed to a real
// election (startRealElection: increment currentTerm, vote for self,
// persist, RequestVote). PreVote is what keeps a node that has been
// isolated and timed out repeatedly from bumping the cluster term on
// every attempt — it can never even get that far without first proving
// it could win.
//
// StartElection does not retry and does not loop waiting for further
// responses after this attempt's round(s) come back; a subsequent
// election (whether from Run's timer or another explicit call) is a new
// attempt, normally in a higher term.
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

	prospectiveTerm := n.persistent.CurrentTerm + 1
	lastIndex, lastTerm := n.lastLogInfo()
	// A coherent membership snapshot for this one round (item 106): every
	// quorum decision below — the self-count fast path, and the final
	// tally — uses this exact value, never a freshly re-read n.membership
	// that could reflect a config change mid-round.
	roundMembership := n.membership
	granted := map[NodeID]bool{n.id: true}
	if roundMembership.HasQuorum(granted) {
		// Single-node (or otherwise self-sufficient) cluster: no need to
		// ask anyone hypothetically — go straight to a real election.
		n.mu.Unlock()
		return n.startRealElection(ctx)
	}
	peers := n.resolveTargetsLocked()
	n.mu.Unlock()

	req := PreVoteRequest{
		ProspectiveTerm: prospectiveTerm,
		CandidateID:     n.id,
		LastLogIndex:    lastIndex,
		LastLogTerm:     lastTerm,
	}

	// granted accumulates responses from multiple goroutines below, each
	// write guarded by n.mu (see applyPreVoteResponse) — not Node state,
	// just a plain map local to this one round, so a concurrent round (a
	// second StartElection call racing this one) cannot cross-contaminate
	// or be contaminated by this one; there is nothing here for a stale
	// response to corrupt (item 97/98).
	var wg sync.WaitGroup
	for id, addr := range peers {
		wg.Add(1)
		go func(id NodeID, addr string) {
			defer wg.Done()
			resp, err := n.sendPreVote(ctx, addr, req)
			if err != nil {
				return
			}
			n.applyPreVoteResponse(id, resp, granted)
		}(id, addr)
	}
	wg.Wait()

	n.mu.Lock()
	// Re-verify nothing moved on while this round was in flight: a higher
	// term learned from a PreVote response's real evidence (see
	// applyPreVoteResponse), a concurrent election already won, or a
	// membership change — any of these makes this round's tally stale, so
	// discard it rather than acting on it (item 106/107).
	stale := n.role == Leader || n.persistent.CurrentTerm != prospectiveTerm-1 || !n.membership.Equal(roundMembership)
	won := !stale && roundMembership.HasQuorum(granted)
	n.mu.Unlock()
	if !won {
		return nil // PreVote failed (or went stale): currentTerm/votedFor untouched, timer will retry
	}
	return n.startRealElection(ctx)
}

// applyPreVoteResponse validates a PreVote response before counting it in
// this round's local granted tally. A higher ACTUAL term in the response
// is real evidence this node is behind — unlike merely receiving a
// request for a higher prospective term, this is processed exactly like
// any other higher-term evidence (persist, clear votedFor, step down);
// PreVote must not suppress genuine higher-term information (item 21/90).
// granted is this one round's local map (see StartElection) — n.mu here
// only serializes concurrent writes to it from multiple response
// goroutines, the same map is never touched outside this round.
func (n *Node) applyPreVoteResponse(from NodeID, resp PreVoteResponse, granted map[NodeID]bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.persistent.CurrentTerm {
		_ = n.stepDownLocked(resp.Term) // best-effort; on failure state is unchanged
		return
	}
	if resp.VoteGranted {
		granted[from] = true
	}
}

// startRealElection performs the actual Raft election attempt: increment
// currentTerm, vote for self, persist that before sending anything, then
// request votes from every peer concurrently. If self-vote alone is
// already a majority (a single-node cluster), it becomes Leader with no
// network requests at all.
//
// Shared by two callers with different authorization: StartElection,
// only after its own PreVote round reaches quorum, and an authorized
// TimeoutNow-triggered leadership-transfer election (see
// HandleTimeoutNow), which deliberately bypasses PreVote entirely — the
// current leader has already authorized the transfer, so there is no
// disruption risk PreVote needs to guard against here (see
// docs/leadership-transfer.md).
func (n *Node) startRealElection(ctx context.Context) error {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return nil
	}
	if !n.membership.IsVoter(n.id) {
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

	peers := n.resolveTargetsLocked()
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

// HandlePreVote implements the Raft PreVote RPC handler. Unlike
// HandleRequestVote, it never mutates persistent state and never resets
// the election timer — a PreVote request is not evidence a valid leader
// exists, so receiving one must not be treated as leader contact (see
// docs/raft-election.md). The returned Term is always this node's actual
// current term, never a claim about having entered ProspectiveTerm.
func (n *Node) HandlePreVote(req PreVoteRequest) (PreVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.ProspectiveTerm <= n.persistent.CurrentTerm {
		// Would not advance the election; never grant for a term that
		// isn't actually ahead of what this node already has.
		return PreVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: false}, nil
	}
	if n.hasRecentLeaderContactLocked() {
		// The core PreVote-disruption-prevention rule: a node that has
		// recently heard from a healthy leader does not grant a
		// hypothetical vote just because some other node timed out.
		return PreVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: false}, nil
	}
	if !n.membership.IsVoter(req.CandidateID) || !n.membership.IsVoter(n.id) {
		// The candidate must be a member of this node's own effective
		// configuration, and this node itself must be a voter to
		// contribute a counted PreVote (passive/removed nodes do not).
		return PreVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: false}, nil
	}
	lastIndex, lastTerm := n.lastLogInfo()
	if !LogUpToDate(req.LastLogTerm, req.LastLogIndex, lastTerm, lastIndex) {
		return PreVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: false}, nil
	}
	return PreVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: true}, nil
}

// HandleTimeoutNow implements the Raft TimeoutNow RPC handler: an
// authorized leadership-transfer trigger, never a general-purpose
// "campaign now" request any peer can issue. It is accepted only from
// the exact leader/term this node currently recognizes, and only if this
// node is itself an effective voter; a genuinely higher term in the
// request is still processed as ordinary higher-term evidence first
// (persist, clear votedFor, step down — the same as every other RPC
// handler), but that alone never authorizes a campaign: stepping down
// clears leaderID, so the identity check below will then correctly fail
// for a request that turns out not to be from a leader this node
// actually recognized at that term (item 113) — no separate special case
// is needed to prevent an arbitrary higher-term peer from forcing a
// campaign this way.
//
// Once accepted, the real election (bypassing PreVote — see
// startRealElection) is kicked off in the background so this RPC itself
// returns promptly; Accepted=true means only that an election attempt
// was started, not that it will succeed (see leadership_transfer.go for
// how the transfer's actual success is observed).
func (n *Node) HandleTimeoutNow(req TimeoutNowRequest) (TimeoutNowResponse, error) {
	n.mu.Lock()
	if req.Term > n.persistent.CurrentTerm {
		if err := n.stepDownLocked(req.Term); err != nil {
			n.mu.Unlock()
			return TimeoutNowResponse{}, err
		}
	}
	if req.Term != n.persistent.CurrentTerm || n.leaderID == nil || *n.leaderID != req.LeaderID || !n.membership.IsVoter(n.id) {
		term := n.persistent.CurrentTerm
		n.mu.Unlock()
		return TimeoutNowResponse{Term: term, Accepted: false}, nil
	}
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	n.bgWG.Add(1)
	go func() {
		defer n.bgWG.Done()
		_ = n.startRealElection(n.bgCtx)
	}()
	return TimeoutNowResponse{Term: term, Accepted: true}, nil
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
	// is, too, and record it as the leader-contact evidence PreVote's
	// safeguard relies on (see hasRecentLeaderContactLocked) — a
	// leadership-transfer waiter may also be watching for this exact
	// contact as evidence its target became leader (see
	// leadership_transfer.go).
	n.resetTimer()
	leader := req.LeaderID
	n.leaderID = &leader
	n.lastLeaderContact = n.nowFunc()
	n.pingTransferChanged()

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
			// The truncated-away suffix may have carried an uncommitted
			// Configuration entry (this follower's effective membership
			// must revert to whatever preceded it), and/or the newly
			// appended suffix may carry one (which must activate
			// immediately) — either way, re-derive from scratch.
			n.rebuildMembershipLocked()
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
				// A previously-appended-but-uncommitted final Stable
				// configuration entry may now be covered by newCommit.
				n.rebuildMembershipLocked()
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

	targets := n.resolveTargetsLocked()
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
			// A leadership-transfer catch-up waiter may be watching this
			// peer's matchIndex specifically (see leadership_transfer.go).
			n.pingTransferChanged()
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
			// A previously-appended-but-uncommitted final Stable
			// configuration entry may now be covered by N.
			n.rebuildMembershipLocked()
			n.maybeCompleteMembershipTransitionLocked()
			n.stepDownIfNoLongerVoterLocked()
			return
		}
	}
}

// stepDownIfNoLongerVoterLocked converts a Leader to a passive Follower
// once its own committed final Stable configuration entry excludes it —
// self-removal (see RemoveVoter) is allowed to complete with this leader
// still leading right up until that point, but the moment the removal is
// truly final it must stop heartbeating/leading rather than continuing to
// act as leader of a cluster it is no longer a member of. This does not
// require a higher term first: membership, not term, is what disqualifies
// it. Must be called with n.mu held.
func (n *Node) stepDownIfNoLongerVoterLocked() {
	if n.role == Leader && !n.membership.IsVoter(n.id) {
		n.stepToFollowerLocked()
		n.leaderID = nil
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
	if n.transfer != nil && n.transfer.phase == transferHandoff {
		// Handoff freeze (see leadership_transfer.go): once the target is
		// caught up and TimeoutNow is about to be (or has been) sent, no
		// new entry may be appended — a further write here could make the
		// target stale again right as it's declared ready. This also
		// correctly blocks ensureCurrentTermCommitted's internal no-op
		// barrier (see read_index.go), which calls this same path.
		n.mu.Unlock()
		return 0, 0, ErrLeadershipTransferInProgress
	}
	term := n.persistent.CurrentTerm
	entry := LogEntry{Term: term, Command: cloneBytes(command)}
	if err := n.log.Append([]LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return 0, 0, err
	}
	index := n.log.LastIndex()
	n.maybeAdvanceCommitIndexLocked() // handles the single-node-cluster case
	n.pingTransferChanged()           // LastIndex moved: a transfer catch-up waiter may need to keep replicating
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
