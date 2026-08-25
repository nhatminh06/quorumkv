package raft

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrBackpressure is returned by Propose when admitting the command
// would exceed this node's configured proposal-queue bounds (count or
// bytes — see MaxQueuedProposals/MaxQueuedProposalBytes). It is distinct
// from ErrNotLeader (this node may well be a healthy leader, just
// momentarily overloaded) and from any storage error (nothing was
// written or even attempted): the command was never admitted, never
// appended, and never applied — safe to retry with the same request
// identity, and retrying does not risk a duplicate effect (see
// docs/request-dedup.md).
var ErrBackpressure = errors.New("raft: proposal queue is full")

// Default proposal admission/batching bounds (see SetProposalLimits to
// override, e.g. for deterministic backpressure tests).
//
// MaxQueuedProposals/MaxQueuedProposalBytes bound how much unpersisted
// client work this node will hold onto at once: 4096 proposals or 32 MiB
// of command bytes, whichever is hit first — generous enough that
// ordinary bursts never see ErrBackpressure, small enough that a
// persistently overloaded node fails fast (bounded memory) rather than
// building an unbounded backlog.
//
// MaxProposalBatchEntries/MaxProposalBatchBytes bound how much a single
// worker iteration will drain into one Log.Append/fsync: 256 entries or
// 4 MiB, whichever is hit first. This is deliberately much smaller than
// the queue bounds — it caps how large one durable rewrite (and one
// AppendEntries backlog) can get from a single burst, while the worker
// simply loops again immediately for whatever is left (see
// proposalWorker). A single command larger than MaxProposalBatchBytes is
// still admitted and still gets its own batch of one — this limit only
// stops MORE entries from being added alongside it, never rejects it.
const (
	DefaultMaxQueuedProposals      = 4096
	DefaultMaxQueuedProposalBytes  = 32 << 20
	DefaultMaxProposalBatchEntries = 256
	DefaultMaxProposalBatchBytes   = 4 << 20
)

// pendingProposal is one Propose call's admitted-but-not-yet-persisted
// command, from admission until proposalWorker delivers its result.
// command is already a private clone (see propose) — nothing outside
// this package's own log-append path ever aliases it.
type pendingProposal struct {
	command  []byte
	resultCh chan proposalResult
}

type proposalResult struct {
	index LogIndex
	term  Term
	err   error
}

// nodeStats holds observational-only counters (see docs item 114-117):
// they never influence consensus behavior, only report on it. Safe for
// concurrent use via atomics; Stats() takes a consistent-enough snapshot
// for diagnostics/tests, not a linearizable one.
type nodeStats struct {
	proposalsAdmitted     atomic.Int64
	proposalsRejectedBusy atomic.Int64
	proposalBatches       atomic.Int64
	proposalBatchEntries  atomic.Int64 // sum, across all batches — divide by proposalBatches for the average
	maxQueueDepth         atomic.Int64
	maxQueueBytes         atomic.Int64
}

// NodeStats is a point-in-time, read-only copy of a Node's observational
// counters — see Node.Stats.
type NodeStats struct {
	ProposalsAdmitted     int64
	ProposalsRejectedBusy int64
	ProposalBatches       int64
	ProposalBatchEntries  int64
	MaxQueueDepth         int64
	MaxQueueBytes         int64
}

// Stats returns a snapshot of this node's observational counters. Purely
// diagnostic — nothing in this package reads it back to make decisions.
func (n *Node) Stats() NodeStats {
	return NodeStats{
		ProposalsAdmitted:     n.stats.proposalsAdmitted.Load(),
		ProposalsRejectedBusy: n.stats.proposalsRejectedBusy.Load(),
		ProposalBatches:       n.stats.proposalBatches.Load(),
		ProposalBatchEntries:  n.stats.proposalBatchEntries.Load(),
		MaxQueueDepth:         n.stats.maxQueueDepth.Load(),
		MaxQueueBytes:         n.stats.maxQueueBytes.Load(),
	}
}

// propose is Propose's implementation for a non-empty command, and the
// only caller of the admission/queue machinery below.
//
// Admission (the bound check, the count/byte reservation, and the
// channel send) all happen in one queueMu critical section — see
// admitProposal — specifically so it can race safely against Close:
// Close also takes queueMu to set n.closed before canceling n.bgCtx, so
// by the time it does, no goroutine can still be mid-admission (either
// it already finished — its proposal is safely sitting in the channel —
// or it has not started and will see n.closed and bail out immediately).
// Without that, a proposal could be sent to the channel after
// proposalWorker's on-close drain already ran, leaking the caller
// forever waiting on a result nothing will ever deliver.
func (n *Node) propose(command []byte) (LogIndex, Term, error) {
	cloned := cloneBytes(command)
	p := &pendingProposal{command: cloned, resultCh: make(chan proposalResult, 1)}
	if err := n.admitProposal(p, len(cloned)); err != nil {
		n.stats.proposalsRejectedBusy.Add(1)
		return 0, 0, err
	}
	n.stats.proposalsAdmitted.Add(1)
	res := <-p.resultCh
	return res.index, res.term, res.err
}

// admitProposal reserves size bytes (one more proposal) against the
// configured bounds and, only if that succeeds, sends p on n.proposalCh
// — all under one queueMu critical section (see propose's doc comment
// for why). The send itself can never block here: n.proposalCh's
// capacity always equals n.maxQueuedProposals, and n.queuedProposals
// (checked/incremented in this same critical section) is a strict upper
// bound on how many proposals are currently either sitting in that
// channel or already dequeued but not yet released by
// persistProposalBatch/failBatch/drainProposalsOnClose — so the buffer
// can never actually be full when this send happens.
func (n *Node) admitProposal(p *pendingProposal, size int) error {
	n.queueMu.Lock()
	defer n.queueMu.Unlock()
	if n.closed {
		return ErrNodeClosed
	}
	if n.queuedProposals+1 > n.maxQueuedProposals || n.queuedProposalBytes+int64(size) > n.maxQueuedProposalBytes {
		return ErrBackpressure
	}
	n.queuedProposals++
	n.queuedProposalBytes += int64(size)
	if int64(n.queuedProposals) > n.stats.maxQueueDepth.Load() {
		n.stats.maxQueueDepth.Store(int64(n.queuedProposals))
	}
	if n.queuedProposalBytes > n.stats.maxQueueBytes.Load() {
		n.stats.maxQueueBytes.Store(n.queuedProposalBytes)
	}
	n.proposalCh <- p
	return nil
}

func (n *Node) releaseQueueCapacity(size int) {
	n.queueMu.Lock()
	n.queuedProposals--
	n.queuedProposalBytes -= int64(size)
	n.queueMu.Unlock()
}

// proposalWorker is the single goroutine (started once, in NewNode) that
// turns admitted proposals into durable log entries: it blocks for the
// first available proposal, then — without any artificial delay —
// drains whatever else is already queued up to the batch bounds, and
// persists the whole group with one Log.Append. Under low concurrency
// this naturally produces batches of size 1 (no different from the old
// direct-append path); under concurrent load, proposals that arrive
// while a batch is being drained/persisted share that batch's single
// fsync instead of each paying for their own.
func (n *Node) proposalWorker(ctx context.Context) {
	defer n.bgWG.Done()
	var carry *pendingProposal
	for {
		var first *pendingProposal
		if carry != nil {
			first, carry = carry, nil
		} else {
			select {
			case <-ctx.Done():
				n.drainProposalsOnClose(nil)
				return
			case p := <-n.proposalCh:
				first = p
			}
		}

		n.queueMu.Lock()
		hook := n.testBeforeBatch
		n.queueMu.Unlock()
		if hook != nil {
			hook()
		}

		batch := []*pendingProposal{first}
		batchBytes := int64(len(first.command))
	drain:
		for len(batch) < n.maxProposalBatchEntries {
			select {
			case p := <-n.proposalCh:
				if batchBytes+int64(len(p.command)) > n.maxProposalBatchBytes {
					carry = p
					break drain
				}
				batch = append(batch, p)
				batchBytes += int64(len(p.command))
			default:
				break drain
			}
		}

		n.persistProposalBatch(batch)

		select {
		case <-ctx.Done():
			n.drainProposalsOnClose(carry)
			return
		default:
		}
	}
}

// drainProposalsOnClose fails every proposal still sitting in the
// channel (plus extra, a proposalWorker carry-over already pulled out
// but not yet batched, if any) with ErrNodeClosed, so no caller of
// Propose can block forever past Close — see item 62/103. Only called
// from proposalWorker's own goroutine, after it has committed to
// exiting, so there is no concurrent drain.
func (n *Node) drainProposalsOnClose(extra *pendingProposal) {
	if extra != nil {
		n.failOne(extra, ErrNodeClosed)
	}
	for {
		select {
		case p := <-n.proposalCh:
			n.failOne(p, ErrNodeClosed)
		default:
			return
		}
	}
}

func (n *Node) failOne(p *pendingProposal, err error) {
	n.releaseQueueCapacity(len(p.command))
	p.resultCh <- proposalResult{err: err}
}

func (n *Node) failBatch(batch []*pendingProposal, err error) {
	for _, p := range batch {
		n.failOne(p, err)
	}
}

// persistProposalBatch performs the one durable operation a whole batch
// shares: under a single n.mu critical section (the same lock every
// other log-mutating path in this package already uses — config-change
// entries, the ReadIndex no-op barrier — so batched application entries
// can never be reordered around them; whichever caller next acquires
// n.mu simply goes next), it re-validates leadership/transfer-freeze
// (a batch may have been sitting queued since before either changed —
// see item 60), assigns contiguous indexes in queue order, and issues
// exactly one Log.Append for the whole batch. On success, every
// proposal's own (index, term) is delivered and replication is kicked
// off once for the batch; on failure, every proposal in the batch fails
// together — none of them may believe their entry exists durably.
func (n *Node) persistProposalBatch(batch []*pendingProposal) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		n.failBatch(batch, ErrNotLeader)
		return
	}
	if n.transfer != nil && n.transfer.phase == transferHandoff {
		// Handoff freeze (see leadership_transfer.go): once the target is
		// caught up and TimeoutNow is about to be (or has been) sent, no
		// new entry may be appended.
		n.mu.Unlock()
		n.failBatch(batch, ErrLeadershipTransferInProgress)
		return
	}
	term := n.persistent.CurrentTerm
	entries := make([]LogEntry, len(batch))
	for i, p := range batch {
		entries[i] = LogEntry{Term: term, Command: p.command}
	}
	if err := n.log.Append(entries); err != nil {
		n.mu.Unlock()
		n.failBatch(batch, err)
		return
	}
	last := n.log.LastIndex()
	startIndex := last - LogIndex(len(entries)) + 1
	n.maybeAdvanceCommitIndexLocked() // handles the single-node-cluster case
	n.pingTransferChanged()           // LastIndex moved: a transfer catch-up waiter may need to keep replicating
	// Wake every replication worker before unlocking (see
	// replication_worker.go) — a coalescing channel ping, not network
	// I/O, so doing it under the lock is cheap and gives an even
	// stronger ordering guarantee than the old code's "spawn a
	// replication goroutine before returning": by the time Propose
	// returns to its caller, every worker has already observed the wake
	// (some tests key timing-sensitive setup, e.g. blocking a peer
	// address, off replication having been scheduled by then).
	n.wakeAllReplicationLocked()
	n.mu.Unlock()

	n.stats.proposalBatches.Add(1)
	n.stats.proposalBatchEntries.Add(int64(len(batch)))

	for i, p := range batch {
		n.releaseQueueCapacity(len(p.command))
		p.resultCh <- proposalResult{index: startIndex + LogIndex(i), term: term, err: nil}
	}
}
