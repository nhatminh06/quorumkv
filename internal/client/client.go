// Package client provides a small reusable Go client for QuorumKV's
// bounded binary client protocol (package clientproto) over Milestone 2's
// TCP transport. It is leader-aware: given one or more seed addresses, it
// follows a bounded number of NOT_LEADER redirects to find the current
// leader and remembers it as a hint for the next call.
//
// Since Milestone 9, PUT/DELETE carry a stable request identity
// (ClientID + a monotonic per-client Sequence — see internal/reqid) so a
// server can recognize and safely suppress a retried write's second
// effect. This lets the client safely retry a PUT/DELETE after a
// transport failure, a server-reported TIMEOUT, or a NOT_LEADER
// redirect — cases Milestone 5-8 conservatively treated as unretryable
// because the outcome was ambiguous. GET is unaffected: it carries no
// request identity and is not retried by this client (it is already
// side-effect-free and quorum-confirmed — see docs/read-index.md).
//
// A default Client (New) generates a fresh, process-lifetime ClientID
// and keeps its next-sequence counter in memory: it preserves safe retry
// identity for its own lifetime, not across a process restart. A caller
// that needs retry safety across reconstructing its Client object must
// use NewWithID to supply (and, after a successful write, itself
// persist) a stable ClientID — this package does not implement client-
// side session persistence. See docs/request-dedup.md.
package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"quorumkv/internal/clientproto"
	"quorumkv/internal/reqid"
	"quorumkv/internal/transport"
)

// maxRedirects bounds how many NOT_LEADER hints a single call will
// follow before falling back to seed rotation, so a misbehaving or
// flapping cluster cannot cause an unbounded tight redirect loop.
const maxRedirects = 3

// writeRetryDelay is a small fixed backoff between write retry passes
// (transport failure, TIMEOUT, or a redirect chain exceeding
// maxRedirects) — enough to avoid busy-looping an unavailable cluster,
// small enough not to matter for a healthy one. It is context-aware (a
// timer/select, never an unconditional sleep), so it never outlasts the
// caller's ctx.
const writeRetryDelay = 20 * time.Millisecond

var (
	// ErrNoLeaderKnown means every node contacted either had no leader
	// hint to offer, or redirects were exhausted without reaching one.
	ErrNoLeaderKnown = errors.New("client: no leader currently known")
	// ErrTimeout means the contacted node reported it gave up waiting for
	// commit+apply — the operation's outcome is uncertain, not negative.
	// For GET this is terminal; for PUT/DELETE, do implicitly retries
	// with the same request identity (see the package doc).
	ErrTimeout = errors.New("client: server-side wait timed out; outcome is uncertain")
	// ErrTooManyRedirects means maxRedirects NOT_LEADER hints were
	// followed without success (GET only — PUT/DELETE fall back to seed
	// rotation instead of failing outright, see the package doc).
	ErrTooManyRedirects = errors.New("client: too many leader redirects")
	// ErrBadRequest means the request was rejected as malformed/oversized
	// before ever reaching Raft. Not retried.
	ErrBadRequest = errors.New("client: bad request")
	// ErrInternal means the contacted node reported an internal failure.
	ErrInternal = errors.New("client: internal server error")
	// ErrRequestConflict means the server reported that this Client's
	// (ClientID, Sequence) was already used for a different operation —
	// a serious client-identity bug, not a legitimate retry outcome.
	// Terminal: never retried, and the local sequence is not advanced.
	ErrRequestConflict = errors.New("client: request identity reused for a different operation")
	// ErrStaleRequest means the server reported that this Client's
	// Sequence does not match what it expects next for this ClientID —
	// the server and this Client's local session state disagree.
	// Terminal: never retried, and the local sequence is not advanced.
	ErrStaleRequest = errors.New("client: sequence rejected as stale by the server")
	// ErrSequenceExhausted means this Client's Sequence counter has used
	// every value up to the maximum representable Sequence. Practically
	// unreachable; see internal/reqid.ErrSequenceExhausted.
	ErrSequenceExhausted = reqid.ErrSequenceExhausted
	// ErrBusy means the contacted node rejected the request due to
	// bounded overload (a full proposal queue or a full service-level
	// concurrency bound) before it ever touched Raft — nothing was
	// proposed or applied. For PUT/DELETE this Client already retries a
	// BUSY response automatically with the same request identity (see
	// doWrite); ErrBusy from Get is returned to the caller instead,
	// since a read carries no request identity and this package's GET
	// path has always been a single conservative attempt (see doRead) —
	// GET is safe to retry as-is, at the caller's discretion.
	ErrBusy = errors.New("client: server reported it is busy")
)

// Client is a leader-aware QuorumKV client seeded with one or more static
// node addresses. It is safe for concurrent use: GET calls run
// concurrently with each other and with writes, but PUT/DELETE calls from
// one Client are serialized against each other (see writeMu) so a simple
// last-sequence dedup model can assume requests reach the replicated
// state machine in the order this Client issued them.
type Client struct {
	id    reqid.ClientID
	seeds []string

	mu sync.Mutex
	// leader is the last address that answered OK or gave a redirect
	// hint; purely a cache to skip unnecessary NOT_LEADER round trips; a
	// stale value is harmless since the server would just redirect again.
	// Guarded by mu since concurrent calls on one Client share it.
	leader string

	// writeMu serializes PUT/DELETE (see the type doc) and guards
	// nextSeq: a request allocates its sequence once, retains it across
	// every retry of that same logical operation, and only advances
	// nextSeq once that operation reaches a terminal outcome.
	writeMu sync.Mutex
	nextSeq reqid.Sequence
}

// New constructs a Client with a freshly generated random ClientID (see
// internal/reqid.NewClientID). At least one seed address is required.
// Panics if the underlying crypto/rand source fails, which does not
// happen on any supported platform under normal operation — the same
// posture Go's stdlib crypto/rand callers generally take.
func New(seedAddrs ...string) *Client {
	id, err := reqid.NewClientID()
	if err != nil {
		panic(err)
	}
	return NewWithID(id, seedAddrs...)
}

// NewWithID constructs a Client with a caller-supplied, stable ClientID —
// required for deterministic tests, and for a caller that wants to
// preserve safe retry identity across reconstructing its Client object
// (e.g. after its own process restart): persist id and the next sequence
// number externally, and reconstruct with the same id and an equivalent
// starting sequence. id must be non-zero (the all-zero ClientID is
// reserved invalid).
func NewWithID(id reqid.ClientID, seedAddrs ...string) *Client {
	if id.IsZero() {
		panic("client: NewWithID called with the reserved all-zero ClientID")
	}
	seeds := append([]string(nil), seedAddrs...)
	leader := ""
	if len(seeds) > 0 {
		leader = seeds[0]
	}
	return &Client{id: id, seeds: seeds, leader: leader, nextSeq: 1}
}

// ID returns this Client's ClientID.
func (c *Client) ID() reqid.ClientID { return c.id }

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.write(ctx, clientproto.OpPut, key, value)
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.write(ctx, clientproto.OpDelete, key, nil)
}

// write implements the safe-retry PUT/DELETE path: allocate this
// logical operation's sequence once (serialized against every other
// write from this Client via writeMu), retry the exact same
// (ClientID, Sequence, operation, key, value) across transport failures,
// TIMEOUT, and NOT_LEADER redirects until it reaches a terminal outcome
// or ctx is done, then — only on success — advance the local sequence.
func (c *Client) write(ctx context.Context, op clientproto.Operation, key, value []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.nextSeq == 0 {
		return ErrSequenceExhausted
	}
	seq := c.nextSeq
	req := clientproto.Request{Operation: op, ClientID: c.id, Sequence: seq, Key: key, Value: value}

	if err := c.doWrite(ctx, req); err != nil {
		return err
	}
	if c.nextSeq == math.MaxUint64 {
		c.nextSeq = 0 // exhausted; the next write call reports ErrSequenceExhausted
	} else {
		c.nextSeq++
	}
	return nil
}

// doWrite sends req, retrying with the exact same bytes (per
// docs/request-dedup.md's "never change request identity during retry")
// on any outcome ambiguous enough to be safely retryable — a transport
// failure, StatusTimeout, or a NOT_LEADER chain exceeding maxRedirects —
// until a terminal outcome or ctx.Done(). Never advances or mutates req.
func (c *Client) doWrite(ctx context.Context, req clientproto.Request) error {
	c.mu.Lock()
	addr := c.leader
	c.mu.Unlock()
	if addr == "" && len(c.seeds) > 0 {
		addr = c.seeds[0]
	}
	seedIdx := 0
	hintStreak := 0

	for {
		if addr == "" {
			addr = c.nextSeed(&seedIdx)
		}
		if addr != "" {
			resp, sendErr := c.attempt(ctx, addr, req)
			if sendErr == nil {
				switch resp.Status {
				case clientproto.StatusOK, clientproto.StatusNotFound:
					c.mu.Lock()
					c.leader = addr
					c.mu.Unlock()
					return nil
				case clientproto.StatusNotLeader:
					hint := string(resp.LeaderHint)
					if hint != "" && hintStreak < maxRedirects {
						hintStreak++
						addr = hint
						continue // try the hint immediately, no backoff
					}
					hintStreak = 0
					addr = ""
				case clientproto.StatusTimeout:
					addr = ""
				case clientproto.StatusBusy:
					// Same treatment as StatusTimeout: retry the exact
					// same request identity after the existing
					// context-aware backoff below, never allocating a
					// new sequence — nothing was proposed or applied for
					// a BUSY rejection (see docs/request-dedup.md).
					addr = ""
				case clientproto.StatusRequestConflict:
					return ErrRequestConflict
				case clientproto.StatusStaleRequest:
					return ErrStaleRequest
				case clientproto.StatusBadRequest:
					return ErrBadRequest
				default:
					return ErrInternal
				}
			} else if errors.Is(sendErr, ErrBadRequest) {
				// A request that fails to even encode (oversized
				// key/value) will fail identically on every retry —
				// terminal, not a transport failure.
				return sendErr
			} else {
				// Transport-level failure: previously treated as
				// unretryable (the outcome on the far end was unknown).
				// It is now safely retryable with the same request
				// identity — see the package doc.
				addr = ""
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(writeRetryDelay):
		}
	}
}

// nextSeed rotates deterministically through the configured seeds so a
// write retry loop does not hammer one unreachable endpoint indefinitely.
func (c *Client) nextSeed(idx *int) string {
	if len(c.seeds) == 0 {
		return ""
	}
	addr := c.seeds[*idx%len(c.seeds)]
	*idx++
	return addr
}

// Get returns (value, true, nil) if key is present, (nil, false, nil) if
// it is not found, or a non-nil error otherwise. Unlike PUT/DELETE, GET
// carries no request identity and is not retried by this Client — it is
// already side-effect-free (a caller may simply call Get again), and its
// consistency comes from raft.Node.ReadIndex on the server, unrelated to
// this milestone's write-dedup mechanism (see docs/read-index.md).
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	resp, err := c.doRead(ctx, clientproto.Request{Operation: clientproto.OpGet, Key: key})
	if err != nil {
		return nil, false, err
	}
	if resp.Status == clientproto.StatusNotFound {
		return nil, false, nil
	}
	return resp.Value, true, nil
}

// doRead is the original (Milestone 5-8) conservative single-attempt
// request path, preserved unchanged for GET: it follows NOT_LEADER
// redirects up to maxRedirects times but never retries after a
// transport-level failure.
func (c *Client) doRead(ctx context.Context, req clientproto.Request) (clientproto.Response, error) {
	c.mu.Lock()
	addr := c.leader
	c.mu.Unlock()
	if addr == "" && len(c.seeds) > 0 {
		addr = c.seeds[0]
	}
	tried := make(map[string]bool)

	for attempt := 0; attempt <= maxRedirects; attempt++ {
		if addr == "" {
			return clientproto.Response{}, ErrNoLeaderKnown
		}
		tried[addr] = true

		resp, err := c.attempt(ctx, addr, req)
		if err != nil {
			return clientproto.Response{}, err
		}

		if resp.Status == clientproto.StatusNotLeader {
			hint := string(resp.LeaderHint)
			if hint == "" || tried[hint] {
				return clientproto.Response{}, ErrNoLeaderKnown
			}
			addr = hint
			continue
		}

		c.mu.Lock()
		c.leader = addr
		c.mu.Unlock()
		return resp, statusErr(resp.Status)
	}
	return clientproto.Response{}, ErrTooManyRedirects
}

// attempt sends req to addr once and returns the decoded response, or a
// transport/encode-level error (never a status-derived error — callers
// interpret resp.Status themselves).
func (c *Client) attempt(ctx context.Context, addr string, req clientproto.Request) (clientproto.Response, error) {
	payload, err := clientproto.EncodeRequest(req)
	if err != nil {
		return clientproto.Response{}, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	respMsg, err := transport.Send(ctx, addr, transport.NewMessage(transport.MessageClientRequest, payload))
	if err != nil {
		return clientproto.Response{}, err
	}
	return clientproto.DecodeResponse(respMsg.Payload)
}

func statusErr(status clientproto.Status) error {
	switch status {
	case clientproto.StatusOK, clientproto.StatusNotFound:
		return nil
	case clientproto.StatusTimeout:
		return ErrTimeout
	case clientproto.StatusBusy:
		return ErrBusy
	case clientproto.StatusBadRequest:
		return ErrBadRequest
	case clientproto.StatusRequestConflict:
		return ErrRequestConflict
	case clientproto.StatusStaleRequest:
		return ErrStaleRequest
	default:
		return ErrInternal
	}
}
