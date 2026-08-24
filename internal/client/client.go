// Package client provides a small reusable Go client for QuorumKV's
// bounded binary client protocol (package clientproto) over Milestone 2's
// TCP transport. It is leader-aware: given one or more seed addresses, it
// follows a bounded number of NOT_LEADER redirects to find the current
// leader and remembers it as a hint for the next call.
//
// This client never blindly retries after a transport failure (a reset
// connection, a timeout, an EOF after the request was sent): in every
// such case, whether the write was actually processed by the leader is
// unknown, and retrying could duplicate a logical operation. It also does
// not implement request deduplication or exactly-once semantics.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"quorumkv/internal/clientproto"
	"quorumkv/internal/transport"
)

// maxRedirects bounds how many NOT_LEADER hints a single call will
// follow, so a misbehaving or flapping cluster cannot cause an infinite
// redirect loop.
const maxRedirects = 3

var (
	// ErrNoLeaderKnown means every node contacted either had no leader
	// hint to offer, or redirects were exhausted without reaching one.
	ErrNoLeaderKnown = errors.New("client: no leader currently known")
	// ErrTimeout means the contacted node reported it gave up waiting for
	// commit+apply — the operation's outcome is uncertain, not negative.
	ErrTimeout = errors.New("client: server-side wait timed out; outcome is uncertain")
	// ErrTooManyRedirects means maxRedirects NOT_LEADER hints were
	// followed without success.
	ErrTooManyRedirects = errors.New("client: too many leader redirects")
	// ErrBadRequest means the request was rejected as malformed/oversized
	// before ever reaching Raft.
	ErrBadRequest = errors.New("client: bad request")
	// ErrInternal means the contacted node reported an internal failure.
	ErrInternal = errors.New("client: internal server error")
)

// Client is a leader-aware QuorumKV client seeded with one or more static
// node addresses. It is safe for concurrent use.
type Client struct {
	seeds []string

	mu sync.Mutex
	// leader is the last address that answered OK or gave a redirect
	// hint; purely a cache to skip unnecessary NOT_LEADER round trips; a
	// stale value is harmless since the server would just redirect again.
	// Guarded by mu since concurrent calls on one Client share it.
	leader string
}

// New constructs a Client. At least one seed address is required.
func New(seedAddrs ...string) *Client {
	seeds := append([]string(nil), seedAddrs...)
	leader := ""
	if len(seeds) > 0 {
		leader = seeds[0]
	}
	return &Client{seeds: seeds, leader: leader}
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.do(ctx, clientproto.Request{Operation: clientproto.OpPut, Key: key, Value: value})
	return err
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.do(ctx, clientproto.Request{Operation: clientproto.OpDelete, Key: key})
	return err
}

// Get returns (value, true, nil) if key is present, (nil, false, nil) if
// it is not found, or a non-nil error otherwise.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	resp, err := c.do(ctx, clientproto.Request{Operation: clientproto.OpGet, Key: key})
	if err != nil {
		return nil, false, err
	}
	if resp.Status == clientproto.StatusNotFound {
		return nil, false, nil
	}
	return resp.Value, true, nil
}

// do sends req to the current leader hint (or the first seed if none is
// known yet), following NOT_LEADER redirects up to maxRedirects times. A
// transport-level failure (reset, timeout, EOF) is returned immediately
// without retrying, since the request's outcome on the far end is
// unknown in that case.
func (c *Client) do(ctx context.Context, req clientproto.Request) (clientproto.Response, error) {
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

		payload, err := clientproto.EncodeRequest(req)
		if err != nil {
			return clientproto.Response{}, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		respMsg, err := transport.Send(ctx, addr, transport.NewMessage(transport.MessageClientRequest, payload))
		if err != nil {
			// Transport failure: outcome on the far end is unknown. Do
			// not retry — surface it directly.
			return clientproto.Response{}, err
		}
		resp, err := clientproto.DecodeResponse(respMsg.Payload)
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

func statusErr(status clientproto.Status) error {
	switch status {
	case clientproto.StatusOK, clientproto.StatusNotFound:
		return nil
	case clientproto.StatusTimeout:
		return ErrTimeout
	case clientproto.StatusBadRequest:
		return ErrBadRequest
	default:
		return ErrInternal
	}
}
