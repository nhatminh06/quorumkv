package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrClosed    = errors.New("transport: client closed")
	ErrPeerLimit = errors.New("transport: all peer sessions busy")
)

// Client owns outgoing connection lifecycle. validate runs before a session
// can be reused; protocol decoders can reject an otherwise well-framed reply.
// It must be bounded, must not perform network I/O, and must not call Client.
// Passing nil requests only frame validation. Send never retries an RPC.
type Client interface {
	Send(context.Context, string, Message, func(Message) error) (Message, error)
	Close() error
}

// MaxPeerSessions bounds retained sockets and address metadata, including
// waiters/dialing sessions. Idle sessions are evicted when the bound is reached;
// if all are in use, Send returns ErrPeerLimit rather than growing the pool.
const MaxPeerSessions = 256

// PeerClient maintains one sequential request/response stream per address.
// There are no reader, writer, reconnect, or housekeeping goroutines. Callers
// wait on a context-aware gate, never a mutex held across network I/O.
// Call Close when its owning node stops. The zero value is not usable.
type PeerClient struct {
	mu                                    sync.Mutex
	peers                                 map[string]*peerSession
	ctx                                   context.Context
	cancel                                context.CancelFunc
	closed                                bool
	wg                                    sync.WaitGroup
	closeOnce                             sync.Once
	dial                                  func(context.Context, string, string) (net.Conn, error)
	dialed, reused, failures, closedConns atomic.Uint64
	activeConnections                     atomic.Int64
}

type peerSession struct {
	gate  chan struct{}
	conn  net.Conn // guarded by gate; idle eviction only when users == 0
	users int      // guarded by PeerClient.mu; includes gate waiters
}

// ClientStats is an observational snapshot, never used for correctness.
// Dialed counts successful connections; Reused counts exchanges starting on
// an existing socket (including a stale one); Failures counts failed Sends.
// Closed counts sockets discarded/evicted/shut down, once per owned socket.
type ClientStats struct {
	ConnectionsDialed uint64
	ConnectionsReused uint64
	SendFailures      uint64
	ConnectionsClosed uint64
	ActiveConnections int64
	Waiters           int64
}

func NewPeerClient() *PeerClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &PeerClient{peers: make(map[string]*peerSession), ctx: ctx, cancel: cancel, dial: (&net.Dialer{}).DialContext}
}

func (c *PeerClient) Stats() ClientStats {
	c.mu.Lock()
	var waiters int64
	for _, p := range c.peers {
		if p.users > 1 {
			waiters += int64(p.users - 1)
		}
	}
	c.mu.Unlock()
	return ClientStats{
		ConnectionsDialed: c.dialed.Load(), ConnectionsReused: c.reused.Load(),
		SendFailures: c.failures.Load(), ConnectionsClosed: c.closedConns.Load(),
		ActiveConnections: c.activeConnections.Load(), Waiters: waiters,
	}
}

func (c *PeerClient) acquire(addr string) (*peerSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	p := c.peers[addr]
	if p == nil {
		if len(c.peers) == MaxPeerSessions {
			// Deterministic idle eviction; active sessions and their waiters retain
			// a single shared gate, so two streams for one address cannot emerge.
			victim := ""
			for a, candidate := range c.peers {
				if candidate.users == 0 && (victim == "" || a < victim) {
					victim = a
				}
			}
			if victim == "" {
				return nil, ErrPeerLimit
			}
			c.discard(c.peers[victim])
			delete(c.peers, victim)
		}
		p = &peerSession{gate: make(chan struct{}, 1)}
		c.peers[addr] = p
	}
	p.users++
	// Add and the closed check share mu with Close, before it calls Wait.
	c.wg.Add(1)
	return p, nil
}

func (c *PeerClient) release(p *peerSession) {
	c.mu.Lock()
	p.users--
	c.mu.Unlock()
	c.wg.Done()
}

func (c *PeerClient) discard(p *peerSession) {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
		c.closedConns.Add(1)
		c.activeConnections.Add(-1)
	}
}

// onCancel joins an already-started callback before returning. In particular,
// a late cancellation callback must never close a socket after its gate has
// been handed to the next RPC.
func onCancel(ctx context.Context, f func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); f() })
	return func() {
		if !stop() {
			<-done
		}
	}
}

func (c *PeerClient) Send(ctx context.Context, addr string, m Message, validate func(Message) error) (resp Message, err error) {
	defer func() {
		if err != nil {
			c.failures.Add(1)
		}
	}()
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	p, err := c.acquire(addr)
	if err != nil {
		return Message{}, err
	}
	defer c.release(p)

	// Client shutdown cancels dialing, gate waits, and socket I/O as well as
	// caller cancellation. No lifetime depends on a caller having a deadline.
	rpcCtx, cancel := context.WithCancel(ctx)
	stopClose := onCancel(c.ctx, cancel)
	defer func() { stopClose(); cancel() }()
	select {
	case p.gate <- struct{}{}:
	case <-rpcCtx.Done():
		return Message{}, c.sendError(ctx, rpcCtx.Err())
	}
	defer func() { <-p.gate }()
	if rpcCtx.Err() != nil {
		return Message{}, c.sendError(ctx, rpcCtx.Err())
	}
	if p.conn == nil {
		p.conn, err = c.dial(rpcCtx, "tcp", addr)
		if err != nil {
			return Message{}, c.sendError(ctx, err)
		}
		c.dialed.Add(1)
		c.activeConnections.Add(1)
	} else {
		c.reused.Add(1)
	}
	conn := p.conn
	stopIO := onCancel(rpcCtx, func() { conn.Close() })
	defer func() {
		stopIO()
		if rpcCtx.Err() != nil {
			err = c.sendError(ctx, rpcCtx.Err())
		}
		if err != nil {
			c.discard(p)
		}
	}()
	if err = WriteFrame(conn, m); err != nil {
		return Message{}, err
	}
	resp, err = ReadFrame(conn)
	if err == nil && validate != nil {
		err = validate(resp)
	}
	return resp, err
}

func (c *PeerClient) sendError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	return err
}

// Close rejects new Sends, cancels active exchanges and gate waiters, joins
// every registered Send (including its cancellation callbacks), then closes
// idle sockets. Concurrent Close calls all wait for the same cleanup.
func (c *PeerClient) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.wg.Wait()
		for _, p := range c.peers {
			c.discard(p)
		}
		c.peers = nil
	})
	return nil
}
