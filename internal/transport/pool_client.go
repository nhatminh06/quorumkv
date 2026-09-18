package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

var ErrAddressLimit = errors.New("transport: client address limit reached")

const (
	DefaultClientPoolWidth = 8
	MaxClientPoolAddresses = 64
)

// PoolClient owns a bounded set of sequential persistent sessions for each
// address. One physical session carries exactly one request/response exchange
// at a time; concurrency comes from the fixed number of sessions, never wire
// multiplexing. Send performs one logical RPC and never retries it.
type PoolClient struct {
	mu        sync.Mutex
	pools     map[string]*addressPool
	width     int
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	closeOnce sync.Once
	wg        sync.WaitGroup
	dial      func(context.Context, string, string) (net.Conn, error)

	dialed, reused, failures, closedConns atomic.Uint64
	activeConnections, waiters            atomic.Int64
}

type addressPool struct {
	mu      sync.Mutex
	idle    chan *clientSession
	created int
}

type clientSession struct {
	conn net.Conn
}

func NewPoolClient(width int) *PoolClient {
	if width <= 0 {
		panic("transport: pool width must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &PoolClient{
		pools: make(map[string]*addressPool), width: width, ctx: ctx,
		cancel: cancel, dial: (&net.Dialer{}).DialContext,
	}
}

func (c *PoolClient) Stats() ClientStats {
	return ClientStats{
		ConnectionsDialed: c.dialed.Load(), ConnectionsReused: c.reused.Load(),
		SendFailures: c.failures.Load(), ConnectionsClosed: c.closedConns.Load(),
		ActiveConnections: c.activeConnections.Load(), Waiters: c.waiters.Load(),
	}
}

func (c *PoolClient) register(addr string) (*addressPool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	p := c.pools[addr]
	if p == nil {
		if len(c.pools) >= MaxClientPoolAddresses {
			return nil, ErrAddressLimit
		}
		p = &addressPool{idle: make(chan *clientSession, c.width)}
		c.pools[addr] = p
	}
	c.wg.Add(1)
	return p, nil
}

func (c *PoolClient) acquire(ctx context.Context, p *addressPool) (*clientSession, bool, error) {
	for {
		p.mu.Lock()
		select {
		case s := <-p.idle:
			p.mu.Unlock()
			if s != nil {
				return s, true, nil
			}
			continue
		default:
		}
		if p.created < c.width {
			p.created++
			p.mu.Unlock()
			return &clientSession{}, false, nil
		}
		p.mu.Unlock()

		c.waiters.Add(1)
		select {
		case s := <-p.idle:
			c.waiters.Add(-1)
			if s != nil {
				return s, true, nil
			}
		case <-ctx.Done():
			c.waiters.Add(-1)
			return nil, false, ctx.Err()
		case <-c.ctx.Done():
			c.waiters.Add(-1)
			return nil, false, ErrClosed
		}
	}
}

func (c *PoolClient) release(p *addressPool, s *clientSession, healthy bool) {
	if !healthy {
		if s.conn != nil {
			s.conn.Close()
			c.closedConns.Add(1)
			c.activeConnections.Add(-1)
		}
		p.mu.Lock()
		p.created--
		p.mu.Unlock()
		p.idle <- nil
		return
	}
	p.idle <- s
}

func (c *PoolClient) Send(ctx context.Context, addr string, m Message, validate func(Message) error) (resp Message, err error) {
	defer func() {
		if err != nil {
			c.failures.Add(1)
		}
	}()
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	p, err := c.register(addr)
	if err != nil {
		return Message{}, err
	}
	defer c.wg.Done()

	s, reused, err := c.acquire(ctx, p)
	if err != nil {
		return Message{}, err
	}
	healthy := false
	defer func() { c.release(p, s, healthy) }()

	rpcCtx, cancel := context.WithCancel(ctx)
	stopClose := onCancel(c.ctx, cancel)
	defer func() { stopClose(); cancel() }()
	if err := rpcCtx.Err(); err != nil {
		return Message{}, c.sendError(ctx, err)
	}
	if s.conn == nil {
		s.conn, err = c.dial(rpcCtx, "tcp", addr)
		if err != nil {
			return Message{}, c.sendError(ctx, err)
		}
		c.dialed.Add(1)
		c.activeConnections.Add(1)
	} else if reused {
		c.reused.Add(1)
	}

	conn := s.conn
	stopIO := onCancel(rpcCtx, func() { conn.Close() })
	defer func() {
		stopIO()
		if rpcCtx.Err() != nil {
			err = c.sendError(ctx, rpcCtx.Err())
		}
		healthy = err == nil
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

func (c *PoolClient) sendError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	return err
}

// Close cancels dialing, active exchanges, and pool waiters, joins all Sends,
// then closes every idle session. It is safe and idempotent under concurrency.
func (c *PoolClient) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.wg.Wait()
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, p := range c.pools {
			p.mu.Lock()
			close(p.idle)
			for s := range p.idle {
				if s != nil && s.conn != nil {
					s.conn.Close()
					c.closedConns.Add(1)
					c.activeConnections.Add(-1)
				}
			}
			p.created = 0
			p.mu.Unlock()
		}
		c.pools = nil
	})
	return nil
}
