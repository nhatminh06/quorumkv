package transport

import (
	"context"
	"net"
	"sync"
)

// Handler processes one inbound Message and produces the response to send
// back on the same connection. Handler knows nothing about transport
// framing or connection lifecycle — Transport calls it once per accepted
// connection, after successfully decoding exactly one request frame. ctx
// is canceled when the owning Transport's Close is called, so a handler
// that wants to return promptly during shutdown should observe ctx.Done().
type Handler func(ctx context.Context, m Message) (Message, error)

// Transport listens for inbound connections and dispatches each decoded
// request to a Handler. Each TCP connection carries exactly one request
// and one response: this keeps connection lifecycle deterministic and easy
// to test, and callers needing a new exchange simply call Send again.
//
// Transport does not retry, pool connections, reconnect, or interpret
// message contents — it only frames and delivers bytes.
type Transport struct {
	ln     net.Listener
	h      Handler
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
}

// Listen starts accepting TCP connections on addr and dispatching decoded
// requests to h. Use "127.0.0.1:0" to let the OS choose a port.
func Listen(addr string, h Handler) (*Transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		ln:     ln,
		h:      h,
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[net.Conn]struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Addr returns the address the listener is bound to.
func (t *Transport) Addr() string {
	return t.ln.Addr().String()
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			// Close() closes the listener to unblock Accept; any other
			// Accept error is treated the same way, since this transport
			// has no retry/backoff policy for listener-level failures.
			return
		}

		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			return
		}
		t.conns[conn] = struct{}{}
		t.mu.Unlock()

		t.wg.Add(1)
		go t.handleConn(conn)
	}
}

// handleConn reads exactly one request frame, dispatches it to the
// handler, and writes exactly one response frame. A malformed request or
// a handler error closes this connection only — the listener and all
// other connections are unaffected.
func (t *Transport) handleConn(conn net.Conn) {
	defer t.wg.Done()
	defer func() {
		t.mu.Lock()
		delete(t.conns, conn)
		t.mu.Unlock()
		conn.Close()
	}()

	msg, err := ReadFrame(conn)
	if err != nil {
		return
	}

	resp, err := t.h(t.ctx, msg)
	if err != nil {
		return
	}

	_ = WriteFrame(conn, resp)
}

// Close stops accepting new connections, cancels the context passed to any
// in-flight handler, closes all connections currently being handled
// (unblocking any in-flight reads/writes so their goroutines can exit),
// and waits for the accept loop and all connection handlers to finish.
// After Close returns, no transport goroutines remain running.
//
// A handler that does not observe context cancellation and is not blocked
// on I/O (e.g. blocked on its own internal channel) will make Close block
// until that handler returns — Close waits for handlers rather than
// abandoning them.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()

	err := t.ln.Close()
	t.wg.Wait()
	return err
}

// Send dials addr, writes m as a single request frame, and reads back the
// response frame. The connection is closed once the exchange completes.
//
// ctx bounds the whole exchange: dialing, writing the request, and
// reading the response. If ctx is canceled or its deadline passes while
// blocked on I/O, the connection is closed to unblock it and ctx.Err() is
// returned. Send never retries — a returned error does not indicate
// whether the remote side processed the request; that judgment belongs to
// the caller.
func Send(ctx context.Context, addr string, m Message) (Message, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return Message{}, err
	}
	defer conn.Close()

	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() { conn.Close() })
		defer stop()
	}

	if err := WriteFrame(conn, m); err != nil {
		return Message{}, contextErr(ctx, err)
	}

	resp, err := ReadFrame(conn)
	if err != nil {
		return Message{}, contextErr(ctx, err)
	}
	return resp, nil
}

// contextErr reports ctx's own error when I/O failed because Send closed
// the connection due to cancellation/deadline, rather than the resulting
// generic "use of closed network connection" error.
func contextErr(ctx context.Context, ioErr error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ioErr
}
