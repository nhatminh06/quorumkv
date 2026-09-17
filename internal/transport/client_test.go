package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func echoServer(t testing.TB) *Transport {
	t.Helper()
	tr, err := Listen("127.0.0.1:0", func(_ context.Context, m Message) (Message, error) { return m, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func testPeerClient(t testing.TB) *PeerClient {
	t.Helper()
	c := NewPeerClient()
	t.Cleanup(func() { c.Close() })
	return c
}

func exchange(t testing.TB, c *PeerClient, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg := Message{Type: MessageTest, Payload: []byte("hello")}
	resp, err := c.Send(ctx, addr, msg, nil)
	if err != nil || !bytes.Equal(msg.Payload, resp.Payload) {
		t.Fatalf("exchange: %+v, %v", resp, err)
	}
}

func TestPeerClientReusesOneConnection(t *testing.T) {
	tr := echoServer(t)
	c := testPeerClient(t)
	for i := 0; i < 100; i++ {
		exchange(t, c, tr.Addr())
	}
	stats := c.Stats()
	if stats.ConnectionsDialed != 1 || stats.ConnectionsReused != 99 || stats.SendFailures != 0 {
		t.Fatalf("stats: %+v", stats)
	}
	tr.mu.Lock()
	connections := len(tr.conns)
	tr.mu.Unlock()
	if connections != 1 {
		t.Fatalf("server has %d connections", connections)
	}
	c.Close()
	if c.Stats().ConnectionsClosed != 1 {
		t.Fatal("idle socket not closed")
	}
}

func TestPeerClientConcurrentResponses(t *testing.T) {
	tr := echoServer(t)
	c := testPeerClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				payload := []byte(fmt.Sprintf("%d/%d", i, j))
				resp, err := c.Send(ctx, tr.Addr(), Message{Type: MessageTest, Payload: payload}, nil)
				if err != nil || !bytes.Equal(resp.Payload, payload) {
					t.Errorf("response mismatch: %q %v, want %q", resp.Payload, err, payload)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if c.Stats().ConnectionsDialed != 1 {
		t.Fatalf("stats: %+v", c.Stats())
	}
}

func TestPeerClientReconnectAfterBrokenConnection(t *testing.T) {
	tr := echoServer(t)
	c := testPeerClient(t)
	exchange(t, c, tr.Addr())
	tr.mu.Lock()
	for conn := range tr.conns {
		conn.Close()
	}
	tr.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Send(ctx, tr.Addr(), Message{Type: MessageTest}, nil); err == nil {
		t.Fatal("broken session succeeded")
	}
	exchange(t, c, tr.Addr())
	if s := c.Stats(); s.ConnectionsDialed != 2 || s.SendFailures != 1 || s.ConnectionsClosed != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestPeerClientCancellationWhileWaitingDoesNotBreakOwner(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	tr, err := Listen("127.0.0.1:0", func(ctx context.Context, m Message) (Message, error) {
		if string(m.Payload) == "block" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return m, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	defer close(release)
	c := testPeerClient(t)
	result := make(chan error, 1)
	go func() {
		_, err := c.Send(context.Background(), tr.Addr(), Message{Type: MessageTest, Payload: []byte("block")}, nil)
		result <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.Send(ctx, tr.Addr(), Message{Type: MessageTest}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting: %v", err)
	}
	// A different peer never waits behind this peer's occupied stream.
	other := echoServer(t)
	exchange(t, c, other.Addr())
	c.Close() // also proves shutdown cancels the active owner
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("owner: %v", err)
	}
	if _, err := c.Send(context.Background(), tr.Addr(), Message{Type: MessageTest}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
}

func TestPeerClientCancellationUnblocksIOAndDiscardsSocket(t *testing.T) {
	for _, phase := range []string{"read", "write", "dial"} {
		t.Run(phase, func(t *testing.T) {
			c := testPeerClient(t)
			entered := make(chan struct{})
			var remote net.Conn
			if phase == "dial" {
				c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}
			} else {
				local, peer := net.Pipe()
				remote = peer
				defer peer.Close()
				c.dial = func(context.Context, string, string) (net.Conn, error) { close(entered); return local, nil }
			}
			readDone := make(chan struct{})
			if phase == "read" {
				go func() { defer close(readDone); ReadFrame(remote) }()
			} else {
				close(readDone)
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { _, err := c.Send(ctx, "peer", Message{Type: MessageTest}, nil); result <- err }()
			<-entered
			if phase == "read" {
				<-readDone
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation blocked")
			}
			if phase != "dial" && c.Stats().ConnectionsClosed != 1 {
				t.Fatal("unsafe connection retained")
			}
		})
	}
}

func TestPeerClientRejectsMalformedResponsesBeforeReuse(t *testing.T) {
	for _, badFrame := range []bool{false, true} {
		t.Run(fmt.Sprint(badFrame), func(t *testing.T) {
			c := testPeerClient(t)
			local, remote := net.Pipe()
			defer remote.Close()
			c.dial = func(context.Context, string, string) (net.Conn, error) { return local, nil }
			done := make(chan struct{})
			go func() {
				defer close(done)
				ReadFrame(remote)
				if badFrame {
					remote.Write(make([]byte, fixedHeaderSize))
				} else {
					WriteFrame(remote, Message{Type: MessageTest})
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			invalid := errors.New("invalid protocol response")
			_, err := c.Send(ctx, "peer", Message{Type: MessageTest}, func(Message) error { return invalid })
			if err == nil || c.Stats().ConnectionsClosed != 1 {
				t.Fatalf("err=%v stats=%+v", err, c.Stats())
			}
			<-done
			// Fresh connection after either frame or protocol validation failure.
			c.dial = (&net.Dialer{}).DialContext
			tr := echoServer(t)
			exchange(t, c, tr.Addr())
		})
	}
}

func TestPeerClientServerShutdownClosesPersistentSession(t *testing.T) {
	tr := echoServer(t)
	c := testPeerClient(t)
	exchange(t, c, tr.Addr())
	tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Send(ctx, tr.Addr(), Message{Type: MessageTest}, nil); err == nil {
		t.Fatal("server shutdown not detected")
	}
}

func TestPeerClientConcurrentCloseAndSend(t *testing.T) {
	tr := echoServer(t)
	for round := 0; round < 20; round++ {
		c := NewPeerClient()
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); c.Send(context.Background(), tr.Addr(), Message{Type: MessageTest}, nil) }()
		}
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); c.Close() }()
		}
		wg.Wait()
		if s := c.Stats(); s.ConnectionsDialed != s.ConnectionsClosed {
			t.Fatalf("leaked sockets: %+v", s)
		}
	}
}

func TestPeerClientSessionBound(t *testing.T) {
	c := testPeerClient(t)
	// Register occupied sessions without dialing: exercise the exact admission
	// path without requiring hundreds of open OS sockets.
	var sessions []*peerSession
	for i := 0; i < MaxPeerSessions; i++ {
		p, err := c.acquire(fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		sessions = append(sessions, p)
	}
	if _, err := c.acquire("overflow"); !errors.Is(err, ErrPeerLimit) {
		t.Fatalf("overflow: %v", err)
	}
	for _, p := range sessions {
		c.release(p)
	}
	p, err := c.acquire("replacement")
	if err != nil {
		t.Fatal(err)
	}
	c.release(p)
	if len(c.peers) != MaxPeerSessions {
		t.Fatal("session bound exceeded")
	}
}
