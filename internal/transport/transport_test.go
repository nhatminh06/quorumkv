package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func echoUpperHandler(_ context.Context, m Message) (Message, error) {
	if m.Type == MessageTest {
		return NewMessage(MessageTest, []byte("world")), nil
	}
	return NewMessage(MessagePong, nil), nil
}

func TestLoopbackRequestResponse(t *testing.T) {
	tr, err := Listen("127.0.0.1:0", echoUpperHandler)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := Send(ctx, tr.Addr(), NewMessage(MessageTest, []byte("hello")))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Type != MessageTest || string(resp.Payload) != "world" {
		t.Fatalf("resp = %+v, want MessageTest(world)", resp)
	}
}

func TestConcurrentClients(t *testing.T) {
	const clients = 20

	tr, err := Listen("127.0.0.1:0", func(_ context.Context, m Message) (Message, error) {
		// Echo the payload back so each client can verify it got its own
		// response rather than another client's.
		return NewMessage(MessageTest, m.Payload), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			payload := []byte{byte(i)}
			resp, err := Send(ctx, tr.Addr(), NewMessage(MessageTest, payload))
			if err != nil {
				errs <- err
				return
			}
			if len(resp.Payload) != 1 || resp.Payload[0] != byte(i) {
				errs <- errors.New("mismatched response payload")
				return
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("client error: %v", err)
	}
}

func TestMalformedPeerDoesNotKillListener(t *testing.T) {
	tr, err := Listen("127.0.0.1:0", echoUpperHandler)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	// Bad magic.
	sendRaw(t, tr.Addr(), []byte{0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0})

	// Truncated frame (only 3 bytes).
	sendRaw(t, tr.Addr(), []byte{'Q', 'K', 'V'})

	// Bad checksum on an otherwise well-formed frame.
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	buf[len(buf)-1] ^= 0xFF
	sendRaw(t, tr.Addr(), buf)

	// The listener must still be healthy for a real client.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := Send(ctx, tr.Addr(), NewMessage(MessageTest, []byte("hello")))
	if err != nil {
		t.Fatalf("Send after malformed peers: %v", err)
	}
	if string(resp.Payload) != "world" {
		t.Fatalf("resp = %+v, want world", resp)
	}
}

func TestOversizedFrameRejectedWithoutAllocatingPayload(t *testing.T) {
	tr, err := Listen("127.0.0.1:0", echoUpperHandler)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	header := make([]byte, fixedHeaderSize)
	copy(header[0:4], magic[:])
	header[4] = protocolVersion
	header[5] = byte(MessageTest)
	oversized := uint32(MaxPayloadSize) + 1
	header[6] = byte(oversized >> 24)
	header[7] = byte(oversized >> 16)
	header[8] = byte(oversized >> 8)
	header[9] = byte(oversized)
	// Deliberately do not send MaxPayloadSize+1 bytes of payload — a
	// correct server rejects based on the declared length alone.
	sendRaw(t, tr.Addr(), header)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := Send(ctx, tr.Addr(), NewMessage(MessageTest, []byte("hello")))
	if err != nil {
		t.Fatalf("Send after oversized frame: %v", err)
	}
	if string(resp.Payload) != "world" {
		t.Fatalf("resp = %+v, want world", resp)
	}
}

func sendRaw(t *testing.T, addr string, b []byte) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(b); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func TestDialFailureIsBoundedAndExplicit(t *testing.T) {
	// Dial an address nothing listens on. Loopback connection refused is
	// immediate, so this test needs no long timeout to stay deterministic.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // free the port; nothing listens on it now

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = Send(ctx, addr, NewMessage(MessagePing, nil))
	if err == nil {
		t.Fatalf("Send to closed port succeeded, want error")
	}
}

func TestClientContextTimeout(t *testing.T) {
	// A listener that accepts connections but never reads or writes,
	// forcing the client to rely on ctx cancellation to unblock.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-time.After(2 * time.Second) // hold the connection open, unresponsive
		conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = Send(ctx, ln.Addr().String(), NewMessage(MessagePing, nil))
	elapsed := time.Since(start)

	<-accepted
	if err == nil {
		t.Fatalf("Send succeeded, want timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("Send took %v, want well under 1s", elapsed)
	}
}

func TestHandlerErrorClosesConnectionWithoutResponse(t *testing.T) {
	tr, err := Listen("127.0.0.1:0", func(_ context.Context, m Message) (Message, error) {
		return Message{}, errors.New("handler failure")
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = Send(ctx, tr.Addr(), NewMessage(MessagePing, nil))
	if err == nil {
		t.Fatalf("Send succeeded, want error since handler failed")
	}
}

func TestCloseIsClean(t *testing.T) {
	tr, err := Listen("127.0.0.1:0", echoUpperHandler)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := Send(ctx, tr.Addr(), NewMessage(MessageTest, []byte("hi"))); err != nil {
		t.Fatalf("Send before Close: %v", err)
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close must be idempotent.
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, err := Send(ctx2, tr.Addr(), NewMessage(MessagePing, nil)); err == nil {
		t.Fatalf("Send succeeded after Close, want error")
	}
}

// TestCloseCancelsHandlerContext proves Close cancels the context passed
// to an in-flight handler, so a cooperative handler can return promptly
// during shutdown instead of Close blocking on it indefinitely.
func TestCloseCancelsHandlerContext(t *testing.T) {
	handlerStarted := make(chan struct{})
	tr, err := Listen("127.0.0.1:0", func(ctx context.Context, m Message) (Message, error) {
		close(handlerStarted)
		<-ctx.Done()
		return Message{}, ctx.Err()
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		Send(ctx, tr.Addr(), NewMessage(MessagePing, nil))
	}()
	<-handlerStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- tr.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not return after cancellation; handler did not observe ctx.Done()")
	}
}

// TestCloseWaitsForUncooperativeHandler proves Close does not abandon a
// handler that ignores context cancellation and is not blocked on
// connection I/O — it waits for the handler to actually return.
func TestCloseWaitsForUncooperativeHandler(t *testing.T) {
	unblock := make(chan struct{})
	handlerStarted := make(chan struct{})
	tr, err := Listen("127.0.0.1:0", func(ctx context.Context, m Message) (Message, error) {
		close(handlerStarted)
		<-unblock // ignores ctx cancellation on purpose
		return NewMessage(MessagePong, nil), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		Send(ctx, tr.Addr(), NewMessage(MessagePing, nil))
	}()
	<-handlerStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- tr.Close() }()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned (%v) before the uncooperative handler released, want Close to keep waiting", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: Close is still waiting for the handler goroutine.
	}

	close(unblock)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not return after handler unblocked")
	}
}
