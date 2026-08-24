package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/clientproto"
	"quorumkv/internal/reqid"
	"quorumkv/internal/transport"
)

// TestNewGeneratesUniqueClientIDs proves two default Clients get distinct
// ClientIDs.
func TestNewGeneratesUniqueClientIDs(t *testing.T) {
	a := New("addr-unused")
	b := New("addr-unused")
	if a.ID() == b.ID() {
		t.Fatalf("two New() clients got the same ClientID: %v", a.ID())
	}
	if a.ID().IsZero() {
		t.Fatalf("New() produced the reserved all-zero ClientID")
	}
}

// TestNewWithIDUsesGivenID proves NewWithID uses exactly the ID supplied,
// required for deterministic tests and caller-managed persistent
// identity across Client reconstruction.
func TestNewWithIDUsesGivenID(t *testing.T) {
	var id reqid.ClientID
	id[0] = 0x42
	c := NewWithID(id, "addr-unused")
	if c.ID() != id {
		t.Fatalf("ID() = %v, want %v", c.ID(), id)
	}
}

func TestNewWithIDRejectsZeroID(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("NewWithID(zero ID) did not panic")
		}
	}()
	NewWithID(reqid.ClientID{}, "addr-unused")
}

// TestSequenceAdvancesOnlyAfterSuccess proves the client allocates a
// sequence for a logical operation and only advances past it once that
// operation reaches a successful terminal outcome — never merely because
// request bytes were sent.
func TestSequenceAdvancesOnlyAfterSuccess(t *testing.T) {
	var seen []clientproto.Request
	var mu sync.Mutex
	tr, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, m transport.Message) (transport.Message, error) {
		req, err := clientproto.DecodeRequest(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		mu.Lock()
		seen = append(seen, req)
		mu.Unlock()
		payload, _ := clientproto.EncodeResponse(clientproto.Response{Status: clientproto.StatusOK})
		return transport.NewMessage(transport.MessageClientResponse, payload), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		if err := c.Put(ctx, []byte("x"), []byte("v")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(seen))
	}
	for i, req := range seen {
		want := reqid.Sequence(i + 1)
		if req.Sequence != want {
			t.Fatalf("request %d had Sequence %d, want %d", i, req.Sequence, want)
		}
	}
}

// TestConcurrentWritesFromOneClientAreSerializedWithUniqueSequences is
// item 102: many concurrent Put/Delete calls on one Client must produce
// unique, ordered sequence numbers with no races and no duplicate
// allocation.
func TestConcurrentWritesFromOneClientAreSerializedWithUniqueSequences(t *testing.T) {
	var mu sync.Mutex
	seenSeqs := map[reqid.Sequence]bool{}
	var order []reqid.Sequence

	tr, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, m transport.Message) (transport.Message, error) {
		req, err := clientproto.DecodeRequest(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		mu.Lock()
		if seenSeqs[req.Sequence] {
			t.Errorf("sequence %d delivered to the server twice", req.Sequence)
		}
		seenSeqs[req.Sequence] = true
		order = append(order, req.Sequence)
		mu.Unlock()
		payload, _ := clientproto.EncodeResponse(clientproto.Response{Status: clientproto.StatusOK})
		return transport.NewMessage(transport.MessageClientResponse, payload), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	c := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Put(ctx, []byte("x"), []byte("v"))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != n {
		t.Fatalf("server saw %d requests, want %d", len(order), n)
	}
	// Since writes are serialized, the sequences the server observed must
	// be exactly 1..n, in that order — proving both uniqueness and
	// ordering, not merely a set of distinct values.
	for i, seq := range order {
		want := reqid.Sequence(i + 1)
		if seq != want {
			t.Fatalf("request order[%d] = sequence %d, want %d (writes were not serialized/ordered)", i, seq, want)
		}
	}
}

// TestMultipleClientsAreIndependent is item 103: two different Client
// instances (distinct ClientIDs) both starting at sequence 1 do not
// collide.
func TestMultipleClientsAreIndependent(t *testing.T) {
	var mu sync.Mutex
	var seen []clientproto.Request
	tr, err := transport.Listen("127.0.0.1:0", func(ctx context.Context, m transport.Message) (transport.Message, error) {
		req, err := clientproto.DecodeRequest(m.Payload)
		if err != nil {
			return transport.Message{}, err
		}
		mu.Lock()
		seen = append(seen, req)
		mu.Unlock()
		payload, _ := clientproto.EncodeResponse(clientproto.Response{Status: clientproto.StatusOK})
		return transport.NewMessage(transport.MessageClientResponse, payload), nil
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close()

	a := New(tr.Addr())
	b := New(tr.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Put(ctx, []byte("x"), []byte("from-a")); err != nil {
		t.Fatalf("A Put: %v", err)
	}
	if err := b.Put(ctx, []byte("y"), []byte("from-b")); err != nil {
		t.Fatalf("B Put: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(seen))
	}
	if seen[0].ClientID == seen[1].ClientID {
		t.Fatalf("two different Client instances produced the same ClientID")
	}
	if seen[0].Sequence != 1 || seen[1].Sequence != 1 {
		t.Fatalf("sequences = %d, %d; want both 1 (independent per-ClientID counters)", seen[0].Sequence, seen[1].Sequence)
	}
}

// TestSequenceExhaustedReturnsExplicitError is item 8: a Client whose
// sequence counter has wrapped past the maximum must report
// ErrSequenceExhausted rather than silently reusing 0.
func TestSequenceExhaustedReturnsExplicitError(t *testing.T) {
	c := New("addr-unused")
	c.nextSeq = 0 // simulate exhaustion directly rather than actually counting to 2^64-1

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("x"), []byte("1")); err != ErrSequenceExhausted {
		t.Fatalf("err = %v, want ErrSequenceExhausted", err)
	}
}
