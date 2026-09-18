package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Independent M21 wire oracle; do not delegate to the production encoder.
func legacyAppendBytes(req AppendEntriesRequest) []byte {
	var b bytes.Buffer
	for _, v := range []uint64{uint64(req.Term), uint64(req.LeaderID), uint64(req.PrevLogIndex), uint64(req.PrevLogTerm), uint64(req.LeaderCommit), uint64(req.ReadContext)} {
		_ = binary.Write(&b, binary.BigEndian, v)
	}
	_ = binary.Write(&b, binary.BigEndian, uint32(len(req.Entries)))
	for _, e := range req.Entries {
		_ = binary.Write(&b, binary.BigEndian, uint64(e.Term))
		b.WriteByte(byte(e.Kind))
		_ = binary.Write(&b, binary.BigEndian, uint32(len(e.Command)))
		b.Write(e.Command)
	}
	return b.Bytes()
}

func TestReplicationPayloadWireCompatibility(t *testing.T) {
	r := rand.New(rand.NewSource(22))
	for i := 0; i < 1007; i++ {
		count, size := r.Intn(65), r.Intn(2048)
		if i < 7 {
			count = []int{0, 1, 1, 1, 64, 1, 0}[i]
			size = []int{0, 16, 0, 32, 1024, maxCommandSize, 0}[i]
		}
		l := &Log{entries: make([]LogEntry, count)}
		for j := range l.entries {
			l.entries[j] = LogEntry{Term: Term(r.Intn(99)), Kind: EntryKind(r.Intn(3)), Command: make([]byte, size)}
			if i < 7 {
				l.entries[j].Kind = EntryApplication
				if i == 2 {
					l.entries[j].Kind = EntryNoop
				}
				if i == 3 {
					l.entries[j].Kind = EntryConfiguration
				}
			}
			_, _ = r.Read(l.entries[j].Command)
		}
		req := AppendEntriesRequest{Term: 99, LeaderID: 7, PrevLogTerm: 5, LeaderCommit: LogIndex(r.Intn(100)), ReadContext: ReadContext(r.Uint64())}
		if i < 6 {
			req.ReadContext = 0
		}
		n := &Node{log: l}
		got, meta, err := n.encodeReplicationLocked(1, req)
		if err != nil {
			t.Fatal(err)
		}
		req.Entries = l.EntriesRange(1, 64, MaxAppendEntriesBytes)
		want, err := EncodeAppendEntries(req)
		if err != nil || !bytes.Equal(got, want) || !bytes.Equal(got, legacyAppendBytes(req)) {
			t.Fatalf("case %d incompatible", i)
		}
		if meta.entryCount != len(req.Entries) {
			t.Fatal("entry count")
		}
		saved := bytes.Clone(got)
		for j := range l.entries {
			for k := range l.entries[j].Command {
				l.entries[j].Command[k] ^= 0xff
			}
		}
		l.entries = append(l.entries, LogEntry{Term: 100})
		l.entries = l.entries[:0]
		if !bytes.Equal(got, saved) {
			t.Fatal("payload aliases log")
		}
	}
}

func TestReplicationPayloadLimitsAndDecodeOwnership(t *testing.T) {
	n := &Node{log: &Log{entries: []LogEntry{{Command: make([]byte, maxCommandSize+1)}}}}
	if _, _, err := n.encodeReplicationLocked(1, AppendEntriesRequest{}); err == nil {
		t.Fatal("oversized command accepted")
	}
	n.log.entries[0].Command = make([]byte, maxCommandSize)
	p, meta, err := n.encodeReplicationLocked(1, AppendEntriesRequest{})
	if err != nil || meta.entryCount != 1 || len(p) != appendEntriesFixedSize+perEntryHeaderSize+maxCommandSize {
		t.Fatalf("large payload: %d %+v %v", len(p), meta, err)
	}
	req, err := DecodeAppendEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	p[len(p)-1] = 1
	if req.Entries[0].Command[maxCommandSize-1] != 0 {
		t.Fatal("decoded command aliases transport payload")
	}
}

func TestReplicationRangeSelection(t *testing.T) {
	l := &Log{baseIndex: 10, entries: make([]LogEntry, 100)}
	for i := range l.entries {
		l.entries[i] = LogEntry{Term: Term(i), Command: make([]byte, 1+i*100)}
	}
	for _, from := range []LogIndex{0, 10, 11, 35, 110, 111, 1000} {
		for _, count := range []int{-1, 0, 1, 64, 200} {
			for _, limit := range []int{0, 16, 1000, MaxAppendEntriesBytes} {
				view := l.entriesRangeView(from, count, limit)
				copy := l.EntriesRange(from, count, limit)
				if !reflect.DeepEqual(view, copy) {
					t.Fatalf("selection %d/%d/%d", from, count, limit)
				}
				if len(copy) > 0 {
					copy[0].Command[0]++
					if bytes.Equal(copy[0].Command, view[0].Command) {
						t.Fatal("public API aliases")
					}
				}
			}
		}
	}
}

func TestReplicationPayloadAllocations(t *testing.T) {
	for _, count := range []int{0, 1, 64} {
		n := &Node{log: &Log{entries: make([]LogEntry, count)}}
		for i := range n.log.entries {
			n.log.entries[i].Command = make([]byte, 1024)
		}
		if allocs := testing.AllocsPerRun(100, func() {
			if _, _, err := n.encodeReplicationLocked(1, AppendEntriesRequest{}); err != nil {
				panic(err)
			}
		}); allocs > 2 {
			t.Fatalf("count=%d allocs=%f", count, allocs)
		}
	}
}

func payloadTestNode(t testing.TB) *Node {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 2}, map[NodeID]string{2: "b"})
	n.role = Leader
	n.workers = map[NodeID]*replicationWorker{2: {cancel: func() {}}}
	n.nextIndex = map[NodeID]LogIndex{2: 1}
	n.matchIndex = map[NodeID]LogIndex{2: 0}
	n.replicationGeneration = map[NodeID]uint64{2: 1}
	n.log.entries = []LogEntry{{Term: 1, Kind: EntryApplication, Command: []byte("original")}}
	return n
}

func TestReplicationPayloadSenderUnlockedAndEquivalent(t *testing.T) {
	var requests []AppendEntriesRequest
	for _, injected := range []bool{false, true} {
		n := payloadTestNode(t)
		check := func(req AppendEntriesRequest) (AppendEntriesResponse, error) {
			if !n.mu.TryLock() {
				t.Fatal("network sender called under Node.mu")
			}
			n.log.entries[0].Command[0] = 'X'
			n.mu.Unlock()
			if string(req.Entries[0].Command) != "original" {
				t.Fatal("request aliases log")
			}
			requests = append(requests, req)
			return AppendEntriesResponse{Term: 2, Success: true}, nil
		}
		if injected {
			n.SetAppendSend(func(_ context.Context, _ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
				return check(req)
			})
		} else {
			n.sendEncodedAppend = func(_ context.Context, _ string, p []byte) (AppendEntriesResponse, error) {
				req, err := DecodeAppendEntries(p)
				if err != nil {
					t.Fatal(err)
				}
				return check(req)
			}
		}
		n.replicationStep(context.Background(), 2)
		if n.matchIndex[2] != 1 {
			t.Fatal("sent metadata did not advance progress")
		}
	}
	if !reflect.DeepEqual(requests[0], requests[1]) {
		t.Fatal("sender paths differ")
	}
}

func TestReplicationPayloadStaleAndRetry(t *testing.T) {
	for _, change := range []string{"snapshot", "removed", "stepdown", "failure"} {
		t.Run(change, func(t *testing.T) {
			n := payloadTestNode(t)
			n.sendEncodedAppend = func(_ context.Context, _ string, p []byte) (AppendEntriesResponse, error) {
				n.mu.Lock()
				defer n.mu.Unlock()
				switch change {
				case "snapshot":
					n.replicationGeneration[2]++
					n.log.baseIndex = 1
					n.log.entries = nil
				case "removed":
					delete(n.workers, 2)
				case "stepdown":
					n.role = Follower
					n.persistent.CurrentTerm++
				case "failure":
					n.log.entries[0].Command = []byte("retry")
					return AppendEntriesResponse{}, errors.New("unavailable")
				}
				return AppendEntriesResponse{Term: 2, Success: true}, nil
			}
			n.replicationStep(context.Background(), 2)
			if n.matchIndex[2] != 0 {
				t.Fatal("stale response advanced progress")
			}
			if change == "failure" {
				n.sendEncodedAppend = func(_ context.Context, _ string, p []byte) (AppendEntriesResponse, error) {
					req, err := DecodeAppendEntries(p)
					if err != nil || string(req.Entries[0].Command) != "retry" {
						t.Fatal("retry used stale payload")
					}
					return AppendEntriesResponse{Term: 2, Success: true}, nil
				}
				n.replicationStep(context.Background(), 2)
			}
		})
	}
}

func TestReplicationPayloadSlowPeerDoesNotBlockQuorum(t *testing.T) {
	n := newTestNode(t, 1, PersistentState{CurrentTerm: 2}, map[NodeID]string{2: "slow", 3: "fast"})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	n.sendEncodedAppend = func(ctx context.Context, addr string, p []byte) (AppendEntriesResponse, error) {
		req, err := DecodeAppendEntries(p)
		if err != nil {
			return AppendEntriesResponse{}, err
		}
		if addr == "slow" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
				return AppendEntriesResponse{}, ctx.Err()
			}
		}
		return AppendEntriesResponse{Term: req.Term, Success: true, ReadContext: req.ReadContext}, nil
	}
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()
	defer close(release)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow worker did not start")
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := n.Propose(make([]byte, maxCommandSize))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = n.ReadIndex(ctx)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow sender blocked proposals or reads")
	}
	n.mu.Lock()
	match := n.matchIndex[3]
	n.mu.Unlock()
	if match == 0 {
		t.Fatal("other worker made no progress")
	}
}
