package raft

import (
	"bytes"
	"errors"
	"testing"
)

func sampleAppendEntries() AppendEntriesRequest {
	return AppendEntriesRequest{
		Term:         4,
		LeaderID:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		Entries:      []LogEntry{{Term: 4, Command: []byte("x")}},
		LeaderCommit: 1,
	}
}

func TestAppendEntriesEncodeDecodeRoundTrip(t *testing.T) {
	req := sampleAppendEntries()
	buf, err := EncodeAppendEntries(req)
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}
	got, err := DecodeAppendEntries(buf)
	if err != nil {
		t.Fatalf("DecodeAppendEntries: %v", err)
	}
	if got.Term != req.Term || got.LeaderID != req.LeaderID || got.PrevLogIndex != req.PrevLogIndex ||
		got.PrevLogTerm != req.PrevLogTerm || got.LeaderCommit != req.LeaderCommit || len(got.Entries) != 1 ||
		got.Entries[0].Term != 4 || string(got.Entries[0].Command) != "x" {
		t.Fatalf("got %+v, want %+v", got, req)
	}
}

func TestAppendEntriesEmptyEntriesRoundTrip(t *testing.T) {
	req := AppendEntriesRequest{Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 0}
	buf, err := EncodeAppendEntries(req)
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}
	got, err := DecodeAppendEntries(buf)
	if err != nil {
		t.Fatalf("DecodeAppendEntries: %v", err)
	}
	if len(got.Entries) != 0 {
		t.Fatalf("Entries = %v, want empty (heartbeat)", got.Entries)
	}
}

// TestAppendEntriesKnownByteVector independently derives the expected
// wire bytes for term=4, leaderId=1, prevLogIndex=2, prevLogTerm=3,
// entries=[{term=4,command="x"}], leaderCommit=1, readContext=0 (ordinary
// replication — not a ReadIndex probe).
func TestAppendEntriesKnownByteVector(t *testing.T) {
	got, err := EncodeAppendEntries(sampleAppendEntries())
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}

	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 4, // term
		0, 0, 0, 0, 0, 0, 0, 1, // leaderID
		0, 0, 0, 0, 0, 0, 0, 2, // prevLogIndex
		0, 0, 0, 0, 0, 0, 0, 3, // prevLogTerm
		0, 0, 0, 0, 0, 0, 0, 1, // leaderCommit
		0, 0, 0, 0, 0, 0, 0, 0, // readContext = 0
		0, 0, 0, 1, // entryCount = 1
		0, 0, 0, 0, 0, 0, 0, 4, // entry term
		0, 0, 0, 1, // entry command length
		'x', // command
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestAppendEntriesReadProbeKnownByteVector independently derives the
// expected wire bytes for a ReadIndex probe: term=9, leaderId=2,
// prevLogIndex=20, prevLogTerm=8, no entries, leaderCommit=20,
// readContext=12345 — proving the field is encoded independently of the
// ordinary (readContext=0) path above, not merely round-tripped through
// the production encoder.
func TestAppendEntriesReadProbeKnownByteVector(t *testing.T) {
	req := AppendEntriesRequest{
		Term: 9, LeaderID: 2, PrevLogIndex: 20, PrevLogTerm: 8,
		LeaderCommit: 20, ReadContext: 12345,
	}
	got, err := EncodeAppendEntries(req)
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}

	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 9, // term
		0, 0, 0, 0, 0, 0, 0, 2, // leaderID
		0, 0, 0, 0, 0, 0, 0, 20, // prevLogIndex
		0, 0, 0, 0, 0, 0, 0, 8, // prevLogTerm
		0, 0, 0, 0, 0, 0, 0, 20, // leaderCommit
		0, 0, 0, 0, 0, 0, 48, 57, // readContext = 12345 (0x3039)
		0, 0, 0, 0, // entryCount = 0
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}

	back, err := DecodeAppendEntries(got)
	if err != nil {
		t.Fatalf("DecodeAppendEntries: %v", err)
	}
	if back.ReadContext != 12345 {
		t.Fatalf("ReadContext = %d, want 12345", back.ReadContext)
	}
}

func TestDecodeAppendEntriesTooShort(t *testing.T) {
	_, err := DecodeAppendEntries([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeAppendEntriesEntryCountTooLarge(t *testing.T) {
	buf, _ := EncodeAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1})
	// entryCount field is the last 4 bytes of the fixed header.
	buf[appendEntriesFixedSize-4] = 0xFF
	buf[appendEntriesFixedSize-3] = 0xFF
	buf[appendEntriesFixedSize-2] = 0xFF
	buf[appendEntriesFixedSize-1] = 0xFF
	_, err := DecodeAppendEntries(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeAppendEntriesCommandLengthTooLarge(t *testing.T) {
	req := AppendEntriesRequest{Term: 1, LeaderID: 1, Entries: []LogEntry{{Term: 1, Command: []byte("x")}}}
	buf, err := EncodeAppendEntries(req)
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}
	// The entry's command-length field is the 4 bytes right after its
	// 8-byte term, at the start of the (only) entry.
	cmdLenOff := appendEntriesFixedSize + 8
	buf[cmdLenOff] = 0xFF
	buf[cmdLenOff+1] = 0xFF
	buf[cmdLenOff+2] = 0xFF
	buf[cmdLenOff+3] = 0xFF
	_, err = DecodeAppendEntries(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeAppendEntriesTruncatedCommand(t *testing.T) {
	req := AppendEntriesRequest{Term: 1, LeaderID: 1, Entries: []LogEntry{{Term: 1, Command: []byte("hello")}}}
	buf, err := EncodeAppendEntries(req)
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}
	_, err = DecodeAppendEntries(buf[:len(buf)-2]) // cut into the command bytes
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeAppendEntriesTrailingBytesRejected(t *testing.T) {
	buf, err := EncodeAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1})
	if err != nil {
		t.Fatalf("EncodeAppendEntries: %v", err)
	}
	buf = append(buf, 0xFF) // unexpected trailing byte
	_, err = DecodeAppendEntries(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestEncodeAppendEntriesRejectsTooManyEntries(t *testing.T) {
	entries := make([]LogEntry, maxEntriesPerAppend+1)
	for i := range entries {
		entries[i] = LogEntry{Term: 1, Command: []byte("x")}
	}
	_, err := EncodeAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1, Entries: entries})
	if err == nil {
		t.Fatalf("EncodeAppendEntries succeeded with too many entries, want error")
	}
}

func TestEncodeAppendEntriesRejectsOversizedCommand(t *testing.T) {
	oversized := make([]byte, maxCommandSize+1)
	_, err := EncodeAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: 1, Entries: []LogEntry{{Term: 1, Command: oversized}}})
	if err == nil {
		t.Fatalf("EncodeAppendEntries succeeded with oversized command, want error")
	}
}

func TestAppendEntriesResponseEncodeDecodeRoundTrip(t *testing.T) {
	for _, success := range []bool{true, false} {
		resp := AppendEntriesResponse{Term: 5, Success: success, MatchIndex: 9, ReadContext: 777}
		got, err := DecodeAppendEntriesResponse(EncodeAppendEntriesResponse(resp))
		if err != nil {
			t.Fatalf("DecodeAppendEntriesResponse: %v", err)
		}
		if got != resp {
			t.Fatalf("got %+v, want %+v", got, resp)
		}
	}
}

// TestAppendEntriesResponseKnownByteVector independently derives the
// expected wire bytes for term=5, success=false, matchIndex=0,
// readContext=12345 — the shape a read-probe response takes when the
// follower rejects the log-prefix check (Success=false) but still echoes
// the ReadContext, which is what lets it count toward ReadIndex quorum
// despite the replication failure (see docs/read-index.md).
func TestAppendEntriesResponseKnownByteVector(t *testing.T) {
	resp := AppendEntriesResponse{Term: 5, Success: false, MatchIndex: 0, ReadContext: 12345}
	got := EncodeAppendEntriesResponse(resp)
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 5, // term
		0,                      // success = false
		0, 0, 0, 0, 0, 0, 0, 0, // matchIndex
		0, 0, 0, 0, 0, 0, 48, 57, // readContext = 12345 (0x3039)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestAppendEntriesResponseTooShort(t *testing.T) {
	_, err := DecodeAppendEntriesResponse([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestAppendEntriesResponseInvalidSuccessByte(t *testing.T) {
	buf := EncodeAppendEntriesResponse(AppendEntriesResponse{Term: 1, Success: true})
	buf[8] = 7
	_, err := DecodeAppendEntriesResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestAppendEntriesResponseTrailingBytes(t *testing.T) {
	buf := EncodeAppendEntriesResponse(AppendEntriesResponse{Term: 1, Success: true})
	buf = append(buf, 0xFF)
	_, err := DecodeAppendEntriesResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}
