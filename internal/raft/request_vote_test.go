package raft

import (
	"bytes"
	"errors"
	"testing"
)

func TestRequestVoteEncodeDecodeRoundTrip(t *testing.T) {
	req := RequestVoteRequest{Term: 7, CandidateID: 3, LastLogIndex: 11, LastLogTerm: 6}
	got, err := DecodeRequestVote(EncodeRequestVote(req))
	if err != nil {
		t.Fatalf("DecodeRequestVote: %v", err)
	}
	if got != req {
		t.Fatalf("got %+v, want %+v", got, req)
	}
}

// TestRequestVoteKnownByteVector independently derives the expected wire
// bytes for term=7, candidateID=3, lastLogIndex=11, lastLogTerm=6 rather
// than round-tripping encode->decode.
func TestRequestVoteKnownByteVector(t *testing.T) {
	got := EncodeRequestVote(RequestVoteRequest{Term: 7, CandidateID: 3, LastLogIndex: 11, LastLogTerm: 6})

	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 7, // term
		0, 0, 0, 0, 0, 0, 0, 3, // candidateID
		0, 0, 0, 0, 0, 0, 0, 11, // lastLogIndex
		0, 0, 0, 0, 0, 0, 0, 6, // lastLogTerm
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeRequestVoteRejectsWrongLength(t *testing.T) {
	_, err := DecodeRequestVote([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestRequestVoteResponseEncodeDecodeRoundTrip(t *testing.T) {
	for _, granted := range []bool{true, false} {
		resp := RequestVoteResponse{Term: 9, VoteGranted: granted}
		got, err := DecodeRequestVoteResponse(EncodeRequestVoteResponse(resp))
		if err != nil {
			t.Fatalf("DecodeRequestVoteResponse: %v", err)
		}
		if got != resp {
			t.Fatalf("got %+v, want %+v", got, resp)
		}
	}
}

func TestRequestVoteResponseKnownByteVector(t *testing.T) {
	got := EncodeRequestVoteResponse(RequestVoteResponse{Term: 9, VoteGranted: true})
	want := []byte{0, 0, 0, 0, 0, 0, 0, 9, 1}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeRequestVoteResponseRejectsWrongLength(t *testing.T) {
	_, err := DecodeRequestVoteResponse([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeRequestVoteResponseRejectsInvalidBoolean(t *testing.T) {
	buf := EncodeRequestVoteResponse(RequestVoteResponse{Term: 9, VoteGranted: true})
	buf[8] = 7
	_, err := DecodeRequestVoteResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestLogUpToDateHigherTermWins(t *testing.T) {
	if !LogUpToDate(5, 0, 4, 100) {
		t.Fatalf("higher candidate term should be up to date regardless of index")
	}
}

func TestLogUpToDateLowerTermLoses(t *testing.T) {
	if LogUpToDate(3, 100, 4, 0) {
		t.Fatalf("lower candidate term should not be up to date regardless of index")
	}
}

func TestLogUpToDateSameTermHigherIndexWins(t *testing.T) {
	if !LogUpToDate(4, 10, 4, 5) {
		t.Fatalf("same term, higher candidate index should be up to date")
	}
}

func TestLogUpToDateSameTermSameIndexWins(t *testing.T) {
	if !LogUpToDate(4, 5, 4, 5) {
		t.Fatalf("same term, equal index should be up to date")
	}
}

func TestLogUpToDateSameTermLowerIndexLoses(t *testing.T) {
	if LogUpToDate(4, 3, 4, 5) {
		t.Fatalf("same term, lower candidate index should not be up to date")
	}
}

func TestMajority(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3}
	for clusterSize, want := range cases {
		if got := Majority(clusterSize); got != want {
			t.Errorf("Majority(%d) = %d, want %d", clusterSize, got, want)
		}
	}
}
