package raft

import (
	"bytes"
	"errors"
	"testing"
)

func sampleInstallSnapshot() InstallSnapshotRequest {
	return InstallSnapshotRequest{
		Term: 7, LeaderID: 1, LastIncludedIndex: 100, LastIncludedTerm: 6,
		Offset: 0, Data: []byte("abc"), Done: false,
	}
}

func TestInstallSnapshotEncodeDecodeRoundTrip(t *testing.T) {
	req := sampleInstallSnapshot()
	buf, err := EncodeInstallSnapshot(req)
	if err != nil {
		t.Fatalf("EncodeInstallSnapshot: %v", err)
	}
	got, err := DecodeInstallSnapshot(buf)
	if err != nil {
		t.Fatalf("DecodeInstallSnapshot: %v", err)
	}
	if got.Term != req.Term || got.LeaderID != req.LeaderID || got.LastIncludedIndex != req.LastIncludedIndex ||
		got.LastIncludedTerm != req.LastIncludedTerm || got.Offset != req.Offset || got.Done != req.Done ||
		string(got.Data) != string(req.Data) {
		t.Fatalf("got %+v, want %+v", got, req)
	}
}

func TestInstallSnapshotZeroLengthFinalChunkRoundTrip(t *testing.T) {
	req := InstallSnapshotRequest{Term: 1, LeaderID: 1, LastIncludedIndex: 5, LastIncludedTerm: 1, Offset: 10, Done: true}
	buf, err := EncodeInstallSnapshot(req)
	if err != nil {
		t.Fatalf("EncodeInstallSnapshot: %v", err)
	}
	got, err := DecodeInstallSnapshot(buf)
	if err != nil {
		t.Fatalf("DecodeInstallSnapshot: %v", err)
	}
	if !got.Done || len(got.Data) != 0 {
		t.Fatalf("got %+v, want Done=true, empty Data", got)
	}
}

// TestInstallSnapshotKnownByteVector independently derives the expected
// wire bytes for term=7, leaderID=1, lastIncludedIndex=100,
// lastIncludedTerm=6, offset=0, data="abc", done=false.
func TestInstallSnapshotKnownByteVector(t *testing.T) {
	got, err := EncodeInstallSnapshot(sampleInstallSnapshot())
	if err != nil {
		t.Fatalf("EncodeInstallSnapshot: %v", err)
	}
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 7, // term
		0, 0, 0, 0, 0, 0, 0, 1, // leaderID
		0, 0, 0, 0, 0, 0, 0, 100, // lastIncludedIndex
		0, 0, 0, 0, 0, 0, 0, 6, // lastIncludedTerm
		0, 0, 0, 0, 0, 0, 0, 0, // offset
		0,          // done = false
		0, 0, 0, 3, // data length
		'a', 'b', 'c',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeInstallSnapshotTooShort(t *testing.T) {
	_, err := DecodeInstallSnapshot([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeInstallSnapshotInvalidDoneByte(t *testing.T) {
	buf, err := EncodeInstallSnapshot(sampleInstallSnapshot())
	if err != nil {
		t.Fatalf("EncodeInstallSnapshot: %v", err)
	}
	buf[40] = 7 // done byte offset: 8*5 = 40
	_, err = DecodeInstallSnapshot(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeInstallSnapshotOversizedChunkRejected(t *testing.T) {
	buf, err := EncodeInstallSnapshot(sampleInstallSnapshot())
	if err != nil {
		t.Fatalf("EncodeInstallSnapshot: %v", err)
	}
	// data length field: offset 41..45.
	buf[41], buf[42], buf[43], buf[44] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err = DecodeInstallSnapshot(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeInstallSnapshotTrailingBytes(t *testing.T) {
	buf, err := EncodeInstallSnapshot(sampleInstallSnapshot())
	if err != nil {
		t.Fatalf("EncodeInstallSnapshot: %v", err)
	}
	buf = append(buf, 0xFF)
	_, err = DecodeInstallSnapshot(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestEncodeInstallSnapshotRejectsOversizedChunk(t *testing.T) {
	req := InstallSnapshotRequest{Term: 1, LeaderID: 1, Data: make([]byte, maxSnapshotChunkSize+1)}
	_, err := EncodeInstallSnapshot(req)
	if err == nil {
		t.Fatalf("EncodeInstallSnapshot succeeded with oversized chunk, want error")
	}
}

func TestInstallSnapshotResponseEncodeDecodeRoundTrip(t *testing.T) {
	for _, success := range []bool{true, false} {
		resp := InstallSnapshotResponse{Term: 9, Success: success, NextOffset: 12345}
		got, err := DecodeInstallSnapshotResponse(EncodeInstallSnapshotResponse(resp))
		if err != nil {
			t.Fatalf("DecodeInstallSnapshotResponse: %v", err)
		}
		if got != resp {
			t.Fatalf("got %+v, want %+v", got, resp)
		}
	}
}

func TestDecodeInstallSnapshotResponseTooShort(t *testing.T) {
	_, err := DecodeInstallSnapshotResponse([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeInstallSnapshotResponseInvalidSuccessByte(t *testing.T) {
	buf := EncodeInstallSnapshotResponse(InstallSnapshotResponse{Term: 1, Success: true})
	buf[8] = 7
	_, err := DecodeInstallSnapshotResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeInstallSnapshotResponseTrailingBytes(t *testing.T) {
	buf := EncodeInstallSnapshotResponse(InstallSnapshotResponse{Term: 1, Success: true})
	buf = append(buf, 0xFF)
	_, err := DecodeInstallSnapshotResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}
