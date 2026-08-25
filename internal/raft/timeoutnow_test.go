package raft

import (
	"bytes"
	"errors"
	"testing"
)

func sampleTimeoutNow() TimeoutNowRequest {
	return TimeoutNowRequest{Term: 12, LeaderID: 1}
}

func TestTimeoutNowEncodeDecodeRoundTrip(t *testing.T) {
	req := sampleTimeoutNow()
	got, err := DecodeTimeoutNow(EncodeTimeoutNow(req))
	if err != nil {
		t.Fatalf("DecodeTimeoutNow: %v", err)
	}
	if got != req {
		t.Fatalf("got %+v, want %+v", got, req)
	}
}

// TestTimeoutNowKnownByteVector independently derives the expected wire
// bytes for term=12, leaderID=1.
func TestTimeoutNowKnownByteVector(t *testing.T) {
	got := EncodeTimeoutNow(sampleTimeoutNow())
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 12, // term
		0, 0, 0, 0, 0, 0, 0, 1, // leaderID
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeTimeoutNowTooShort(t *testing.T) {
	_, err := DecodeTimeoutNow([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeTimeoutNowTrailingBytes(t *testing.T) {
	buf := append(EncodeTimeoutNow(sampleTimeoutNow()), 0xFF)
	_, err := DecodeTimeoutNow(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestTimeoutNowResponseEncodeDecodeRoundTrip(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		resp := TimeoutNowResponse{Term: 12, Accepted: accepted}
		got, err := DecodeTimeoutNowResponse(EncodeTimeoutNowResponse(resp))
		if err != nil {
			t.Fatalf("DecodeTimeoutNowResponse: %v", err)
		}
		if got != resp {
			t.Fatalf("got %+v, want %+v", got, resp)
		}
	}
}

// TestTimeoutNowResponseKnownByteVector independently derives the
// expected wire bytes for term=12, accepted=true.
func TestTimeoutNowResponseKnownByteVector(t *testing.T) {
	got := EncodeTimeoutNowResponse(TimeoutNowResponse{Term: 12, Accepted: true})
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 12, // term
		1, // accepted = true
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeTimeoutNowResponseTooShort(t *testing.T) {
	_, err := DecodeTimeoutNowResponse([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeTimeoutNowResponseInvalidAcceptedByte(t *testing.T) {
	buf := EncodeTimeoutNowResponse(TimeoutNowResponse{Term: 1, Accepted: true})
	buf[8] = 7
	_, err := DecodeTimeoutNowResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}

func TestDecodeTimeoutNowResponseTrailingBytes(t *testing.T) {
	buf := append(EncodeTimeoutNowResponse(TimeoutNowResponse{Term: 1, Accepted: true}), 0xFF)
	_, err := DecodeTimeoutNowResponse(buf)
	if !errors.Is(err, ErrMalformedRPC) {
		t.Fatalf("err = %v, want ErrMalformedRPC", err)
	}
}
