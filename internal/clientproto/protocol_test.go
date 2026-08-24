package clientproto

import (
	"bytes"
	"errors"
	"testing"
)

func TestRequestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Request{
		{Operation: OpPut, Key: []byte("x"), Value: []byte("1")},
		{Operation: OpGet, Key: []byte("x")},
		{Operation: OpDelete, Key: []byte("x")},
	}
	for _, r := range cases {
		buf, err := EncodeRequest(r)
		if err != nil {
			t.Fatalf("EncodeRequest(%v): %v", r.Operation, err)
		}
		got, err := DecodeRequest(buf)
		if err != nil {
			t.Fatalf("DecodeRequest: %v", err)
		}
		if got.Operation != r.Operation || !bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) {
			t.Fatalf("got %+v, want %+v", got, r)
		}
	}
}

// TestPutRequestKnownByteVector independently derives the expected wire
// bytes for a PUT x=1 request.
func TestPutRequestKnownByteVector(t *testing.T) {
	got, err := EncodeRequest(Request{Operation: OpPut, Key: []byte("x"), Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	want := []byte{
		1, 1, // version, operation=PUT
		0, 0, 0, 1, // key length
		0, 0, 0, 1, // value length
		'x', '1',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestNotLeaderResponseKnownByteVector independently derives the expected
// wire bytes for a NOT_LEADER response carrying leader hint
// "127.0.0.1:9001".
func TestNotLeaderResponseKnownByteVector(t *testing.T) {
	got, err := EncodeResponse(Response{Status: StatusNotLeader, LeaderHint: []byte("127.0.0.1:9001")})
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	want := []byte{
		1, 3, // version, status=NOT_LEADER
		0, 14, // leader hint length = 14
		0, 0, 0, 0, // value length = 0
	}
	want = append(want, []byte("127.0.0.1:9001")...)
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestOKGetResponseKnownByteVector independently derives the expected
// wire bytes for an OK GET response with value "1".
func TestOKGetResponseKnownByteVector(t *testing.T) {
	got, err := EncodeResponse(Response{Status: StatusOK, Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	want := []byte{
		1, 1, // version, status=OK
		0, 0, // leader hint length = 0
		0, 0, 0, 1, // value length = 1
		'1',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeRequestTooShort(t *testing.T) {
	_, err := DecodeRequest([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestUnsupportedVersion(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	b[0] = 99
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestUnknownOperation(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	b[1] = 99
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestOversizedKeyRejected(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	b[2], b[3], b[4], b[5] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestOversizedValueRejected(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpPut, Key: []byte("x"), Value: []byte("1")})
	b[6], b[7], b[8], b[9] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestTruncatedKey(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpPut, Key: []byte("hello"), Value: []byte("1")})
	_, err := DecodeRequest(b[:len(b)-3])
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestTruncatedValue(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpPut, Key: []byte("x"), Value: []byte("hello")})
	_, err := DecodeRequest(b[:len(b)-2])
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestGetCarryingValueRejected(t *testing.T) {
	b := []byte{1, byte(OpGet), 0, 0, 0, 1, 0, 0, 0, 1, 'x', '1'}
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestDeleteCarryingValueRejected(t *testing.T) {
	b := []byte{1, byte(OpDelete), 0, 0, 0, 1, 0, 0, 0, 1, 'x', '1'}
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestTrailingBytes(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	b = append(b, 0xFF)
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestEncodeRequestGetWithValueRejected(t *testing.T) {
	_, err := EncodeRequest(Request{Operation: OpGet, Key: []byte("x"), Value: []byte("1")})
	if err == nil {
		t.Fatalf("EncodeRequest succeeded for GET with a value, want error")
	}
}

func TestResponseEncodeDecodeRoundTrip(t *testing.T) {
	r := Response{Status: StatusOK, Value: []byte("v"), LeaderHint: nil}
	got, err := DecodeResponse(mustEncodeResponse(t, r))
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if got.Status != r.Status || !bytes.Equal(got.Value, r.Value) {
		t.Fatalf("got %+v, want %+v", got, r)
	}
}

func mustEncodeResponse(t *testing.T, r Response) []byte {
	t.Helper()
	b, err := EncodeResponse(r)
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	return b
}

func TestDecodeResponseTooShort(t *testing.T) {
	_, err := DecodeResponse([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("err = %v, want ErrMalformedResponse", err)
	}
}

func TestDecodeResponseUnknownStatus(t *testing.T) {
	b := mustEncodeResponse(t, Response{Status: StatusOK})
	b[1] = 99
	_, err := DecodeResponse(b)
	if !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("err = %v, want ErrMalformedResponse", err)
	}
}

func TestDecodeResponseTrailingBytes(t *testing.T) {
	b := mustEncodeResponse(t, Response{Status: StatusOK})
	b = append(b, 0xFF)
	_, err := DecodeResponse(b)
	if !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("err = %v, want ErrMalformedResponse", err)
	}
}

func TestDecodeResponseOversizedLeaderHintRejected(t *testing.T) {
	b := mustEncodeResponse(t, Response{Status: StatusNotLeader})
	b[2], b[3] = 0xFF, 0xFF
	_, err := DecodeResponse(b)
	if !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("err = %v, want ErrMalformedResponse", err)
	}
}

func TestEncodeResponseOversizedLeaderHintRejected(t *testing.T) {
	_, err := EncodeResponse(Response{Status: StatusNotLeader, LeaderHint: make([]byte, MaxLeaderHintSize+1)})
	if err == nil {
		t.Fatalf("EncodeResponse succeeded with oversized leader hint, want error")
	}
}
