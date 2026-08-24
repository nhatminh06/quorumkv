package clientproto

import (
	"bytes"
	"errors"
	"testing"

	"quorumkv/internal/reqid"
)

func sampleClientID() (id reqid.ClientID) {
	for i := range id {
		id[i] = byte(i)
	}
	return id
}

func TestRequestEncodeDecodeRoundTrip(t *testing.T) {
	id := sampleClientID()
	cases := []Request{
		{Operation: OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")},
		{Operation: OpGet, Key: []byte("x")},
		{Operation: OpDelete, ClientID: id, Sequence: 2, Key: []byte("x")},
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
		if got.Operation != r.Operation || got.ClientID != r.ClientID || got.Sequence != r.Sequence ||
			!bytes.Equal(got.Key, r.Key) || !bytes.Equal(got.Value, r.Value) {
			t.Fatalf("got %+v, want %+v", got, r)
		}
	}
}

// TestPutRequestKnownByteVector independently derives the expected wire
// bytes for a PUT x=1 request with ClientID=00010203...0f, Sequence=7.
func TestPutRequestKnownByteVector(t *testing.T) {
	id := sampleClientID()
	got, err := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 7, Key: []byte("x"), Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	want := []byte{
		2, 1, // version=2, operation=PUT
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, // clientID
		0, 0, 0, 0, 0, 0, 0, 7, // sequence = 7
		0, 0, 0, 1, // key length
		0, 0, 0, 1, // value length
		'x', '1',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestDeleteRequestKnownByteVector independently derives the expected
// wire bytes for a DELETE x request with ClientID=00010203...0f,
// Sequence=3.
func TestDeleteRequestKnownByteVector(t *testing.T) {
	id := sampleClientID()
	got, err := EncodeRequest(Request{Operation: OpDelete, ClientID: id, Sequence: 3, Key: []byte("x")})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	want := []byte{
		2, 3, // version=2, operation=DELETE
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, // clientID
		0, 0, 0, 0, 0, 0, 0, 3, // sequence = 3
		0, 0, 0, 1, // key length
		0, 0, 0, 0, // value length
		'x',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestGetRequestKnownByteVector independently derives the expected wire
// bytes for a GET x request — zero ClientID/Sequence.
func TestGetRequestKnownByteVector(t *testing.T) {
	got, err := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	want := []byte{
		2, 2, // version=2, operation=GET
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // clientID = zero
		0, 0, 0, 0, 0, 0, 0, 0, // sequence = 0
		0, 0, 0, 1, // key length
		0, 0, 0, 0, // value length
		'x',
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
		2, 3, // version, status=NOT_LEADER
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
		2, 1, // version, status=OK
		0, 0, // leader hint length = 0
		0, 0, 0, 1, // value length = 1
		'1',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestRequestConflictResponseKnownByteVector independently derives the
// expected wire bytes for a REQUEST_CONFLICT response.
func TestRequestConflictResponseKnownByteVector(t *testing.T) {
	got, err := EncodeResponse(Response{Status: StatusRequestConflict})
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	want := []byte{2, byte(StatusRequestConflict), 0, 0, 0, 0, 0, 0}
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

// keyLenOff/valLenOff/dataOff locate the variable-size fields within the
// fixed v2 header: version(1) op(1) clientID(16) sequence(8) keyLen(4)
// valLen(4).
const (
	keyLenOff = 2 + 16 + 8
	valLenOff = keyLenOff + 4
	dataOff   = valLenOff + 4
)

func TestDecodeRequestOversizedKeyRejected(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	b[keyLenOff], b[keyLenOff+1], b[keyLenOff+2], b[keyLenOff+3] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestOversizedValueRejected(t *testing.T) {
	id := sampleClientID()
	b, _ := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	b[valLenOff], b[valLenOff+1], b[valLenOff+2], b[valLenOff+3] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestTruncatedKey(t *testing.T) {
	id := sampleClientID()
	b, _ := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 1, Key: []byte("hello"), Value: []byte("1")})
	_, err := DecodeRequest(b[:len(b)-3])
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestTruncatedValue(t *testing.T) {
	id := sampleClientID()
	b, _ := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("hello")})
	_, err := DecodeRequest(b[:len(b)-2])
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestGetCarryingValueRejected(t *testing.T) {
	b := make([]byte, dataOff+2)
	b[0], b[1] = protocolVersion, byte(OpGet)
	b[keyLenOff+3] = 1 // key length = 1
	b[valLenOff+3] = 1 // value length = 1
	b[dataOff], b[dataOff+1] = 'x', '1'
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestDeleteCarryingValueRejected(t *testing.T) {
	id := sampleClientID()
	b := make([]byte, dataOff+2)
	b[0], b[1] = protocolVersion, byte(OpDelete)
	copy(b[2:18], id[:])
	b[25] = 1 // sequence = 1
	b[keyLenOff+3] = 1
	b[valLenOff+3] = 1
	b[dataOff], b[dataOff+1] = 'x', '1'
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

// --- Request identity validation (Milestone 9) ---

func TestEncodeRequestPutRequiresClientID(t *testing.T) {
	_, err := EncodeRequest(Request{Operation: OpPut, Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	if err == nil {
		t.Fatalf("EncodeRequest succeeded for PUT with a zero ClientID, want error")
	}
}

func TestEncodeRequestPutRequiresNonZeroSequence(t *testing.T) {
	id := sampleClientID()
	_, err := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 0, Key: []byte("x"), Value: []byte("1")})
	if err == nil {
		t.Fatalf("EncodeRequest succeeded for PUT with Sequence 0, want error")
	}
}

func TestEncodeRequestDeleteRequiresIdentity(t *testing.T) {
	_, err := EncodeRequest(Request{Operation: OpDelete, Key: []byte("x")})
	if err == nil {
		t.Fatalf("EncodeRequest succeeded for DELETE with no request identity, want error")
	}
}

func TestEncodeRequestGetRejectsIdentity(t *testing.T) {
	id := sampleClientID()
	_, err := EncodeRequest(Request{Operation: OpGet, ClientID: id, Sequence: 1, Key: []byte("x")})
	if err == nil {
		t.Fatalf("EncodeRequest succeeded for GET carrying a request identity, want error")
	}
}

func TestDecodeRequestPutZeroClientIDRejected(t *testing.T) {
	id := sampleClientID()
	b, _ := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	for i := 2; i < 18; i++ {
		b[i] = 0
	}
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestPutZeroSequenceRejected(t *testing.T) {
	id := sampleClientID()
	b, _ := EncodeRequest(Request{Operation: OpPut, ClientID: id, Sequence: 1, Key: []byte("x"), Value: []byte("1")})
	for i := 18; i < 26; i++ {
		b[i] = 0
	}
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
	}
}

func TestDecodeRequestGetCarryingIdentityRejected(t *testing.T) {
	b, _ := EncodeRequest(Request{Operation: OpGet, Key: []byte("x")})
	b[2] = 1 // give the zero ClientID a non-zero byte
	_, err := DecodeRequest(b)
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("err = %v, want ErrMalformedRequest", err)
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

func TestDecodeResponseStaleRequestAndConflictRoundTrip(t *testing.T) {
	for _, status := range []Status{StatusStaleRequest, StatusRequestConflict} {
		got, err := DecodeResponse(mustEncodeResponse(t, Response{Status: status}))
		if err != nil {
			t.Fatalf("DecodeResponse(%v): %v", status, err)
		}
		if got.Status != status {
			t.Fatalf("Status = %v, want %v", got.Status, status)
		}
	}
}
