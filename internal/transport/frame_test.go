package transport

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Message{
		{Type: MessagePing, Payload: nil},
		{Type: MessageTest, Payload: []byte("hello")},
		{Type: MessagePong, Payload: bytes.Repeat([]byte{0x42}, 4096)},
	}
	for _, m := range cases {
		buf, err := EncodeFrame(m)
		if err != nil {
			t.Fatalf("EncodeFrame(%v): %v", m.Type, err)
		}
		got, err := ReadFrame(bytes.NewReader(buf))
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if got.Type != m.Type || !bytes.Equal(got.Payload, m.Payload) {
			t.Fatalf("round trip = %+v, want %+v", got, m)
		}
	}
}

func TestEmptyPayload(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessagePing})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	got, err := ReadFrame(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("Payload = %v, want empty", got.Payload)
	}
}

func TestMaximumAllowedPayload(t *testing.T) {
	m := Message{Type: MessageTest, Payload: bytes.Repeat([]byte{0x01}, MaxPayloadSize)}
	buf, err := EncodeFrame(m)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	got, err := ReadFrame(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(got.Payload) != MaxPayloadSize {
		t.Fatalf("Payload len = %d, want %d", len(got.Payload), MaxPayloadSize)
	}
}

func TestPayloadOneByteTooLargeRejectedAtEncode(t *testing.T) {
	m := Message{Type: MessageTest, Payload: bytes.Repeat([]byte{0x01}, MaxPayloadSize+1)}
	if _, err := EncodeFrame(m); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("EncodeFrame error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestPayloadOneByteTooLargeRejectedAtDecode(t *testing.T) {
	// Build a frame whose header declares MaxPayloadSize+1 without ever
	// allocating that much payload, to mirror what a hostile peer could
	// send: the declared length alone must be rejected before allocation.
	valid, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	tampered := append([]byte(nil), valid...)
	tampered[6] = 0x00
	tampered[7] = 0x10
	tampered[8] = 0x00
	tampered[9] = 0x01 // payload length = MaxPayloadSize + 1

	_, err = ReadFrame(bytes.NewReader(tampered))
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("ReadFrame error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestInvalidMagic(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessagePing})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	buf[0] ^= 0xFF

	_, err = ReadFrame(bytes.NewReader(buf))
	if !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("ReadFrame error = %v, want ErrInvalidMagic", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessagePing})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	buf[4] = 99

	_, err = ReadFrame(bytes.NewReader(buf))
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("ReadFrame error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestUnknownMessageType(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessagePing})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	buf[5] = 0xFF

	_, err = ReadFrame(bytes.NewReader(buf))
	if !errors.Is(err, ErrUnknownMessageType) {
		t.Fatalf("ReadFrame error = %v, want ErrUnknownMessageType", err)
	}
}

func TestChecksumMismatch(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	buf[len(buf)-1] ^= 0xFF

	_, err = ReadFrame(bytes.NewReader(buf))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("ReadFrame error = %v, want ErrChecksumMismatch", err)
	}
}

func TestTruncatedFixedHeader(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	_, err = ReadFrame(bytes.NewReader(buf[:5]))
	if !errors.Is(err, ErrTruncatedFrame) {
		t.Fatalf("ReadFrame error = %v, want ErrTruncatedFrame", err)
	}
}

func TestTruncatedPayload(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	_, err = ReadFrame(bytes.NewReader(buf[:fixedHeaderSize+2]))
	if !errors.Is(err, ErrTruncatedFrame) {
		t.Fatalf("ReadFrame error = %v, want ErrTruncatedFrame", err)
	}
}

func TestTruncatedChecksum(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	_, err = ReadFrame(bytes.NewReader(buf[:len(buf)-2]))
	if !errors.Is(err, ErrTruncatedFrame) {
		t.Fatalf("ReadFrame error = %v, want ErrTruncatedFrame", err)
	}
}

func TestCleanEOFAtFrameBoundary(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("ReadFrame error = %v, want io.EOF", err)
	}
}

// TestKnownByteVector independently derives the expected on-wire bytes for
// a MessageTest("hello") frame rather than round-tripping encode->decode,
// so a shared encoder/decoder bug cannot pass this test. The checksum was
// computed with the standard hash/crc32 Castagnoli table outside this
// package.
func TestKnownByteVector(t *testing.T) {
	got, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	want := []byte{
		'Q', 'K', 'V', '1', // magic
		0x01,                   // version = 1
		0x03,                   // type = MessageTest
		0x00, 0x00, 0x00, 0x05, // payload length = 5
		'h', 'e', 'l', 'l', 'o', // payload
		0xe7, 0x70, 0x2a, 0x30, // CRC32C(version|type|length|payload)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("frame bytes:\n got  % x\n want % x", got, want)
	}
}

// chunkedReader returns bytes in the sizes given by chunks, proving the
// decoder makes no assumption about how a TCP stream is fragmented into
// individual Read calls.
type chunkedReader struct {
	data   []byte
	chunks []int
	pos    int
	next   int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	size := 1
	if r.next < len(r.chunks) {
		size = r.chunks[r.next]
		r.next++
	}
	if size > len(p) {
		size = len(p)
	}
	if r.pos+size > len(r.data) {
		size = len(r.data) - r.pos
	}
	n := copy(p, r.data[r.pos:r.pos+size])
	r.pos += n
	return n, nil
}

func TestFragmentedStreamOneByteAtATime(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello world")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	r := &chunkedReader{data: buf, chunks: []int{1}}
	got, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Type != MessageTest || string(got.Payload) != "hello world" {
		t.Fatalf("got %+v", got)
	}
}

func TestFragmentedStreamIrregularChunks(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello world, this is quorumkv")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	r := &chunkedReader{data: buf, chunks: []int{1, 2, 1, 3, 5, 7, 1, 4}}
	got, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Type != MessageTest || string(got.Payload) != "hello world, this is quorumkv" {
		t.Fatalf("got %+v", got)
	}
}

func TestMultipleFramesInOneStream(t *testing.T) {
	a, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("A")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	b, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("B")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	c, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("C")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	stream := bytes.NewReader(append(append(a, b...), c...))

	for _, want := range []string{"A", "B", "C"} {
		got, err := ReadFrame(stream)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if string(got.Payload) != want {
			t.Fatalf("got payload %q, want %q", got.Payload, want)
		}
	}

	if _, err := ReadFrame(stream); err != io.EOF {
		t.Fatalf("final ReadFrame error = %v, want io.EOF", err)
	}
}

func TestNewMessageDoesNotAliasCallerSlice(t *testing.T) {
	payload := []byte("hello")
	m := NewMessage(MessageTest, payload)
	payload[0] = 'H'

	if string(m.Payload) != "hello" {
		t.Fatalf("Message payload changed after caller mutation: got %q", m.Payload)
	}
}

func TestDecodedPayloadDoesNotAliasInputBuffer(t *testing.T) {
	buf, err := EncodeFrame(Message{Type: MessageTest, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	got, err := ReadFrame(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	got.Payload[0] = 'H'

	if buf[fixedHeaderSize] != 'h' {
		t.Fatalf("mutating decoded payload changed source bytes")
	}
}
