package kv

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodePutRoundTrip(t *testing.T) {
	cmd := NewPutCommand([]byte("x"), []byte("1"))
	got, err := DecodeCommand(mustEncode(t, cmd))
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.Type != CommandPut || string(got.Key) != "x" || string(got.Value) != "1" {
		t.Fatalf("got %+v", got)
	}
}

func TestEncodeDecodeDeleteRoundTrip(t *testing.T) {
	cmd := NewDeleteCommand([]byte("x"))
	got, err := DecodeCommand(mustEncode(t, cmd))
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.Type != CommandDelete || string(got.Key) != "x" || len(got.Value) != 0 {
		t.Fatalf("got %+v", got)
	}
}

func mustEncode(t *testing.T, cmd Command) []byte {
	t.Helper()
	b, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	return b
}

// TestPutKnownByteVector independently derives the expected bytes for
// PUT x=1 rather than round-tripping encode->decode.
func TestPutKnownByteVector(t *testing.T) {
	got := mustEncode(t, NewPutCommand([]byte("x"), []byte("1")))
	want := []byte{
		0x01,       // version
		0x01,       // operation = PUT
		0, 0, 0, 1, // key length = 1
		0, 0, 0, 1, // value length = 1
		'x', // key
		'1', // value
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestDeleteKnownByteVector independently derives the expected bytes for
// DELETE x.
func TestDeleteKnownByteVector(t *testing.T) {
	got := mustEncode(t, NewDeleteCommand([]byte("x")))
	want := []byte{
		0x01,       // version
		0x02,       // operation = DELETE
		0, 0, 0, 1, // key length = 1
		0, 0, 0, 0, // value length = 0
		'x', // key
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func TestDecodeCommandTooShort(t *testing.T) {
	_, err := DecodeCommand([]byte{1, 2, 3})
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandUnsupportedVersion(t *testing.T) {
	b := mustEncode(t, NewPutCommand([]byte("x"), []byte("1")))
	b[0] = 99
	_, err := DecodeCommand(b)
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandUnknownOperation(t *testing.T) {
	b := mustEncode(t, NewPutCommand([]byte("x"), []byte("1")))
	b[1] = 99
	_, err := DecodeCommand(b)
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandTruncatedKey(t *testing.T) {
	b := mustEncode(t, NewPutCommand([]byte("hello"), []byte("1")))
	_, err := DecodeCommand(b[:len(b)-3]) // cut into key+value region
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandTruncatedValue(t *testing.T) {
	b := mustEncode(t, NewPutCommand([]byte("x"), []byte("hello")))
	_, err := DecodeCommand(b[:len(b)-2])
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandTrailingBytes(t *testing.T) {
	b := mustEncode(t, NewPutCommand([]byte("x"), []byte("1")))
	b = append(b, 0xFF)
	_, err := DecodeCommand(b)
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandOversizedKeyLengthRejected(t *testing.T) {
	b := mustEncode(t, NewPutCommand([]byte("x"), []byte("1")))
	// Declare a key length far beyond MaxKeySize without allocating that
	// much data — the check must fire before any such allocation.
	b[2], b[3], b[4], b[5] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := DecodeCommand(b)
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestDecodeCommandDeleteWithValueRejected(t *testing.T) {
	// Hand-construct a DELETE record carrying an (invalid) value.
	b := []byte{1, byte(CommandDelete), 0, 0, 0, 1, 0, 0, 0, 1, 'x', '1'}
	_, err := DecodeCommand(b)
	if !errors.Is(err, ErrMalformedCommand) {
		t.Fatalf("err = %v, want ErrMalformedCommand", err)
	}
}

func TestEncodeCommandDeleteWithValueRejected(t *testing.T) {
	cmd := Command{Type: CommandDelete, Key: []byte("x"), Value: []byte("1")}
	if _, err := EncodeCommand(cmd); err == nil {
		t.Fatalf("EncodeCommand succeeded for DELETE with a value, want error")
	}
}

func TestEncodeCommandOversizedKeyRejected(t *testing.T) {
	cmd := NewPutCommand(make([]byte, MaxKeySize+1), []byte("v"))
	if _, err := EncodeCommand(cmd); err == nil {
		t.Fatalf("EncodeCommand succeeded with oversized key, want error")
	}
}

func TestEncodeCommandOversizedValueRejected(t *testing.T) {
	cmd := NewPutCommand([]byte("k"), make([]byte, MaxValueSize+1))
	if _, err := EncodeCommand(cmd); err == nil {
		t.Fatalf("EncodeCommand succeeded with oversized value, want error")
	}
}
