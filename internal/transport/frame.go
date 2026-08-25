package transport

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// magic identifies bytes on the wire as a QuorumKV transport frame so
// arbitrary TCP noise is not mistaken for one.
var magic = [4]byte{'Q', 'K', 'V', '1'}

const protocolVersion = 1

// MaxPayloadSize bounds a frame's declared payload length. It must be
// checked before allocating a payload buffer for peer-supplied data.
const MaxPayloadSize = 1 << 20 // 1 MiB

const (
	magicSize       = 4
	fixedHeaderSize = magicSize + 1 /* version */ + 1 /* type */ + 4 /* payload length */
	checksumSize    = 4
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrInvalidMagic       = errors.New("transport: invalid frame magic")
	ErrUnsupportedVersion = errors.New("transport: unsupported protocol version")
	ErrUnknownMessageType = errors.New("transport: unknown message type")
	ErrPayloadTooLarge    = errors.New("transport: payload exceeds MaxPayloadSize")
	ErrChecksumMismatch   = errors.New("transport: checksum mismatch")
	ErrTruncatedFrame     = errors.New("transport: truncated frame")
)

func isKnownMessageType(t MessageType) bool {
	switch t {
	case MessagePing, MessagePong, MessageTest, MessageRequestVote, MessageRequestVoteResponse, MessageAppendEntries, MessageAppendEntriesResponse, MessageClientRequest, MessageClientResponse, MessageInstallSnapshot, MessageInstallSnapshotResponse, MessagePreVote, MessagePreVoteResponse, MessageTimeoutNow, MessageTimeoutNowResponse, MessageAdminRequest, MessageAdminResponse:
		return true
	default:
		return false
	}
}

// EncodeFrame produces the exact wire bytes for m:
//
//	magic (4B) | version (1B) | type (1B) | payload length (4B, BE) | payload | checksum (4B, BE)
//
// The CRC32C checksum covers version, type, payload length, and payload —
// not the magic and not itself. All integers are big-endian.
func EncodeFrame(m Message) ([]byte, error) {
	if !isKnownMessageType(m.Type) {
		return nil, ErrUnknownMessageType
	}
	if len(m.Payload) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}

	buf := make([]byte, fixedHeaderSize+len(m.Payload)+checksumSize)
	copy(buf[0:4], magic[:])
	buf[4] = protocolVersion
	buf[5] = byte(m.Type)
	binary.BigEndian.PutUint32(buf[6:10], uint32(len(m.Payload)))
	copy(buf[fixedHeaderSize:], m.Payload)

	checksumStart := fixedHeaderSize + len(m.Payload)
	checksum := crc32.Checksum(buf[4:checksumStart], crc32cTable)
	binary.BigEndian.PutUint32(buf[checksumStart:checksumStart+checksumSize], checksum)

	return buf, nil
}

// WriteFrame encodes m and writes it to w, looping over any short write.
func WriteFrame(w io.Writer, m Message) error {
	buf, err := EncodeFrame(m)
	if err != nil {
		return err
	}
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// ReadFrame reads and validates one frame from r.
//
// If r is at a clean frame boundary and has no more data, ReadFrame
// returns io.EOF. Any other incomplete read (a partial header, payload, or
// checksum) is a truncated frame and returns ErrTruncatedFrame — it is
// never treated as a valid message.
//
// The declared payload length is validated against MaxPayloadSize before
// any payload buffer is allocated.
func ReadFrame(r io.Reader) (Message, error) {
	header := make([]byte, fixedHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF {
			return Message{}, io.EOF
		}
		return Message{}, ErrTruncatedFrame
	}

	if [4]byte(header[0:4]) != magic {
		return Message{}, ErrInvalidMagic
	}
	version := header[4]
	if version != protocolVersion {
		return Message{}, ErrUnsupportedVersion
	}
	typ := MessageType(header[5])
	if !isKnownMessageType(typ) {
		return Message{}, ErrUnknownMessageType
	}
	payloadLen := binary.BigEndian.Uint32(header[6:10])
	if payloadLen > MaxPayloadSize {
		return Message{}, ErrPayloadTooLarge
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Message{}, ErrTruncatedFrame
	}

	checksumBuf := make([]byte, checksumSize)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return Message{}, ErrTruncatedFrame
	}
	wantChecksum := binary.BigEndian.Uint32(checksumBuf)

	gotChecksum := crc32.Checksum(header[4:fixedHeaderSize], crc32cTable)
	gotChecksum = crc32.Update(gotChecksum, crc32cTable, payload)
	if gotChecksum != wantChecksum {
		return Message{}, ErrChecksumMismatch
	}

	return Message{Type: typ, Payload: payload}, nil
}
