// Package wal implements an append-only write-ahead log used to persist
// kv.Command values in the order they were applied. See docs/wal.md for the
// on-disk record format and the corruption/truncation policy this package
// implements.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"quorumkv/internal/kv"
)

const (
	// headerSize is the fixed-width portion of a record body: type (1) +
	// keyLen (4) + valLen (4).
	headerSize = 1 + 4 + 4
	// checksumSize is the width of the trailing CRC32C field.
	checksumSize = 4
	// lengthPrefixSize is the width of the leading record-length field.
	lengthPrefixSize = 4

	maxKeySize   = 1 << 16 // 64 KiB
	maxValueSize = 1 << 20 // 1 MiB

	// maxRecordBodySize bounds the declared record length so a corrupt or
	// hostile length prefix cannot trigger an oversized allocation.
	maxRecordBodySize = headerSize + maxKeySize + maxValueSize + checksumSize
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt indicates a structurally complete record failed validation
// (bad checksum, invalid type, inconsistent lengths, or an oversized
// declared length). This is distinct from a torn final record: replay
// stops immediately and does not attempt to interpret anything after it.
var ErrCorrupt = errors.New("wal: corrupt record")

// WAL is an append-only log of kv.Command records backed by a single file.
//
// WAL is not safe for concurrent use.
type WAL struct {
	f *os.File
}

// Open opens (creating if necessary) the WAL at path, replays it, and
// returns the ordered commands found plus the ready-to-append WAL.
//
// If the file ends with an incomplete final record (a torn tail, as would
// result from a crash mid-write), that incomplete tail is truncated from
// the file and replay succeeds with the preceding complete records. If a
// structurally complete record fails validation (checksum mismatch,
// invalid type, or inconsistent lengths) anywhere in the file, Open
// returns ErrCorrupt and the file is left untouched.
func Open(path string) (*WAL, []kv.Command, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, err
	}

	cmds, validEnd, err := replay(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if err := f.Truncate(validEnd); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}

	return &WAL{f: f}, cmds, nil
}

// replay reads records sequentially from the start of f and returns the
// decoded commands along with the file offset immediately following the
// last complete, valid record. A torn tail is reported via that offset,
// not as an error; a corrupt complete record is reported as an error.
func replay(f *os.File) ([]kv.Command, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	r := bufio.NewReader(f)

	var cmds []kv.Command
	var offset int64

	lenBuf := make([]byte, lengthPrefixSize)
	for {
		n, err := io.ReadFull(r, lenBuf)
		if err == io.EOF {
			// Clean end of file: no bytes belonged to a next record.
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Torn tail: fewer than lengthPrefixSize bytes remained.
			break
		}
		if err != nil {
			return nil, 0, err
		}

		recordLen := binary.BigEndian.Uint32(lenBuf[:n])
		if recordLen < headerSize+checksumSize || recordLen > maxRecordBodySize {
			return nil, 0, fmt.Errorf("%w: declared record length %d out of bounds", ErrCorrupt, recordLen)
		}

		body := make([]byte, recordLen)
		if _, err := io.ReadFull(r, body); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Torn tail: the length prefix was complete but the body
				// was not fully written before the crash.
				break
			}
			return nil, 0, err
		}

		cmd, err := decodeRecord(body)
		if err != nil {
			return nil, 0, err
		}

		cmds = append(cmds, cmd)
		offset += lengthPrefixSize + int64(recordLen)
	}

	return cmds, offset, nil
}

// decodeRecord validates and decodes a single record body (everything
// after the length prefix).
func decodeRecord(body []byte) (kv.Command, error) {
	if len(body) < headerSize+checksumSize {
		return kv.Command{}, fmt.Errorf("%w: record body too short", ErrCorrupt)
	}

	typ := kv.CommandType(body[0])
	keyLen := binary.BigEndian.Uint32(body[1:5])
	valLen := binary.BigEndian.Uint32(body[5:9])

	if keyLen > maxKeySize || valLen > maxValueSize {
		return kv.Command{}, fmt.Errorf("%w: declared key/value length out of bounds", ErrCorrupt)
	}
	wantLen := uint32(headerSize) + keyLen + valLen + checksumSize
	if uint32(len(body)) != wantLen {
		return kv.Command{}, fmt.Errorf("%w: inconsistent record length", ErrCorrupt)
	}

	keyStart := headerSize
	valStart := keyStart + int(keyLen)
	checksumStart := valStart + int(valLen)

	key := body[keyStart:valStart]
	value := body[valStart:checksumStart]
	wantChecksum := binary.BigEndian.Uint32(body[checksumStart : checksumStart+checksumSize])
	gotChecksum := crc32.Checksum(body[:checksumStart], crc32cTable)
	if gotChecksum != wantChecksum {
		return kv.Command{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	switch typ {
	case kv.CommandPut:
		return kv.NewPutCommand(key, value), nil
	case kv.CommandDelete:
		return kv.NewDeleteCommand(key), nil
	default:
		return kv.Command{}, fmt.Errorf("%w: invalid command type %d", ErrCorrupt, typ)
	}
}

// encodeRecord produces the on-disk bytes for cmd, including the leading
// length prefix.
func encodeRecord(cmd kv.Command) ([]byte, error) {
	if len(cmd.Key) > maxKeySize {
		return nil, fmt.Errorf("wal: key length %d exceeds max %d", len(cmd.Key), maxKeySize)
	}
	if len(cmd.Value) > maxValueSize {
		return nil, fmt.Errorf("wal: value length %d exceeds max %d", len(cmd.Value), maxValueSize)
	}

	recordLen := headerSize + len(cmd.Key) + len(cmd.Value) + checksumSize
	buf := make([]byte, lengthPrefixSize+recordLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(recordLen))
	buf[4] = byte(cmd.Type)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(cmd.Key)))
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(cmd.Value)))
	keyStart := 13
	valStart := keyStart + len(cmd.Key)
	checksumStart := valStart + len(cmd.Value)
	copy(buf[keyStart:valStart], cmd.Key)
	copy(buf[valStart:checksumStart], cmd.Value)

	checksum := crc32.Checksum(buf[4:checksumStart], crc32cTable)
	binary.BigEndian.PutUint32(buf[checksumStart:checksumStart+checksumSize], checksum)

	return buf, nil
}

// Append encodes cmd and writes it to the log. A successful return means
// the record was fully passed to the operating system (all bytes written),
// not that it is durable against a crash or power loss — call Sync for
// that guarantee.
func (w *WAL) Append(cmd kv.Command) error {
	buf, err := encodeRecord(cmd)
	if err != nil {
		return err
	}
	for len(buf) > 0 {
		n, err := w.f.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// Sync flushes previously written records to stable storage via fsync.
// Only records that have been both Appended and Synced are guaranteed to
// survive a crash or power loss.
func (w *WAL) Sync() error {
	return w.f.Sync()
}

// Close closes the underlying file. The WAL must not be used afterward.
func (w *WAL) Close() error {
	return w.f.Close()
}
