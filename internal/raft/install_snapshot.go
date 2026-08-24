package raft

import (
	"encoding/binary"
	"fmt"
)

// InstallSnapshotRequest carries one chunk of a snapshot transfer. A
// snapshot may exceed the transport's 1 MiB frame limit, so it is always
// sent as a sequence of chunks at increasing offsets, the last one
// marked Done.
type InstallSnapshotRequest struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Offset            uint64
	Data              []byte
	Done              bool
}

// InstallSnapshotResponse acknowledges one chunk. NextOffset tells the
// leader what byte offset the follower expects next — on success that is
// Offset+len(Data); on a rejected/out-of-order chunk it is whatever
// offset the follower actually still expects, so the leader can resume
// from the right place rather than guessing.
type InstallSnapshotResponse struct {
	Term       Term
	Success    bool
	NextOffset uint64
}

// maxSnapshotChunkSize bounds a single InstallSnapshot chunk, comfortably
// under the transport's 1 MiB frame limit once RPC metadata overhead is
// included.
const maxSnapshotChunkSize = 256 * 1024 // 256 KiB

// installSnapshotFixedSize: term(8) + leaderID(8) + lastIncludedIndex(8) +
// lastIncludedTerm(8) + offset(8) + done(1) + dataLength(4).
const installSnapshotFixedSize = 8 + 8 + 8 + 8 + 8 + 1 + 4

// installSnapshotResponseSize: term(8) + success(1) + nextOffset(8).
const installSnapshotResponseSize = 8 + 1 + 8

// EncodeInstallSnapshot produces the exact wire bytes for req. All
// integers big-endian.
func EncodeInstallSnapshot(req InstallSnapshotRequest) ([]byte, error) {
	if len(req.Data) > maxSnapshotChunkSize {
		return nil, fmt.Errorf("raft: snapshot chunk length %d exceeds max %d", len(req.Data), maxSnapshotChunkSize)
	}
	buf := make([]byte, installSnapshotFixedSize+len(req.Data))
	off := 0
	binary.BigEndian.PutUint64(buf[off:], uint64(req.Term))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.LeaderID))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.LastIncludedIndex))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.LastIncludedTerm))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], req.Offset)
	off += 8
	if req.Done {
		buf[off] = 1
	}
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(req.Data)))
	off += 4
	copy(buf[off:], req.Data)
	return buf, nil
}

// DecodeInstallSnapshot validates and decodes an InstallSnapshotRequest
// payload. The declared data length is validated against
// maxSnapshotChunkSize before any allocation based on it.
func DecodeInstallSnapshot(b []byte) (InstallSnapshotRequest, error) {
	if len(b) < installSnapshotFixedSize {
		return InstallSnapshotRequest{}, fmt.Errorf("%w: InstallSnapshot too short", ErrMalformedRPC)
	}
	off := 0
	term := Term(binary.BigEndian.Uint64(b[off:]))
	off += 8
	leaderID := NodeID(binary.BigEndian.Uint64(b[off:]))
	off += 8
	lastIncludedIndex := LogIndex(binary.BigEndian.Uint64(b[off:]))
	off += 8
	lastIncludedTerm := Term(binary.BigEndian.Uint64(b[off:]))
	off += 8
	offset := binary.BigEndian.Uint64(b[off:])
	off += 8
	doneByte := b[off]
	if doneByte > 1 {
		return InstallSnapshotRequest{}, fmt.Errorf("%w: invalid done encoding %d", ErrMalformedRPC, doneByte)
	}
	off++
	dataLen := binary.BigEndian.Uint32(b[off:])
	off += 4
	if dataLen > maxSnapshotChunkSize {
		return InstallSnapshotRequest{}, fmt.Errorf("%w: chunk length %d exceeds max %d", ErrMalformedRPC, dataLen, maxSnapshotChunkSize)
	}
	if off+int(dataLen) != len(b) {
		return InstallSnapshotRequest{}, fmt.Errorf("%w: length mismatch", ErrMalformedRPC)
	}
	data := make([]byte, dataLen)
	copy(data, b[off:])

	return InstallSnapshotRequest{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            offset,
		Data:              data,
		Done:              doneByte == 1,
	}, nil
}

// EncodeInstallSnapshotResponse produces the exact wire bytes for resp.
func EncodeInstallSnapshotResponse(resp InstallSnapshotResponse) []byte {
	buf := make([]byte, installSnapshotResponseSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(resp.Term))
	if resp.Success {
		buf[8] = 1
	}
	binary.BigEndian.PutUint64(buf[9:17], resp.NextOffset)
	return buf
}

// DecodeInstallSnapshotResponse validates and decodes an
// InstallSnapshotResponse payload.
func DecodeInstallSnapshotResponse(b []byte) (InstallSnapshotResponse, error) {
	if len(b) != installSnapshotResponseSize {
		return InstallSnapshotResponse{}, fmt.Errorf("%w: InstallSnapshotResponse length %d, want %d", ErrMalformedRPC, len(b), installSnapshotResponseSize)
	}
	success := b[8]
	if success > 1 {
		return InstallSnapshotResponse{}, fmt.Errorf("%w: invalid success encoding %d", ErrMalformedRPC, success)
	}
	return InstallSnapshotResponse{
		Term:       Term(binary.BigEndian.Uint64(b[0:8])),
		Success:    success == 1,
		NextOffset: binary.BigEndian.Uint64(b[9:17]),
	}, nil
}
