package raft

import (
	"encoding/binary"
	"fmt"
)

// AppendEntriesRequest is the Raft AppendEntries RPC. A heartbeat is an
// AppendEntriesRequest with zero Entries — there is no separate heartbeat
// RPC.
//
// ReadContext (since Milestone 8) is 0 for ordinary replication/heartbeat
// traffic. A leader confirming ReadIndex quorum sends a non-zero
// ReadContext on an otherwise-normal, entries-free AppendEntries — see
// docs/read-index.md.
type AppendEntriesRequest struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
	ReadContext  ReadContext
}

// AppendEntriesResponse is the Raft AppendEntries RPC response.
// MatchIndex is only meaningful when Success is true, where it reports
// the highest index this follower now has that matches the leader's log
// as of this request (prevLogIndex + len(entries)) — the leader uses it
// to advance matchIndex/nextIndex directly on success. On failure this
// milestone relies on simple nextIndex-- backtracking rather than a
// conflict-index hint, so MatchIndex is 0 and unused.
//
// ReadContext (since Milestone 8) always echoes the request's
// ReadContext, even when Success is false due to a log-prefix mismatch —
// log replication success and ReadIndex quorum confirmation are different
// properties; a current-term response from a live peer proves the latter
// regardless of the former. See docs/read-index.md.
type AppendEntriesResponse struct {
	Term        Term
	Success     bool
	MatchIndex  LogIndex
	ReadContext ReadContext
}

// maxEntriesPerAppend bounds how many log entries a single AppendEntries
// RPC may carry. Combined with maxCommandSize this keeps a request
// comfortably under the transport's 1 MiB frame limit without needing a
// dynamic byte-budget calculation; a leader with more to replicate simply
// sends further batches on the next round (heartbeat tick or immediate
// post-Propose replication).
const maxEntriesPerAppend = 64

// appendEntriesFixedSize is the wire size of everything in
// AppendEntriesRequest except Entries: term(8) + leaderID(8) +
// prevLogIndex(8) + prevLogTerm(8) + leaderCommit(8) + readContext(8) +
// entryCount(4).
const appendEntriesFixedSize = 8*6 + 4

// perEntryHeaderSize is the wire size of one entry's header within an
// AppendEntries payload: term(8) + commandLength(4). The command bytes
// follow inline; there is no per-entry checksum here because the
// enclosing transport.Message frame already carries a CRC32C over the
// whole payload.
const perEntryHeaderSize = 8 + 4

// EncodeAppendEntries produces the exact wire bytes for req.
func EncodeAppendEntries(req AppendEntriesRequest) ([]byte, error) {
	if len(req.Entries) > maxEntriesPerAppend {
		return nil, fmt.Errorf("raft: %d entries exceeds max %d per AppendEntries", len(req.Entries), maxEntriesPerAppend)
	}
	size := appendEntriesFixedSize
	for _, e := range req.Entries {
		if len(e.Command) > maxCommandSize {
			return nil, fmt.Errorf("raft: command length %d exceeds max %d", len(e.Command), maxCommandSize)
		}
		size += perEntryHeaderSize + len(e.Command)
	}

	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint64(buf[off:], uint64(req.Term))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.LeaderID))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.PrevLogIndex))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.PrevLogTerm))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.LeaderCommit))
	off += 8
	binary.BigEndian.PutUint64(buf[off:], uint64(req.ReadContext))
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(len(req.Entries)))
	off += 4
	for _, e := range req.Entries {
		binary.BigEndian.PutUint64(buf[off:], uint64(e.Term))
		off += 8
		binary.BigEndian.PutUint32(buf[off:], uint32(len(e.Command)))
		off += 4
		off += copy(buf[off:], e.Command)
	}
	return buf, nil
}

// DecodeAppendEntries validates and decodes an AppendEntriesRequest
// payload. Entry count and each entry's command length are validated
// against fixed bounds before any per-entry allocation, so a corrupt or
// hostile peer cannot force an oversized allocation by declaring a huge
// count or length.
func DecodeAppendEntries(b []byte) (AppendEntriesRequest, error) {
	if len(b) < appendEntriesFixedSize {
		return AppendEntriesRequest{}, fmt.Errorf("%w: AppendEntries too short", ErrMalformedRPC)
	}
	off := 0
	term := Term(binary.BigEndian.Uint64(b[off:]))
	off += 8
	leaderID := NodeID(binary.BigEndian.Uint64(b[off:]))
	off += 8
	prevLogIndex := LogIndex(binary.BigEndian.Uint64(b[off:]))
	off += 8
	prevLogTerm := Term(binary.BigEndian.Uint64(b[off:]))
	off += 8
	leaderCommit := LogIndex(binary.BigEndian.Uint64(b[off:]))
	off += 8
	readContext := ReadContext(binary.BigEndian.Uint64(b[off:]))
	off += 8
	count := binary.BigEndian.Uint32(b[off:])
	off += 4
	if count > maxEntriesPerAppend {
		return AppendEntriesRequest{}, fmt.Errorf("%w: entry count %d exceeds max %d", ErrMalformedRPC, count, maxEntriesPerAppend)
	}

	entries := make([]LogEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+perEntryHeaderSize > len(b) {
			return AppendEntriesRequest{}, fmt.Errorf("%w: truncated entry header", ErrMalformedRPC)
		}
		entryTerm := Term(binary.BigEndian.Uint64(b[off:]))
		off += 8
		cmdLen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if cmdLen > maxCommandSize {
			return AppendEntriesRequest{}, fmt.Errorf("%w: command length %d exceeds max %d", ErrMalformedRPC, cmdLen, maxCommandSize)
		}
		if off+int(cmdLen) > len(b) {
			return AppendEntriesRequest{}, fmt.Errorf("%w: truncated command", ErrMalformedRPC)
		}
		command := make([]byte, cmdLen)
		copy(command, b[off:off+int(cmdLen)])
		off += int(cmdLen)
		entries = append(entries, LogEntry{Term: entryTerm, Command: command})
	}
	if off != len(b) {
		return AppendEntriesRequest{}, fmt.Errorf("%w: trailing bytes after AppendEntries", ErrMalformedRPC)
	}

	return AppendEntriesRequest{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
		ReadContext:  readContext,
	}, nil
}

// appendEntriesResponseSize is the fixed wire size of an
// AppendEntriesResponse: term(8) + success(1) + matchIndex(8) +
// readContext(8).
const appendEntriesResponseSize = 8 + 1 + 8 + 8

// EncodeAppendEntriesResponse produces the exact wire bytes for resp.
func EncodeAppendEntriesResponse(resp AppendEntriesResponse) []byte {
	buf := make([]byte, appendEntriesResponseSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(resp.Term))
	if resp.Success {
		buf[8] = 1
	}
	binary.BigEndian.PutUint64(buf[9:17], uint64(resp.MatchIndex))
	binary.BigEndian.PutUint64(buf[17:25], uint64(resp.ReadContext))
	return buf
}

// DecodeAppendEntriesResponse validates and decodes an
// AppendEntriesResponse payload.
func DecodeAppendEntriesResponse(b []byte) (AppendEntriesResponse, error) {
	if len(b) != appendEntriesResponseSize {
		return AppendEntriesResponse{}, fmt.Errorf("%w: AppendEntriesResponse length %d, want %d", ErrMalformedRPC, len(b), appendEntriesResponseSize)
	}
	success := b[8]
	if success > 1 {
		return AppendEntriesResponse{}, fmt.Errorf("%w: invalid success encoding %d", ErrMalformedRPC, success)
	}
	return AppendEntriesResponse{
		Term:        Term(binary.BigEndian.Uint64(b[0:8])),
		Success:     success == 1,
		MatchIndex:  LogIndex(binary.BigEndian.Uint64(b[9:17])),
		ReadContext: ReadContext(binary.BigEndian.Uint64(b[17:25])),
	}, nil
}
