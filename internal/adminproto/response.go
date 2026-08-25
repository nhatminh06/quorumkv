package adminproto

import (
	"encoding/binary"
	"fmt"
)

func validStatus(s Status) bool {
	switch s {
	case StatusOK, StatusNotLeader, StatusBadRequest, StatusInternalError,
		StatusMembershipChangeInProgress, StatusLeadershipTransferInProgress,
		StatusNotAVoter, StatusInvalidConfiguration, StatusTimeout, StatusCannotTransferToSelf,
		StatusTransferRejected:
		return true
	default:
		return false
	}
}

func validRole(r Role) bool {
	switch r {
	case RoleFollower, RoleCandidate, RoleLeader:
		return true
	default:
		return false
	}
}

func validMode(m MembershipMode) bool {
	switch m {
	case MembershipStable, MembershipJoint:
		return true
	default:
		return false
	}
}

// infoFixedSize: nodeID(8) + role(1) + term(8) + leaderID(8) +
// lastLogIndex(8) + commitIndex(8) + lastApplied(8) + snapshotIndex(8) +
// snapshotTerm(8) + mode(1).
const infoFixedSize = 8 + 1 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 1

func encodedVoterListSize(voters []Voter) int {
	size := 1 // count
	for _, v := range voters {
		size += 8 + 2 + len(v.Addr)
	}
	return size
}

func appendVoterList(buf []byte, off int, voters []Voter) int {
	buf[off] = byte(len(voters))
	off++
	for _, v := range voters {
		binary.BigEndian.PutUint64(buf[off:off+8], v.ID)
		off += 8
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(v.Addr)))
		off += 2
		off += copy(buf[off:], v.Addr)
	}
	return off
}

func decodeVoterList(b []byte, off int) ([]Voter, int, error) {
	if off >= len(b) {
		return nil, 0, fmt.Errorf("%w: truncated voter list", ErrMalformedResponse)
	}
	count := int(b[off])
	off++
	if count > MaxVoters {
		return nil, 0, fmt.Errorf("%w: %d voters exceeds max %d", ErrMalformedResponse, count, MaxVoters)
	}
	voters := make([]Voter, 0, count)
	for i := 0; i < count; i++ {
		if off+10 > len(b) {
			return nil, 0, fmt.Errorf("%w: truncated voter entry", ErrMalformedResponse)
		}
		id := binary.BigEndian.Uint64(b[off : off+8])
		off += 8
		addrLen := int(binary.BigEndian.Uint16(b[off : off+2]))
		off += 2
		if addrLen > MaxAddrLen {
			return nil, 0, fmt.Errorf("%w: voter address length %d exceeds max %d", ErrMalformedResponse, addrLen, MaxAddrLen)
		}
		if off+addrLen > len(b) {
			return nil, 0, fmt.Errorf("%w: truncated voter address", ErrMalformedResponse)
		}
		voters = append(voters, Voter{ID: id, Addr: cloneBytes(b[off : off+addrLen])})
		off += addrLen
	}
	return voters, off, nil
}

// EncodeResponse produces the exact wire bytes for r.
func EncodeResponse(r Response) ([]byte, error) {
	if !validStatus(r.Status) {
		return nil, fmt.Errorf("adminproto: unknown status %d", r.Status)
	}
	if len(r.LeaderHint) > MaxAddrLen {
		return nil, fmt.Errorf("adminproto: leader hint length %d exceeds max %d", len(r.LeaderHint), MaxAddrLen)
	}
	info := r.Info
	if info.Role == 0 {
		info.Role = RoleFollower // encodable zero default when Info is unused
	}
	if info.Mode == 0 {
		info.Mode = MembershipStable
	}
	if !validRole(info.Role) {
		return nil, fmt.Errorf("adminproto: unknown role %d", info.Role)
	}
	if !validMode(info.Mode) {
		return nil, fmt.Errorf("adminproto: unknown membership mode %d", info.Mode)
	}
	for _, list := range [][]Voter{info.StableVoters, info.OldVoters, info.NewVoters} {
		if len(list) > MaxVoters {
			return nil, fmt.Errorf("adminproto: %d voters exceeds max %d", len(list), MaxVoters)
		}
		for _, v := range list {
			if len(v.Addr) > MaxAddrLen {
				return nil, fmt.Errorf("adminproto: voter address length %d exceeds max %d", len(v.Addr), MaxAddrLen)
			}
		}
	}

	size := 1 + 1 + 2 + len(r.LeaderHint) + 8 + 8 + infoFixedSize +
		encodedVoterListSize(info.StableVoters) + encodedVoterListSize(info.OldVoters) + encodedVoterListSize(info.NewVoters)
	buf := make([]byte, size)
	off := 0
	buf[off] = protocolVersion
	off++
	buf[off] = byte(r.Status)
	off++
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(r.LeaderHint)))
	off += 2
	off += copy(buf[off:], r.LeaderHint)
	binary.BigEndian.PutUint64(buf[off:off+8], r.SnapshotIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], r.SnapshotTerm)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.NodeID)
	off += 8
	buf[off] = byte(info.Role)
	off++
	binary.BigEndian.PutUint64(buf[off:off+8], info.Term)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.LeaderID)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.LastLogIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.CommitIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.LastApplied)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.SnapshotIndex)
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], info.SnapshotTerm)
	off += 8
	buf[off] = byte(info.Mode)
	off++
	off = appendVoterList(buf, off, info.StableVoters)
	off = appendVoterList(buf, off, info.OldVoters)
	off = appendVoterList(buf, off, info.NewVoters)
	if off != size {
		return nil, fmt.Errorf("adminproto: internal encode size mismatch: wrote %d, want %d", off, size)
	}
	return buf, nil
}

// DecodeResponse validates and decodes a response payload. Declared
// lengths are validated before any allocation based on them.
func DecodeResponse(b []byte) (Response, error) {
	if len(b) < 1+1+2 {
		return Response{}, fmt.Errorf("%w: too short", ErrMalformedResponse)
	}
	if b[0] != protocolVersion {
		return Response{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedResponse, b[0])
	}
	status := Status(b[1])
	if !validStatus(status) {
		return Response{}, fmt.Errorf("%w: unknown status %d", ErrMalformedResponse, status)
	}
	off := 2
	hintLen := int(binary.BigEndian.Uint16(b[off : off+2]))
	off += 2
	if hintLen > MaxAddrLen {
		return Response{}, fmt.Errorf("%w: leader hint length %d exceeds max %d", ErrMalformedResponse, hintLen, MaxAddrLen)
	}
	if off+hintLen > len(b) {
		return Response{}, fmt.Errorf("%w: truncated leader hint", ErrMalformedResponse)
	}
	hint := cloneBytes(b[off : off+hintLen])
	off += hintLen

	if off+8+8+infoFixedSize > len(b) {
		return Response{}, fmt.Errorf("%w: truncated fixed fields", ErrMalformedResponse)
	}
	snapIndex := binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	snapTerm := binary.BigEndian.Uint64(b[off : off+8])
	off += 8

	var info StatusInfo
	info.NodeID = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.Role = Role(b[off])
	off++
	if !validRole(info.Role) {
		return Response{}, fmt.Errorf("%w: unknown role %d", ErrMalformedResponse, info.Role)
	}
	info.Term = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.LeaderID = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.LastLogIndex = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.CommitIndex = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.LastApplied = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.SnapshotIndex = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.SnapshotTerm = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	info.Mode = MembershipMode(b[off])
	off++
	if !validMode(info.Mode) {
		return Response{}, fmt.Errorf("%w: unknown membership mode %d", ErrMalformedResponse, info.Mode)
	}

	var err error
	info.StableVoters, off, err = decodeVoterList(b, off)
	if err != nil {
		return Response{}, err
	}
	info.OldVoters, off, err = decodeVoterList(b, off)
	if err != nil {
		return Response{}, err
	}
	info.NewVoters, off, err = decodeVoterList(b, off)
	if err != nil {
		return Response{}, err
	}
	if off != len(b) {
		return Response{}, fmt.Errorf("%w: trailing bytes", ErrMalformedResponse)
	}

	return Response{
		Status:        status,
		LeaderHint:    hint,
		Info:          info,
		SnapshotIndex: snapIndex,
		SnapshotTerm:  snapTerm,
	}, nil
}
