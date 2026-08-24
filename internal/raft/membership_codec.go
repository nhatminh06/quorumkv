package raft

import (
	"encoding/binary"
	"fmt"
)

// EncodeMembership/DecodeMembership define the deterministic binary
// encoding of a Membership — used both as a Configuration log entry's
// payload (see EntryConfiguration) and as a Raft snapshot's stable-
// membership metadata (see Snapshot). No JSON, no gob: an explicit,
// bounded, sorted-by-NodeID byte layout, so two calls encoding logically
// identical membership always produce identical bytes regardless of Go
// map iteration order.
//
//	version(1B) | mode(1B)
//	Stable: voterCount(4B) | voters[]
//	Joint:  oldCount(4B) | oldVoters[] | newCount(4B) | newVoters[]
//
// Each voter: nodeID(8B) | addrLength(2B) | address(N bytes), voters
// sorted by nodeID ascending within each configuration.

const membershipVersion1 = 1

// membershipFixedHeaderSize: version(1) + mode(1).
const membershipFixedHeaderSize = 1 + 1

// configCountSize: a Configuration's own voterCount field.
const configCountSize = 4

// voterFixedSize: nodeID(8) + addrLength(2). The address bytes follow
// inline.
const voterFixedSize = 8 + 2

var ErrMalformedMembership = fmt.Errorf("raft: malformed membership payload")

// EncodeMembership produces the exact wire/persistent bytes for m.
func EncodeMembership(m Membership) ([]byte, error) {
	switch m.Mode {
	case ModeStable:
		size := membershipFixedHeaderSize + configSize(m.Stable)
		buf := make([]byte, size)
		buf[0] = membershipVersion1
		buf[1] = byte(ModeStable)
		if err := encodeConfigInto(buf[membershipFixedHeaderSize:], m.Stable); err != nil {
			return nil, err
		}
		return buf, nil
	case ModeJoint:
		size := membershipFixedHeaderSize + configSize(m.Old) + configSize(m.New)
		buf := make([]byte, size)
		buf[0] = membershipVersion1
		buf[1] = byte(ModeJoint)
		off := membershipFixedHeaderSize
		if err := encodeConfigInto(buf[off:], m.Old); err != nil {
			return nil, err
		}
		off += configSize(m.Old)
		if err := encodeConfigInto(buf[off:], m.New); err != nil {
			return nil, err
		}
		return buf, nil
	default:
		return nil, fmt.Errorf("raft: unknown membership mode %d", m.Mode)
	}
}

func configSize(c Configuration) int {
	size := configCountSize
	for _, addr := range c.Voters {
		size += voterFixedSize + len(addr)
	}
	return size
}

// encodeConfigInto writes c's encoding (voterCount + sorted voters) into
// the front of buf, which must be exactly configSize(c) bytes.
func encodeConfigInto(buf []byte, c Configuration) error {
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(c.Voters)))
	off := configCountSize
	for _, id := range c.sortedIDs() {
		addr := c.Voters[id]
		if len(addr) > MaxPeerAddrLen {
			return fmt.Errorf("raft: node %d address length %d exceeds max %d", id, len(addr), MaxPeerAddrLen)
		}
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(id))
		off += 8
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(addr)))
		off += 2
		off += copy(buf[off:], addr)
	}
	return nil
}

// DecodeMembership validates and decodes a Membership payload. Declared
// counts/lengths are validated against MaxVoters/MaxPeerAddrLen before
// any allocation based on them. A decoded Configuration is validated the
// same way NewConfiguration validates one built programmatically (no
// zero NodeID, no empty address, no duplicate — duplicates are
// impossible here since decoding writes directly into a map keyed by
// NodeID, so a repeated NodeID in the stream silently coalesces; that
// would only occur for corrupt/hostile input given this package's own
// encoder never repeats one).
func DecodeMembership(b []byte) (Membership, error) {
	if len(b) < membershipFixedHeaderSize {
		return Membership{}, fmt.Errorf("%w: too short", ErrMalformedMembership)
	}
	if b[0] != membershipVersion1 {
		return Membership{}, fmt.Errorf("%w: unsupported version %d", ErrMalformedMembership, b[0])
	}
	mode := MembershipMode(b[1])
	off := membershipFixedHeaderSize
	switch mode {
	case ModeStable:
		cfg, next, err := decodeConfig(b, off)
		if err != nil {
			return Membership{}, err
		}
		if next != len(b) {
			return Membership{}, fmt.Errorf("%w: trailing bytes", ErrMalformedMembership)
		}
		return Membership{Mode: ModeStable, Stable: cfg}, nil
	case ModeJoint:
		oldCfg, next, err := decodeConfig(b, off)
		if err != nil {
			return Membership{}, err
		}
		newCfg, next2, err := decodeConfig(b, next)
		if err != nil {
			return Membership{}, err
		}
		if next2 != len(b) {
			return Membership{}, fmt.Errorf("%w: trailing bytes", ErrMalformedMembership)
		}
		return Membership{Mode: ModeJoint, Old: oldCfg, New: newCfg}, nil
	default:
		return Membership{}, fmt.Errorf("%w: unknown mode %d", ErrMalformedMembership, b[1])
	}
}

// decodeConfig decodes one Configuration starting at offset off in b,
// returning it and the offset just past it.
func decodeConfig(b []byte, off int) (Configuration, int, error) {
	if off+configCountSize > len(b) {
		return Configuration{}, 0, fmt.Errorf("%w: truncated voter count", ErrMalformedMembership)
	}
	count := binary.BigEndian.Uint32(b[off : off+4])
	off += configCountSize
	if count == 0 {
		return Configuration{}, 0, fmt.Errorf("%w: empty configuration", ErrMalformedMembership)
	}
	if count > MaxVoters {
		return Configuration{}, 0, fmt.Errorf("%w: voter count %d exceeds max %d", ErrMalformedMembership, count, MaxVoters)
	}
	voters := make(map[NodeID]string, count)
	for i := uint32(0); i < count; i++ {
		if off+voterFixedSize > len(b) {
			return Configuration{}, 0, fmt.Errorf("%w: truncated voter header", ErrMalformedMembership)
		}
		id := NodeID(binary.BigEndian.Uint64(b[off : off+8]))
		off += 8
		addrLen := binary.BigEndian.Uint16(b[off : off+2])
		off += 2
		if id == 0 {
			return Configuration{}, 0, fmt.Errorf("%w: zero NodeID", ErrMalformedMembership)
		}
		if int(addrLen) > MaxPeerAddrLen {
			return Configuration{}, 0, fmt.Errorf("%w: address length %d exceeds max %d", ErrMalformedMembership, addrLen, MaxPeerAddrLen)
		}
		if off+int(addrLen) > len(b) {
			return Configuration{}, 0, fmt.Errorf("%w: truncated address", ErrMalformedMembership)
		}
		addr := string(b[off : off+int(addrLen)])
		off += int(addrLen)
		if addr == "" {
			return Configuration{}, 0, fmt.Errorf("%w: node %d has an empty address", ErrMalformedMembership, id)
		}
		voters[id] = addr
	}
	return Configuration{Voters: voters}, off, nil
}
