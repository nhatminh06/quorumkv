package raft

import (
	"errors"
	"fmt"
	"sort"
)

// MaxVoters bounds a single Configuration's voter count. MaxPeerAddrLen
// bounds one voter's address string. Both are simple defensive bounds —
// nothing about the joint-consensus protocol itself needs a specific
// number.
const (
	MaxVoters      = 31
	MaxPeerAddrLen = 256
)

// ErrInvalidConfiguration indicates a Configuration failed validation:
// empty, a zero/duplicate NodeID, a malformed/empty/oversized address, or
// too many voters.
var ErrInvalidConfiguration = errors.New("raft: invalid configuration")

// Configuration is one set of voters: NodeID -> address. It is the unit
// Raft's majority quorum rule applies to — see Membership for how a
// joint transition combines two of these.
type Configuration struct {
	Voters map[NodeID]string
}

// NewConfiguration validates voters and returns a Configuration wrapping
// a defensive copy of it. Rejects: an empty map, a zero NodeID, an empty
// or oversized address, and more than MaxVoters entries. Map key
// uniqueness already rules out duplicate NodeIDs.
func NewConfiguration(voters map[NodeID]string) (Configuration, error) {
	if len(voters) == 0 {
		return Configuration{}, fmt.Errorf("%w: empty configuration", ErrInvalidConfiguration)
	}
	if len(voters) > MaxVoters {
		return Configuration{}, fmt.Errorf("%w: %d voters exceeds max %d", ErrInvalidConfiguration, len(voters), MaxVoters)
	}
	out := make(map[NodeID]string, len(voters))
	for id, addr := range voters {
		if id == 0 {
			return Configuration{}, fmt.Errorf("%w: zero NodeID is reserved invalid", ErrInvalidConfiguration)
		}
		if addr == "" {
			return Configuration{}, fmt.Errorf("%w: node %d has an empty address", ErrInvalidConfiguration, id)
		}
		if len(addr) > MaxPeerAddrLen {
			return Configuration{}, fmt.Errorf("%w: node %d address length %d exceeds max %d", ErrInvalidConfiguration, id, len(addr), MaxPeerAddrLen)
		}
		out[id] = addr
	}
	return Configuration{Voters: out}, nil
}

// sortedIDs returns c's voter IDs in ascending order — the canonical
// iteration order for both deterministic encoding and quorum counting
// (which doesn't itself care about order, but every caller that
// serializes a Configuration must agree on one).
func (c Configuration) sortedIDs() []NodeID {
	ids := make([]NodeID, 0, len(c.Voters))
	for id := range c.Voters {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Has reports whether id is a voter in c.
func (c Configuration) Has(id NodeID) bool {
	_, ok := c.Voters[id]
	return ok
}

// hasQuorum reports whether acked contains a majority of c's voters.
// Only IDs present in c count — a response from a node outside this
// configuration contributes nothing to it (relevant during Joint, where
// a config-old-only or config-new-only ack must not count toward the
// other side).
func (c Configuration) hasQuorum(acked map[NodeID]bool) bool {
	need := Majority(len(c.Voters))
	count := 0
	for id := range c.Voters {
		if acked[id] {
			count++
		}
	}
	return count >= need
}

// Equal reports whether c and other have exactly the same voter IDs
// mapped to exactly the same addresses. Map insertion/iteration order
// never matters.
func (c Configuration) Equal(other Configuration) bool {
	if len(c.Voters) != len(other.Voters) {
		return false
	}
	for id, addr := range c.Voters {
		oaddr, ok := other.Voters[id]
		if !ok || oaddr != addr {
			return false
		}
	}
	return true
}

// clone returns a defensive copy of c.
func (c Configuration) clone() Configuration {
	out := make(map[NodeID]string, len(c.Voters))
	for id, addr := range c.Voters {
		out[id] = addr
	}
	return Configuration{Voters: out}
}

// MembershipMode identifies whether a Membership is a single stable
// configuration or a joint transition between two.
type MembershipMode uint8

const (
	// ModeStable means Membership.Stable is the sole effective
	// configuration; Old/New are unused.
	ModeStable MembershipMode = iota + 1
	// ModeJoint means Membership.Old/New are both effective simultaneously
	// (see docs/membership.md): every quorum-based decision requires a
	// majority of Old AND a majority of New. Stable is unused.
	ModeJoint
)

func (m MembershipMode) String() string {
	switch m {
	case ModeStable:
		return "Stable"
	case ModeJoint:
		return "Joint"
	default:
		return "Unknown"
	}
}

// Membership is a Raft node's effective configuration: either a single
// Stable Configuration, or a Joint transition between Old and New. This
// is the single representation every quorum-based decision (election,
// commit, ReadIndex, the current-term no-op barrier) must go through —
// see HasQuorum.
type Membership struct {
	Mode   MembershipMode
	Stable Configuration
	Old    Configuration
	New    Configuration
}

// StableMembership returns a Membership in ModeStable wrapping c.
func StableMembership(c Configuration) Membership {
	return Membership{Mode: ModeStable, Stable: c}
}

// JointMembership returns a Membership in ModeJoint transitioning from
// oldC to newC.
func JointMembership(oldC, newC Configuration) Membership {
	return Membership{Mode: ModeJoint, Old: oldC, New: newC}
}

// HasQuorum reports whether acked (a set of NodeIDs that have
// acknowledged something — a vote, a replicated index, a read probe)
// satisfies this Membership's quorum rule:
//
//	Stable: a majority of Stable.
//	Joint:  a majority of Old AND a majority of New (never a majority of
//	        their union — that is a materially weaker, incorrect rule;
//	        see docs/membership.md).
//
// A NodeID present in both Old and New (the common case — most voters
// don't change) correctly contributes to both majority counts from a
// single acknowledgement.
func (m Membership) HasQuorum(acked map[NodeID]bool) bool {
	switch m.Mode {
	case ModeStable:
		return m.Stable.hasQuorum(acked)
	case ModeJoint:
		return m.Old.hasQuorum(acked) && m.New.hasQuorum(acked)
	default:
		return false
	}
}

// IsVoter reports whether id is a voter in this Membership's effective
// configuration: Stable itself, or the union of Old/New during a Joint
// transition.
func (m Membership) IsVoter(id NodeID) bool {
	switch m.Mode {
	case ModeStable:
		return m.Stable.Has(id)
	case ModeJoint:
		return m.Old.Has(id) || m.New.Has(id)
	default:
		return false
	}
}

// Targets returns every effective voter's address except self's — the
// set a leader replicates/heartbeats to, a candidate requests votes
// from, and ReadIndex probes. Stable: Stable minus self. Joint: union(Old,
// New) minus self. The returned map is a fresh copy, safe to iterate
// without holding any lock afterward.
func (m Membership) Targets(self NodeID) map[NodeID]string {
	out := make(map[NodeID]string)
	switch m.Mode {
	case ModeStable:
		for id, addr := range m.Stable.Voters {
			if id != self {
				out[id] = addr
			}
		}
	case ModeJoint:
		for id, addr := range m.Old.Voters {
			if id != self {
				out[id] = addr
			}
		}
		for id, addr := range m.New.Voters {
			if id != self {
				out[id] = addr
			}
		}
	}
	return out
}

// PeerAddr resolves id's address in this Membership's effective
// configuration, if it is currently a voter.
func (m Membership) PeerAddr(id NodeID) (string, bool) {
	switch m.Mode {
	case ModeStable:
		addr, ok := m.Stable.Voters[id]
		return addr, ok
	case ModeJoint:
		if addr, ok := m.Old.Voters[id]; ok {
			return addr, true
		}
		addr, ok := m.New.Voters[id]
		return addr, ok
	default:
		return "", false
	}
}

// Equal reports whether m and other represent the same effective
// membership (same mode, same underlying configuration(s)).
func (m Membership) Equal(other Membership) bool {
	if m.Mode != other.Mode {
		return false
	}
	switch m.Mode {
	case ModeStable:
		return m.Stable.Equal(other.Stable)
	case ModeJoint:
		return m.Old.Equal(other.Old) && m.New.Equal(other.New)
	default:
		return false
	}
}

// clone returns a defensive deep copy of m.
func (m Membership) clone() Membership {
	switch m.Mode {
	case ModeStable:
		return Membership{Mode: ModeStable, Stable: m.Stable.clone()}
	case ModeJoint:
		return Membership{Mode: ModeJoint, Old: m.Old.clone(), New: m.New.clone()}
	default:
		return Membership{}
	}
}
