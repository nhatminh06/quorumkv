package service

import (
	"context"
	"errors"

	"quorumkv/internal/adminproto"
	"quorumkv/internal/raft"
	"quorumkv/internal/transport"
)

// handleAdmin decodes and dispatches one operational admin request (see
// internal/adminproto) — status inspection, snapshot creation,
// leadership transfer, and voter add/remove. Every operation beyond
// OpStatus goes straight through the corresponding real raft.Node method
// (Node.CreateSnapshot, Node.TransferLeadership, Node.AddVoter,
// Node.RemoveVoter): this is a thin wire wrapper, not a second
// implementation. Unauthenticated, like the client protocol it shares a
// connection with — see docs/operations.md.
func (s *Service) handleAdmin(ctx context.Context, m transport.Message) (transport.Message, error) {
	req, err := adminproto.DecodeRequest(m.Payload)
	if err != nil {
		return s.respondAdmin(adminproto.Response{Status: adminproto.StatusBadRequest})
	}

	var resp adminproto.Response
	switch req.Operation {
	case adminproto.OpStatus:
		resp = s.adminStatus()
	case adminproto.OpSnapshot:
		resp = s.adminSnapshot()
	case adminproto.OpTransferLeadership:
		resp = s.adminTransferLeadership(ctx, raft.NodeID(req.TransferTarget))
	case adminproto.OpAddVoter:
		resp = s.adminAddVoter(ctx, raft.NodeID(req.VoterID), string(req.VoterAddr))
	case adminproto.OpRemoveVoter:
		resp = s.adminRemoveVoter(ctx, raft.NodeID(req.VoterID))
	default:
		resp = adminproto.Response{Status: adminproto.StatusBadRequest}
	}
	return s.respondAdmin(resp)
}

func (s *Service) respondAdmin(r adminproto.Response) (transport.Message, error) {
	payload, err := adminproto.EncodeResponse(r)
	if err != nil {
		return transport.Message{}, err
	}
	return transport.NewMessage(transport.MessageAdminResponse, payload), nil
}

// adminNotLeaderResponse mirrors notLeaderResponse for the admin
// protocol's own Response type.
func (s *Service) adminNotLeaderResponse() adminproto.Response {
	var hint []byte
	if id, ok := s.node.LeaderHint(); ok {
		if addr, ok := s.peers[id]; ok {
			hint = []byte(addr)
		}
	}
	return adminproto.Response{Status: adminproto.StatusNotLeader, LeaderHint: hint}
}

// adminStatus is intentionally NOT gated on leadership: an operator must
// be able to inspect a follower (or a candidate) too, not just the
// leader. It is read-only observational metadata — see StatusInfo's own
// doc comment — never a linearizable read: no ReadIndex, no quorum
// round trip, no mutation of term/log/membership/commit/timers.
func (s *Service) adminStatus() adminproto.Response {
	status := s.node.MembershipStatus()
	snapIndex, snapTerm := s.node.SnapshotBoundary()
	info := adminproto.StatusInfo{
		Role:          adminRole(s.node.Role()),
		Term:          uint64(s.node.CurrentTerm()),
		LastLogIndex:  uint64(s.node.LastLogIndex()),
		CommitIndex:   uint64(s.node.CommitIndex()),
		LastApplied:   uint64(s.node.LastApplied()),
		SnapshotIndex: uint64(snapIndex),
		SnapshotTerm:  uint64(snapTerm),
		Mode:          adminMode(status.Mode),
	}
	if id, ok := s.node.LeaderHint(); ok {
		info.LeaderID = uint64(id)
	}
	switch status.Mode {
	case raft.ModeStable:
		info.StableVoters = adminVoters(status.Stable)
	case raft.ModeJoint:
		info.OldVoters = adminVoters(status.Old)
		info.NewVoters = adminVoters(status.New)
	}
	return adminproto.Response{Status: adminproto.StatusOK, Info: info}
}

func adminRole(r raft.Role) adminproto.Role {
	switch r {
	case raft.Leader:
		return adminproto.RoleLeader
	case raft.Candidate:
		return adminproto.RoleCandidate
	default:
		return adminproto.RoleFollower
	}
}

func adminMode(m raft.MembershipMode) adminproto.MembershipMode {
	if m == raft.ModeJoint {
		return adminproto.MembershipJoint
	}
	return adminproto.MembershipStable
}

func adminVoters(c raft.Configuration) []adminproto.Voter {
	out := make([]adminproto.Voter, 0, len(c.Voters))
	for id, addr := range c.Voters {
		out = append(out, adminproto.Voter{ID: uint64(id), Addr: []byte(addr)})
	}
	return out
}

func (s *Service) adminSnapshot() adminproto.Response {
	if s.node.Role() != raft.Leader {
		return s.adminNotLeaderResponse()
	}
	if err := s.node.CreateSnapshot(); err != nil {
		return adminErrorResponse(err)
	}
	idx, term := s.node.SnapshotBoundary()
	return adminproto.Response{Status: adminproto.StatusOK, SnapshotIndex: uint64(idx), SnapshotTerm: uint64(term)}
}

func (s *Service) adminTransferLeadership(ctx context.Context, target raft.NodeID) adminproto.Response {
	if s.node.Role() != raft.Leader {
		return s.adminNotLeaderResponse()
	}
	if err := s.node.TransferLeadership(ctx, target); err != nil {
		return adminErrorResponse(err)
	}
	return adminproto.Response{Status: adminproto.StatusOK}
}

func (s *Service) adminAddVoter(ctx context.Context, id raft.NodeID, addr string) adminproto.Response {
	if s.node.Role() != raft.Leader {
		return s.adminNotLeaderResponse()
	}
	if err := s.node.AddVoter(ctx, id, addr); err != nil {
		return adminErrorResponse(err)
	}
	return adminproto.Response{Status: adminproto.StatusOK}
}

func (s *Service) adminRemoveVoter(ctx context.Context, id raft.NodeID) adminproto.Response {
	if s.node.Role() != raft.Leader {
		return s.adminNotLeaderResponse()
	}
	if err := s.node.RemoveVoter(ctx, id); err != nil {
		return adminErrorResponse(err)
	}
	return adminproto.Response{Status: adminproto.StatusOK}
}

// adminErrorResponse maps a real raft.Node error to the admin protocol's
// small fixed status set — never a raw error string over the wire (same
// discipline as clientproto). A context deadline/cancellation, or any
// error this mapping doesn't specifically recognize, becomes
// StatusTimeout: the underlying operation's outcome is not necessarily
// negative, just unconfirmed — see docs/runbook-membership.md and
// docs/runbook-leadership-transfer.md on why that must not be
// auto-retried blindly.
func adminErrorResponse(err error) adminproto.Response {
	switch {
	case errors.Is(err, raft.ErrMembershipChangeInProgress):
		return adminproto.Response{Status: adminproto.StatusMembershipChangeInProgress}
	case errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return adminproto.Response{Status: adminproto.StatusLeadershipTransferInProgress}
	case errors.Is(err, raft.ErrNotAVoter):
		return adminproto.Response{Status: adminproto.StatusNotAVoter}
	case errors.Is(err, raft.ErrInvalidConfiguration):
		return adminproto.Response{Status: adminproto.StatusInvalidConfiguration}
	case errors.Is(err, raft.ErrCannotTransferToSelf):
		return adminproto.Response{Status: adminproto.StatusCannotTransferToSelf}
	case errors.Is(err, raft.ErrLeadershipTransferRejected):
		return adminproto.Response{Status: adminproto.StatusTransferRejected}
	case errors.Is(err, raft.ErrUnknownTargetAddress):
		return adminproto.Response{Status: adminproto.StatusBadRequest}
	case errors.Is(err, raft.ErrNotLeader):
		return adminproto.Response{Status: adminproto.StatusNotLeader}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, raft.ErrNodeClosed):
		return adminproto.Response{Status: adminproto.StatusTimeout}
	default:
		return adminproto.Response{Status: adminproto.StatusInternalError}
	}
}
