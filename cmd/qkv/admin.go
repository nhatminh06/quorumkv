package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"quorumkv/internal/adminproto"
	"quorumkv/internal/transport"
)

// maxAdminRedirects bounds how many NOT_LEADER hints sendAdmin will
// follow before giving up — same reasoning as internal/client's own
// bounded redirect chain: a misbehaving or flapping cluster must not
// cause an unbounded tight loop.
const maxAdminRedirects = 3

// sendAdmin sends req to the first reachable address among addrs,
// following a NOT_LEADER redirect hint (bounded) so the common case —
// "send this admin command to the leader" — does not require the
// operator to already know who the leader is.
func sendAdmin(ctx context.Context, addrs []string, req adminproto.Request) (adminproto.Response, error) {
	payload, err := adminproto.EncodeRequest(req)
	if err != nil {
		return adminproto.Response{}, err
	}
	msg := transport.NewMessage(transport.MessageAdminRequest, payload)

	var lastErr error
	addr := addrs[0]
	tried := map[string]bool{}
	for hop := 0; hop <= maxAdminRedirects; hop++ {
		if tried[addr] {
			break
		}
		tried[addr] = true
		respMsg, err := transport.Send(ctx, addr, msg)
		if err != nil {
			lastErr = err
			// Try the next configured seed, if any.
			next := nextUntried(addrs, tried)
			if next == "" {
				break
			}
			addr = next
			continue
		}
		resp, err := adminproto.DecodeResponse(respMsg.Payload)
		if err != nil {
			return adminproto.Response{}, err
		}
		if resp.Status == adminproto.StatusNotLeader && len(resp.LeaderHint) > 0 && !tried[string(resp.LeaderHint)] {
			addr = string(resp.LeaderHint)
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return adminproto.Response{}, lastErr
	}
	return adminproto.Response{}, fmt.Errorf("qkv: could not reach a leader among %v", addrs)
}

func nextUntried(addrs []string, tried map[string]bool) string {
	for _, a := range addrs {
		if !tried[a] {
			return a
		}
	}
	return ""
}

// humanAdminStatus turns an adminproto.Status into an operator-facing
// message — see docs/operations.md.
func humanAdminStatus(resp adminproto.Response) string {
	switch resp.Status {
	case adminproto.StatusNotLeader:
		if len(resp.LeaderHint) > 0 {
			return fmt.Sprintf("qkv: node is not leader; leader hint: %s", resp.LeaderHint)
		}
		return "qkv: node is not leader; no leader hint known"
	case adminproto.StatusBadRequest:
		return "qkv: request rejected as invalid"
	case adminproto.StatusInternalError:
		return "qkv: server reported an internal error"
	case adminproto.StatusMembershipChangeInProgress:
		return "qkv: a membership change is already in progress; check status before retrying"
	case adminproto.StatusLeadershipTransferInProgress:
		return "qkv: a leadership transfer is already in progress; check status before retrying"
	case adminproto.StatusNotAVoter:
		return "qkv: that node ID is not a current voter"
	case adminproto.StatusInvalidConfiguration:
		return "qkv: that change would produce an invalid configuration (e.g. removing the last voter)"
	case adminproto.StatusTimeout:
		return "qkv: timed out waiting for a definite outcome; the operation may or may not have completed — check status before retrying (see docs/operations.md)"
	case adminproto.StatusCannotTransferToSelf:
		return "qkv: cannot transfer leadership to this node itself"
	case adminproto.StatusTransferRejected:
		return "qkv: the target explicitly declined the leadership transfer"
	default:
		return fmt.Sprintf("qkv: unexpected status %d", resp.Status)
	}
}

func cmdStatus(gf globalFlags, args []string) int {
	fs := flag.NewFlagSet("qkv status", flag.ContinueOnError)
	all := fs.Bool("all", false, "query every configured --addr instead of just the first reachable one")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()

	targets := gf.addrs
	if !*all {
		targets = gf.addrs[:1]
	}
	exit := exitOK
	for _, addr := range targets {
		resp, err := sendAdmin(ctx, []string{addr}, adminproto.Request{Operation: adminproto.OpStatus})
		if err != nil {
			fmt.Fprintf(os.Stderr, "qkv: %s: %v\n", addr, err)
			exit = exitFailure
			continue
		}
		if resp.Status != adminproto.StatusOK {
			fmt.Fprintf(os.Stderr, "qkv: %s: %s\n", addr, humanAdminStatus(resp))
			exit = exitFailure
			continue
		}
		if len(targets) > 1 {
			fmt.Printf("== %s ==\n", addr)
		}
		printStatus(resp.Info)
	}
	return exit
}

func printStatus(info adminproto.StatusInfo) {
	fmt.Printf("node:           %d\n", info.NodeID)
	fmt.Printf("role:           %s\n", roleString(info.Role))
	fmt.Printf("term:           %d\n", info.Term)
	if info.LeaderID != 0 {
		fmt.Printf("leader:         %d\n", info.LeaderID)
	} else {
		fmt.Printf("leader:         unknown\n")
	}
	fmt.Printf("last-log-index: %d\n", info.LastLogIndex)
	fmt.Printf("commit-index:   %d\n", info.CommitIndex)
	fmt.Printf("last-applied:   %d\n", info.LastApplied)
	fmt.Printf("snapshot-index: %d\n", info.SnapshotIndex)
	fmt.Printf("snapshot-term:  %d\n", info.SnapshotTerm)
	switch info.Mode {
	case adminproto.MembershipJoint:
		fmt.Printf("membership:     joint\n")
		fmt.Printf("old voters:\n")
		for _, v := range info.OldVoters {
			fmt.Printf("  %d %s\n", v.ID, v.Addr)
		}
		fmt.Printf("new voters:\n")
		for _, v := range info.NewVoters {
			fmt.Printf("  %d %s\n", v.ID, v.Addr)
		}
	default:
		fmt.Printf("membership:     stable\n")
		fmt.Printf("voters:\n")
		for _, v := range info.StableVoters {
			fmt.Printf("  %d %s\n", v.ID, v.Addr)
		}
	}
}

func roleString(r adminproto.Role) string {
	switch r {
	case adminproto.RoleLeader:
		return "leader"
	case adminproto.RoleCandidate:
		return "candidate"
	default:
		return "follower"
	}
}

func cmdSnapshot(gf globalFlags, args []string) int {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: qkv snapshot")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	resp, err := sendAdmin(ctx, gf.addrs, adminproto.Request{Operation: adminproto.OpSnapshot})
	if err != nil {
		fmt.Fprintln(os.Stderr, "qkv:", err)
		return exitFailure
	}
	if resp.Status != adminproto.StatusOK {
		fmt.Fprintln(os.Stderr, humanAdminStatus(resp))
		return exitFailure
	}
	fmt.Printf("OK snapshot-index=%d snapshot-term=%d\n", resp.SnapshotIndex, resp.SnapshotTerm)
	return exitOK
}

func cmdTransferLeadership(gf globalFlags, args []string) int {
	fs := flag.NewFlagSet("qkv transfer-leadership", flag.ContinueOnError)
	target := fs.Uint64("target", 0, "node ID to transfer leadership to (required)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *target == 0 {
		fmt.Fprintln(os.Stderr, "usage: qkv transfer-leadership --target ID")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	resp, err := sendAdmin(ctx, gf.addrs, adminproto.Request{Operation: adminproto.OpTransferLeadership, TransferTarget: *target})
	if err != nil {
		fmt.Fprintln(os.Stderr, "qkv:", err)
		return exitFailure
	}
	if resp.Status != adminproto.StatusOK {
		fmt.Fprintln(os.Stderr, humanAdminStatus(resp))
		return exitFailure
	}
	fmt.Printf("leadership transferred to node %d\n", *target)
	return exitOK
}

func cmdAddVoter(gf globalFlags, args []string) int {
	fs := flag.NewFlagSet("qkv add-voter", flag.ContinueOnError)
	id := fs.Uint64("id", 0, "new voter's node ID (required)")
	addr := fs.String("peer-address", "", "new voter's dialable address (required)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *id == 0 || *addr == "" {
		fmt.Fprintln(os.Stderr, "usage: qkv add-voter --id ID --peer-address ADDR")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	resp, err := sendAdmin(ctx, gf.addrs, adminproto.Request{Operation: adminproto.OpAddVoter, VoterID: *id, VoterAddr: []byte(*addr)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "qkv:", err)
		return exitFailure
	}
	if resp.Status != adminproto.StatusOK {
		fmt.Fprintln(os.Stderr, humanAdminStatus(resp))
		return exitFailure
	}
	fmt.Printf("node %d added as a voter\n", *id)
	return exitOK
}

func cmdRemoveVoter(gf globalFlags, args []string) int {
	fs := flag.NewFlagSet("qkv remove-voter", flag.ContinueOnError)
	id := fs.Uint64("id", 0, "voter's node ID to remove (required)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *id == 0 {
		fmt.Fprintln(os.Stderr, "usage: qkv remove-voter --id ID")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), gf.timeout)
	defer cancel()
	resp, err := sendAdmin(ctx, gf.addrs, adminproto.Request{Operation: adminproto.OpRemoveVoter, VoterID: *id})
	if err != nil {
		fmt.Fprintln(os.Stderr, "qkv:", err)
		return exitFailure
	}
	if resp.Status != adminproto.StatusOK {
		fmt.Fprintln(os.Stderr, humanAdminStatus(resp))
		return exitFailure
	}
	fmt.Printf("node %d removed as a voter\n", *id)
	return exitOK
}
