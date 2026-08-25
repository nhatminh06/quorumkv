package adminproto

import (
	"bytes"
	"testing"
)

func TestRequestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Request{
		{Operation: OpStatus},
		{Operation: OpSnapshot},
		{Operation: OpTransferLeadership, TransferTarget: 2},
		{Operation: OpAddVoter, VoterID: 4, VoterAddr: []byte("127.0.0.1:7004")},
		{Operation: OpRemoveVoter, VoterID: 4},
	}
	for _, want := range cases {
		b, err := EncodeRequest(want)
		if err != nil {
			t.Fatalf("EncodeRequest(%+v): %v", want, err)
		}
		got, err := DecodeRequest(b)
		if err != nil {
			t.Fatalf("DecodeRequest: %v", err)
		}
		if got.Operation != want.Operation || got.TransferTarget != want.TransferTarget ||
			got.VoterID != want.VoterID || !bytes.Equal(got.VoterAddr, want.VoterAddr) {
			t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeRequestUnknownOperationRejected(t *testing.T) {
	b, err := EncodeRequest(Request{Operation: OpStatus})
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	b[1] = 99
	if _, err := DecodeRequest(b); err == nil {
		t.Fatalf("DecodeRequest with unknown operation succeeded, want error")
	}
}

func TestDecodeRequestTooShortRejected(t *testing.T) {
	if _, err := DecodeRequest([]byte{1, 2, 3}); err == nil {
		t.Fatalf("DecodeRequest(too short) succeeded, want error")
	}
}

func TestDecodeRequestOversizedAddrRejected(t *testing.T) {
	_, err := EncodeRequest(Request{Operation: OpAddVoter, VoterID: 1, VoterAddr: make([]byte, MaxAddrLen+1)})
	if err == nil {
		t.Fatalf("EncodeRequest(oversized addr) succeeded, want error")
	}
}

func TestResponseEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Response{
		{Status: StatusOK},
		{Status: StatusNotLeader, LeaderHint: []byte("127.0.0.1:7002")},
		{Status: StatusMembershipChangeInProgress},
		{
			Status:        StatusOK,
			SnapshotIndex: 100,
			SnapshotTerm:  3,
		},
		{
			Status: StatusOK,
			Info: StatusInfo{
				NodeID:        1,
				Role:          RoleLeader,
				Term:          7,
				LeaderID:      1,
				LastLogIndex:  142,
				CommitIndex:   142,
				LastApplied:   142,
				SnapshotIndex: 100,
				SnapshotTerm:  3,
				Mode:          MembershipStable,
				StableVoters: []Voter{
					{ID: 1, Addr: []byte("127.0.0.1:7001")},
					{ID: 2, Addr: []byte("127.0.0.1:7002")},
					{ID: 3, Addr: []byte("127.0.0.1:7003")},
				},
			},
		},
		{
			Status: StatusOK,
			Info: StatusInfo{
				NodeID: 2,
				Role:   RoleFollower,
				Mode:   MembershipJoint,
				OldVoters: []Voter{
					{ID: 1, Addr: []byte("A")},
					{ID: 2, Addr: []byte("B")},
					{ID: 3, Addr: []byte("C")},
				},
				NewVoters: []Voter{
					{ID: 1, Addr: []byte("A")},
					{ID: 2, Addr: []byte("B")},
					{ID: 3, Addr: []byte("C")},
					{ID: 4, Addr: []byte("D")},
				},
			},
		},
	}
	for i, want := range cases {
		b, err := EncodeResponse(want)
		if err != nil {
			t.Fatalf("case %d: EncodeResponse: %v", i, err)
		}
		got, err := DecodeResponse(b)
		if err != nil {
			t.Fatalf("case %d: DecodeResponse: %v", i, err)
		}
		if got.Status != want.Status || !bytes.Equal(got.LeaderHint, want.LeaderHint) ||
			got.SnapshotIndex != want.SnapshotIndex || got.SnapshotTerm != want.SnapshotTerm {
			t.Fatalf("case %d: top-level mismatch: got %+v, want %+v", i, got, want)
		}
		wantInfo := want.Info
		if wantInfo.Role == 0 {
			wantInfo.Role = RoleFollower
		}
		if wantInfo.Mode == 0 {
			wantInfo.Mode = MembershipStable
		}
		if got.Info.NodeID != wantInfo.NodeID || got.Info.Role != wantInfo.Role || got.Info.Term != wantInfo.Term ||
			got.Info.LeaderID != wantInfo.LeaderID || got.Info.LastLogIndex != wantInfo.LastLogIndex ||
			got.Info.CommitIndex != wantInfo.CommitIndex || got.Info.LastApplied != wantInfo.LastApplied ||
			got.Info.SnapshotIndex != wantInfo.SnapshotIndex || got.Info.SnapshotTerm != wantInfo.SnapshotTerm ||
			got.Info.Mode != wantInfo.Mode {
			t.Fatalf("case %d: info mismatch: got %+v, want %+v", i, got.Info, wantInfo)
		}
		if len(got.Info.StableVoters) != len(wantInfo.StableVoters) ||
			len(got.Info.OldVoters) != len(wantInfo.OldVoters) ||
			len(got.Info.NewVoters) != len(wantInfo.NewVoters) {
			t.Fatalf("case %d: voter list length mismatch: got %+v, want %+v", i, got.Info, wantInfo)
		}
		for j, v := range wantInfo.StableVoters {
			if got.Info.StableVoters[j].ID != v.ID || !bytes.Equal(got.Info.StableVoters[j].Addr, v.Addr) {
				t.Fatalf("case %d: stable voter %d mismatch: got %+v, want %+v", i, j, got.Info.StableVoters[j], v)
			}
		}
	}
}

func TestDecodeResponseUnknownStatusRejected(t *testing.T) {
	b, err := EncodeResponse(Response{Status: StatusOK})
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	b[1] = 99
	if _, err := DecodeResponse(b); err == nil {
		t.Fatalf("DecodeResponse with unknown status succeeded, want error")
	}
}

func TestDecodeResponseTrailingBytesRejected(t *testing.T) {
	b, err := EncodeResponse(Response{Status: StatusOK})
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	b = append(b, 0)
	if _, err := DecodeResponse(b); err == nil {
		t.Fatalf("DecodeResponse with trailing bytes succeeded, want error")
	}
}

func TestDecodeResponseTooManyVotersRejected(t *testing.T) {
	voters := make([]Voter, MaxVoters+1)
	for i := range voters {
		voters[i] = Voter{ID: uint64(i + 1), Addr: []byte("x")}
	}
	if _, err := EncodeResponse(Response{Status: StatusOK, Info: StatusInfo{StableVoters: voters}}); err == nil {
		t.Fatalf("EncodeResponse(too many voters) succeeded, want error")
	}
}

// TestStatusOKKnownByteVector independently derives the expected wire
// bytes for a minimal StatusOK response (no leader hint, no voters, zero
// fields) rather than round-tripping through the encoder alone deciding
// what "correct" means.
func TestStatusOKKnownByteVector(t *testing.T) {
	got, err := EncodeResponse(Response{Status: StatusOK})
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	want := []byte{
		1,              // version
		byte(StatusOK), // status
		0, 0,           // leaderHintLen = 0
		0, 0, 0, 0, 0, 0, 0, 0, // snapshotIndex = 0
		0, 0, 0, 0, 0, 0, 0, 0, // snapshotTerm = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.NodeID = 0
		byte(RoleFollower),     // info.Role default
		0, 0, 0, 0, 0, 0, 0, 0, // info.Term = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.LeaderID = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.LastLogIndex = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.CommitIndex = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.LastApplied = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.SnapshotIndex = 0
		0, 0, 0, 0, 0, 0, 0, 0, // info.SnapshotTerm = 0
		byte(MembershipStable), // info.Mode default
		0,                      // StableVoters count = 0
		0,                      // OldVoters count = 0
		0,                      // NewVoters count = 0
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}
