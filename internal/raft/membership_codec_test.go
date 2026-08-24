package raft

import (
	"bytes"
	"testing"
)

// TestEncodeMembershipStableKnownByteVector independently derives the
// expected bytes for a stable configuration {1:"127.0.0.1:10001",
// 2:"127.0.0.1:10002", 3:"127.0.0.1:10003"}.
func TestEncodeMembershipStableKnownByteVector(t *testing.T) {
	m := StableMembership(cfg3())
	got, err := EncodeMembership(m)
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}

	want := []byte{0x01, 0x01}      // version=1, mode=Stable(1)
	want = append(want, 0, 0, 0, 3) // voterCount = 3
	want = append(want, voterBytes(1, "127.0.0.1:10001")...)
	want = append(want, voterBytes(2, "127.0.0.1:10002")...)
	want = append(want, voterBytes(3, "127.0.0.1:10003")...)

	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

// TestEncodeMembershipJointKnownByteVector independently derives the
// expected bytes for a joint transition old={1:"A"} new={1:"A",2:"B"}.
func TestEncodeMembershipJointKnownByteVector(t *testing.T) {
	oldC, err := NewConfiguration(map[NodeID]string{1: "A"})
	if err != nil {
		t.Fatalf("NewConfiguration: %v", err)
	}
	newC, err := NewConfiguration(map[NodeID]string{1: "A", 2: "B"})
	if err != nil {
		t.Fatalf("NewConfiguration: %v", err)
	}
	m := JointMembership(oldC, newC)
	got, err := EncodeMembership(m)
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}

	want := []byte{0x01, 0x02}      // version=1, mode=Joint(2)
	want = append(want, 0, 0, 0, 1) // oldCount = 1
	want = append(want, voterBytes(1, "A")...)
	want = append(want, 0, 0, 0, 2) // newCount = 2
	want = append(want, voterBytes(1, "A")...)
	want = append(want, voterBytes(2, "B")...)

	if !bytes.Equal(got, want) {
		t.Fatalf("bytes:\n got  % x\n want % x", got, want)
	}
}

func voterBytes(id NodeID, addr string) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(id)
		id >>= 8
	}
	lenBuf := []byte{byte(len(addr) >> 8), byte(len(addr))}
	out := append(b, lenBuf...)
	out = append(out, []byte(addr)...)
	return out
}

func cfg3() Configuration {
	c, err := NewConfiguration(map[NodeID]string{
		1: "127.0.0.1:10001",
		2: "127.0.0.1:10002",
		3: "127.0.0.1:10003",
	})
	if err != nil {
		panic(err)
	}
	return c
}

func TestMembershipEncodeDecodeRoundTrip(t *testing.T) {
	stable := StableMembership(cfg3())
	got, err := DecodeMembership(mustEncodeMembership(t, stable))
	if err != nil {
		t.Fatalf("DecodeMembership: %v", err)
	}
	if !got.Equal(stable) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, stable)
	}

	joint := JointMembership(cfg(1, 2, 3), cfg(1, 2, 3, 4))
	got2, err := DecodeMembership(mustEncodeMembership(t, joint))
	if err != nil {
		t.Fatalf("DecodeMembership: %v", err)
	}
	if !got2.Equal(joint) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got2, joint)
	}
}

func mustEncodeMembership(t *testing.T, m Membership) []byte {
	t.Helper()
	b, err := EncodeMembership(m)
	if err != nil {
		t.Fatalf("EncodeMembership: %v", err)
	}
	return b
}

func TestDecodeMembershipTooShort(t *testing.T) {
	if _, err := DecodeMembership([]byte{1}); err == nil {
		t.Fatalf("DecodeMembership(too short) succeeded, want error")
	}
}

func TestDecodeMembershipUnsupportedVersion(t *testing.T) {
	b := mustEncodeMembership(t, StableMembership(cfg3()))
	b[0] = 99
	if _, err := DecodeMembership(b); err == nil {
		t.Fatalf("DecodeMembership(bad version) succeeded, want error")
	}
}

func TestDecodeMembershipUnknownMode(t *testing.T) {
	b := mustEncodeMembership(t, StableMembership(cfg3()))
	b[1] = 99
	if _, err := DecodeMembership(b); err == nil {
		t.Fatalf("DecodeMembership(bad mode) succeeded, want error")
	}
}

func TestDecodeMembershipTrailingBytes(t *testing.T) {
	b := mustEncodeMembership(t, StableMembership(cfg3()))
	b = append(b, 0xFF)
	if _, err := DecodeMembership(b); err == nil {
		t.Fatalf("DecodeMembership(trailing bytes) succeeded, want error")
	}
}

func TestDecodeMembershipOversizedVoterCountRejected(t *testing.T) {
	b := []byte{1, 1, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := DecodeMembership(b); err == nil {
		t.Fatalf("DecodeMembership(huge voter count) succeeded, want error")
	}
}

func TestDecodeMembershipTruncatedAddress(t *testing.T) {
	b := mustEncodeMembership(t, StableMembership(cfg3()))
	if _, err := DecodeMembership(b[:len(b)-3]); err == nil {
		t.Fatalf("DecodeMembership(truncated) succeeded, want error")
	}
}

func TestDecodeMembershipZeroNodeIDRejected(t *testing.T) {
	// Hand-construct: version, mode=Stable, voterCount=1, nodeID=0, addrLen=1, 'x'.
	b := []byte{1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 'x'}
	if _, err := DecodeMembership(b); err == nil {
		t.Fatalf("DecodeMembership(zero NodeID) succeeded, want error")
	}
}
