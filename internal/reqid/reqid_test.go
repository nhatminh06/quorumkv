package reqid

import "testing"

func TestNewClientIDNonZeroAndUnique(t *testing.T) {
	a, err := NewClientID()
	if err != nil {
		t.Fatalf("NewClientID: %v", err)
	}
	if a.IsZero() {
		t.Fatalf("NewClientID returned the reserved all-zero ID")
	}
	b, err := NewClientID()
	if err != nil {
		t.Fatalf("NewClientID: %v", err)
	}
	if a == b {
		t.Fatalf("two calls to NewClientID produced the same ID: %v", a)
	}
}

func TestClientIDIsZero(t *testing.T) {
	var zero ClientID
	if !zero.IsZero() {
		t.Fatalf("zero-value ClientID.IsZero() = false, want true")
	}
	var nonZero ClientID
	nonZero[0] = 1
	if nonZero.IsZero() {
		t.Fatalf("non-zero ClientID.IsZero() = true, want false")
	}
}

func TestClientIDString(t *testing.T) {
	var id ClientID
	for i := range id {
		id[i] = byte(i)
	}
	got := id.String()
	want := "000102030405060708090a0b0c0d0e0f"
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
