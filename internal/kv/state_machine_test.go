package kv

import "testing"

func TestPutGet(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))

	v, ok := m.Get([]byte("a"))
	if !ok || string(v) != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", v, ok)
	}
}

func TestOverwrite(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("a"), []byte("2"))

	v, ok := m.Get([]byte("a"))
	if !ok || string(v) != "2" {
		t.Fatalf("Get(a) = %q, %v; want 2, true", v, ok)
	}
}

func TestDelete(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	m.Delete([]byte("a"))

	if _, ok := m.Get([]byte("a")); ok {
		t.Fatalf("Get(a) found a value after Delete")
	}
}

func TestDeleteMissingKeyIsNoop(t *testing.T) {
	m := NewStateMachine()
	m.Delete([]byte("missing")) // must not panic

	if _, ok := m.Get([]byte("missing")); ok {
		t.Fatalf("Get(missing) found a value")
	}
}

func TestOrderingMatters(t *testing.T) {
	m := NewStateMachine()
	m.Apply(NewPutCommand([]byte("x"), []byte("1")))
	m.Apply(NewPutCommand([]byte("x"), []byte("2")))
	m.Apply(NewDeleteCommand([]byte("x")))
	m.Apply(NewPutCommand([]byte("x"), []byte("3")))

	v, ok := m.Get([]byte("x"))
	if !ok || string(v) != "3" {
		t.Fatalf("Get(x) = %q, %v; want 3, true", v, ok)
	}
}

func TestPutDoesNotAliasCallerSlice(t *testing.T) {
	m := NewStateMachine()
	value := []byte("hello")
	m.Put([]byte("x"), value)

	value[0] = 'H' // mutate caller's slice after Put

	v, _ := m.Get([]byte("x"))
	if string(v) != "hello" {
		t.Fatalf("stored value changed after caller mutation: got %q", v)
	}
}

func TestGetDoesNotExposeInternalState(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("x"), []byte("hello"))

	v, _ := m.Get([]byte("x"))
	v[0] = 'H' // mutate the returned slice

	v2, _ := m.Get([]byte("x"))
	if string(v2) != "hello" {
		t.Fatalf("internal state changed after mutating returned slice: got %q", v2)
	}
}

func TestMultipleKeysDoNotCorruptEachOther(t *testing.T) {
	m := NewStateMachine()
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("c"), []byte("3"))
	m.Put([]byte("b"), []byte("22"))
	m.Delete([]byte("a"))

	if _, ok := m.Get([]byte("a")); ok {
		t.Fatalf("a should have been deleted")
	}
	if v, ok := m.Get([]byte("b")); !ok || string(v) != "22" {
		t.Fatalf("b = %q, %v; want 22, true", v, ok)
	}
	if v, ok := m.Get([]byte("c")); !ok || string(v) != "3" {
		t.Fatalf("c = %q, %v; want 3, true", v, ok)
	}
}
