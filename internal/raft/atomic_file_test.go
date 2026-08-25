package raft

import (
	"bytes"
	"errors"
	"testing"
)

// shortWriter is a controllable io.Writer-shaped test double that writes
// at most maxPerCall bytes per call, and returns errAfter (if set) once
// its cumulative written count reaches errAt — used to prove writeFull
// loops to completion across successive short writes, and to prove it
// surfaces a real error immediately rather than continuing past it.
type shortWriter struct {
	buf         bytes.Buffer
	maxPerCall  int
	zeroAtCall  int // if > 0, the N-th call (1-indexed) writes 0 bytes with a nil error
	call        int
	errAtCall   int // if > 0, the N-th call fails outright, no bytes written
	errToReturn error
}

func (w *shortWriter) Write(p []byte) (int, error) {
	w.call++
	if w.errAtCall > 0 && w.call == w.errAtCall {
		return 0, w.errToReturn
	}
	if w.zeroAtCall > 0 && w.call == w.zeroAtCall {
		return 0, nil
	}
	n := len(p)
	if w.maxPerCall > 0 && n > w.maxPerCall {
		n = w.maxPerCall
	}
	w.buf.Write(p[:n])
	return n, nil
}

// TestWriteFullHandlesShortWrites proves writeFull loops across
// successive partial writes (n < len(p), nil error) until every byte is
// written — the defensive handling this package's own single
// atomicWriteFile seam relies on for every durable file, even though the
// real os.File it wraps in production is documented to never actually
// do this.
func TestWriteFullHandlesShortWrites(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog")
	w := &shortWriter{maxPerCall: 3}
	if err := writeFull(w, data); err != nil {
		t.Fatalf("writeFull: %v", err)
	}
	if !bytes.Equal(w.buf.Bytes(), data) {
		t.Fatalf("written = %q, want %q", w.buf.Bytes(), data)
	}
	if w.call <= 1 {
		t.Fatalf("test bug: writer never actually exercised multiple short-write calls (call=%d)", w.call)
	}
}

// TestWriteFullRejectsNoProgress proves a write that reports zero bytes
// written with a nil error — which would otherwise loop forever — is
// treated as a hard failure instead.
func TestWriteFullRejectsNoProgress(t *testing.T) {
	w := &shortWriter{zeroAtCall: 1}
	err := writeFull(w, []byte("x"))
	if !errors.Is(err, errNoWriteProgress) {
		t.Fatalf("err = %v, want errNoWriteProgress", err)
	}
}

// TestWriteFullSurfacesUnderlyingError proves a real error from the
// underlying Writer propagates immediately, without writeFull silently
// continuing past it.
func TestWriteFullSurfacesUnderlyingError(t *testing.T) {
	wantErr := errors.New("disk full")
	w := &shortWriter{errAtCall: 1, errToReturn: wantErr}
	err := writeFull(w, []byte("x"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// TestWriteFullEmptyDataIsNoop proves writeFull never calls Write at all
// for empty input — matching atomicWriteFile's existing convention that
// an empty payload is still a legitimate (if unusual) file to publish.
func TestWriteFullEmptyDataIsNoop(t *testing.T) {
	w := &shortWriter{}
	if err := writeFull(w, nil); err != nil {
		t.Fatalf("writeFull(nil): %v", err)
	}
	if w.call != 0 {
		t.Fatalf("Write was called %d times for empty input, want 0", w.call)
	}
}
