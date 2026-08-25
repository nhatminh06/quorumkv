package raft

import "sync"

// failpointFunc is called by checkFailpoint for every atomicWriteFile
// durability stage, identified as "<domain>.<stage>" (e.g.
// "stable.after-rename" — see atomicWriteFile for the domain/stage
// vocabulary). Returning a non-nil error injects a controlled I/O
// failure at that exact point, indistinguishable to the caller from a
// real one — this is how this package's in-process failure-injection
// tests work (see crashpoint_test.go). A test may instead terminate the
// process inside this func (e.g. os.Exit) to simulate a real crash at
// that exact durability boundary, with none of the calling operation's
// own error-handling/cleanup ever running — this is how the subprocess
// crash helper works (see cmd_crashhelper_test.go); a returned error is
// never equivalent to that, since a real crash allows no defers, no
// Close, no in-memory rollback.
type failpointFunc func(name string) error

var (
	failpointMu sync.Mutex
	failpoint   failpointFunc
)

// setFailpoint installs fn as the active failpoint and returns a func
// that restores whatever was active before — intended for `defer`.
// Guarded by failpointMu so concurrent goroutines within one test (e.g.
// a leader replicating to several peers) never race on the package
// global, but this package's tests never run two failpoint-injecting
// tests concurrently with each other (no t.Parallel() on them) since a
// second test installing its own failpoint mid-test would otherwise
// silently redirect the first test's injection.
func setFailpoint(fn failpointFunc) (restore func()) {
	failpointMu.Lock()
	prev := failpoint
	failpoint = fn
	failpointMu.Unlock()
	return func() {
		failpointMu.Lock()
		failpoint = prev
		failpointMu.Unlock()
	}
}

// checkFailpoint invokes the active failpoint (if any) for
// "<domain>.<stage>", returning nil when none is installed — the
// zero-overhead, always-real-behavior production path.
func checkFailpoint(domain, stage string) error {
	failpointMu.Lock()
	fn := failpoint
	failpointMu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(domain + "." + stage)
}
