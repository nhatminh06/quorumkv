package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// snapshottingNode bundles a Node together with the fake state machine
// backing its SnapshotFunc/RestoreFunc, and the on-disk paths it was
// opened from (so a test can close it and reopen a fresh Node from the
// same files to prove restart-from-snapshot behavior).
type snapshottingNode struct {
	*Node
	dir   string
	state *fakeStateMachine
}

// fakeStateMachine is a minimal deterministic "application" for exercising
// CreateSnapshot/RestoreFunc without depending on internal/kv: Apply
// appends each command's bytes (as a marker), Snapshot encodes the
// commands applied so far, Restore replaces them wholesale.
type fakeStateMachine struct {
	mu       chan struct{} // 1-buffered mutex substitute avoiding import cycles with sync in this small helper
	commands []string
}

func newFakeStateMachine() *fakeStateMachine {
	m := &fakeStateMachine{mu: make(chan struct{}, 1)}
	m.mu <- struct{}{}
	return m
}

func (m *fakeStateMachine) lock()   { <-m.mu }
func (m *fakeStateMachine) unlock() { m.mu <- struct{}{} }

func (m *fakeStateMachine) apply(_ LogIndex, command []byte) error {
	m.lock()
	defer m.unlock()
	m.commands = append(m.commands, string(command))
	return nil
}

// snapshot/restore use a 4-byte big-endian length prefix per command (not
// 1 byte) so this fake state machine can represent commands of any size,
// including the large (>256 KiB) payloads the mandatory large-snapshot
// test exercises.
func (m *fakeStateMachine) snapshot() ([]byte, error) {
	m.lock()
	defer m.unlock()
	out := make([]byte, 0)
	var lenBuf [4]byte
	for _, c := range m.commands {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(c)))
		out = append(out, lenBuf[:]...)
		out = append(out, c...)
	}
	return out, nil
}

func (m *fakeStateMachine) restore(data []byte) error {
	m.lock()
	defer m.unlock()
	var cmds []string
	for i := 0; i < len(data); {
		n := int(binary.BigEndian.Uint32(data[i : i+4]))
		i += 4
		cmds = append(cmds, string(data[i:i+n]))
		i += n
	}
	m.commands = cmds
	return nil
}

// encodeCmd encodes a single command in fakeStateMachine's wire format
// (4-byte big-endian length prefix + bytes), for tests that build
// InstallSnapshot payloads by hand.
func encodeCmd(s string) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
	return append(lenBuf[:], s...)
}

func (m *fakeStateMachine) snapshotOf() []string {
	m.lock()
	defer m.unlock()
	out := make([]string, len(m.commands))
	copy(out, m.commands)
	return out
}

func newSnapshottingNode(t *testing.T, id NodeID, peers map[NodeID]string) *snapshottingNode {
	t.Helper()
	dir := t.TempDir()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(t, dir, id, peers, sm)
	return &snapshottingNode{Node: n, dir: dir, state: sm}
}

func openSnapshottingNode(t *testing.T, dir string, id NodeID, peers map[NodeID]string, sm *fakeStateMachine) *Node {
	t.Helper()
	store := NewStore(filepath.Join(dir, "state"))
	log, err := OpenLog(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	commitStore := NewCommitStore(filepath.Join(dir, "commit"))
	snapStore := NewSnapshotStore(filepath.Join(dir, "snapshot"))
	n, err := NewNode(id, store, log, commitStore, snapStore, peers, sm.apply, sm.snapshot, sm.restore)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(n.Close)
	return n
}

func proposeAsLeaderAndWaitApplied(t *testing.T, n *Node, cmd string) LogIndex {
	t.Helper()
	index, term, err := n.Propose([]byte(cmd))
	if err != nil {
		t.Fatalf("Propose(%q): %v", cmd, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, index, term); err != nil {
		t.Fatalf("WaitApplied(%d): %v", index, err)
	}
	return index
}

// TestCreateSnapshotPersistsAndCompacts proves the documented ordering
// end to end: after CreateSnapshot, the store holds a snapshot matching
// exactly the applied commands, and the log's base has advanced to that
// same boundary (entries at/under it are no longer retained).
func TestCreateSnapshotPersistsAndCompacts(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}

	i1 := proposeAsLeaderAndWaitApplied(t, sn.Node, "one")
	i2 := proposeAsLeaderAndWaitApplied(t, sn.Node, "two")
	_ = i1

	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	store := NewSnapshotStore(filepath.Join(sn.dir, "snapshot"))
	snap, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap == nil {
		t.Fatalf("no snapshot persisted")
	}
	if snap.LastIncludedIndex != i2 {
		t.Fatalf("LastIncludedIndex = %d, want %d", snap.LastIncludedIndex, i2)
	}
	if sn.log.BaseIndex() != i2 {
		t.Fatalf("log.BaseIndex() = %d, want %d (log must be compacted after a successful snapshot)", sn.log.BaseIndex(), i2)
	}
	if _, ok := sn.log.Entry(i2); ok {
		t.Fatalf("entry at compacted boundary %d must no longer be retained", i2)
	}
}

// TestCreateSnapshotNothingAppliedYet proves CreateSnapshot refuses to run
// before anything has been applied — there is no meaningful state to
// capture, and lastApplied == 0 must never be treated as a valid boundary.
func TestCreateSnapshotNothingAppliedYet(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	if err := sn.CreateSnapshot(); err == nil {
		t.Fatalf("CreateSnapshot() = nil, want error when nothing has been applied")
	}
}

// TestCreateSnapshotNoFunctionConfigured proves a node with no SnapshotFunc
// (e.g. a pure-Raft test node) fails explicitly rather than silently
// producing an empty/garbage snapshot.
func TestCreateSnapshotNoFunctionConfigured(t *testing.T) {
	n := newNodeWithApply(t, 1, nil, func(LogIndex, []byte) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, n, "x")
	if err := n.CreateSnapshot(); err == nil {
		t.Fatalf("CreateSnapshot() = nil, want error with no SnapshotFunc configured")
	}
}

// TestCreateSnapshotAlreadyCompactedIsNoop proves calling CreateSnapshot
// again at (or behind) the current boundary is a harmless no-op, not an
// error — a caller invoking it on a schedule shouldn't need to track
// whether it already ran.
func TestCreateSnapshotAlreadyCompactedIsNoop(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, sn.Node, "one")
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("first CreateSnapshot: %v", err)
	}
	baseBefore := sn.log.BaseIndex()
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("second CreateSnapshot: %v", err)
	}
	if sn.log.BaseIndex() != baseBefore {
		t.Fatalf("BaseIndex changed on a no-op snapshot: %d -> %d", baseBefore, sn.log.BaseIndex())
	}
}

// TestCreateSnapshotSaveFailureLeavesLogUncompacted proves the "never
// compact before the replacement snapshot is durably persisted" ordering:
// if persisting fails, the log must remain exactly as it was.
func TestCreateSnapshotSaveFailureLeavesLogUncompacted(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sn.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, sn.Node, "one")

	// Point snapshotStore at a path inside a directory that does not
	// exist so Save fails.
	sn.mu.Lock()
	sn.snapshotStore = NewSnapshotStore(filepath.Join(sn.dir, "missing-subdir", "snapshot"))
	sn.mu.Unlock()

	baseBefore := sn.log.BaseIndex()
	if err := sn.CreateSnapshot(); err == nil {
		t.Fatalf("CreateSnapshot() = nil, want error when Save fails")
	}
	if sn.log.BaseIndex() != baseBefore {
		t.Fatalf("log was compacted despite a failed snapshot save: BaseIndex %d -> %d", baseBefore, sn.log.BaseIndex())
	}
}

// TestRestartFromSnapshotOnly proves a node that only has a snapshot (its
// log has been fully compacted through the snapshot boundary and the
// process is then restarted) rebuilds application state purely from the
// snapshot, with lastApplied/commitIndex starting at its boundary.
func TestRestartFromSnapshotOnly(t *testing.T) {
	dir := t.TempDir()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(t, dir, 1, nil, sm)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, n, "alpha")
	last := proposeAsLeaderAndWaitApplied(t, n, "beta")

	sn := &snapshottingNode{Node: n, dir: dir, state: sm}
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	n.Close()

	sm2 := newFakeStateMachine()
	n2 := openSnapshottingNode(t, dir, 1, nil, sm2)

	if n2.LastApplied() != last {
		t.Fatalf("LastApplied() = %d, want %d", n2.LastApplied(), last)
	}
	if n2.CommitIndex() != last {
		t.Fatalf("CommitIndex() = %d, want %d", n2.CommitIndex(), last)
	}
	if got := sm2.snapshotOf(); len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Fatalf("restored state = %v, want [alpha beta]", got)
	}
}

// TestRestartFromSnapshotPlusSuffix proves the mixed case: a snapshot plus
// additional entries committed after it (never compacted, since no second
// CreateSnapshot ran) — restart must apply the snapshot, then replay
// exactly the retained suffix on top of it, without re-applying anything
// the snapshot already covers.
func TestRestartFromSnapshotPlusSuffix(t *testing.T) {
	dir := t.TempDir()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(t, dir, 1, nil, sm)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	proposeAsLeaderAndWaitApplied(t, n, "alpha")

	sn := &snapshottingNode{Node: n, dir: dir, state: sm}
	if err := sn.CreateSnapshot(); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	last := proposeAsLeaderAndWaitApplied(t, n, "beta")
	n.Close()

	sm2 := newFakeStateMachine()
	n2 := openSnapshottingNode(t, dir, 1, nil, sm2)
	if err := n2.WaitApplied(ctx, last, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	if n2.LastApplied() != last {
		t.Fatalf("LastApplied() = %d, want %d", n2.LastApplied(), last)
	}
	if got := sm2.snapshotOf(); len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Fatalf("restored+replayed state = %v, want [alpha beta]", got)
	}
}

// TestRestartFinishesInterruptedCompaction simulates a crash between
// "persist snapshot" and "compact log" (item 28): the snapshot file is
// saved directly, but the log is left uncompacted. NewNode must finish
// the compaction on startup rather than treating the mismatch as
// corruption.
func TestRestartFinishesInterruptedCompaction(t *testing.T) {
	dir := t.TempDir()
	sm := newFakeStateMachine()
	n := openSnapshottingNode(t, dir, 1, nil, sm)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.StartElection(ctx); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	i1 := proposeAsLeaderAndWaitApplied(t, n, "alpha")
	term := n.CurrentTerm()

	// Persist the snapshot directly (bypassing CreateSnapshot's compact
	// step) to model the crash window.
	store := NewSnapshotStore(filepath.Join(dir, "snapshot"))
	if err := store.Save(Snapshot{LastIncludedIndex: i1, LastIncludedTerm: term, Data: encodeCmd("alpha")}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	n.Close()

	if n.log.BaseIndex() != 0 {
		t.Fatalf("precondition: log should not be compacted yet, BaseIndex() = %d", n.log.BaseIndex())
	}

	sm2 := newFakeStateMachine()
	n2 := openSnapshottingNode(t, dir, 1, nil, sm2)
	if n2.log.BaseIndex() != i1 {
		t.Fatalf("NewNode did not finish the interrupted compaction: BaseIndex() = %d, want %d", n2.log.BaseIndex(), i1)
	}
	if n2.LastApplied() != i1 {
		t.Fatalf("LastApplied() = %d, want %d", n2.LastApplied(), i1)
	}
}

// --- HandleInstallSnapshot ---

// snapshotOf builds a single-chunk InstallSnapshot request whose payload
// is encoded in fakeStateMachine's own format (one command: data).
func snapshotOf(index LogIndex, term Term, data string) InstallSnapshotRequest {
	payload := encodeCmd(data)
	return InstallSnapshotRequest{
		Term: term, LeaderID: 1,
		LastIncludedIndex: index, LastIncludedTerm: term,
		Offset: 0, Data: payload, Done: true,
	}
}

// TestHandleInstallSnapshotStaleTermRejected proves an InstallSnapshot from
// a stale term is rejected exactly like a stale AppendEntries.
func TestHandleInstallSnapshotStaleTermRejected(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	sn.mu.Lock()
	sn.persistent = PersistentState{CurrentTerm: 5}
	sn.mu.Unlock()

	resp, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{Term: 3, LeaderID: 2, LastIncludedIndex: 10, LastIncludedTerm: 3, Done: true, Data: []byte("x")})
	if err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if resp.Success || resp.Term != 5 {
		t.Fatalf("resp = %+v, want Success=false Term=5", resp)
	}
}

// TestHandleInstallSnapshotHigherTermStepsDown proves a higher-term
// InstallSnapshot forces step-down, mirroring AppendEntries.
func TestHandleInstallSnapshotHigherTermStepsDown(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	resp, err := sn.HandleInstallSnapshot(snapshotOf(1, 7, "x"))
	if err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if !resp.Success || resp.Term != 7 {
		t.Fatalf("resp = %+v, want Success=true Term=7", resp)
	}
	if sn.CurrentTerm() != 7 || sn.Role() != Follower {
		t.Fatalf("CurrentTerm()=%d Role()=%v, want 7/Follower", sn.CurrentTerm(), sn.Role())
	}
}

// TestHandleInstallSnapshotSingleChunkInstalls proves a single-chunk
// (Done=true from Offset=0) transfer installs immediately: RestoreFunc is
// called, lastApplied/commitIndex advance, and the log boundary is set.
func TestHandleInstallSnapshotSingleChunkInstalls(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	data := encodeCmd("one")
	resp, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 5, LastIncludedTerm: 1, Offset: 0, Data: data, Done: true,
	})
	if err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp.Success = false, want true")
	}
	if sn.LastApplied() != 5 {
		t.Fatalf("LastApplied() = %d, want 5", sn.LastApplied())
	}
	if sn.CommitIndex() != 5 {
		t.Fatalf("CommitIndex() = %d, want 5", sn.CommitIndex())
	}
	if sn.log.BaseIndex() != 5 {
		t.Fatalf("log.BaseIndex() = %d, want 5", sn.log.BaseIndex())
	}
	if got := sn.state.snapshotOf(); len(got) != 1 || got[0] != "one" {
		t.Fatalf("restored state = %v, want [one]", got)
	}
}

// TestHandleInstallSnapshotMultiChunkAccumulates proves a multi-chunk
// transfer (offset tracking across calls) reconstructs the exact payload
// before installing anything, and rejects an offset that doesn't match
// what has been received so far.
func TestHandleInstallSnapshotMultiChunkAccumulates(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	full := encodeCmd("hello")
	chunk1, chunk2 := full[:3], full[3:]

	resp1, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 1, Offset: 0, Data: chunk1, Done: false,
	})
	if err != nil {
		t.Fatalf("chunk1: %v", err)
	}
	if !resp1.Success || resp1.NextOffset != uint64(len(chunk1)) {
		t.Fatalf("resp1 = %+v, want Success=true NextOffset=%d", resp1, len(chunk1))
	}
	if sn.LastApplied() != 0 {
		t.Fatalf("LastApplied() = %d, want 0 before the final chunk", sn.LastApplied())
	}

	// A mismatched offset must be rejected with the correct expected
	// offset, not silently accepted.
	badResp, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 1, Offset: 0, Data: chunk2, Done: true,
	})
	if err != nil {
		t.Fatalf("bad-offset chunk: %v", err)
	}
	if badResp.Success || badResp.NextOffset != uint64(len(chunk1)) {
		t.Fatalf("badResp = %+v, want Success=false NextOffset=%d", badResp, len(chunk1))
	}

	resp2, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 1, Offset: uint64(len(chunk1)), Data: chunk2, Done: true,
	})
	if err != nil {
		t.Fatalf("chunk2: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("resp2.Success = false, want true")
	}
	if got := sn.state.snapshotOf(); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("restored state = %v, want [hello]", got)
	}
}

// TestHandleInstallSnapshotSessionMismatchRestartsAtZero proves that a
// chunk whose (leaderID, term, boundary) doesn't match the in-progress
// session is treated as the start of a fresh session: it must arrive at
// offset 0, and it discards whatever partial bytes were accumulated for
// the old session.
func TestHandleInstallSnapshotSessionMismatchRestartsAtZero(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	_, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 1, Offset: 0, Data: []byte("partial"), Done: false,
	})
	if err != nil {
		t.Fatalf("first chunk: %v", err)
	}

	// A different lastIncludedIndex at a nonzero offset must be rejected.
	resp, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 20, LastIncludedTerm: 1, Offset: 7, Data: []byte("x"), Done: false,
	})
	if err != nil {
		t.Fatalf("mismatched-session chunk: %v", err)
	}
	if resp.Success || resp.NextOffset != 0 {
		t.Fatalf("resp = %+v, want Success=false NextOffset=0", resp)
	}

	// The same new session starting at offset 0 must be accepted.
	resp2, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 20, LastIncludedTerm: 1, Offset: 0, Data: encodeCmd("z"), Done: true,
	})
	if err != nil {
		t.Fatalf("fresh session chunk: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("resp2.Success = false, want true")
	}
	if got := sn.state.snapshotOf(); len(got) != 1 || got[0] != "z" {
		t.Fatalf("restored state = %v, want [z]", got)
	}
}

// TestHandleInstallSnapshotStaleSnapshotIsIdempotent proves a snapshot
// whose boundary is at or behind lastApplied is acknowledged successfully
// without reinstalling anything — a duplicate/superseded transfer must not
// regress state or error.
func TestHandleInstallSnapshotStaleSnapshotIsIdempotent(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	if _, err := sn.HandleInstallSnapshot(snapshotOf(10, 1, "first")); err != nil {
		t.Fatalf("first install: %v", err)
	}
	if sn.LastApplied() != 10 {
		t.Fatalf("LastApplied() = %d, want 10", sn.LastApplied())
	}

	resp, err := sn.HandleInstallSnapshot(snapshotOf(3, 1, "stale"))
	if err != nil {
		t.Fatalf("stale install: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp.Success = false, want true (stale snapshot must be acknowledged, not errored)")
	}
	if sn.LastApplied() != 10 {
		t.Fatalf("LastApplied() = %d, want unchanged 10 after a stale snapshot", sn.LastApplied())
	}
	if got := sn.state.snapshotOf(); len(got) != 1 || got[0] != "first" {
		t.Fatalf("state = %v, want unchanged [first]", got)
	}
}

// TestHandleInstallSnapshotBoundaryMatchRetainsSuffix proves that when the
// follower's existing log has an entry at the snapshot boundary whose term
// matches, the verified suffix beyond the boundary is retained rather than
// discarded.
func TestHandleInstallSnapshotBoundaryMatchRetainsSuffix(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	if err := sn.log.Append([]LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 1, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := sn.HandleInstallSnapshot(snapshotOf(2, 1, "snap")); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if sn.log.BaseIndex() != 2 {
		t.Fatalf("log.BaseIndex() = %d, want 2", sn.log.BaseIndex())
	}
	if sn.log.LastIndex() != 3 {
		t.Fatalf("log.LastIndex() = %d, want 3 (matching suffix must be retained)", sn.log.LastIndex())
	}
	if e, ok := sn.log.Entry(3); !ok || string(e.Command) != "c" {
		t.Fatalf("Entry(3) = %+v, ok=%v, want c/true", e, ok)
	}
}

// TestHandleInstallSnapshotBoundaryMismatchDiscardsSuffix proves that when
// the follower's log at the boundary index has a conflicting term (or is
// shorter than the boundary), the entire local log is discarded rather
// than trusting any of it.
func TestHandleInstallSnapshotBoundaryMismatchDiscardsSuffix(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	if err := sn.log.Append([]LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Snapshot boundary at index 2 with term 9 conflicts with the local
	// entry's term (1).
	if _, err := sn.HandleInstallSnapshot(snapshotOf(2, 9, "snap")); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if sn.log.BaseIndex() != 2 || sn.log.BaseTerm() != 9 {
		t.Fatalf("log boundary = (%d,%d), want (2,9)", sn.log.BaseIndex(), sn.log.BaseTerm())
	}
	if sn.log.LastIndex() != 2 {
		t.Fatalf("log.LastIndex() = %d, want 2 (conflicting suffix must be discarded)", sn.log.LastIndex())
	}
}

// TestHandleInstallSnapshotTermChangeMidTransferAborts proves that if this
// node's term advances (e.g. it granted a vote to a different candidate)
// between chunks of an in-progress transfer, a subsequent chunk claiming
// the old, now-stale term is rejected rather than completing the install.
func TestHandleInstallSnapshotTermChangeMidTransferAborts(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	if _, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 1, Offset: 0, Data: []byte("partial"), Done: false,
	}); err != nil {
		t.Fatalf("first chunk: %v", err)
	}

	sn.mu.Lock()
	sn.persistent = PersistentState{CurrentTerm: 5}
	sn.mu.Unlock()

	resp, err := sn.HandleInstallSnapshot(InstallSnapshotRequest{
		Term: 1, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 1, Offset: 7, Data: []byte("x"), Done: true,
	})
	if err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if resp.Success || resp.Term != 5 {
		t.Fatalf("resp = %+v, want Success=false Term=5 (stale-term chunk must be rejected)", resp)
	}
	if sn.LastApplied() != 0 {
		t.Fatalf("LastApplied() = %d, want 0 — a stale-term transfer must never install", sn.LastApplied())
	}
}

// TestSetInstallSnapshotSend proves the setter exists and is honored, the
// same way SetVoteSend/SetAppendSend are — required for deterministic
// fault-injection tests outside this package to intercept InstallSnapshot.
func TestSetInstallSnapshotSend(t *testing.T) {
	sn := newSnapshottingNode(t, 1, nil)
	called := false
	sn.SetInstallSnapshotSend(func(ctx context.Context, addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
		called = true
		return InstallSnapshotResponse{}, errors.New("intercepted")
	})
	sn.mu.Lock()
	send := sn.sendInstallSnapshot
	sn.mu.Unlock()
	if _, err := send(context.Background(), "addr", InstallSnapshotRequest{}); err == nil {
		t.Fatalf("expected the intercepted error")
	}
	if !called {
		t.Fatalf("SetInstallSnapshotSend's replacement was not invoked")
	}
}
