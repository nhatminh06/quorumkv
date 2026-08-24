package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// applyRecorderFor builds an ApplyFunc backed by a simple in-memory
// key/value map driven by "PUT k v" / "DELETE k" style test commands, so
// scenario tests can assert real applied KV state without depending on
// package kv.
type fakeKV struct {
	state map[string]string
}

func newFakeKV() *fakeKV { return &fakeKV{state: map[string]string{}} }

func (f *fakeKV) apply(_ LogIndex, command []byte) error {
	var op, key, val string
	n, err := sscanFakeCmd(string(command), &op, &key, &val)
	if err != nil {
		return err
	}
	switch op {
	case "PUT":
		if n < 3 {
			return errors.New("fakeKV: malformed PUT")
		}
		f.state[key] = val
	case "DELETE":
		delete(f.state, key)
	default:
		return errors.New("fakeKV: unknown op " + op)
	}
	return nil
}

// sscanFakeCmd parses "PUT key val" or "DELETE key" without pulling in
// fmt.Sscanf's reflection-heavy machinery for this narrow test format.
func sscanFakeCmd(s string, op, key, val *string) (int, error) {
	fields := splitFields(s)
	if len(fields) < 2 {
		return 0, errors.New("fakeKV: too few fields")
	}
	*op = fields[0]
	*key = fields[1]
	if len(fields) >= 3 {
		*val = fields[2]
		return 3, nil
	}
	return 2, nil
}

func splitFields(s string) []string {
	var out []string
	start := -1
	for i := 0; i <= len(s); i++ {
		if i < len(s) && s[i] != ' ' {
			if start == -1 {
				start = i
			}
			continue
		}
		if start != -1 {
			out = append(out, s[start:i])
			start = -1
		}
	}
	return out
}

func proposeAndWait(t *testing.T, n *Node, cmd string) LogIndex {
	t.Helper()
	index, term, err := n.Propose([]byte(cmd))
	if err != nil {
		t.Fatalf("Propose(%q): %v", cmd, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, index, term); err != nil {
		t.Fatalf("WaitApplied(%q): %v", cmd, err)
	}
	return index
}

func electAndWaitLeader(t *testing.T, n *Node) {
	t.Helper()
	if err := n.StartElection(context.Background()); err != nil {
		t.Fatalf("StartElection: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("Role() = %v, want Leader", n.Role())
	}
}

// TestCommittedWriteSurvivesLeaderCrash is Scenario 1: a write
// acknowledged (committed + applied) by the leader must still be present
// — as the identical log entry, not just an equal final KV value — in
// whichever node the surviving majority elects next.
func TestCommittedWriteSurvivesLeaderCrash(t *testing.T) {
	kvA, kvB, kvC := newFakeKV(), newFakeKV(), newFakeKV()
	c := newFaultCluster(t, 3, nil)
	c.nodes[1].applyFunc, c.nodes[2].applyFunc, c.nodes[3].applyFunc = kvA.apply, kvB.apply, kvC.apply

	electAndWaitLeader(t, c.node(1))
	index := proposeAndWait(t, c.node(1), "PUT x 1")
	committedEntry, _ := c.node(1).LogEntry(index)

	// A's own commit is immediate on majority ack, but a follower only
	// learns commitIndex from the *next* heartbeat/AppendEntries — wait
	// for that to actually land before killing A. Without it, B would
	// have the entry replicated but wouldn't yet know it was committed,
	// and (per the current-term commit rule) a new leader cannot commit
	// an old-term entry through its own majority computation either — it
	// can only inherit that knowledge via leaderCommit while the old
	// leader was still alive, or by committing a fresh entry of its own
	// term (which implicitly commits everything before it too).
	eventually(t, time.Second, func() bool {
		return c.node(2).CommitIndex() >= index && c.node(3).CommitIndex() >= index
	}, func() string { return statusString(c.nodes) })

	// A crashes.
	c.stop(1)

	// B is elected among the survivors.
	electAndWaitLeader(t, c.node(2))

	entry, ok := c.node(2).LogEntry(index)
	if !ok {
		t.Fatalf("new leader is missing the committed entry at index %d", index)
	}
	if entry.Term != committedEntry.Term || string(entry.Command) != string(committedEntry.Command) {
		t.Fatalf("new leader's entry = %+v, want %+v (leader completeness)", entry, committedEntry)
	}
	eventually(t, time.Second, func() bool { return c.node(2).LastApplied() >= index },
		func() string { return statusString(c.nodes) })
	if kvB.state["x"] != "1" {
		t.Fatalf("new leader applied state x = %q, want 1", kvB.state["x"])
	}
}

// TestOneFollowerDownStillPermitsQuorumWrites is Scenario 2: in a
// three-node cluster, one follower being unavailable does not prevent
// the remaining leader+follower majority from committing writes. This is
// evidence for that specific tolerance, not a general "high
// availability" claim.
func TestOneFollowerDownStillPermitsQuorumWrites(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))
	c.stop(3)

	i1 := proposeAndWait(t, c.node(1), "PUT x 1")
	i2 := proposeAndWait(t, c.node(1), "PUT y 2")
	i3 := proposeAndWait(t, c.node(1), "DELETE x")

	if c.node(1).CommitIndex() < i3 {
		t.Fatalf("commitIndex = %d, want >= %d", c.node(1).CommitIndex(), i3)
	}
	eventually(t, time.Second, func() bool { return c.node(2).LastApplied() >= i3 },
		func() string { return statusString(c.nodes) })
	_ = i1
	_ = i2
}

// TestIsolatedLeaderCannotCommitButMajorityElectsNewLeader is Scenario 3
// + 4: once A is isolated from both followers, it may still append a
// proposal locally but can never reach majority replication, so
// commitIndex/lastApplied for that entry must never advance on A. B and
// C, still connected to each other, elect a replacement leader in a
// higher term.
func TestIsolatedLeaderCannotCommitButMajorityElectsNewLeader(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))

	c.net.partition(1, 2)
	c.net.partition(1, 3)

	index, term, err := c.node(1).Propose([]byte("PUT ghost 1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	// Give replication attempts a real chance to run (and fail) before
	// asserting the invariant holds — this is checking it holds
	// throughout a window, not polling for a one-shot condition.
	time.Sleep(150 * time.Millisecond)
	if c.node(1).CommitIndex() >= index {
		t.Fatalf("isolated leader's commitIndex = %d, want < %d (no quorum)", c.node(1).CommitIndex(), index)
	}
	if c.node(1).LastApplied() >= index {
		t.Fatalf("isolated leader's lastApplied = %d, want < %d (never applied without commit)", c.node(1).LastApplied(), index)
	}

	// B and C, still connected to each other, can elect a leader in a
	// higher term despite A believing it is still Leader.
	oldTerm := c.node(1).CurrentTerm()
	electAndWaitLeader(t, c.node(2))
	if c.node(2).CurrentTerm() <= oldTerm {
		t.Fatalf("new leader term = %d, want > %d", c.node(2).CurrentTerm(), oldTerm)
	}
	_ = term
}

// TestOldLeaderStepsDownAndDivergentEntryIsRepaired covers Scenario 4:
// after the partition heals, the old leader must learn of the higher
// term through the protocol itself (not a manual role reset) and its
// uncommitted divergent entry must be repaired away — it must never have
// been, and must never become, applied anywhere.
func TestOldLeaderStepsDownAndDivergentEntryIsRepaired(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))
	baseline := proposeAndWait(t, c.node(1), "PUT base 1")
	eventually(t, time.Second, func() bool { return c.node(2).LastApplied() >= baseline && c.node(3).LastApplied() >= baseline },
		func() string { return statusString(c.nodes) })

	c.net.partition(1, 2)
	c.net.partition(1, 3)

	ghostIndex, _, err := c.node(1).Propose([]byte("PUT ghost 1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the doomed replication attempt fail

	electAndWaitLeader(t, c.node(2))
	newIndex := proposeAndWait(t, c.node(2), "PUT real 2")
	eventually(t, time.Second, func() bool { return c.node(3).LastApplied() >= newIndex },
		func() string { return statusString(c.nodes) })

	// Heal the partition; the new leader's heartbeats/AppendEntries reach
	// A again.
	c.net.heal(1, 2)
	c.net.heal(1, 3)

	eventually(t, 2*time.Second, func() bool {
		return c.node(1).Role() == Follower && c.node(1).CurrentTerm() == c.node(2).CurrentTerm()
	}, func() string { return statusString(c.nodes) })

	eventually(t, 2*time.Second, func() bool {
		e, ok := c.node(1).LogEntry(ghostIndex)
		return ok && e.Term == c.node(2).CurrentTerm()
	}, func() string { return statusString(c.nodes) })

	// The repaired entry at ghostIndex must now match the new leader's
	// entry there, not the old "ghost" content.
	repaired, _ := c.node(1).LogEntry(ghostIndex)
	authoritative, _ := c.node(2).LogEntry(ghostIndex)
	if repaired.Term != authoritative.Term || string(repaired.Command) != string(authoritative.Command) {
		t.Fatalf("A's repaired entry = %+v, want %+v", repaired, authoritative)
	}
	if string(repaired.Command) == "PUT ghost 1" {
		t.Fatalf("A's divergent uncommitted entry was never overwritten")
	}
	// The ghost write must never have been committed or applied anywhere.
	for id, n := range c.nodes {
		if n.CommitIndex() >= ghostIndex {
			if e, ok := n.LogEntry(ghostIndex); ok && string(e.Command) == "PUT ghost 1" {
				t.Fatalf("node %d committed the ghost entry", id)
			}
		}
	}
}

// TestStaleFollowerCatchesUpAfterPartitionHeal is Scenario 5: entries
// committed while a follower is partitioned away are replicated to it
// once the partition heals, without a restart.
func TestStaleFollowerCatchesUpAfterPartitionHeal(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))
	c.net.partition(1, 3)
	c.net.partition(2, 3)

	proposeAndWait(t, c.node(1), "PUT a 1")
	proposeAndWait(t, c.node(1), "PUT b 2")
	i3 := proposeAndWait(t, c.node(1), "PUT c 3")
	i4 := proposeAndWait(t, c.node(1), "DELETE a")

	if c.node(3).LastLogIndex() != 0 {
		t.Fatalf("partitioned C should not have received anything yet")
	}

	c.net.heal(1, 3)
	c.net.heal(2, 3)

	eventually(t, 2*time.Second, func() bool { return c.node(3).CommitIndex() >= i4 },
		func() string { return statusString(c.nodes) })
	eventually(t, 2*time.Second, func() bool { return c.node(3).LastApplied() >= i4 },
		func() string { return statusString(c.nodes) })
	logsAgreeUpTo(t, c.nodes, i3)
}

// TestStaleFollowerCatchesUpAfterRestart strengthens Scenario 5 per item
// 25: the follower doesn't just reconnect — it is stopped, its
// persistent files are reused to construct a genuinely new Node, and
// only then reconnected, proving persisted stale state plus replication
// converges.
func TestStaleFollowerCatchesUpAfterRestart(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))
	c.stop(3)

	proposeAndWait(t, c.node(1), "PUT a 1")
	proposeAndWait(t, c.node(1), "PUT b 2")
	last := proposeAndWait(t, c.node(1), "PUT c 3")

	c.restart(3, nil)
	if c.node(3).Role() != Follower {
		t.Fatalf("restarted node role = %v, want Follower", c.node(3).Role())
	}
	if c.node(3).LastLogIndex() != 0 {
		t.Fatalf("restarted node should still have an empty log (it missed everything)")
	}

	eventually(t, 2*time.Second, func() bool { return c.node(3).CommitIndex() >= last },
		func() string { return statusString(c.nodes) })
	logsAgreeUpTo(t, c.nodes, last)
}

// TestDivergentUncommittedSuffixIsRepairedPreservingPrefix is Scenario 6:
// a follower with a matching committed prefix but a divergent
// uncommitted suffix must have exactly that suffix replaced — the
// matching prefix must be byte-identical before and after repair — and
// the repair must persist across a restart.
func TestDivergentUncommittedSuffixIsRepairedPreservingPrefix(t *testing.T) {
	c := newFaultCluster(t, 2, nil)
	leader := c.node(1)
	follower := c.node(2)

	// Shared committed prefix, constructed directly for precise control
	// over terms (bypassing an actual election so both start at term 4).
	prefix := []LogEntry{
		{Term: 1, Command: []byte("A")},
		{Term: 2, Command: []byte("B")},
	}
	if err := leader.log.Append(prefix); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := follower.log.Append(prefix); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Follower's divergent uncommitted suffix.
	if err := follower.log.Append([]LogEntry{
		{Term: 3, Command: []byte("X")},
		{Term: 3, Command: []byte("Y")},
		{Term: 3, Command: []byte("Z")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	req := AppendEntriesRequest{
		Term: 4, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []LogEntry{
			{Term: 4, Command: []byte("C")},
			{Term: 4, Command: []byte("D")},
		},
	}
	// Bring the follower to term 4 first (as real AppendEntries from a
	// term-4 leader would), then deliver the conflicting entries.
	if _, err := follower.HandleAppendEntries(AppendEntriesRequest{Term: 4, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 2}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	resp, err := follower.HandleAppendEntries(req)
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp = %+v, want success", resp)
	}

	if follower.LastLogIndex() != 4 {
		t.Fatalf("LastLogIndex() = %d, want 4", follower.LastLogIndex())
	}
	for i, want := range []string{"A", "B", "C", "D"} {
		e, ok := follower.LogEntry(LogIndex(i + 1))
		if !ok || string(e.Command) != want {
			t.Fatalf("entry %d = %+v, ok=%v, want %q", i+1, e, ok, want)
		}
	}
	// Matching prefix (A, B) must be untouched.
	a, _ := follower.LogEntry(1)
	b, _ := follower.LogEntry(2)
	if a.Term != 1 || string(a.Command) != "A" || b.Term != 2 || string(b.Command) != "B" {
		t.Fatalf("matching prefix was altered: entry1=%+v entry2=%+v", a, b)
	}

	// Persist across restart.
	c.restart(2, nil)
	reopened := c.node(2)
	if reopened.LastLogIndex() != 4 {
		t.Fatalf("after restart LastLogIndex() = %d, want 4", reopened.LastLogIndex())
	}
	for i, want := range []string{"A", "B", "C", "D"} {
		e, ok := reopened.LogEntry(LogIndex(i + 1))
		if !ok || string(e.Command) != want {
			t.Fatalf("after restart entry %d = %+v, ok=%v, want %q", i+1, e, ok, want)
		}
	}
}

// TestStaleCandidateCannotWinAgainstMajorityWithNewerLog is Scenario 7,
// using real persisted logs (not standalone lastLogIndex/lastLogTerm
// variables): a node with a strictly older log cannot win an election
// against a majority holding newer entries.
func TestStaleCandidateCannotWinAgainstMajorityWithNewerLog(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))
	c.net.partition(1, 3)
	c.net.partition(2, 3)

	proposeAndWait(t, c.node(1), "PUT a 1")
	proposeAndWait(t, c.node(1), "PUT b 2")

	c.net.heal(1, 3)
	c.net.heal(2, 3)
	// Leave C's log stale on purpose: do not wait for it to catch up.
	c.stop(1) // remove the current leader so an election is needed

	if c.node(3).LastLogIndex() >= c.node(2).LastLogIndex() {
		t.Skip("C unexpectedly caught up before the election attempt; scenario setup race")
	}

	// C tries first with its stale log.
	if err := c.node(3).StartElection(context.Background()); err != nil {
		t.Fatalf("C StartElection: %v", err)
	}
	if c.node(3).Role() == Leader {
		t.Fatalf("stale-log candidate C must not win against B, which holds a newer log")
	}

	// B, with the up-to-date log, wins.
	electAndWaitLeader(t, c.node(2))
}

// TestRestartRestoresCommittedNodeState is Scenario 8: after committing
// several operations and restarting one node, its Raft log, commitIndex,
// and applied KV state must all be rebuilt from disk before any new
// replication occurs.
func TestRestartRestoresCommittedNodeState(t *testing.T) {
	kv := newFakeKV()
	c := newFaultCluster(t, 1, kv.apply)
	electAndWaitLeader(t, c.node(1))
	proposeAndWait(t, c.node(1), "PUT x 1")
	proposeAndWait(t, c.node(1), "PUT y 2")
	last := proposeAndWait(t, c.node(1), "DELETE x")

	wantTerm := c.node(1).CurrentTerm()
	c.stop(1)

	kv2 := newFakeKV()
	c.restart(1, kv2.apply)
	n := c.node(1)

	if n.Role() != Follower {
		t.Fatalf("Role() = %v, want Follower", n.Role())
	}
	if n.CurrentTerm() != wantTerm {
		t.Fatalf("CurrentTerm() = %d, want %d", n.CurrentTerm(), wantTerm)
	}
	if n.CommitIndex() != last {
		t.Fatalf("CommitIndex() = %d, want %d", n.CommitIndex(), last)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, last, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if n.LastApplied() != n.CommitIndex() {
		t.Fatalf("LastApplied() = %d, want == CommitIndex() %d", n.LastApplied(), n.CommitIndex())
	}
	if _, ok := kv2.state["x"]; ok {
		t.Fatalf("x should be absent (deleted) after rebuild")
	}
	if kv2.state["y"] != "2" {
		t.Fatalf("y = %q, want 2", kv2.state["y"])
	}
}

// TestRestartWithUncommittedSuffixAppliesOnlyCommittedPrefix is Scenario
// 9: a log with committed entries 1..N and an uncommitted suffix N+1..M
// must, after restart, retain the whole log but apply only 1..N.
func TestRestartWithUncommittedSuffixAppliesOnlyCommittedPrefix(t *testing.T) {
	c := newFaultCluster(t, 1, nil)
	n := c.node(1)
	if err := n.log.Append([]LogEntry{
		{Term: 1, Command: []byte("PUT a 1")},
		{Term: 1, Command: []byte("PUT b 2")},
		{Term: 1, Command: []byte("PUT c 3")}, // uncommitted
		{Term: 1, Command: []byte("PUT d 4")}, // uncommitted
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	n.mu.Lock()
	n.commitStore.Save(2)
	n.commitIndex = 2
	n.mu.Unlock()

	kv := newFakeKV()
	c.restart(1, kv.apply)
	n = c.node(1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, 2, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if n.LastApplied() != 2 {
		t.Fatalf("LastApplied() = %d, want 2", n.LastApplied())
	}
	if n.LastLogIndex() != 4 {
		t.Fatalf("LastLogIndex() = %d, want 4 (suffix retained)", n.LastLogIndex())
	}
	if kv.state["a"] != "1" || kv.state["b"] != "2" {
		t.Fatalf("kv state = %+v, want a=1 b=2 only", kv.state)
	}
	if _, ok := kv.state["c"]; ok {
		t.Fatalf("uncommitted c must not be applied")
	}
}

// TestTermPersistsAcrossCrashAndRejectsStaleTerm is Scenario 10:
// currentTerm must not regress across a restart, and once restored, a
// request from an older term is still rejected.
func TestTermPersistsAcrossCrashAndRejectsStaleTerm(t *testing.T) {
	c := newFaultCluster(t, 1, nil)
	electAndWaitLeader(t, c.node(1)) // term 1
	higherTerm := c.node(1).CurrentTerm() + 4
	if _, err := c.node(1).HandleAppendEntries(AppendEntriesRequest{Term: higherTerm, LeaderID: 2}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}

	c.restart(1, nil)
	n := c.node(1)
	if n.CurrentTerm() != higherTerm {
		t.Fatalf("CurrentTerm() after restart = %d, want %d", n.CurrentTerm(), higherTerm)
	}

	resp, err := n.HandleAppendEntries(AppendEntriesRequest{Term: higherTerm - 1, LeaderID: 3})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if resp.Success {
		t.Fatalf("stale-term AppendEntries was accepted after restart")
	}
	if n.CurrentTerm() != higherTerm {
		t.Fatalf("CurrentTerm() regressed to %d after a stale-term request", n.CurrentTerm())
	}
}

// TestVotedForPersistsAcrossCrash proves one-vote-per-term survives a
// restart (item 33): after voting for candidate A in term T and
// restarting before the term changes, a different candidate B requesting
// a vote in the same term T must be denied.
func TestVotedForPersistsAcrossCrash(t *testing.T) {
	c := newFaultCluster(t, 1, nil)
	n := c.node(1)
	resp, err := n.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 10})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("first vote should have been granted")
	}

	c.restart(1, nil)
	n = c.node(1)
	if n.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm() = %d, want 5", n.CurrentTerm())
	}
	if v := n.VotedFor(); v == nil || *v != 10 {
		t.Fatalf("VotedFor() = %v, want 10", v)
	}

	resp, err = n.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: 20})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("a second candidate in the same term was granted a vote after restart")
	}
}

// TestCommitMetaSurvivesCrash is Scenario 11: an entry's committed
// status, once durably recorded, survives an immediate restart.
func TestCommitMetaSurvivesCrash(t *testing.T) {
	kv := newFakeKV()
	c := newFaultCluster(t, 1, kv.apply)
	electAndWaitLeader(t, c.node(1))
	index := proposeAndWait(t, c.node(1), "PUT x 1")
	wantCommit := c.node(1).CommitIndex()
	if wantCommit < index {
		t.Fatalf("commitIndex = %d, want >= %d", wantCommit, index)
	}

	c.stop(1)
	kv2 := newFakeKV()
	c.restart(1, kv2.apply)
	n := c.node(1)

	if n.CommitIndex() < index {
		t.Fatalf("CommitIndex() after restart = %d, want >= %d", n.CommitIndex(), index)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, index, 0); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if kv2.state["x"] != "1" {
		t.Fatalf("kv2.state[x] = %q, want 1", kv2.state["x"])
	}
}

// TestQuorumDenominatorDoesNotShrinkWithDeadNodes is item 94: a
// three-node cluster's majority requirement stays 2 even when only the
// leader is alive — a failed node is unavailable, not removed from the
// cluster's quorum denominator.
func TestQuorumDenominatorDoesNotShrinkWithDeadNodes(t *testing.T) {
	c := newFaultCluster(t, 3, nil)
	electAndWaitLeader(t, c.node(1))
	c.stop(2)
	c.stop(3)

	index, _, err := c.node(1).Propose([]byte("PUT x 1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if c.node(1).CommitIndex() >= index {
		t.Fatalf("CommitIndex() = %d, want < %d — the sole surviving node cannot form a majority of 3 alone", c.node(1).CommitIndex(), index)
	}
}

// TestRepeatedFailoverCyclesRemainStable is Scenario 14: a scripted
// sequence of elect/commit/crash/restart cycles across all three nodes,
// checked for goroutine/lifecycle correctness under -race and for final
// convergence.
func TestRepeatedFailoverCyclesRemainStable(t *testing.T) {
	kv1, kv2, kv3 := newFakeKV(), newFakeKV(), newFakeKV()
	c := newFaultCluster(t, 3, nil)
	c.nodes[1].applyFunc, c.nodes[2].applyFunc, c.nodes[3].applyFunc = kv1.apply, kv2.apply, kv3.apply

	electAndWaitLeader(t, c.node(1))
	var last LogIndex
	for i := 0; i < 3; i++ {
		last = proposeAndWait(t, c.node(1), "PUT a1 x")
	}
	c.stop(1)

	electAndWaitLeader(t, c.node(2))
	for i := 0; i < 3; i++ {
		last = proposeAndWait(t, c.node(2), "PUT a2 y")
	}
	kvA := newFakeKV()
	c.restart(1, kvA.apply)
	eventually(t, 2*time.Second, func() bool { return c.node(1).CommitIndex() >= last },
		func() string { return statusString(c.nodes) })

	c.stop(2)
	electAndWaitLeader(t, c.node(3))
	for i := 0; i < 3; i++ {
		last = proposeAndWait(t, c.node(3), "PUT a3 z")
	}
	kvB := newFakeKV()
	c.restart(2, kvB.apply)
	eventually(t, 2*time.Second, func() bool { return c.node(2).CommitIndex() >= last },
		func() string { return statusString(c.nodes) })

	eventually(t, 2*time.Second, func() bool {
		return c.node(1).LastApplied() >= last && c.node(2).LastApplied() >= last && c.node(3).LastApplied() >= last
	}, func() string { return statusString(c.nodes) })

	logsAgreeUpTo(t, c.nodes, last)
}
