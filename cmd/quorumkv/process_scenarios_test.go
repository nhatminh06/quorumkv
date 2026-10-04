package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/reqid"
)

type processLoadStats struct {
	mu                 sync.Mutex
	acked              map[string]string
	ambiguousWrites    atomic.Int64
	transientReadFails atomic.Int64
	before             atomic.Int64
	after              atomic.Int64
	unexpect           atomic.Value
}

func newProcessLoadStats() *processLoadStats {
	return &processLoadStats{acked: make(map[string]string)}
}

func (s *processLoadStats) recordWrite(key, value string, phase int32) {
	s.mu.Lock()
	s.acked[key] = value
	s.mu.Unlock()
	if phase == 0 {
		s.before.Add(1)
	} else {
		s.after.Add(1)
	}
}

func (s *processLoadStats) snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.acked))
	for key, value := range s.acked {
		out[key] = value
	}
	return out
}

func isAmbiguousWriteOutcome(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, client.ErrNoLeaderKnown) ||
		errors.Is(err, client.ErrTimeout) ||
		errors.Is(err, client.ErrClosed) ||
		isTransportFailure(err)
}

func isExpectedReadFailure(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, client.ErrNoLeaderKnown) ||
		errors.Is(err, client.ErrTooManyRedirects) ||
		errors.Is(err, client.ErrTimeout) ||
		errors.Is(err, client.ErrBusy) ||
		isTransportFailure(err)
}

func isTransportFailure(err error) bool {
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}

func m24ClientID(workerID, session int) reqid.ClientID {
	var id reqid.ClientID
	id[0] = 0x24
	id[1] = byte(workerID)
	id[2] = byte(session)
	id[3] = byte(session >> 8)
	return id
}

func runProcessLoad(t *testing.T, addrs map[int]string, stats *processLoadStats, phase *atomic.Int32, stop <-chan struct{}, clients int) (done chan struct{}) {
	t.Helper()
	all := []string{addrs[1], addrs[2], addrs[3]}
	done = make(chan struct{})
	var wg sync.WaitGroup
	var unexpected atomic.Value
	for workerID := 0; workerID < clients; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			session := 0
			sessionWrites := 0
			c := client.NewWithID(m24ClientID(workerID, session), all...)
			defer func() { _ = c.Close() }()
			for operation := 0; ; operation++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
				if operation%10 < 7 {
					key := fmt.Sprintf("load-client-%02d-write-%05d", workerID, operation)
					value := fmt.Sprintf("value-%s", key)
					err := c.Put(ctx, []byte(key), []byte(value))
					cancel()
					if err == nil {
						sessionWrites++
						stats.recordWrite(key, value, phase.Load())
					} else if isAmbiguousWriteOutcome(err) {
						stats.ambiguousWrites.Add(1)
						_ = c.Close()
						session++
						sessionWrites = 0
						c = client.NewWithID(m24ClientID(workerID, session), all...)
					} else if errors.Is(err, client.ErrBusy) {
						// BUSY is a definite rejection before a proposal; the
						// client may safely retain this session and retry the
						// next logical write with its current sequence.
					} else if unexpected.Load() == nil {
						unexpected.Store(fmt.Sprintf("worker %d session %d PUT %s (next sequence %d): %v", workerID, session, key, sessionWrites+1, err))
						return
					}
				} else {
					_, _, err := c.Get(ctx, []byte("load-probe"))
					cancel()
					if err == nil {
						continue
					}
					if isExpectedReadFailure(err) {
						stats.transientReadFails.Add(1)
					} else if unexpected.Load() == nil {
						unexpected.Store(fmt.Sprintf("worker %d session %d GET: %v", workerID, session, err))
						return
					}
				}
			}
		}(workerID)
	}
	go func() {
		wg.Wait()
		if value := unexpected.Load(); value != nil {
			stats.unexpect.Store(value.(string))
		}
		close(done)
	}()
	return done
}

func requireNoUnexpectedLoadError(t *testing.T, stats *processLoadStats) {
	t.Helper()
	if value := stats.unexpect.Load(); value != nil {
		t.Fatalf("unexpected client error during load: %s", value.(string))
	}
}

func statusValue(status, prefix string) (int, bool) {
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			var value int
			if _, err := fmt.Sscanf(line, prefix+" %d", &value); err == nil {
				return value, true
			}
		}
	}
	return 0, false
}

func nodeStatus(t *testing.T, qkvPath, addr string) (string, bool) {
	t.Helper()
	out, _, code := runQkv(t, qkvPath, "--addr", addr, "--timeout", "1s", "status")
	return out, code == 0
}

func TestM24WriteOutcomeClassification(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		ambiguous bool
	}{
		{name: "deadline", err: context.DeadlineExceeded, ambiguous: true},
		{name: "canceled", err: context.Canceled, ambiguous: true},
		{name: "server timeout", err: client.ErrTimeout, ambiguous: true},
		{name: "no leader", err: client.ErrNoLeaderKnown, ambiguous: true},
		{name: "busy", err: client.ErrBusy, ambiguous: false},
		{name: "stale", err: client.ErrStaleRequest, ambiguous: false},
		{name: "conflict", err: client.ErrRequestConflict, ambiguous: false},
		{name: "bad request", err: client.ErrBadRequest, ambiguous: false},
		{name: "internal", err: client.ErrInternal, ambiguous: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isAmbiguousWriteOutcome(test.err); got != test.ambiguous {
				t.Fatalf("isAmbiguousWriteOutcome(%v) = %v, want %v", test.err, got, test.ambiguous)
			}
		})
	}
}

func TestRealProcessObservabilityEndpoints(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	nodeAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	n := &nodeProcess{
		id: 1, addr: nodeAddr, dataDir: t.TempDir(), binPath: quorumkvPath,
		args: []string{"node", "--id", "1", "--listen", nodeAddr, "--metrics-listen", metricsAddr, "--log-level", "debug", "--data", t.TempDir()},
		out:  &lockedBuffer{},
	}
	n.launch(t)
	t.Cleanup(func() { n.stopGracefully(t) })
	waitForAnyLeader(t, qkvPath, []string{nodeAddr}, 10*time.Second)
	if out, stderr, code := runQkv(t, qkvPath, "--addr", nodeAddr, "put", "observed", "value"); code != 0 {
		t.Fatalf("put: code=%d out=%q stderr=%q", code, out, stderr)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		var resp *http.Response
		if !waitFor(t, 5*time.Second, func() bool { var err error; resp, err = client.Get("http://" + metricsAddr + path); return err == nil }) {
			t.Fatalf("%s unavailable; node log:\n%s", path, n.output())
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: code=%d body=%q", path, resp.StatusCode, body)
		}
		if path == "/metrics" {
			for _, name := range []string{"quorumkv_raft_term", "quorumkv_requests_total", "quorumkv_persistence_duration_seconds"} {
				if !strings.Contains(string(body), name) {
					t.Errorf("metrics missing %s", name)
				}
			}
		}
	}
	if output := n.output(); !strings.Contains(output, `"event":"node_started"`) || !strings.Contains(output, `"level":"INFO"`) {
		t.Fatalf("structured lifecycle log missing:\n%s", output)
	}
}

func TestRealProcessLargeFollowerCatchUpAndRestart(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)
	nodes := make(map[int]*nodeProcess, 3)
	for _, id := range []int{1, 2, 3} {
		nodes[id] = startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id))
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.kill(t)
		}
	})
	waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	leaderID := findLeaderID(t, qkvPath, addrs)
	followerID := leaderID%3 + 1

	c := client.New(addrs[1], addrs[2], addrs[3])
	defer c.Close()
	value := bytes.Repeat([]byte("v"), 8*1024)
	putRange := func(from, to int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		for i := from; i < to; i++ {
			if err := c.Put(ctx, []byte(fmt.Sprintf("large-%04d", i)), value); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
	}
	putRange(0, 100)
	nodes[followerID].kill(t)
	putRange(100, 800)

	waitCaughtUp := func() {
		t.Helper()
		if !waitFor(t, 30*time.Second, func() bool {
			out, _, code := runQkv(t, qkvPath, "--addr", addrs[followerID], "--timeout", "1s", "status")
			return code == 0 && lastAppliedAtLeast(out, 800)
		}) {
			t.Fatalf("node %d did not catch up; log:\n%s", followerID, nodes[followerID].output())
		}
	}
	nodes[followerID].launch(t)
	waitCaughtUp()
	segments, err := filepath.Glob(fmt.Sprintf("%s/node%d/log.segments/*/*.seg", dataRoot, followerID))
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("large catch-up created %d segment files, want multiple", len(segments))
	}
	nodes[followerID].kill(t)
	nodes[followerID].launch(t)
	waitCaughtUp()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, index := range []int{0, 799} {
		got, found, err := c.Get(ctx, []byte(fmt.Sprintf("large-%04d", index)))
		if err != nil || !found || !bytes.Equal(got, value) {
			t.Fatalf("get %d after catch-up restart: found=%v err=%v bytes=%d", index, found, err, len(got))
		}
	}
}

// threeNodeAddrs picks 3 free loopback ports and returns each node's own
// address plus its peer map.
func threeNodeAddrs(t *testing.T) (addrs map[int]string) {
	t.Helper()
	addrs = make(map[int]string, 3)
	// Hold all reservations together so the OS cannot return the same port
	// twice within this cluster. Release immediately before starting nodes.
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	for _, id := range []int{1, 2, 3} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		addrs[id] = l.Addr().String()
	}

	return addrs
}

func peersFor(addrs map[int]string, self int) map[int]string {
	out := make(map[int]string, len(addrs)-1)
	for id, addr := range addrs {
		if id != self {
			out[id] = addr
		}
	}
	return out
}

// TestRealProcessThreeNodeClusterPutGet is the mandatory M15 process
// integration test: three real "quorumkv node" OS processes, real TCP,
// real persistent directories, a real election, and a real PUT/GET
// through the actual qkv binary -- no in-process test cluster, no
// test-only hooks.
func TestRealProcessThreeNodeClusterPutGet(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)

	var nodes []*nodeProcess
	for _, id := range []int{1, 2, 3} {
		n := startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id))
		nodes = append(nodes, n)
	}
	t.Cleanup(func() { stopAll(t, nodes...) })

	leaderAddr, leaderStatus := waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	if !strings.Contains(leaderStatus, "voters:") {
		t.Fatalf("leader status missing voter list:\n%s", leaderStatus)
	}

	// Put followers before the observed leader. Each qkv invocation creates
	// a fresh client, so GET must discover the leader independently of PUT.
	var seeds []string
	for _, id := range []int{1, 2, 3} {
		if addrs[id] != leaderAddr {
			seeds = append(seeds, addrs[id])
		}
	}
	seeds = append(seeds, leaderAddr)
	status, stderr, code := runQkv(t, qkvPath, "--addr", seeds[0], "status")
	if code != 0 || !strings.Contains(status, "role:           follower") {
		t.Fatalf("first seed is not a follower: code=%d status=%q stderr=%q", code, status, stderr)
	}
	args := addrJoin(seeds, "--addr")
	out, stderr, code := runQkv(t, qkvPath, append(args, "put", "hello", "world")...)
	if code != 0 || strings.TrimSpace(out) != "OK" {
		t.Fatalf("put: code=%d out=%q stderr=%q", code, out, stderr)
	}

	// A loaded CI runner can make one quorum-confirmed ReadIndex attempt hit
	// its client deadline even after the write completed. GET is safe to retry;
	// require the real processes to converge within a fixed outer bound.
	if !waitFor(t, 10*time.Second, func() bool {
		out, stderr, code = runQkv(t, qkvPath, append(args, "--timeout", "2s", "get", "hello")...)
		return code == 0 && strings.TrimSpace(out) == "world"
	}) {
		t.Fatalf("get: code=%d out=%q stderr=%q", code, out, stderr)
	}

	// 3 is qkv's own exitNotFound (cmd/qkv/main.go) -- a different
	// package main in a different directory, so its unexported constant
	// isn't reachable from here; the value is part of qkv's documented,
	// stable exit-code contract (see docs/operations.md).
	const qkvExitNotFound = 3
	out, _, code = runQkv(t, qkvPath, append(args, "get", "does-not-exist")...)
	if code != qkvExitNotFound || strings.TrimSpace(out) != "not found" {
		t.Fatalf("get(missing key): code=%d out=%q, want code=%d out=\"not found\"", code, out, qkvExitNotFound)
	}
}

// TestRealProcessFailover is the mandatory failover scenario: kill the
// real leader OS process (SIGKILL, a genuine crash, not graceful
// shutdown), confirm the surviving majority elects a replacement and
// the previously committed key is still readable, write a second key
// through the new leader, then restart the old leader from its SAME
// persistent directory and confirm it catches up on both keys.
func TestRealProcessFailover(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)

	nodes := make(map[int]*nodeProcess, 3)
	for _, id := range []int{1, 2, 3} {
		nodes[id] = startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id))
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.kill(t) // best-effort cleanup; already-stopped nodes are a no-op
		}
	})

	waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	leaderID := findLeaderID(t, qkvPath, addrs)

	// Establish the acknowledged write before testing the crash. Prefer the
	// observed leader here; follower-first discovery is covered separately by
	// TestRealProcessThreeNodeClusterPutGet. Give this setup operation the same
	// bounded budget as leader discovery on a loaded process-test runner.
	seedArgs := addrJoin(append([]string{addrs[leaderID]}, survivorAddrs(addrs, leaderID)...), "--addr")
	if out, stderr, code := runQkv(t, qkvPath, append(seedArgs, "--timeout", "10s", "put", "x", "1")...); code != 0 {
		for id, node := range nodes {
			t.Logf("node %d:\n%s", id, node.output())
		}
		t.Fatalf("put x=1: code=%d out=%q stderr=%q", code, out, stderr)
	}

	nodes[leaderID].kill(t) // a real crash -- SIGKILL, no graceful shutdown

	survivors := survivorAddrs(addrs, leaderID)
	waitForAnyLeader(t, qkvPath, survivors, 10*time.Second)

	getXArgs := append(append([]string{}, addrJoin(survivors, "--addr")...), "--timeout", "10s", "get", "x")
	out, _, code := runQkv(t, qkvPath, getXArgs...)
	if code != 0 || strings.TrimSpace(out) != "1" {
		t.Fatalf("get x after failover: code=%d out=%q", code, out)
	}
	putYArgs := append(append([]string{}, addrJoin(survivors, "--addr")...), "--timeout", "10s", "put", "y", "2")
	if out, _, code := runQkv(t, qkvPath, putYArgs...); code != 0 {
		t.Fatalf("put y=2 after failover: code=%d out=%q", code, out)
	}

	// Restart the old leader from the SAME data directory/args -- proving
	// real disk persistence, not in-memory survival.
	nodes[leaderID].launch(t)
	t.Cleanup(func() { nodes[leaderID].kill(t) })

	// Poll status DIRECTLY on the restarted node (no redirect, no
	// ReadIndex) so this actually proves ITS OWN local state caught up
	// -- a GET against this node's address would just follow a
	// NOT_LEADER redirect to whichever node is currently leader and
	// prove nothing about this specific node's own replication state.
	//
	// The catch-up target is ">= 2", not "== 2": the new leader's first
	// ReadIndex GET (the "get x after failover" call above) appends its
	// own mandatory current-term no-op commit barrier before it can
	// safely serve that read (see docs/read-index.md), so the real
	// final index legitimately depends on exactly when that happened
	// relative to "put y" -- both are still committed and applied
	// regardless of which index each one landed at.
	if !waitFor(t, 15*time.Second, func() bool {
		out, _, code := runQkv(t, qkvPath, "--addr", addrs[leaderID], "--timeout", "1s", "status")
		return code == 0 && lastAppliedAtLeast(out, 2)
	}) {
		t.Fatalf("restarted node %d never caught up (last-applied never reached 2); log:\n%s", leaderID, nodes[leaderID].output())
	}

	// Now confirm the actual values via a normal (redirect-following)
	// GET against the whole cluster.
	out, _, code = runQkv(t, qkvPath, "--addr", addrs[1], "--addr", addrs[2], "--addr", addrs[3], "get", "x")
	if code != 0 || strings.TrimSpace(out) != "1" {
		t.Fatalf("get x after restart: code=%d out=%q", code, out)
	}
	out, _, code = runQkv(t, qkvPath, "--addr", addrs[1], "--addr", addrs[2], "--addr", addrs[3], "get", "y")
	if code != 0 || strings.TrimSpace(out) != "2" {
		t.Fatalf("get y after restart: code=%d out=%q", code, out)
	}
}

func TestRealProcessLeaderFailoverUnderLoad(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)
	nodes := make(map[int]*nodeProcess, 3)
	for _, id := range []int{1, 2, 3} {
		nodes[id] = startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id))
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.kill(t)
		}
	})

	waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	leaderID := findLeaderID(t, qkvPath, addrs)

	stats := newProcessLoadStats()
	var phase atomic.Int32
	stop := make(chan struct{})
	done := runProcessLoad(t, addrs, stats, &phase, stop, 16)
	if !waitFor(t, 10*time.Second, func() bool { return stats.before.Load() >= 20 }) {
		close(stop)
		<-done
		t.Fatalf("load did not establish successful traffic; acknowledged=%d ambiguous=%d read-transient=%d", stats.before.Load(), stats.ambiguousWrites.Load(), stats.transientReadFails.Load())
	}

	leaderID = waitForLeaderID(t, qkvPath, addrs, 10*time.Second)
	oldStatus, ok := nodeStatus(t, qkvPath, addrs[leaderID])
	if !ok {
		close(stop)
		<-done
		t.Fatalf("current leader %d became unreachable before crash", leaderID)
	}
	oldTerm, _ := statusValue(oldStatus, "term:")
	nodes[leaderID].kill(t)
	phase.Store(1)
	var replacementID, replacementTerm int
	if !waitFor(t, 15*time.Second, func() bool {
		for _, id := range []int{1, 2, 3} {
			if id == leaderID {
				continue
			}
			status, reachable := nodeStatus(t, qkvPath, addrs[id])
			if reachable && strings.Contains(status, "role:           leader") {
				replacementID = id
				replacementTerm, _ = statusValue(status, "term:")
				return true
			}
		}
		return false
	}) {
		close(stop)
		<-done
		t.Fatalf("surviving majority did not elect a replacement; old leader=%d term=%d", leaderID, oldTerm)
	}
	if replacementID == leaderID || replacementTerm < oldTerm {
		t.Fatalf("replacement leader=%d term=%d, want different node and term >= %d", replacementID, replacementTerm, oldTerm)
	}
	if !waitFor(t, 10*time.Second, func() bool { return stats.after.Load() >= 12 }) {
		close(stop)
		<-done
		t.Fatalf("clients did not make post-failover progress; before=%d after=%d ambiguous=%d read-transient=%d", stats.before.Load(), stats.after.Load(), stats.ambiguousWrites.Load(), stats.transientReadFails.Load())
	}
	close(stop)
	<-done
	if value := stats.unexpect.Load(); value != nil {
		for id, node := range nodes {
			t.Logf("node %d output:\n%s", id, node.output())
		}
	}
	requireNoUnexpectedLoadError(t, stats)

	nodes[leaderID].launch(t)
	if !waitForNodeCatchUp(t, qkvPath, nodes[leaderID], addrs, leaderID, 20*time.Second) {
		t.Fatalf("restarted leader %d did not catch up; output:\n%s", leaderID, nodes[leaderID].output())
	}
	verifyAcknowledgedWrites(t, qkvPath, addrs, stats.snapshot())
	assertStableProcessCluster(t, qkvPath, addrs)
	t.Logf("leader failover under load: before=%d after=%d ambiguous=%d read-transient=%d crashed=%d replacement=%d term=%d verified=%d", stats.before.Load(), stats.after.Load(), stats.ambiguousWrites.Load(), stats.transientReadFails.Load(), leaderID, replacementID, replacementTerm, len(stats.snapshot()))
}

func TestRealProcessFollowerDownUnderLoad(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)
	nodes := make(map[int]*nodeProcess, 3)
	for _, id := range []int{1, 2, 3} {
		nodes[id] = startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id))
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.kill(t)
		}
	})
	waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	stats := newProcessLoadStats()
	var phase atomic.Int32
	stop := make(chan struct{})
	done := runProcessLoad(t, addrs, stats, &phase, stop, 16)
	if !waitFor(t, 10*time.Second, func() bool { return stats.before.Load() >= 20 }) {
		close(stop)
		<-done
		t.Fatalf("load did not establish successful traffic; acknowledged=%d ambiguous=%d read-transient=%d", stats.before.Load(), stats.ambiguousWrites.Load(), stats.transientReadFails.Load())
	}
	leaderID := waitForLeaderID(t, qkvPath, addrs, 10*time.Second)
	followerID := leaderID%3 + 1
	nodes[followerID].kill(t)
	phase.Store(1)
	if !waitFor(t, 10*time.Second, func() bool { return stats.after.Load() >= 12 }) {
		close(stop)
		<-done
		t.Fatalf("quorum did not continue writing with follower %d down; after=%d ambiguous=%d read-transient=%d", followerID, stats.after.Load(), stats.ambiguousWrites.Load(), stats.transientReadFails.Load())
	}
	close(stop)
	<-done
	requireNoUnexpectedLoadError(t, stats)
	nodes[followerID].launch(t)
	if !waitForNodeCatchUp(t, qkvPath, nodes[followerID], addrs, followerID, 20*time.Second) {
		t.Fatalf("restarted follower %d did not catch up; output:\n%s", followerID, nodes[followerID].output())
	}
	verifyAcknowledgedWrites(t, qkvPath, addrs, stats.snapshot())
	assertStableProcessCluster(t, qkvPath, addrs)
	t.Logf("follower down under load: follower=%d before=%d after=%d ambiguous=%d read-transient=%d verified=%d", followerID, stats.before.Load(), stats.after.Load(), stats.ambiguousWrites.Load(), stats.transientReadFails.Load(), len(stats.snapshot()))
}

func TestRealProcessRepeatedCrashRecovery(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)
	nodes := make(map[int]*nodeProcess, 3)
	for _, id := range []int{1, 2, 3} {
		nodes[id] = startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id))
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.kill(t)
		}
	})
	waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	c := client.New(addrs[1], addrs[2], addrs[3])
	defer c.Close()
	verified := make(map[string]string)
	killSequence := []int{1, 0, 1}
	for round, selector := range killSequence {
		leaderID := waitForLeaderID(t, qkvPath, addrs, 10*time.Second)
		crashID := selector
		if selector == 0 {
			crashID = leaderID
		}
		if crashID == leaderID && selector != 0 {
			crashID = leaderID%3 + 1
		}
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("recovery-round-%d-write-%02d", round+1, i)
			value := fmt.Sprintf("value-%s", key)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.Put(ctx, []byte(key), []byte(value))
			cancel()
			if err != nil {
				t.Fatalf("round %d put %s: %v", round+1, key, err)
			}
			verified[key] = value
		}
		nodes[crashID].kill(t)
		if crashID == leaderID {
			survivors := survivorAddrs(addrs, crashID)
			if !waitFor(t, 15*time.Second, func() bool {
				for _, addr := range survivors {
					status, ok := nodeStatus(t, qkvPath, addr)
					if ok && strings.Contains(status, "role:           leader") {
						return true
					}
				}
				return false
			}) {
				t.Fatalf("round %d did not elect a replacement after killing leader %d", round+1, crashID)
			}
		}
		nodes[crashID].launch(t)
		if !waitForNodeCatchUp(t, qkvPath, nodes[crashID], addrs, crashID, 20*time.Second) {
			t.Fatalf("round %d node %d did not catch up", round+1, crashID)
		}
		verifyAcknowledgedWrites(t, qkvPath, addrs, verified)
		t.Logf("repeated crash round %d: crashed=%d acknowledged=%d", round+1, crashID, len(verified))
	}
	assertStableProcessCluster(t, qkvPath, addrs)
}

func waitForNodeCatchUp(t *testing.T, qkvPath string, node *nodeProcess, addrs map[int]string, restartedID int, timeout time.Duration) bool {
	t.Helper()
	return waitFor(t, timeout, func() bool {
		maxCommit := 0
		seenSurvivor := false
		for id, addr := range addrs {
			if id == restartedID {
				continue
			}
			status, ok := nodeStatus(t, qkvPath, addr)
			if !ok {
				continue
			}
			seenSurvivor = true
			if commit, ok := statusValue(status, "commit-index:"); ok && commit > maxCommit {
				maxCommit = commit
			}
		}
		status, ok := nodeStatus(t, qkvPath, node.addr)
		if !ok {
			return false
		}
		applied, appliedOK := statusValue(status, "last-applied:")
		return seenSurvivor && appliedOK && applied >= maxCommit && strings.Contains(status, "membership:     stable")
	})
}

func verifyAcknowledgedWrites(t *testing.T, qkvPath string, addrs map[int]string, writes map[string]string) {
	t.Helper()
	c := client.New(addrs[1], addrs[2], addrs[3])
	defer c.Close()
	for key, expected := range writes {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, found, err := c.Get(ctx, []byte(key))
		cancel()
		if err != nil || !found || string(got) != expected {
			t.Fatalf("acknowledged write %q missing after recovery: found=%v err=%v value=%q want=%q", key, found, err, got, expected)
		}
	}
}

func assertStableProcessCluster(t *testing.T, qkvPath string, addrs map[int]string) {
	t.Helper()
	last := make(map[int]string, len(addrs))
	if !waitFor(t, 15*time.Second, func() bool {
		leaders := 0
		for id, addr := range addrs {
			status, ok := nodeStatus(t, qkvPath, addr)
			if !ok {
				return false
			}
			last[id] = status
			if strings.Contains(status, "role:           leader") {
				leaders++
			}
			if !strings.Contains(status, "membership:     stable") || !strings.Contains(status, "apply-lag:      0") {
				return false
			}
		}
		return leaders == 1
	}) {
		for id, status := range last {
			t.Logf("node %d final status:\n%s", id, status)
		}
		t.Fatalf("cluster did not converge to one stable, fully-applied leader within 15s")
	}
}

// TestRealProcessStatusAndTransfer is the mandatory admin-over-real-
// processes coverage (status and leadership transfer): confirms qkv
// status and qkv transfer-leadership work against a real cluster and
// that transfer only reports success once the target is confirmed
// leader.
func TestRealProcessStatusAndTransfer(t *testing.T) {
	quorumkvPath, qkvPath := buildBinaries(t)
	dataRoot := t.TempDir()
	addrs := threeNodeAddrs(t)

	var nodes []*nodeProcess
	for _, id := range []int{1, 2, 3} {
		nodes = append(nodes, startNode(t, quorumkvPath, id, addrs[id], fmt.Sprintf("%s/node%d", dataRoot, id), peersFor(addrs, id)))
	}
	t.Cleanup(func() { stopAll(t, nodes...) })

	waitForAnyLeader(t, qkvPath, []string{addrs[1], addrs[2], addrs[3]}, 10*time.Second)
	leaderID := findLeaderID(t, qkvPath, addrs)
	var target int
	for _, id := range []int{1, 2, 3} {
		if id != leaderID {
			target = id
			break
		}
	}

	out, stderr, code := runQkv(t, qkvPath, "--addr", addrs[leaderID], "transfer-leadership", "--target", fmt.Sprint(target))
	if code != 0 || !strings.Contains(out, fmt.Sprintf("leadership transferred to node %d", target)) {
		t.Fatalf("transfer-leadership: code=%d out=%q stderr=%q", code, out, stderr)
	}

	status, _, code := runQkv(t, qkvPath, "--addr", addrs[target], "status")
	if code != 0 || !strings.Contains(status, "role:           leader") {
		t.Fatalf("target %d status after transfer does not show leader:\n%s", target, status)
	}
}

func findLeaderID(t *testing.T, qkvPath string, addrs map[int]string) int {
	t.Helper()
	for id, addr := range addrs {
		out, _, code := runQkv(t, qkvPath, "--addr", addr, "--timeout", "1s", "status")
		if code == 0 && strings.Contains(out, "role:           leader") {
			return id
		}
	}
	t.Fatalf("no node currently reports itself as leader")
	return 0
}

func waitForLeaderID(t *testing.T, qkvPath string, addrs map[int]string, timeout time.Duration) int {
	t.Helper()
	var leaderID int
	if !waitFor(t, timeout, func() bool {
		for id, addr := range addrs {
			status, ok := nodeStatus(t, qkvPath, addr)
			if ok && strings.Contains(status, "role:           leader") {
				leaderID = id
				return true
			}
		}
		return false
	}) {
		t.Fatalf("no node currently reports itself as leader within %s", timeout)
	}
	return leaderID
}

func survivorAddrs(addrs map[int]string, exclude int) []string {
	var out []string
	for id, addr := range addrs {
		if id != exclude {
			out = append(out, addr)
		}
	}
	return out
}

func addrJoin(addrs []string, flag string) []string {
	var out []string
	for _, a := range addrs {
		out = append(out, flag, a)
	}
	return out
}

// lastAppliedAtLeast parses the "last-applied: N" line from qkv status
// output and reports whether N >= want.
func lastAppliedAtLeast(status string, want int) bool {
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "last-applied:") {
			continue
		}
		var got int
		if _, err := fmt.Sscanf(line, "last-applied: %d", &got); err != nil {
			return false
		}
		return got >= want
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return cond()
}
