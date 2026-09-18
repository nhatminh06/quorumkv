package observability

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsRepresentativeValuesAndBoundedLabels(t *testing.T) {
	m := New()
	m.RequestStarted()
	m.RecordRequest("put", "ok", 2*time.Millisecond)
	m.RequestFinished()
	m.RecordRequest("user-supplied-key", "arbitrary error", time.Second) // ignored operation
	m.RecordRPC("append_entries", time.Millisecond)
	m.RecordReadIndex(3*time.Millisecond, true)
	m.RecordPersistence("log", 123, 4*time.Millisecond, 2*time.Millisecond)
	m.RecordRaftLogWrite(100, 125)
	m.RaftLogRotated()
	m.RaftLogTruncated()
	m.SetRaftLogSegments(3)
	m.SnapshotCreated(5*time.Millisecond, 42, 7)
	m.SnapshotInstalled(99, 11, false)
	m.RecordReplication(2, 5, 6, 80, false, false)
	var out strings.Builder
	err := m.WritePrometheus(&out, NodeSnapshot{Term: 3, Role: "leader", Leader: true, CommitIndex: 5, LastApplied: 9, LastLogIndex: 10})
	if err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"quorumkv_raft_term 3", `quorumkv_raft_role{role="leader"} 1`,
		"quorumkv_raft_apply_lag 0", `quorumkv_requests_total{operation="put",status="ok"} 1`,
		`quorumkv_raft_peer_replication_lag{peer="2"} 5`, `quorumkv_persistence_writes_total{domain="log"} 1`,
		"quorumkv_readindex_failures_total 1", "quorumkv_snapshot_install_total 1",
		"quorumkv_snapshot_size_bytes 99", "quorumkv_snapshot_last_index 11",
		"quorumkv_raft_log_logical_bytes_appended_total 100",
		"quorumkv_raft_log_physical_bytes_written_total 125",
		"quorumkv_raft_log_rotations_total 1", "quorumkv_raft_log_truncations_total 1",
		"quorumkv_raft_log_segments 3",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
	for _, forbidden := range []string{"user-supplied-key", "arbitrary error", "remote_address=", "key=", "client_id="} {
		if strings.Contains(text, forbidden) {
			t.Errorf("unbounded label leaked: %q", forbidden)
		}
	}
}

func TestMetricsHealthReadyAndConcurrentScrapes(t *testing.T) {
	m := New()
	var mu sync.Mutex
	ready := true
	s, err := Listen("127.0.0.1:0", m, func() NodeSnapshot { return NodeSnapshot{Role: "follower"} }, func() (bool, string) {
		mu.Lock()
		defer mu.Unlock()
		if ready {
			return true, "ready"
		}
		return false, "application pipeline halted"
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, err := client.Get("http://" + s.Addr() + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
		}
	}
	mu.Lock()
	ready = false
	mu.Unlock()
	resp, err := client.Get("http://" + s.Addr() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				resp, err := client.Get("http://" + s.Addr() + "/metrics")
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		m.RecordRequest("get", "ok", time.Microsecond)
	}
	cancel()
	wg.Wait()
}
