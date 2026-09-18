package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/observability"
)

func TestObservabilityElectionWriteAndReplicationMetrics(t *testing.T) {
	nodes := startCluster(t, 3)
	for _, n := range nodes {
		n.svc.node.SetObserver(n.svc.Metrics())
	}
	electLeader(t, nodes, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.New(nodes[0].addr()).Put(ctx, []byte("metric-key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	waitForClusterCommit(t, time.Second, nodes, nodes[0].svc.node.LastLogIndex())
	var out strings.Builder
	if err := nodes[0].svc.Metrics().WritePrometheus(&out, nodes[0].svc.node.ObservabilitySnapshot()); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"quorumkv_raft_elections_total 1", `quorumkv_raft_role{role="leader"} 1`,
		`quorumkv_requests_total{operation="put",status="ok"} 1`,
		"quorumkv_proposals_admitted_total 1", `quorumkv_raft_replication_rpcs_total{peer="2"}`,
		`quorumkv_persistence_writes_total{domain="log"}`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestObservabilityConcurrentLoadAndMetricsScrapes(t *testing.T) {
	nodes := startCluster(t, 3)
	for _, n := range nodes {
		n.svc.node.SetObserver(n.svc.Metrics())
	}
	electLeader(t, nodes, 0)
	leader := nodes[0]
	server, err := observability.Listen("127.0.0.1:0", leader.svc.Metrics(), leader.svc.node.ObservabilitySnapshot, func() (bool, string) { return true, "ready" })
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	httpClient := &http.Client{Timeout: 2 * time.Second}
	errCh := make(chan error, 40)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				resp, err := httpClient.Get("http://" + server.Addr() + "/metrics")
				if err != nil {
					errCh <- err
					return
				}
				if resp.StatusCode != http.StatusOK {
					errCh <- fmt.Errorf("scrape status %d", resp.StatusCode)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c := client.New(leader.addr())
			key := []byte(fmt.Sprintf("obs-%d", worker))
			for j := 0; j < 30; j++ {
				value := []byte(fmt.Sprintf("%d", j))
				if err := c.Put(ctx, key, value); err != nil {
					errCh <- err
					return
				}
				got, found, err := c.Get(ctx, key)
				if err != nil || !found || string(got) != string(value) {
					errCh <- fmt.Errorf("Get = %q %v %v", got, found, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
