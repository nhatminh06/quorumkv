package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"quorumkv/internal/client"
	"quorumkv/internal/reqid"
)

type ack struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Phase string `json:"phase"`
}

type stats struct {
	Before             int64
	After              int64
	AmbiguousWrites    int64
	RetiredSessions    int64
	TransientReadFails int64
	StaleRequests      int64
	RequestConflicts   int64
	UnexpectedErrors   int64
	UnexpectedMessage  string
}

func main() {
	workers := flag.Int("workers", 16, "concurrent workers")
	addresses := flag.String("addrs", "", "comma-separated node addresses")
	acksPath := flag.String("acks", "", "acknowledged writes JSONL path")
	statsPath := flag.String("stats", "", "stats JSON path")
	phasePath := flag.String("phase", "", "phase marker path")
	stopPath := flag.String("stop", "", "stop marker path")
	verifyPath := flag.String("verify", "", "acknowledged writes JSONL to verify")
	flag.Parse()
	seeds := split(*addresses)
	if len(seeds) == 0 {
		fatal("no addresses")
	}
	if *verifyPath != "" {
		verify(seeds, *verifyPath)
		return
	}
	if *acksPath == "" || *statsPath == "" || *phasePath == "" || *stopPath == "" {
		fatal("missing workload paths")
	}
	runWorkload(seeds, *workers, *acksPath, *statsPath, *phasePath, *stopPath)
}

func runWorkload(seeds []string, workers int, acksPath, statsPath, phasePath, stopPath string) {
	ackFile, err := os.OpenFile(acksPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fatal(err.Error())
	}
	defer ackFile.Close()
	var fileMu sync.Mutex
	var stateMu sync.Mutex
	var state stats
	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			session := 0
			c := client.NewWithID(clientID(workerID, session), seeds...)
			defer c.Close()
			for operation := 0; ; operation++ {
				if _, err := os.Stat(stopPath); err == nil {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
				if operation%10 < 7 {
					key := fmt.Sprintf("load-client-%02d-write-%05d", workerID, operation)
					value := "value-" + key
					err := c.Put(ctx, []byte(key), []byte(value))
					cancel()
					if err == nil {
						phase := readPhase(phasePath)
						fileMu.Lock()
						_ = json.NewEncoder(ackFile).Encode(ack{Key: key, Value: value, Phase: phase})
						_ = ackFile.Sync()
						fileMu.Unlock()
						stateMu.Lock()
						if phase == "after" { state.After++ } else { state.Before++ }
						stateMu.Unlock()
					} else if ambiguous(err) {
						stateMu.Lock()
						state.AmbiguousWrites++
						state.RetiredSessions++
						stateMu.Unlock()
						_ = c.Close()
						session++
						c = client.NewWithID(clientID(workerID, session), seeds...)
					} else if errors.Is(err, client.ErrBusy) {
						// BUSY is a definite pre-proposal rejection.
					} else {
						stateMu.Lock()
						state.UnexpectedErrors++
						if state.UnexpectedMessage == "" { state.UnexpectedMessage = fmt.Sprintf("worker %d session %d PUT %s: %v", workerID, session, key, err) }
						if errors.Is(err, client.ErrStaleRequest) { state.StaleRequests++ }
						if errors.Is(err, client.ErrRequestConflict) { state.RequestConflicts++ }
						stateMu.Unlock()
						return
					}
				} else {
					_, _, err := c.Get(ctx, []byte("load-probe"))
					cancel()
					if err != nil {
						if expectedRead(err) {
							stateMu.Lock(); state.TransientReadFails++; stateMu.Unlock()
						} else {
							stateMu.Lock(); state.UnexpectedErrors++; if state.UnexpectedMessage == "" { state.UnexpectedMessage = fmt.Sprintf("worker %d session %d GET: %v", workerID, session, err) }; stateMu.Unlock()
							return
						}
					}
				}
			}
		}(workerID)
	}
	for {
		stateMu.Lock(); snapshot := state; stateMu.Unlock()
		writeStats(statsPath, snapshot)
		if finished(&wg, stopPath) { break }
		time.Sleep(100 * time.Millisecond)
	}
	wg.Wait()
	stateMu.Lock(); snapshot := state; stateMu.Unlock()
	writeStats(statsPath, snapshot)
	if snapshot.UnexpectedErrors > 0 { os.Exit(2) }
}

func finished(wg *sync.WaitGroup, stopPath string) bool {
	if _, err := os.Stat(stopPath); err != nil { return false }
	// The caller creates stop only after all desired work has been observed.
	return true
}

func verify(seeds []string, path string) {
	file, err := os.Open(path); if err != nil { fatal(err.Error()) }; defer file.Close()
	c := client.New(seeds...); defer c.Close()
	verified, missing := 0, 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var item ack
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil { fatal(err.Error()) }
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, found, err := c.Get(ctx, []byte(item.Key)); cancel()
		if err == nil && found && string(got) == item.Value { verified++ } else { missing++ }
	}
	if err := scanner.Err(); err != nil { fatal(err.Error()) }
	fmt.Printf("{\"verified\":%d,\"missing\":%d}\n", verified, missing)
	if missing != 0 { os.Exit(3) }
}

func writeStats(path string, s stats) {
	payload := map[string]any{"acknowledged_before": s.Before, "acknowledged_after": s.After, "ambiguous_write_outcomes": s.AmbiguousWrites, "retired_sessions": s.RetiredSessions, "transient_read_failures": s.TransientReadFails, "stale_requests": s.StaleRequests, "request_conflicts": s.RequestConflicts, "unexpected_protocol_errors": s.UnexpectedErrors, "unexpected_message": s.UnexpectedMessage}
	data, _ := json.MarshalIndent(payload, "", "  "); data = append(data, '\n')
	tmp := path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil { _ = os.Rename(tmp, path) }
}

func clientID(worker, session int) reqid.ClientID { var id reqid.ClientID; id[0] = 0x25; id[1] = byte(worker); id[2] = byte(session); id[3] = byte(session >> 8); return id }
func split(value string) []string { var out []string; for _, item := range strings.Split(value, ",") { if strings.TrimSpace(item) != "" { out = append(out, strings.TrimSpace(item)) } }; return out }
func readPhase(path string) string { data, err := os.ReadFile(path); if err == nil && strings.TrimSpace(string(data)) == "after" { return "after" }; return "before" }
func ambiguous(err error) bool { return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, client.ErrNoLeaderKnown) || errors.Is(err, client.ErrTimeout) || errors.Is(err, client.ErrClosed) || transportFailure(err) }
func expectedRead(err error) bool { return ambiguous(err) || errors.Is(err, client.ErrTooManyRedirects) || errors.Is(err, client.ErrBusy) }
func transportFailure(err error) bool { var networkErr net.Error; return errors.As(err, &networkErr) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) }
func fatal(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
