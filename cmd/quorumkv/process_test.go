package main

// This file builds and runs the actual quorumkv and qkv binaries as real
// OS processes over real TCP with real persistent directories — not
// internal test helpers, not in-process clusters. This is the strongest
// possible proof that the executables built in this milestone actually
// work the way an operator would use them.

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	buildOnce           sync.Once
	quorumkvBin, qkvBin string
	buildErr            error
)

// buildBinaries compiles cmd/quorumkv and cmd/qkv once per test binary
// run (not once per test) and returns their paths.
func buildBinaries(t *testing.T) (string, string) {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "quorumkv-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		quorumkvBin = filepath.Join(dir, "quorumkv")
		qkvBin = filepath.Join(dir, "qkv")
		if out, err := exec.Command("go", "build", "-o", quorumkvBin, ".").CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("building quorumkv: %v\n%s", err, out)
			return
		}
		if out, err := exec.Command("go", "build", "-o", qkvBin, "../qkv").CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("building qkv: %v\n%s", err, out)
			return
		}
	})
	if buildErr != nil {
		t.Fatalf("building test binaries: %v", buildErr)
	}
	return quorumkvBin, qkvBin
}

// nodeProcess is one real "quorumkv node" OS process.
type nodeProcess struct {
	id      int
	addr    string
	dataDir string
	args    []string
	binPath string
	cmd     *exec.Cmd
	out     *bytes.Buffer
	outMu   sync.Mutex
}

func startNode(t *testing.T, binPath string, id int, addr, dataDir string, peers map[int]string) *nodeProcess {
	t.Helper()
	args := []string{"node", "--id", fmt.Sprint(id), "--listen", addr, "--data", dataDir}
	for pid, paddr := range peers {
		args = append(args, "--peer", fmt.Sprintf("%d=%s", pid, paddr))
	}
	np := &nodeProcess{id: id, addr: addr, dataDir: dataDir, args: args, binPath: binPath, out: &bytes.Buffer{}}
	np.launch(t)
	return np
}

// launch starts (or restarts, from the same dataDir/args) the process.
func (np *nodeProcess) launch(t *testing.T) {
	t.Helper()
	cmd := exec.Command(np.binPath, np.args...)
	np.outMu.Lock()
	np.out.Reset()
	cmd.Stdout = np.out
	cmd.Stderr = np.out
	np.outMu.Unlock()
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting node %d: %v", np.id, err)
	}
	np.cmd = cmd
}

// output returns a safe snapshot of the process's combined stdout+stderr
// so far (the buffer is written to concurrently by the running process).
func (np *nodeProcess) output() string {
	np.outMu.Lock()
	defer np.outMu.Unlock()
	return np.out.String()
}

// kill sends SIGKILL — simulating a real crash, not a graceful exit.
func (np *nodeProcess) kill(t *testing.T) {
	t.Helper()
	if np.cmd == nil || np.cmd.Process == nil {
		return
	}
	_ = np.cmd.Process.Kill()
	_ = np.cmd.Wait()
}

// stopGracefully sends SIGTERM and waits for real graceful shutdown.
func (np *nodeProcess) stopGracefully(t *testing.T) {
	t.Helper()
	if np.cmd == nil || np.cmd.Process == nil {
		return
	}
	_ = np.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { np.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("node %d did not exit within 5s of SIGTERM", np.id)
	}
}

func stopAll(t *testing.T, nodes ...*nodeProcess) {
	t.Helper()
	for _, n := range nodes {
		if n != nil {
			n.stopGracefully(t)
		}
	}
}

// runQkv runs the qkv binary once and returns its stdout, stderr, and
// exit code.
func runQkv(t *testing.T, qkvPath string, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	cmd := exec.Command(qkvPath, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	code := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("running qkv %v: %v", args, err)
		}
	}
	return outBuf.String(), errBuf.String(), code
}

// waitForAnyLeader polls qkv status against each of addrs in turn until
// SOME node reports role: leader — a real contested election can be won
// by any voter, not necessarily the first address a test happens to
// list — and fails the test after timeout. Returns that node's address
// and status output.
func waitForAnyLeader(t *testing.T, qkvPath string, addrs []string, timeout time.Duration) (leaderAddr, status string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			out, _, code := runQkv(t, qkvPath, "--addr", addr, "--timeout", "1s", "status")
			if code == 0 && strings.Contains(out, "role:           leader") {
				return addr, out
			}
			if out != "" {
				last = out
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no node among %v became leader within %s; last status seen:\n%s", addrs, timeout, last)
	return "", ""
}

// freePort asks the OS for an ephemeral port by briefly binding to it —
// good enough for tests that immediately hand the port to a subprocess;
// see startNode's callers for the (small, standard-for-this-kind-of-test)
// TOCTOU window this leaves.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
