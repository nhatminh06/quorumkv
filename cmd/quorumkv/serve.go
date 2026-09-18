package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"quorumkv/internal/observability"
	"quorumkv/internal/raft"
	"quorumkv/internal/service"
	"quorumkv/internal/transport"
)

// serve runs cfg's node to completion: opens persistent storage,
// constructs and attaches the Service, starts listening, begins the
// election timer, then blocks until SIGINT/SIGTERM triggers a graceful
// shutdown. Storage recovery failure (including canonical persistence
// corruption — see docs/crash-consistency.md) is reported and returned
// without ever serving a single request; nothing is deleted, reset, or
// silently reinterpreted.
func serve(ctx context.Context, cfg nodeConfig) error {
	level := new(slog.LevelVar)
	switch cfg.logLevel {
	case "debug":
		level.Set(slog.LevelDebug)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		level.Set(slog.LevelInfo)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("node_id", uint64(cfg.id))
	if err := prepareDataDir(cfg.data); err != nil {
		return err
	}

	store := raft.NewStore(filepath.Join(cfg.data, "state"))
	logStore, err := raft.OpenLog(filepath.Join(cfg.data, "log"))
	if err != nil {
		return fmt.Errorf("opening log: %w", err)
	}
	commitStore := raft.NewCommitStore(filepath.Join(cfg.data, "commit"))
	snapshotStore := raft.NewSnapshotStore(filepath.Join(cfg.data, "snapshot"))

	svc := service.New(cfg.peers)
	node, err := raft.NewNode(cfg.id, store, logStore, commitStore, snapshotStore, cfg.peers, svc.Apply, svc.Snapshot, svc.Restore)
	if err != nil {
		return fmt.Errorf("recovering persistent state: %w", err)
	}
	// Bootstrap membership (self + configured peers) only matters for a
	// genuinely fresh node — NewNode already loaded any real persisted
	// membership history from the log/snapshot before this point, and
	// that is authoritative forever regardless of what flags a later
	// restart is given (see docs/membership.md and docs/operations.md).
	// SetSelfAddr/SetPeers must still run before this node ever accepts
	// a connection, so a fresh node's bootstrap configuration hands
	// peers this node's own real dialable address rather than the
	// internal placeholder NewNode uses until told otherwise.
	node.SetSelfAddr(cfg.listen)
	node.SetPeers(cfg.peers)
	node.SetObserver(svc.Metrics())
	node.SetLogger(logger)
	svc.SetLogger(logger)
	svc.Attach(node)

	tr, err := transport.Listen(cfg.listen, svc.Handler())
	if err != nil {
		node.Close()
		return fmt.Errorf("listening on %s: %w", cfg.listen, err)
	}

	var obs *observability.Server
	if cfg.metricsListen != "" {
		obs, err = observability.Listen(cfg.metricsListen, svc.Metrics(), node.ObservabilitySnapshot, func() (bool, string) {
			if applyErr := node.ApplyError(); applyErr != nil {
				return false, "application pipeline halted"
			}
			return true, "ready"
		})
		if err != nil {
			_ = tr.Close()
			node.Close()
			return fmt.Errorf("metrics listening on %s: %w", cfg.metricsListen, err)
		}
		logger.Info("observability_started", "event", "observability_started", "listen", obs.Addr())
	}

	logStartup(logger, cfg, node)

	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		node.Run(runCtx)
	}()

	go logRoleChanges(runCtx, logger, node)

	waitForShutdownSignal(ctx)
	logger.Info("node_stopping", "event", "node_stopping")
	if obs != nil {
		if err := obs.Close(); err != nil {
			logger.Warn("observability_close_failed", "event", "observability_close_failed", "error", err)
		}
	}

	// Stop accepting/dispatching inbound work before tearing down the
	// node (see docs/operations.md) — Transport.Close waits for every
	// in-flight handler to finish, so nothing can race a handler against
	// Node.Close.
	if err := tr.Close(); err != nil {
		logger.Warn("transport_close_failed", "event", "transport_close_failed", "error", err)
	}
	cancelRun()
	<-runDone
	node.Close()
	logger.Info("node_stopped", "event", "node_stopped")
	return nil
}

// prepareDataDir creates cfg's data directory if missing, and rejects a
// path that exists but is not a directory.
func prepareDataDir(dir string) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("data path %s exists and is not a directory", dir)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("checking data path %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating data directory %s: %w", dir, err)
	}
	return nil
}

func logStartup(logger *slog.Logger, cfg nodeConfig, node *raft.Node) {
	idx, term := node.SnapshotBoundary()
	logger.Info("node_started", "event", "node_started", "listen", cfg.listen, "peer_count", len(cfg.peers),
		"term", uint64(node.CurrentTerm()), "last_log_index", uint64(node.LastLogIndex()), "commit_index", uint64(node.CommitIndex()), "last_applied", uint64(node.LastApplied()), "snapshot_index", uint64(idx), "snapshot_term", uint64(term))
}

// logRoleChanges polls Role at a modest interval and logs only actual
// transitions — startup/shutdown are logged unconditionally elsewhere;
// this covers "leadership gained/lost" without polling anything else or
// producing output under normal idle operation (no per-heartbeat/
// per-AppendEntries logging).
func logRoleChanges(ctx context.Context, logger *slog.Logger, node *raft.Node) {
	last := node.Role()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			role := node.Role()
			if role == last {
				continue
			}
			last = role
			term := node.CurrentTerm()
			if role == raft.Leader {
				logger.Info("leader_elected", "event", "leader_elected", "term", uint64(term), "role", role.String())
			} else {
				logger.Info("role_changed", "event", "role_changed", "term", uint64(term), "role", role.String())
			}
		}
	}
}

func waitForShutdownSignal(ctx context.Context) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	select {
	case <-sigCh:
	case <-ctx.Done():
	}
}
