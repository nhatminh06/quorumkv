#!/usr/bin/env bash
# Starts a real 3-node QuorumKV cluster as three background OS processes
# on the local machine, using fixed loopback ports and a data directory
# under .local/quorumkv/ (gitignored). Safe to re-run: refuses to start a
# node that is already running.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

require_binaries
mkdir -p "$PID_DIR" "$LOG_DIR" "$DATA_DIR"

for id in "${NODE_IDS[@]}"; do
	if node_running "$id"; then
		echo "node $id already running (pid $(cat "$(node_pid_file "$id")"))"
		continue
	fi

	start_node "$id"
	echo "started node $id: addr=$(node_addr "$id") pid=$(cat "$(node_pid_file "$id")") data=$DATA_DIR/node$id log=$LOG_DIR/node$id.log"
done

echo "waiting for a leader..."
if leader="$(wait_for_leader 10)"; then
	echo "cluster ready: leader is node $leader"
else
	echo "warning: no leader elected within 10s; check logs under $LOG_DIR" >&2
	exit 1
fi
