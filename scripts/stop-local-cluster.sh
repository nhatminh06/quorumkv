#!/usr/bin/env bash
# Gracefully stops only the nodes started by start-local-cluster.sh, by
# PID file. Never signals any process this script didn't itself track.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

for id in "${NODE_IDS[@]}"; do
	pid_file="$(node_pid_file "$id")"
	if [[ ! -f "$pid_file" ]]; then
		echo "node $id: no pid file, nothing to stop"
		continue
	fi
	pid="$(cat "$pid_file")"
	if ! kill -0 "$pid" 2>/dev/null; then
		echo "node $id: pid $pid not running"
		rm -f "$pid_file"
		continue
	fi
	kill -TERM "$pid"
	for _ in $(seq 1 50); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.1
	done
	if kill -0 "$pid" 2>/dev/null; then
		echo "node $id: pid $pid did not exit within 5s" >&2
	else
		echo "node $id: stopped (pid $pid)"
	fi
	rm -f "$pid_file"
done
