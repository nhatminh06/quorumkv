#!/usr/bin/env bash
# Membership-change demo: start a 3-voter cluster (A/B/C), start a 4th
# node D as a standalone process not yet part of that cluster's
# consensus group, AddVoter it in through the real leader, confirm all
# four are stable voters, write/read through the 4-node cluster, then
# RemoveVoter D back out and confirm A/B/C return to a stable 3-voter
# configuration. See docs/demo.md and docs/runbook-membership.md.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

NODE_ADDR_D="127.0.0.1:7004"
DATA_D="$DATA_DIR/node4"
PID_D="$PID_DIR/node4.pid"
LOG_D="$LOG_DIR/node4.log"

cleanup() {
	if [[ -f "$PID_D" ]]; then
		pid="$(cat "$PID_D")"
		kill -TERM "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
		rm -f "$PID_D"
	fi
	"$SCRIPT_DIR/stop-local-cluster.sh" || true
}
trap cleanup EXIT

"$SCRIPT_DIR/reset-local-cluster.sh"
"$SCRIPT_DIR/start-local-cluster.sh"

echo "--- starting node 4 standalone (not yet part of the A/B/C group) ---"
mkdir -p "$DATA_D"
"$QUORUMKV_BIN" node --id 4 --listen "$NODE_ADDR_D" --data "$DATA_D" >"$LOG_D" 2>&1 &
echo "$!" >"$PID_D"

leader="$(leader_id)"
echo "--- current leader: node $leader ---"

echo "--- adding node 4 as a voter ---"
"$QKV_BIN" --addr "$(node_addr "$leader")" --timeout 10s add-voter --id 4 --peer-address "$NODE_ADDR_D"

echo "--- status of node 4 (expect 4 stable voters) ---"
"$QKV_BIN" --addr "$NODE_ADDR_D" --timeout 5s status

FOUR_ADDRS=(--addr "$(node_addr 1)" --addr "$(node_addr 2)" --addr "$(node_addr 3)" --addr "$NODE_ADDR_D")

echo "--- put/get through the 4-node cluster ---"
"$QKV_BIN" "${FOUR_ADDRS[@]}" put m hello
"$QKV_BIN" "${FOUR_ADDRS[@]}" get m

leader="$(leader_id)"
echo "--- current leader: node $leader, removing node 4 ---"
"$QKV_BIN" --addr "$(node_addr "$leader")" --timeout 10s remove-voter --id 4

echo "--- status of node 1 (expect 3 stable voters, no node 4) ---"
"$QKV_BIN" --addr "$(node_addr 1)" status

echo "membership demo complete"
