#!/usr/bin/env bash
# Leader-failover demo: crash the real leader OS process (SIGKILL),
# confirm the surviving majority elects a replacement and previously
# committed data survives, write a second key through the new leader,
# then restart the crashed node from its SAME on-disk data directory and
# confirm it catches up. See docs/demo.md and docs/runbook-failover.md.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

cleanup() { "$SCRIPT_DIR/stop-local-cluster.sh" || true; }
trap cleanup EXIT

"$SCRIPT_DIR/reset-local-cluster.sh"
"$SCRIPT_DIR/start-local-cluster.sh"

ALL_ADDRS=(--addr "$(node_addr 1)" --addr "$(node_addr 2)" --addr "$(node_addr 3)")

leader="$(leader_id)"
echo "--- current leader: node $leader ---"

echo "--- put x=1 ---"
"$QKV_BIN" "${ALL_ADDRS[@]}" put x 1

echo "--- killing node $leader (SIGKILL, simulated crash) ---"
kill_node "$leader"

survivor_addrs=()
for id in "${NODE_IDS[@]}"; do
	if [[ "$id" != "$leader" ]]; then
		survivor_addrs+=(--addr "$(node_addr "$id")")
	fi
done

echo "--- waiting for a new leader among the survivors ---"
deadline=$((SECONDS + 15))
new_leader=""
while (( SECONDS < deadline )); do
	if candidate="$(leader_id)" && [[ "$candidate" != "$leader" ]]; then
		new_leader="$candidate"
		break
	fi
	sleep 0.2
done
if [[ -z "$new_leader" ]]; then
	echo "error: no new leader elected within 15s" >&2
	exit 1
fi
echo "--- new leader: node $new_leader ---"


# qkv only fails over across --addr seeds on a NOT_LEADER redirect, not on
# a connection failure — so the crashed node's address is deliberately
# left out here rather than passed and left to fail the first dial.
echo "--- get x (expect 1, from surviving majority) ---"
"$QKV_BIN" "${survivor_addrs[@]}" get x

echo "--- put y=2 (through new leader) ---"
"$QKV_BIN" "${survivor_addrs[@]}" put y 2

echo "--- restarting node $leader from its same data directory ---"
start_node "$leader"

echo "--- waiting for node $leader to catch up ---"
deadline=$((SECONDS + 15))
caught_up=""
while (( SECONDS < deadline )); do
	if out="$("$QKV_BIN" --addr "$(node_addr "$leader")" --timeout 1s status 2>/dev/null)"; then
		applied="$(sed -n 's/^last-applied:[[:space:]]*//p' <<<"$out")"
		if [[ -n "$applied" && "$applied" -ge 2 ]]; then
			caught_up=1
			break
		fi
	fi
	sleep 0.3
done
if [[ -z "$caught_up" ]]; then
	echo "error: node $leader never caught up" >&2
	exit 1
fi

echo "--- status of every node ---"
for id in "${NODE_IDS[@]}"; do
	echo "node $id:"
	"$QKV_BIN" --addr "$(node_addr "$id")" --timeout 2s status
done

echo "--- verifying x and y across the whole cluster ---"
"$QKV_BIN" "${ALL_ADDRS[@]}" get x
"$QKV_BIN" "${ALL_ADDRS[@]}" get y

echo "failover demo complete"
