#!/usr/bin/env bash
# Snapshot / stale-follower catch-up demo: write some data, snapshot the
# leader, take a follower down, write past the snapshot boundary while
# it's down, then restart it and confirm it catches up via InstallSnapshot
# rather than replaying a log it no longer has. See docs/demo.md and
# docs/architecture.md.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

cleanup() { "$SCRIPT_DIR/stop-local-cluster.sh" || true; }
trap cleanup EXIT

"$SCRIPT_DIR/reset-local-cluster.sh"
"$SCRIPT_DIR/start-local-cluster.sh"

ALL_ADDRS=(--addr "$(node_addr 1)" --addr "$(node_addr 2)" --addr "$(node_addr 3)")
leader="$(leader_id)"
follower=""
for id in "${NODE_IDS[@]}"; do
	if [[ "$id" != "$leader" ]]; then
		follower="$id"
		break
	fi
done
echo "--- leader: node $leader, follower to take offline: node $follower ---"

echo "--- writing k1..k5 ---"
for i in 1 2 3 4 5; do
	"$QKV_BIN" "${ALL_ADDRS[@]}" put "k$i" "v$i"
done

echo "--- snapshotting the leader ---"
"$QKV_BIN" --addr "$(node_addr "$leader")" snapshot

echo "--- taking node $follower offline ---"
kill_node "$follower"

survivor_addrs=(--addr "$(node_addr "$leader")")
for id in "${NODE_IDS[@]}"; do
	if [[ "$id" != "$leader" && "$id" != "$follower" ]]; then
		survivor_addrs+=(--addr "$(node_addr "$id")")
	fi
done

echo "--- writing k6..k10 while node $follower is offline ---"
for i in 6 7 8 9 10; do
	"$QKV_BIN" "${survivor_addrs[@]}" put "k$i" "v$i"
done

echo "--- snapshotting the leader again (compacts past node $follower's last-known index) ---"
"$QKV_BIN" --addr "$(node_addr "$leader")" snapshot

echo "--- restarting node $follower ---"
start_node "$follower"

echo "--- waiting for node $follower to catch up via InstallSnapshot ---"
deadline=$((SECONDS + 15))
caught_up=""
while (( SECONDS < deadline )); do
	if out="$("$QKV_BIN" --addr "$(node_addr "$follower")" --timeout 1s status 2>/dev/null)"; then
		applied="$(sed -n 's/^last-applied:[[:space:]]*//p' <<<"$out")"
		if [[ -n "$applied" && "$applied" -ge 10 ]]; then
			caught_up=1
			break
		fi
	fi
	sleep 0.3
done
if [[ -z "$caught_up" ]]; then
	echo "error: node $follower never caught up" >&2
	exit 1
fi

echo "--- node $follower status after catch-up ---"
"$QKV_BIN" --addr "$(node_addr "$follower")" status

echo "--- verifying all 10 keys across the whole cluster ---"
for i in 1 2 3 4 5 6 7 8 9 10; do
	"$QKV_BIN" "${ALL_ADDRS[@]}" get "k$i"
done

echo "snapshot demo complete"
