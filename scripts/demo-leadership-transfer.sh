#!/usr/bin/env bash
# Leadership-transfer demo: ask the current leader to hand off to a
# specific target node and confirm the transfer actually completed
# (target really became leader), not merely that the request was
# accepted. See docs/demo.md and docs/runbook-leadership-transfer.md.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

cleanup() { "$SCRIPT_DIR/stop-local-cluster.sh" || true; }
trap cleanup EXIT

"$SCRIPT_DIR/reset-local-cluster.sh"
"$SCRIPT_DIR/start-local-cluster.sh"

leader="$(leader_id)"
target=""
for id in "${NODE_IDS[@]}"; do
	if [[ "$id" != "$leader" ]]; then
		target="$id"
		break
	fi
done

echo "--- current leader: node $leader, transferring to node $target ---"
"$QKV_BIN" --addr "$(node_addr "$leader")" --timeout 5s transfer-leadership --target "$target"

echo "--- status of node $target (expect role: leader) ---"
"$QKV_BIN" --addr "$(node_addr "$target")" status

echo "leadership-transfer demo complete"
