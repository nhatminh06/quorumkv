#!/usr/bin/env bash
# Basic KV demo: start a fresh 3-node cluster, PUT/GET/DELETE one key,
# confirm GET returns "not found" after deletion, and print cluster
# status. See docs/demo.md.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

cleanup() { "$SCRIPT_DIR/stop-local-cluster.sh" || true; }
trap cleanup EXIT

"$SCRIPT_DIR/reset-local-cluster.sh"
"$SCRIPT_DIR/start-local-cluster.sh"

ADDRS=(--addr "$(node_addr 1)" --addr "$(node_addr 2)" --addr "$(node_addr 3)")

echo "--- put name=quorumkv ---"
"$QKV_BIN" "${ADDRS[@]}" put name quorumkv

echo "--- get name ---"
"$QKV_BIN" "${ADDRS[@]}" get name

echo "--- delete name ---"
"$QKV_BIN" "${ADDRS[@]}" delete name

echo "--- get name (expect: not found) ---"
if "$QKV_BIN" "${ADDRS[@]}" get name; then
	echo "error: expected 'not found' exit status" >&2
	exit 1
fi

echo "--- status (node 1) ---"
"$QKV_BIN" --addr "$(node_addr 1)" status

echo "basic demo complete"
