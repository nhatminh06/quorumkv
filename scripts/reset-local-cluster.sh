#!/usr/bin/env bash
# Stops the local demo cluster (if running) and removes only its own
# data/log/pid directory (.local/quorumkv/). Never touches anything
# outside that path.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

"$SCRIPT_DIR/stop-local-cluster.sh" || true

rm -rf "$LOCAL_ROOT"
echo "removed $LOCAL_ROOT"
