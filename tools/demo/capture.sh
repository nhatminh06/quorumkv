#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${1:-}"
if [[ -z "$OUTPUT_DIR" ]]; then
	echo "usage: ./tools/demo/capture.sh docs/evidence/canonical" >&2
	exit 2
fi
cd "$ROOT_DIR"
python3 tools/demo/capture.py "$OUTPUT_DIR"
python3 tools/demo/validate.py "$OUTPUT_DIR"
python3 tools/demo/summarize.py "$OUTPUT_DIR"
