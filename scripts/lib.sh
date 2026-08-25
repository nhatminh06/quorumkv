# Shared paths and helpers for the local demo cluster scripts. Sourced,
# not executed directly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

QUORUMKV_BIN="$ROOT_DIR/bin/quorumkv"
QKV_BIN="$ROOT_DIR/bin/qkv"

LOCAL_ROOT="$ROOT_DIR/.local/quorumkv"
PID_DIR="$LOCAL_ROOT/pids"
LOG_DIR="$LOCAL_ROOT/logs"
DATA_DIR="$LOCAL_ROOT/data"

NODE_IDS=(1 2 3)
NODE_ADDR_1="127.0.0.1:7001"
NODE_ADDR_2="127.0.0.1:7002"
NODE_ADDR_3="127.0.0.1:7003"

node_addr() {
	local id="$1"
	local var="NODE_ADDR_${id}"
	echo "${!var}"
}

require_binaries() {
	if [[ ! -x "$QUORUMKV_BIN" || ! -x "$QKV_BIN" ]]; then
		echo "error: bin/quorumkv or bin/qkv not built; run 'make build' first" >&2
		exit 1
	fi
}

node_pid_file() {
	echo "$PID_DIR/node$1.pid"
}

node_running() {
	local pid_file
	pid_file="$(node_pid_file "$1")"
	[[ -f "$pid_file" ]] || return 1
	local pid
	pid="$(cat "$pid_file")"
	[[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# start_node launches (or, if the data directory already has state,
# restarts) node $1 from its fixed address/data directory/peer set.
start_node() {
	local id="$1"
	local args=(node --id "$id" --listen "$(node_addr "$id")" --data "$DATA_DIR/node$id")
	for peer in "${NODE_IDS[@]}"; do
		if [[ "$peer" != "$id" ]]; then
			args+=(--peer "$peer=$(node_addr "$peer")")
		fi
	done
	"$QUORUMKV_BIN" "${args[@]}" >"$LOG_DIR/node$id.log" 2>&1 &
	echo "$!" >"$(node_pid_file "$id")"
}

# kill_node sends SIGKILL to node $1 — a real crash, not a graceful exit.
kill_node() {
	local pid_file
	pid_file="$(node_pid_file "$1")"
	[[ -f "$pid_file" ]] || return 0
	local pid
	pid="$(cat "$pid_file")"
	kill -KILL "$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
	rm -f "$pid_file"
}

# leader_id polls every node's status once and returns the id of any node
# that currently reports itself as leader, or empty if none does.
leader_id() {
	for id in "${NODE_IDS[@]}"; do
		local addr
		addr="$(node_addr "$id")"
		if out="$("$QKV_BIN" --addr "$addr" --timeout 1s status 2>/dev/null)"; then
			if grep -q "role:           leader" <<<"$out"; then
				echo "$id"
				return 0
			fi
		fi
	done
	return 1
}

# wait_for_leader polls qkv status against every node address until one
# reports role: leader, or the timeout elapses. Prints the leader's node
# id on success.
wait_for_leader() {
	local timeout="$1"
	local deadline=$((SECONDS + timeout))
	while (( SECONDS < deadline )); do
		if id="$(leader_id)"; then
			echo "$id"
			return 0
		fi
		sleep 0.2
	done
	return 1
}
