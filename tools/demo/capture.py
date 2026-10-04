#!/usr/bin/env python3
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 1
TOOL_VERSION = "m25-canonical-v1"


def fail(message):
    raise RuntimeError(message)


def run(command, check=True, timeout=30, **kwargs):
    return subprocess.run(command, check=check, timeout=timeout, text=True, capture_output=True, **kwargs)


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def parse_status(text):
    result = {"reachable": True}
    voters = []
    section = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("node:"):
            result["node"] = int(line.split(":", 1)[1])
        elif line.startswith("role:"):
            result["role"] = line.split(":", 1)[1].strip()
        elif line.startswith("term:"):
            result["term"] = int(line.split(":", 1)[1])
        elif line.startswith("leader:"):
            value = line.split(":", 1)[1].strip()
            result["leader"] = None if value == "unknown" else int(value)
        elif line.startswith("last-log-index:"):
            result["last_log_index"] = int(line.split(":", 1)[1])
        elif line.startswith("commit-index:"):
            result["commit_index"] = int(line.split(":", 1)[1])
        elif line.startswith("last-applied:"):
            result["last_applied"] = int(line.split(":", 1)[1])
        elif line.startswith("apply-lag:"):
            result["apply_lag"] = int(line.split(":", 1)[1])
        elif line.startswith("snapshot-index:"):
            result["snapshot_index"] = int(line.split(":", 1)[1])
        elif line.startswith("snapshot-term:"):
            result["snapshot_term"] = int(line.split(":", 1)[1])
        elif line.startswith("membership:"):
            result["membership"] = line.split(":", 1)[1].strip()
            section = "voters" if result["membership"] == "stable" else None
        elif line == "voters:":
            section = "voters"
        elif section == "voters":
            match = re.match(r"(\d+)\s+127\.0\.0\.1:\d+$", line)
            if match:
                voters.append(int(match.group(1)))
    if voters:
        result["voters"] = sorted(voters)
    return result


def query_status(qkv, address):
    proc = run([qkv, "--addr", address, "--timeout", "1s", "status"], check=False, timeout=4)
    if proc.returncode != 0:
        return {"reachable": False}
    return parse_status(proc.stdout)


def wait_until(predicate, timeout, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    value = predicate()
    if not value:
        fail("condition did not become true within %.1fs" % timeout)
    return value


def wait_leader(qkv, addresses, timeout=15):
    def find():
        for node_id, address in addresses.items():
            status = query_status(qkv, address)
            if status.get("role") == "leader":
                return node_id, status
        return None
    return wait_until(find, timeout)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path):
    return json.loads(path.read_text())


def read_stats(path):
    if not path.exists():
        return {}
    return read_json(path)


def relevant_log(text, root):
    wanted = ("node_started", "election_started", "leader_elected", "dedup_stale", "raft_log_migrated")
    lines = []
    for line in text.splitlines():
        if any(('"event":"%s"' % event) in line for event in wanted):
            line = line.replace(str(root), "cluster-data")
            line = line.replace(str(ROOT), "repository")
            lines.append(line)
    return "\n".join(lines[-120:]) + ("\n" if lines else "")


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def capture(bundle):
    bundle = Path(bundle).resolve()
    if bundle.exists() and any(bundle.iterdir()):
        fail("bundle directory must be empty or absent: %s" % bundle)
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "relevant-logs").mkdir()
    temp = Path(tempfile.mkdtemp(prefix="quorumkv-m25-"))
    processes = {}
    load = None
    try:
        commit = run(["git", "rev-parse", "HEAD"]).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            fail("could not determine source commit")
        ports = free_ports(3)
        addresses = {i + 1: "127.0.0.1:%d" % ports[i] for i in range(3)}
        data_root = temp / "cluster-data"
        data_root.mkdir()
        bin_dir = temp / "bin"
        bin_dir.mkdir()
        quorumkv = bin_dir / "quorumkv"
        qkv = bin_dir / "qkv"
        loadgen = bin_dir / "loadgen"
        run(["go", "build", "-o", str(quorumkv), "./cmd/quorumkv"], timeout=120)
        run(["go", "build", "-o", str(qkv), "./cmd/qkv"], timeout=120)
        run(["go", "build", "-o", str(loadgen), "./tools/demo/loadgen.go"], timeout=120)

        def start_node(node_id):
            args = [str(quorumkv), "node", "--id", str(node_id), "--listen", addresses[node_id], "--log-level", "debug", "--data", str(data_root / ("node%d" % node_id))]
            for peer_id, peer_addr in addresses.items():
                if peer_id != node_id:
                    args += ["--peer", "%d=%s" % (peer_id, peer_addr)]
            log = (temp / ("node%d.log" % node_id)).open("w")
            processes[node_id] = {"proc": subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT), "log": log}

        for node_id in addresses:
            start_node(node_id)
        initial_leader, initial_leader_status = wait_leader(str(qkv), addresses)
        initial_term = initial_leader_status["term"]
        phase = temp / "phase"
        stop = temp / "stop"
        acks = temp / "acks.jsonl"
        stats_path = temp / "stats.json"
        phase.write_text("before\n")
        load_args = [str(loadgen), "--workers", "16", "--addrs", ",".join(addresses.values()), "--acks", str(acks), "--stats", str(stats_path), "--phase", str(phase), "--stop", str(stop)]
        load = subprocess.Popen(load_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        timeline = [{"event": "cluster_ready", "leader": initial_leader, "term": initial_term, "at_seconds": 0.0}, {"event": "load_started", "workers": 16, "put_percent": 70, "get_percent": 30, "at_seconds": 0.0}]
        wait_until(lambda: read_stats(stats_path).get("acknowledged_before", 0) >= 20, 15)
        initial_status = {str(node_id): query_status(str(qkv), address) for node_id, address in addresses.items()}
        crash_time = time.monotonic()
        processes[initial_leader]["proc"].send_signal(signal.SIGKILL)
        processes[initial_leader]["proc"].wait(timeout=5)
        phase.write_text("after\n")
        timeline.append({"event": "leader_crashed", "node": initial_leader, "signal": "SIGKILL", "at_seconds": round(time.monotonic() - crash_time, 3)})
        survivors = {node_id: address for node_id, address in addresses.items() if node_id != initial_leader}
        replacement_leader, replacement_status = wait_leader(str(qkv), survivors, 15)
        timeline.append({"event": "replacement_leader", "node": replacement_leader, "term": replacement_status["term"], "at_seconds": round(time.monotonic() - crash_time, 3)})
        wait_until(lambda: read_stats(stats_path).get("acknowledged_after", 0) >= 12, 15)
        timeline.append({"event": "post_failover_progress", "at_seconds": round(time.monotonic() - crash_time, 3)})
        failover_status = {str(initial_leader): {"reachable": False}}
        failover_status.update({str(node_id): query_status(str(qkv), address) for node_id, address in survivors.items()})
        start_node(initial_leader)
        timeline.append({"event": "old_leader_restarted", "node": initial_leader, "same_data_directory": True, "data_directory": "cluster-data/node%d" % initial_leader, "at_seconds": round(time.monotonic() - crash_time, 3)})
        def caught_up():
            survivor_status = [query_status(str(qkv), address) for address in survivors.values()]
            restarted = query_status(str(qkv), addresses[initial_leader])
            commits = [item.get("commit_index", 0) for item in survivor_status if item.get("reachable")]
            return bool(commits) and restarted.get("reachable") and restarted.get("last_applied", -1) >= max(commits) and restarted.get("membership") == "stable"
        wait_until(caught_up, 25)
        timeline.append({"event": "old_leader_caught_up", "node": initial_leader, "at_seconds": round(time.monotonic() - crash_time, 3)})
        stop.write_text("stop\n")
        load.wait(timeout=15)
        if load.returncode not in (0, None):
            stderr = load.stderr.read() if load.stderr else ""
            fail("load generator failed: %s" % stderr)
        final_stats = read_stats(stats_path)
        verify_proc = run([str(loadgen), "--verify", str(acks), "--addrs", ",".join(addresses.values())], timeout=120)
        verification = json.loads(verify_proc.stdout.strip())
        timeline.append({"event": "acknowledged_writes_verified", "count": verification["verified"], "at_seconds": round(time.monotonic() - crash_time, 3)})
        final_status = {str(node_id): query_status(str(qkv), address) for node_id, address in addresses.items()}
        total_ack = final_stats.get("acknowledged_before", 0) + final_stats.get("acknowledged_after", 0)
        if verification["verified"] != total_ack:
            fail("verified count does not equal acknowledged count")
        manifest = {"schema_version": SCHEMA_VERSION, "capture_tool_version": TOOL_VERSION, "quorumkv_commit": commit, "go_version": run(["go", "version"]).stdout.strip(), "platform": "linux/amd64" if platform.system().lower() == "linux" and platform.machine() == "x86_64" else platform.system().lower() + "/" + platform.machine(), "scenario": "leader_failover_under_load", "cluster": {"nodes": 3, "transport": "tcp", "persistence": "disk"}, "workload": {"workers": 16, "put_percent": 70, "get_percent": 30}, "failure": {"method": "SIGKILL", "leader_id": initial_leader}, "recovery": {"replacement_leader_id": replacement_leader, "restarted_node_id": initial_leader, "same_data_directory": True}, "result": {"acknowledged_writes": total_ack, "acknowledged_before_crash": final_stats.get("acknowledged_before", 0), "acknowledged_after_failover": final_stats.get("acknowledged_after", 0), "verified_acknowledged_writes": verification["verified"], "ambiguous_write_outcomes": final_stats.get("ambiguous_write_outcomes", 0), "retired_sessions": final_stats.get("retired_sessions", 0), "transient_read_failures": final_stats.get("transient_read_failures", 0), "stale_request_errors": final_stats.get("stale_requests", 0), "request_conflicts": final_stats.get("request_conflicts", 0), "unexpected_protocol_errors": final_stats.get("unexpected_protocol_errors", 0), "status": "PASS"}}
        write_json(bundle / "manifest.json", manifest)
        write_json(bundle / "timeline.json", timeline)
        write_json(bundle / "workload-summary.json", final_stats)
        write_json(bundle / "initial-status.json", initial_status)
        write_json(bundle / "failover-status.json", failover_status)
        write_json(bundle / "final-status.json", final_status)
        write_json(bundle / "acknowledged-writes-summary.json", {"acknowledged_before_crash": final_stats.get("acknowledged_before", 0), "acknowledged_after_failover": final_stats.get("acknowledged_after", 0), "total_acknowledged": total_ack, "verified_acknowledged": verification["verified"], "verification_missing": verification["missing"]})
        for node_id, info in processes.items():
            log_path = temp / ("node%d.log" % node_id)
            if log_path.exists(): (bundle / "relevant-logs" / ("node%d.txt" % node_id)).write_text(relevant_log(log_path.read_text(errors="replace"), data_root))
        readme = """# Canonical failover evidence\n\nThis sanitized bundle records one real three-process QuorumKV leader-failover-under-load capture. It uses real TCP, disk-backed state, 16 concurrent workers, and a deterministic 70% PUT / 30% GET workload. The current leader was killed with SIGKILL while clients remained active; a replacement leader acknowledged additional writes; the old leader restarted from the same disk directory and caught up; every definitely acknowledged write was verified.\n\nAmbiguous write outcomes are excluded from the acknowledgment set. The workload retires that in-memory client session before issuing another logical write. The bundle records those outcomes and retired sessions separately. No STALE_REQUEST, REQUEST_CONFLICT, or unexpected protocol error is accepted.\n\nM24 previously exposed an incorrect stale response when a replacement leader inspected dedup state before applying its committed prefix. The fix is `f3ff0bfd41c8fb0d2bca3864af96f8dd85b130bd`; this bundle is captured after that fix.\n\nBoundaries: local loopback cluster; crash fault only; one captured run; no network partition or Byzantine fault; not formal verification, availability/SLO evidence, or a throughput benchmark.\n"""
        (bundle / "README.md").write_text(readme)
        sums = []
        for path in sorted(p for p in bundle.rglob("*") if p.is_file() and p.name != "SHA256SUMS"):
            sums.append("%s  %s" % (sha256_file(path), path.relative_to(bundle)))
        (bundle / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    finally:
        if load and load.poll() is None:
            (temp / "stop").write_text("stop\n")
            try: load.wait(timeout=5)
            except subprocess.TimeoutExpired: load.kill()
        for item in processes.values():
            proc = item["proc"]
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait()
            item["log"].close()
        shutil.rmtree(temp, ignore_errors=True)


def main():
    if len(sys.argv) != 2:
        print("usage: capture.py OUTPUT_DIR", file=sys.stderr)
        return 2
    try:
        capture(sys.argv[1])
    except Exception as exc:
        print("capture failed: %s" % exc, file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
