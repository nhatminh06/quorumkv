#!/usr/bin/env python3
import hashlib
import json
import re
import sys
from pathlib import Path

REQUIRED = ["README.md", "manifest.json", "timeline.json", "workload-summary.json", "initial-status.json", "failover-status.json", "final-status.json", "acknowledged-writes-summary.json", "SHA256SUMS"]


def fail(message):
    raise SystemExit("validation failed: " + message)


def load(path):
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        fail("invalid JSON %s: %s" % (path.name, exc))


def main():
    if len(sys.argv) != 2:
        fail("usage: validate.py BUNDLE")
    root = Path(sys.argv[1]).resolve()
    if not root.is_dir():
        fail("bundle does not exist")
    for name in REQUIRED:
        if not (root / name).is_file():
            fail("missing " + name)
    manifest = load(root / "manifest.json")
    workload = load(root / "workload-summary.json")
    initial = load(root / "initial-status.json")
    failover = load(root / "failover-status.json")
    final = load(root / "final-status.json")
    ack = load(root / "acknowledged-writes-summary.json")
    timeline = load(root / "timeline.json")
    if manifest.get("schema_version") != 1:
        fail("unsupported schema")
    if manifest.get("scenario") != "leader_failover_under_load":
        fail("wrong scenario")
    if manifest.get("cluster") != {"nodes": 3, "transport": "tcp", "persistence": "disk"}:
        fail("wrong cluster description")
    if manifest.get("workload", {}).get("workers") != 16 or manifest.get("workload", {}).get("put_percent") != 70 or manifest.get("workload", {}).get("get_percent") != 30:
        fail("wrong workload shape")
    failure = manifest.get("failure", {})
    recovery = manifest.get("recovery", {})
    if failure.get("method") != "SIGKILL":
        fail("failure method is not SIGKILL")
    crashed = failure.get("leader_id")
    replacement = recovery.get("replacement_leader_id")
    if crashed == replacement or not isinstance(crashed, int) or not isinstance(replacement, int):
        fail("replacement leader is not distinct")
    initial_leader = next((int(k) for k, v in initial.items() if v.get("role") == "leader"), None)
    if initial_leader != crashed:
        fail("manifest crashed node was not initial leader")
    initial_term = initial[str(crashed)].get("term")
    replacement_status = failover[str(replacement)]
    if replacement_status.get("role") != "leader" or replacement_status.get("term", -1) < initial_term:
        fail("invalid replacement status")
    if failover[str(crashed)].get("reachable") is not False:
        fail("crashed node is not recorded down")
    if workload.get("stale_requests", 0) != 0 or workload.get("request_conflicts", 0) != 0 or workload.get("unexpected_protocol_errors", 0) != 0:
        fail("unexpected protocol errors present")
    if workload.get("acknowledged_before", 0) <= 0 or workload.get("acknowledged_after", 0) <= 0:
        fail("missing before/after acknowledged writes")
    if ack.get("total_acknowledged") != ack.get("verified_acknowledged") or ack.get("verification_missing") != 0:
        fail("acknowledged-write verification mismatch")
    if manifest.get("result", {}).get("verified_acknowledged_writes") != ack.get("verified_acknowledged"):
        fail("manifest and acknowledgment summary disagree")
    final_leaders = [item for item in final.values() if item.get("reachable") and item.get("role") == "leader"]
    if len(final_leaders) != 1:
        fail("final cluster does not have exactly one leader")
    for node_id, status in final.items():
        if not status.get("reachable") or status.get("membership") != "stable" or status.get("apply_lag") != 0 or len(status.get("voters", [])) != 3:
            fail("final node %s is not fully converged" % node_id)
    restarted = str(recovery.get("restarted_node_id"))
    if not final.get(restarted, {}).get("reachable"):
        fail("restarted node is unreachable")
    if recovery.get("same_data_directory") is not True:
        fail("same-disk restart not recorded")
    event_names = [event.get("event") for event in timeline]
    for required in ["cluster_ready", "load_started", "leader_crashed", "replacement_leader", "post_failover_progress", "old_leader_restarted", "old_leader_caught_up", "acknowledged_writes_verified"]:
        if required not in event_names:
            fail("timeline missing " + required)
    forbidden = re.compile(r"/(?:home|tmp|var|Users)/|[A-Za-z]:\\|hostname|ssh|MAC", re.IGNORECASE)
    for path in root.rglob("*"):
        if path.is_file() and path.name != "SHA256SUMS" and forbidden.search(path.read_text(errors="replace")):
            fail("forbidden path or host data in " + str(path.relative_to(root)))
    for line in (root / "SHA256SUMS").read_text().splitlines():
        digest, relative = line.split("  ", 1)
        target = root / relative
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            fail("checksum mismatch: " + relative)
    print("canonical evidence valid: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
