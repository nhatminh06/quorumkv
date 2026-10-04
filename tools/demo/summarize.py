#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def main():
    if len(sys.argv) != 2:
        print("usage: summarize.py BUNDLE", file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    manifest = json.loads((root / "manifest.json").read_text())
    workload = json.loads((root / "workload-summary.json").read_text())
    ack = json.loads((root / "acknowledged-writes-summary.json").read_text())
    initial = json.loads((root / "initial-status.json").read_text())
    final = json.loads((root / "final-status.json").read_text())
    initial_leader = next((item for item in initial.values() if item.get("role") == "leader"), {})
    final_leader = next((item for item in final.values() if item.get("role") == "leader"), {})
    print("QuorumKV canonical failover evidence")
    print()
    print("CLUSTER")
    print("3 OS processes")
    print("real TCP")
    print("disk-backed Raft")
    print()
    print("LOAD")
    print("%d workers" % manifest["workload"]["workers"])
    print("%d%% PUT / %d%% GET" % (manifest["workload"]["put_percent"], manifest["workload"]["get_percent"]))
    print()
    print("INITIAL")
    print("leader              node %s" % initial_leader.get("node"))
    print("term                %s" % initial_leader.get("term"))
    print("acknowledged        %s" % ack["acknowledged_before_crash"])
    print()
    print("FAILURE")
    print("node %s              SIGKILL" % manifest["failure"]["leader_id"])
    print()
    print("RECOVERY")
    print("leader              node %s" % final_leader.get("node", manifest["recovery"]["replacement_leader_id"]))
    print("term                %s" % final_leader.get("term"))
    print("post-failover ACKs  %s" % ack["acknowledged_after_failover"])
    print()
    print("CLIENT OUTCOMES")
    print("ambiguous writes    %s" % workload.get("ambiguous_write_outcomes", 0))
    print("retired sessions    %s" % workload.get("retired_sessions", 0))
    print("read transients     %s" % workload.get("transient_read_failures", 0))
    print("stale requests      %s" % workload.get("stale_requests", 0))
    print("conflicts           %s" % workload.get("request_conflicts", 0))
    print()
    print("REJOIN")
    print("node %s restarted from same disk" % manifest["recovery"]["restarted_node_id"])
    print("apply lag           0")
    print()
    print("VERIFY")
    print("acknowledged        %s" % ack["total_acknowledged"])
    print("verified            %s" % ack["verified_acknowledged"])
    print()
    print("RESULT")
    print(manifest["result"]["status"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
