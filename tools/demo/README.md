# M25 Evidence Tools

`capture.sh` runs the canonical real-process leader-failover-under-load capture. It builds the current source revision, starts three disk-backed nodes, runs 16 real Go clients with the M24 70% PUT / 30% GET workload, SIGKILLs the observed leader, restarts it from the same data directory, verifies acknowledged writes, sanitizes bounded logs, validates the bundle, and writes checksums.

```text
./tools/demo/capture.sh docs/evidence/canonical
python3 tools/demo/validate.py docs/evidence/canonical
python3 tools/demo/summarize.py docs/evidence/canonical
```

The capture uses only loopback TCP and standard-library Go/Python tooling. It tracks exact child PIDs and removes only its own temporary state. It does not commit changes, include binaries or data directories, or claim failover latency, availability, or throughput.
