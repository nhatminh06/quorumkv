# Canonical failover evidence

This sanitized bundle records one real three-process QuorumKV leader-failover-under-load capture. It uses real TCP, disk-backed state, 16 concurrent workers, and a deterministic 70% PUT / 30% GET workload. The current leader was killed with SIGKILL while clients remained active; a replacement leader acknowledged additional writes; the old leader restarted from the same disk directory and caught up; every definitely acknowledged write was verified.

Ambiguous write outcomes are excluded from the acknowledgment set. The workload retires that in-memory client session before issuing another logical write. The bundle records those outcomes and retired sessions separately. No STALE_REQUEST, REQUEST_CONFLICT, or unexpected protocol error is accepted.

M24 previously exposed an incorrect stale response when a replacement leader inspected dedup state before applying its committed prefix. The fix is `f3ff0bfd41c8fb0d2bca3864af96f8dd85b130bd`; this bundle is captured after that fix.

Boundaries: local loopback cluster; crash fault only; one captured run; no network partition or Byzantine fault; not formal verification, availability/SLO evidence, or a throughput benchmark.
