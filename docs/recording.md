# QuorumKV portfolio recording guide

This guide produces a silent 3:30–3:40 walkthrough of the deployed canonical
failure-under-load report. It presents committed evidence; it does not rerun or
recapture the workload.

## Setup

- Record at 1920×1080 with browser zoom at 90–100%.
- Use a terminal font of at least 18 px if the optional terminal scene is used.
- Hide notifications, bookmarks, unrelated tabs, and personal paths.
- Confirm that no tokens, credentials, hostnames, or private URLs are visible.
- Preload the [showcase](https://nhatminh06.github.io/quorumkv/) and verify its
  JSON requests succeed before recording.
- Use deliberate cuts or deterministic scrolling; do not wait for slow commands.
- Keep the recording silent. Use the short captions below; do not add generated
  narration, fake terminal typing, music, or a cinematic intro/outro.

## Shot list

### 0:00–0:20 — Introduction

Open the showcase at the hero.

Caption: **Consensus and failure recovery exercised through 3 real processes,
real TCP, and disk-backed state.**

Hold on the canonical-capture label, 3 nodes, 16 clients, `SIGKILL leader`, and
the 6,865 / 6,865 summary.

### 0:20–0:50 — Initial cluster

Scroll to Checkpoints and select **Before failure**. Show node 1 as leader,
nodes 2 and 3 as followers, and term 1.

Caption: **16 clients continue issuing 70% PUT / 30% GET traffic.**

### 0:50–1:20 — Leader crash

Scroll to the timeline and select `leader_crashed`. Show node 1 and `SIGKILL`.

Caption: **The actual leader process is killed with SIGKILL while clients
remain active.**

Do not describe this as graceful failover.

### 1:20–1:45 — Replacement election

Select `replacement_leader`. Show node 2 as leader in term 2. The canonical
timeline records the replacement leader, not an observed intermediate
candidate checkpoint, so do not invent or animate one.

Caption: **The surviving quorum elects node 2 as replacement leader in term 2.
The observed timing is not an SLA.**

### 1:45–2:10 — Continued progress

Show the before/after split: 3,621 ACKs before the crash and 3,244 after
failover.

Caption: **The cluster continues committing new writes after leadership
changes: 3,621 before, 3,244 after.**

Do not convert these counts into throughput.

### 2:10–2:35 — Recovery

Show node 1 restarting from `cluster-data/node1`, `same data directory: Yes`,
then its final follower state at term 2, commit/applied 6867, apply lag 0.

Caption: **The failed process restarts from the same persisted data directory
and catches up through normal replication.**

Do not say the node was rebuilt from another node.

### 2:35–2:55 — Correctness result

Show 6,865 acknowledged, 6,865 verified, and 0 missing.

Caption: **Every definitely acknowledged write remained observable: 6,865
verified, 0 missing.**

Avoid the unqualified phrase “zero data loss.”

### 2:55–3:15 — Bug found by stress testing

Show the M24 causal chain and the commit/apply barrier plus atomic pending
reservation.

Caption: **Failure testing exposed and fixed a real protocol bug: a stale dedup
lookup on a replacement leader.**

### 3:15–3:30 — Boundaries and provenance

Show the evidence boundary and provenance sections.

Captions:

- **One crash-fault capture—not formal verification, a production SLO, or a
  throughput benchmark.**
- **Canonical evidence is committed, traceable to source, and SHA-256 verified.**

End on the evidence and repository links. Do not add an animated outro.

## Optional terminal insert

Only if it improves clarity, show this command against the committed bundle:

```bash
python3 tools/demo/summarize.py docs/evidence/canonical
```

Never run `tools/demo/capture.sh` in the portfolio recording. The canonical
evidence is already captured and frozen.

## Review checklist

Watch the complete export before publishing and verify:

- duration is no more than 4:00;
- every canonical number is legible;
- there is no audio stream, secret, hostname, or personal path;
- the leader failure is identified as `SIGKILL`;
- observed timing is not presented as an SLA;
- acknowledged-write preservation is precisely qualified;
- no Byzantine-tolerance or formal-verification claim is made;
- historical performance is not attributed to the M25 capture;
- the public video, showcase, and evidence URLs resolve successfully.

Published recording: [QuorumKV portfolio demo](https://github.com/nhatminh06/quorumkv/releases/download/portfolio-v1/quorumkv-demo.mp4)
