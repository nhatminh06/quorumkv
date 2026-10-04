"use strict";

const evidenceFiles = [
  "manifest.json",
  "timeline.json",
  "workload-summary.json",
  "acknowledged-writes-summary.json",
  "initial-status.json",
  "failover-status.json",
  "final-status.json",
];

const $ = (selector) => document.querySelector(selector);
const number = (value) => new Intl.NumberFormat("en-US").format(value);

const eventMeta = {
  cluster_ready: { label: "Cluster ready", marker: "○", kind: "normal" },
  load_started: { label: "Load started", marker: "○", kind: "normal" },
  leader_crashed: { label: "Leader killed", marker: "×", kind: "failure" },
  replacement_leader: { label: "Replacement leader", marker: "●", kind: "election" },
  post_failover_progress: { label: "Progress resumed", marker: "○", kind: "normal" },
  old_leader_restarted: { label: "Node restarted", marker: "○", kind: "normal" },
  old_leader_caught_up: { label: "Node caught up", marker: "○", kind: "normal" },
  acknowledged_writes_verified: { label: "Writes verified", marker: "✓", kind: "verification" },
};

function setText(selector, value) {
  $(selector).textContent = value;
}

async function loadEvidence() {
  const responses = await Promise.all(evidenceFiles.map((file) => fetch(`data/${file}`)));
  const failure = responses.findIndex((response) => !response.ok);
  if (failure !== -1) throw new Error(`${evidenceFiles[failure]} returned ${responses[failure].status}`);
  const [manifest, timeline, workload, acknowledgements, initial, failover, final] = await Promise.all(responses.map((response) => response.json()));
  if (manifest.schema_version !== 1) throw new Error(`unsupported schema version ${manifest.schema_version}`);
  return { manifest, timeline, workload, acknowledgements, checkpoints: { initial, failover, final } };
}

function eventDescription(event) {
  switch (event.event) {
    case "cluster_ready": return `Node ${event.leader} is leader in term ${event.term}.`;
    case "load_started": return `${event.workers} workers begin ${event.put_percent}% PUT / ${event.get_percent}% GET traffic.`;
    case "leader_crashed": return `Node ${event.node}, the current leader, receives ${event.signal}.`;
    case "replacement_leader": return `Node ${event.node} is observed as replacement leader in term ${event.term}.`;
    case "post_failover_progress": return "The surviving quorum continues committing client writes.";
    case "old_leader_restarted": return `Node ${event.node} restarts from ${event.data_directory}; same data directory: ${event.same_data_directory ? "yes" : "no"}.`;
    case "old_leader_caught_up": return `Node ${event.node} is observed caught up through normal replication.`;
    case "acknowledged_writes_verified": return `${number(event.count)} definitely acknowledged writes remain readable after recovery.`;
    default: return "Recorded canonical event.";
  }
}

function formatTime(seconds) {
  return seconds.toFixed(3).padStart(6, "0");
}

function renderNode(id, node, mode) {
  const element = $(`#node-${id}`);
  const isDown = !node || node.reachable === false;
  const isRestartedPending = mode === "restart-pending" && id === "1";
  const isRecovered = mode === "final" && id === "1";
  let role = isDown ? "DOWN" : String(node.role).toUpperCase();
  if (isRestartedPending) role = "RESTARTED";
  element.className = `topology-node node-${["zero", "one", "two", "three"][Number(id)]} ${isDown ? "down" : node?.role === "leader" ? "leader" : isRecovered ? "recovered" : "follower"}`;
  const values = isDown || isRestartedPending
    ? [["TERM", isRestartedPending ? "—" : node?.term ?? "—"], ["COMMIT", "—"], ["APPLIED", "—"]]
    : [["TERM", node.term], ["COMMIT", number(node.commit_index)], ["APPLIED", number(node.last_applied)]];
  const note = isRecovered ? "RESTARTED / CAUGHT UP" : isRestartedPending ? "STATE NOT RECORDED AT THIS INSTANT" : isDown ? "DISCONNECTED / SIGKILL" : "";
  element.innerHTML = `<header><span class="node-code">NODE 0${id}</span><span class="node-status"><i class="status-indicator" aria-hidden="true"></i>${role}</span></header><strong class="node-role">${role}</strong><dl class="node-values">${values.map(([label, value]) => `<div><dt>${label}</dt><dd>${value}</dd></div>`).join("")}</dl>${note ? `<span class="node-note">${note}</span>` : ""}`;
}

function topologyText(states, mode) {
  return ["1", "2", "3"].map((id) => {
    const node = states[id];
    if (mode === "restart-pending" && id === "1") return "Node 1 restarted; its status at that instant was not captured.";
    if (!node || node.reachable === false) return `Node ${id} is down and unreachable.`;
    return `Node ${id} is ${node.role}, term ${node.term}, commit ${node.commit_index}, applied ${node.last_applied}.`;
  }).join(" ");
}

function renderTopology(states, label, mode = "checkpoint") {
  ["1", "2", "3"].forEach((id) => renderNode(id, states[id], mode));
  setText("#checkpoint-label", label);
  setText("#topology-description", topologyText(states, mode));
  $("#failure-stamp").hidden = mode !== "crash";
  $("#active-links").style.stroke = mode === "crash" ? "var(--red)" : "var(--blue)";
}

function eventState(data, index) {
  if (index <= 1) return { states: data.checkpoints.initial, label: "CHECKPOINT / BEFORE FAILURE", mode: "initial", checkpoint: "initial" };
  if (index === 2) {
    const states = structuredClone(data.checkpoints.initial);
    states["1"] = { reachable: false };
    return { states, label: "EVENT STATE / LEADER KILLED", mode: "crash", checkpoint: null };
  }
  if (index <= 4) return { states: data.checkpoints.failover, label: "CHECKPOINT / DURING FAILOVER", mode: "failover", checkpoint: "failover" };
  if (index === 5) {
    const states = structuredClone(data.checkpoints.failover);
    states["1"] = { reachable: true, role: "restarted" };
    return { states, label: "EVENT STATE / RESTART OBSERVED", mode: "restart-pending", checkpoint: null };
  }
  return { states: data.checkpoints.final, label: "CHECKPOINT / FINAL CONVERGENCE", mode: "final", checkpoint: "final" };
}

function setCheckpointSelection(name) {
  document.querySelectorAll("[role=tab]").forEach((tab) => {
    const selected = tab.dataset.checkpoint === name;
    tab.setAttribute("aria-selected", selected ? "true" : "false");
    tab.tabIndex = selected ? 0 : -1;
  });
}

function setupIncidentControls(data) {
  const list = $("#event-list");
  list.innerHTML = data.timeline.map((event, index) => {
    const meta = eventMeta[event.event];
    return `<li><button class="event-control ${meta.kind}" data-event-index="${index}" aria-selected="false"><span class="event-time">${formatTime(event.at_seconds)}</span><span class="event-marker" aria-hidden="true">${meta.marker}</span><span class="event-name">${meta.label}</span></button></li>`;
  }).join("");
  const controls = [...document.querySelectorAll(".event-control")];

  const selectEvent = (index, focus = false) => {
    controls.forEach((control, item) => {
      const selected = item === index;
      control.setAttribute("aria-selected", selected ? "true" : "false");
      control.tabIndex = selected ? 0 : -1;
    });
    const event = data.timeline[index];
    const meta = eventMeta[event.event];
    const state = eventState(data, index);
    renderTopology(state.states, state.label, state.mode);
    setCheckpointSelection(state.checkpoint);
    setText("#selected-sequence", `EVENT ${String(index + 1).padStart(2, "0")} / ${String(data.timeline.length).padStart(2, "0")}`);
    setText("#selected-event", meta.label);
    setText("#selected-time", `T + ${event.at_seconds.toFixed(3)} S / OBSERVED IN THIS CAPTURE`);
    setText("#selected-detail", eventDescription(event));
    if (focus) controls[index].focus();
  };

  controls.forEach((control, index) => {
    control.addEventListener("click", () => selectEvent(index));
    control.addEventListener("keydown", (event) => {
      if (!["ArrowDown", "ArrowUp", "ArrowRight", "ArrowLeft", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      let next = index + (["ArrowDown", "ArrowRight"].includes(event.key) ? 1 : -1);
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = controls.length - 1;
      next = Math.max(0, Math.min(controls.length - 1, next));
      selectEvent(next, true);
    });
  });

  const tabs = [...document.querySelectorAll("[role=tab]")];
  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => {
      setCheckpointSelection(tab.dataset.checkpoint);
      renderTopology(data.checkpoints[tab.dataset.checkpoint], `CHECKPOINT / ${tab.querySelector("strong").textContent.toUpperCase()}`, tab.dataset.checkpoint);
    });
    tab.addEventListener("keydown", (event) => {
      if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      let next = index + (event.key === "ArrowRight" ? 1 : -1);
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = tabs.length - 1;
      next = Math.max(0, Math.min(tabs.length - 1, next));
      tabs[next].click();
      tabs[next].focus();
    });
  });

  selectEvent(0);
}

function renderStaticEvidence(data) {
  const { manifest, acknowledgements, workload } = data;
  setText("#header-spec", `${manifest.cluster.nodes} NODES · ${manifest.workload.workers} CLIENTS · RAFT · ${manifest.failure.method}`);
  setText("#workload-workers", `${manifest.workload.workers} WORKERS`);
  setText("#workload-mix", `${manifest.workload.put_percent}% PUT · ${manifest.workload.get_percent}% GET`);
  setText("#ack-total", number(acknowledgements.total_acknowledged));
  setText("#ack-verified", number(acknowledgements.verified_acknowledged));
  setText("#ack-missing", number(acknowledgements.verification_missing));
  setText("#ack-before", number(acknowledgements.acknowledged_before_crash));
  setText("#ack-after", number(acknowledgements.acknowledged_after_failover));

  const protocol = [
    ["STALE_REQUEST", workload.stale_requests],
    ["REQUEST_CONFLICT", workload.request_conflicts],
    ["UNEXPECTED", workload.unexpected_protocol_errors],
    ["AMBIGUOUS", workload.ambiguous_write_outcomes],
  ];
  $("#protocol-readout").innerHTML = protocol.map(([label, value]) => `<div><dt>${label}</dt><dd>${number(value)}</dd></div>`).join("");

  const shortCommit = `${manifest.quorumkv_commit.slice(0, 8)}…`;
  setText("#capture-source", shortCommit);
  $("#capture-link").href = `https://github.com/nhatminh06/quorumkv/commit/${manifest.quorumkv_commit}`;
  setText("#tool-schema", manifest.capture_tool_version);
  setText("#platform", manifest.platform);
}

loadEvidence()
  .then((data) => {
    renderStaticEvidence(data);
    setupIncidentControls(data);
    if (window.matchMedia("(max-width: 720px)").matches) {
      const scroller = $(".topology-scroll");
      scroller.scrollLeft = (scroller.scrollWidth - scroller.clientWidth) / 2;
    }
  })
  .catch((error) => {
    console.error("Evidence unavailable", error);
    $("#load-error").hidden = false;
    $("#control-board").setAttribute("aria-disabled", "true");
  });
