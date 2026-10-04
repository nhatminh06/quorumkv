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
const formatNumber = (value) => new Intl.NumberFormat("en-US").format(value);
const eventLabels = {
  cluster_ready: "Cluster ready",
  load_started: "Load started",
  leader_crashed: "Leader crashed",
  replacement_leader: "Replacement leader",
  post_failover_progress: "Post-failover progress",
  old_leader_restarted: "Old leader restarted",
  old_leader_caught_up: "Old leader caught up",
  acknowledged_writes_verified: "ACKed writes verified",
};

async function loadEvidence() {
  const responses = await Promise.all(evidenceFiles.map((file) => fetch(`data/${file}`)));
  const failed = responses.findIndex((response) => !response.ok);
  if (failed !== -1) throw new Error(`${evidenceFiles[failed]} returned ${responses[failed].status}`);
  const [manifest, timeline, workload, acknowledgements, initial, failover, final] = await Promise.all(responses.map((response) => response.json()));
  if (manifest.schema_version !== 1) throw new Error(`unsupported schema version ${manifest.schema_version}`);
  return { manifest, timeline, workload, acknowledgements, checkpoints: { initial, failover, final } };
}

function setText(selector, value) { $(selector).textContent = value; }

function renderSummary({ manifest, acknowledgements }) {
  setText("#hero-nodes", `${manifest.cluster.nodes} nodes`);
  setText("#hero-clients", `${manifest.workload.workers} clients`);
  setText("#hero-failure", `${manifest.failure.method} leader`);
  setText("#hero-verified", `${formatNumber(acknowledgements.verified_acknowledged)} / ${formatNumber(acknowledgements.total_acknowledged)}`);
  setText("#ack-total", formatNumber(acknowledgements.total_acknowledged));
  setText("#ack-verified", formatNumber(acknowledgements.verified_acknowledged));
  setText("#ack-missing", formatNumber(acknowledgements.verification_missing));
  setText("#ack-before", formatNumber(acknowledgements.acknowledged_before_crash));
  setText("#ack-after", formatNumber(acknowledgements.acknowledged_after_failover));
}

function eventDetail(event) {
  switch (event.event) {
    case "cluster_ready": return `Node ${event.leader} is leader in term ${event.term}.`;
    case "load_started": return `${event.workers} workers begin a ${event.put_percent}% PUT / ${event.get_percent}% GET workload.`;
    case "leader_crashed": return `Node ${event.node}, the active leader, receives ${event.signal}.`;
    case "replacement_leader": return `Node ${event.node} is observed as leader in term ${event.term}.`;
    case "post_failover_progress": return "Clients continue receiving definite acknowledgments after leadership changes.";
    case "old_leader_restarted": return `Node ${event.node} restarts from ${event.data_directory}; same data directory: ${event.same_data_directory ? "yes" : "no"}.`;
    case "old_leader_caught_up": return `Node ${event.node} is observed caught up through ordinary replication.`;
    case "acknowledged_writes_verified": return `${formatNumber(event.count)} definitely acknowledged writes are readable after recovery.`;
    default: return "Recorded canonical event.";
  }
}

function svgElement(name, attributes = {}) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, value));
  return element;
}

function renderTimeline({ timeline }) {
  const svg = $("#cluster-timeline");
  const left = 155, right = 1050, width = right - left, maxTime = Math.max(...timeline.map((event) => event.at_seconds));
  const x = (seconds) => left + (seconds / maxTime) * width;
  const crash = timeline.find((event) => event.event === "leader_crashed");
  const replacement = timeline.find((event) => event.event === "replacement_leader");
  const restart = timeline.find((event) => event.event === "old_leader_restarted");
  const rows = [{ label: "NODE 1", sub: "leader → down → follower", y: 72 }, { label: "NODE 2", sub: "follower → leader", y: 142 }, { label: "NODE 3", sub: "follower", y: 212 }, { label: "CLIENTS", sub: "16-worker workload", y: 282 }];
  rows.forEach((row) => {
    const label = svgElement("text", { x: 22, y: row.y - 4, class: "track-label" }); label.textContent = row.label; svg.append(label);
    const sub = svgElement("text", { x: 22, y: row.y + 14, class: "track-sub" }); sub.textContent = row.sub; svg.append(sub);
  });
  svg.append(svgElement("line", { x1: left, y1: 72, x2: x(crash.at_seconds), y2: 72, class: "track-leader" }));
  svg.append(svgElement("line", { x1: x(crash.at_seconds), y1: 72, x2: x(restart.at_seconds), y2: 72, class: "track-down" }));
  svg.append(svgElement("line", { x1: x(restart.at_seconds), y1: 72, x2: right, y2: 72, class: "track-line" }));
  svg.append(svgElement("line", { x1: left, y1: 142, x2: x(replacement.at_seconds), y2: 142, class: "track-line" }));
  svg.append(svgElement("line", { x1: x(replacement.at_seconds), y1: 142, x2: right, y2: 142, class: "track-leader" }));
  svg.append(svgElement("line", { x1: left, y1: 212, x2: right, y2: 212, class: "track-line" }));
  svg.append(svgElement("line", { x1: left, y1: 282, x2: right, y2: 282, class: "track-client" }));

  const markerY = [72, 298, 72, 142, 298, 72, 72, 298];
  timeline.forEach((event, index) => {
    const group = svgElement("g", { class: `event-button ${event.event === "leader_crashed" ? "crash" : event.event.includes("restart") || event.event.includes("caught_up") ? "recovery" : ""}`, role: "button", tabindex: index === 0 ? "0" : "-1", "aria-label": `${eventLabels[event.event]}, ${event.at_seconds.toFixed(3)} seconds` });
    group.dataset.index = index;
    group.append(svgElement("circle", { cx: x(event.at_seconds), cy: markerY[index], r: 8 }));
    const text = svgElement("text", { x: x(event.at_seconds), y: markerY[index] + (markerY[index] > 250 ? 27 : -17), "text-anchor": index < 2 ? "start" : index === timeline.length - 1 ? "end" : "middle" });
    text.textContent = event.event.replaceAll("_", " "); group.append(text); svg.append(group);
  });
  const controls = [...svg.querySelectorAll(".event-button")];
  const select = (index) => {
    controls.forEach((control, item) => { control.setAttribute("aria-current", item === index ? "true" : "false"); control.setAttribute("tabindex", item === index ? "0" : "-1"); });
    const event = timeline[index];
    setText("#event-index", `Event ${String(index + 1).padStart(2, "0")} / ${String(timeline.length).padStart(2, "0")}`);
    setText("#event-name", eventLabels[event.event]);
    setText("#event-time", `T + ${event.at_seconds.toFixed(3)} s · observed in this capture`);
    setText("#event-detail", eventDetail(event));
  };
  controls.forEach((control, index) => {
    control.addEventListener("click", () => select(index));
    control.addEventListener("keydown", (event) => {
      if (!["ArrowRight", "ArrowLeft", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      let target = index + (event.key === "ArrowRight" ? 1 : -1);
      if (event.key === "Home") target = 0;
      if (event.key === "End") target = controls.length - 1;
      target = Math.max(0, Math.min(controls.length - 1, target)); select(target); controls[target].focus();
    });
  });
  $("#timeline-text").innerHTML = timeline.map((event) => `<li>${event.at_seconds.toFixed(3)} seconds: ${eventLabels[event.event]}. ${eventDetail(event)}</li>`).join("");
  select(0);
}

function renderCheckpoint(name, data) {
  const ids = ["1", "2", "3"];
  $("#checkpoint-nodes").innerHTML = ids.map((id) => {
    const node = data[id];
    const role = node?.reachable === false || !node ? "down" : node.role;
    const meta = node?.reachable === false || !node ? "Process unreachable" : `Term ${node.term} · leader ${node.leader}`;
    return `<article class="node-card ${role}"><span class="node-id">NODE ${id}</span><strong class="role">${role.toUpperCase()}</strong><span class="meta">${meta}</span></article>`;
  }).join("");
  const reachable = ids.map((id) => data[id]).filter((node) => node?.reachable !== false);
  const leader = reachable.find((node) => node.role === "leader") || reachable[0];
  const details = name === "final" ? [["Term", leader.term], ["Commit", leader.commit_index], ["Applied", leader.last_applied], ["Apply lag", leader.apply_lag], ["Membership", `${leader.voters.length} voters`]] : [["Term", leader.term], ["Leader", `Node ${leader.leader}`]];
  $("#checkpoint-details").innerHTML = details.map(([term, value]) => `<div><dt>${term}</dt><dd>${value}</dd></div>`).join("");
}

function setupCheckpointTabs(data) {
  const tabs = [...document.querySelectorAll("[role=tab]")];
  const activate = (tab) => {
    tabs.forEach((item) => { const selected = item === tab; item.setAttribute("aria-selected", selected); item.tabIndex = selected ? 0 : -1; });
    $("#checkpoint-panel").setAttribute("aria-labelledby", tab.id); renderCheckpoint(tab.dataset.checkpoint, data.checkpoints[tab.dataset.checkpoint]);
  };
  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => activate(tab));
    tab.addEventListener("keydown", (event) => { if (!event.key.startsWith("Arrow")) return; event.preventDefault(); const next = (index + (event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : tabs.length - 1)) % tabs.length; activate(tabs[next]); tabs[next].focus(); });
  });
  activate(tabs[0]);
}

function renderProtocol({ workload }) {
  const counts = [["STALE_REQUEST", workload.stale_requests], ["REQUEST_CONFLICT", workload.request_conflicts], ["Unexpected errors", workload.unexpected_protocol_errors], ["Ambiguous writes", workload.ambiguous_write_outcomes], ["Retired sessions", workload.retired_sessions], ["Transient reads", workload.transient_read_failures]];
  $("#protocol-counts").innerHTML = counts.map(([label, value]) => `<div><dt>${label}</dt><dd>${formatNumber(value)}</dd></div>`).join("");
}

function renderRecovery({ manifest, timeline, checkpoints }) {
  const restart = timeline.find((event) => event.event === "old_leader_restarted");
  const node = checkpoints.final[String(manifest.recovery.restarted_node_id)];
  setText("#recovery-path", restart.data_directory); setText("#same-directory", manifest.recovery.same_data_directory ? "Yes" : "No");
  setText("#recovery-role", node.role.toUpperCase()); setText("#recovery-term", node.term); setText("#recovery-commit", formatNumber(node.commit_index)); setText("#recovery-applied", formatNumber(node.last_applied)); setText("#recovery-lag", node.apply_lag);
}

function renderProvenance({ manifest }) {
  const shortCommit = `${manifest.quorumkv_commit.slice(0, 8)}…`;
  setText("#capture-source", shortCommit); $("#capture-link").href = `https://github.com/nhatminh06/quorumkv/commit/${manifest.quorumkv_commit}`;
  setText("#tool-schema", manifest.capture_tool_version); setText("#platform", manifest.platform);
}

loadEvidence().then((data) => { renderSummary(data); renderTimeline(data); setupCheckpointTabs(data); renderProtocol(data); renderRecovery(data); renderProvenance(data); }).catch((error) => { console.error("Evidence unavailable", error); $("#load-error").hidden = false; });
