import { readControlPlane } from "@/lib/control-plane";
import { ActiveTasks } from "./ActiveTasks";
import { IncidentTimeline } from "./IncidentTimeline";
import { QualificationPanel } from "./QualificationPanel";
import { RuntimeOverview } from "./RuntimeOverview";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function ControlPlanePage() {
  const view = await readControlPlane();
  if (view.status === "unavailable") {
    return <main style={{ padding: 24 }}><h1>M1 control-plane runtime</h1><p style={{ color: "#fecaca" }}>Unavailable: {view.reason}. This is not a healthy or empty state.</p></main>;
  }
  const evidence = view.soak_evidence;
  const evidenceAge = evidence ? Math.max(0, Math.floor((Date.now() - Date.parse(evidence.latest_observed_at)) / 1000)) : null;
  const usage = view.cloud_usage;
  return <main style={{ padding: 24, maxWidth: 1200 }}>
    <h1>M1 control-plane runtime</h1>
    <RuntimeOverview
      controller={view.runtime_controller}
      quotePointer={view.quote.current_pointer}
      structureManifest={view.structure.latest_manifest}
      jobCounts={view.job_counts}
    />
    <ActiveTasks tasks={view.active_tasks.items} total={view.active_tasks.total} />
    <IncidentTimeline
      incidents={view.runtime_incidents.items}
      recoveryActions={view.recovery_actions.items}
    />
    <QualificationPanel qualification={view.qualification} />
    <section style={{ padding: 16, border: `1px solid ${usage.threshold_percent >= 90 ? "#ef4444" : usage.threshold_percent >= 75 ? "#f59e0b" : "#4b5563"}`, background: usage.threshold_percent >= 90 ? "#341414" : "#111", borderRadius: 8, marginBottom: 16 }}>
      <h2 style={{ marginTop: 0 }}>Cloud egress budget</h2>
      <p><strong>{usage.used_bytes.toLocaleString()} bytes</strong> / {usage.daily_budget_bytes?.toLocaleString() ?? "not observed"} · {usage.threshold_percent}% · UTC {usage.budget_day}</p>
      {usage.latest_observation ? <p style={{ color: "#aaa" }}>Latest: {usage.latest_observation.source} / {usage.latest_observation.operation} · {usage.latest_observation.bytes_received.toLocaleString()} bytes · {usage.latest_observation.observed_at}</p> : <p style={{ color: "#fecaca" }}>No metered cloud input is present for this UTC day.</p>}
    </section>
    <section style={{ padding: 16, border: `1px solid ${evidence ? "#4b5563" : "#ef4444"}`, background: evidence ? "#111" : "#341414", borderRadius: 8, marginBottom: 16 }}>
      <h2 style={{ marginTop: 0 }}>Immutable cloud evidence</h2>
      {evidence ? <><p>Latest sample: {evidence.latest_observed_at} ({evidenceAge}s ago)</p><p style={{ color: "#aaa" }}>Run: {evidence.latest_run_id}</p></> : <p style={{ color: "#fecaca" }}>No cloud evidence sample is present. This is an operational failure, not a healthy empty state.</p>}
    </section>
    <h2>Runtime incident and recovery ledger</h2>
    {view.runtime_watchdog.recent_events.length === 0 ? <p>No watchdog transitions recorded yet.</p> : <ol>{view.runtime_watchdog.recent_events.map((event, index) => <li key={`${event.incident_key}-${event.occurred_at}-${index}`} style={{ marginBottom: 14, padding: 12, borderLeft: `4px solid ${event.kind === "detected" ? "#ef4444" : "#22c55e"}`, background: "#111" }}><strong style={{ color: event.kind === "detected" ? "#fecaca" : "#bbf7d0" }}>{event.kind === "detected" ? "Detected — open until a recovery event" : "Recovered"}</strong> · {event.occurred_at}<br /><strong>{event.summary}</strong><br /><span style={{ color: "#aaa" }}>Severity: {event.severity} · Observed by: {event.detail.source ?? "legacy-runtime-watchdog"} · Incident: {event.incident_key}</span><br /><span style={{ color: "#aaa" }}>Affected checks: {event.detail.failures.length ? event.detail.failures.join("; ") : "control API and monitored machines healthy"}</span></li>)}</ol>}
  </main>;
}
