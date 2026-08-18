import { readControlPlane } from "@/lib/control-plane";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function ControlPlanePage() {
  const view = await readControlPlane();
  if (view.status === "unavailable") {
    return <main style={{ padding: 24 }}><h1>M1 control-plane runtime</h1><p style={{ color: "#fecaca" }}>Unavailable: {view.reason}. This is not a healthy or empty state.</p></main>;
  }
  const active = view.runtime_watchdog.current;
  const evidence = view.soak_evidence;
  const evidenceAge = evidence ? Math.max(0, Math.floor((Date.now() - Date.parse(evidence.latest_observed_at)) / 1000)) : null;
  return <main style={{ padding: 24, maxWidth: 1200 }}>
    <h1>M1 control-plane runtime</h1>
    <section style={{ padding: 16, border: `1px solid ${active ? "#ef4444" : "#4b5563"}`, background: active ? "#341414" : "#111", borderRadius: 8, marginBottom: 16 }}>
      <h2 style={{ marginTop: 0 }}>{active ? "Active runtime incident — action required" : "No active runtime-watchdog incident"}</h2>
      <p>{active?.summary ?? "The independent watchdog currently reports healthy."}</p>
      {active && <><p><strong>Severity:</strong> {active.severity} · <strong>Observed by:</strong> {active.source}</p><p><strong>Detected:</strong> {active.opened_at} · <strong>Incident:</strong> {active.incident_key}</p><p style={{ color: "#fecaca" }}><strong>Affected checks:</strong> {active.failures.join("; ") || "unclassified runtime failure"}</p></>}
      <p style={{ color: "#aaa" }}>Durable jobs: {Object.entries(view.job_counts).map(([state, count]) => `${state}=${count}`).join(" · ")}</p>
    </section>
    <section style={{ padding: 16, border: `1px solid ${evidence ? "#4b5563" : "#ef4444"}`, background: evidence ? "#111" : "#341414", borderRadius: 8, marginBottom: 16 }}>
      <h2 style={{ marginTop: 0 }}>Immutable cloud evidence</h2>
      {evidence ? <><p>Latest sample: {evidence.latest_observed_at} ({evidenceAge}s ago)</p><p style={{ color: "#aaa" }}>Run: {evidence.latest_run_id}</p></> : <p style={{ color: "#fecaca" }}>No cloud evidence sample is present. This is an operational failure, not a healthy empty state.</p>}
    </section>
    <h2>Runtime incident and recovery ledger</h2>
    {view.runtime_watchdog.recent_events.length === 0 ? <p>No watchdog transitions recorded yet.</p> : <ol>{view.runtime_watchdog.recent_events.map((event, index) => <li key={`${event.incident_key}-${event.occurred_at}-${index}`} style={{ marginBottom: 14, padding: 12, borderLeft: `4px solid ${event.kind === "detected" ? "#ef4444" : "#22c55e"}`, background: "#111" }}><strong style={{ color: event.kind === "detected" ? "#fecaca" : "#bbf7d0" }}>{event.kind === "detected" ? "Detected — open until a recovery event" : "Recovered"}</strong> · {event.occurred_at}<br /><strong>{event.summary}</strong><br /><span style={{ color: "#aaa" }}>Severity: {event.severity} · Observed by: {event.detail.source ?? "legacy-runtime-watchdog"} · Incident: {event.incident_key}</span><br /><span style={{ color: "#aaa" }}>Affected checks: {event.detail.failures.length ? event.detail.failures.join("; ") : "control API and monitored machines healthy"}</span></li>)}</ol>}
  </main>;
}
