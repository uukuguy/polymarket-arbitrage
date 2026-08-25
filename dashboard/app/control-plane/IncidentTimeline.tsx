import type { RecoveryAction, RuntimeIncident } from "@/lib/control-plane";

const panel = {
  padding: 16,
  border: "1px solid #4b5563",
  background: "#111",
  borderRadius: 8,
  marginBottom: 16,
} as const;

const muted = { color: "#aaa" } as const;
const danger = { color: "#fecaca" } as const;
const cell = { padding: "6px 8px", verticalAlign: "top" } as const;

function actionResult(action: RecoveryAction): string {
  return action.result_code ?? action.state;
}

export function IncidentTimeline({
  incidents,
  recoveryActions,
}: {
  incidents: RuntimeIncident[];
  recoveryActions: RecoveryAction[];
}) {
  return (
    <section style={panel}>
      <h2 style={{ marginTop: 0 }}>Incident timeline</h2>
      {incidents.length === 0 ? (
        <p style={muted}>No runtime or recovery incidents are open.</p>
      ) : (
        <ol>
          {incidents.map((incident) => (
            <li key={incident.incident_key} style={{ marginBottom: 16 }}>
              <strong>{incident.incident_key}</strong> · {incident.component} · {incident.severity} ·{" "}
              {incident.state}
              <br />
              <span>{incident.summary}</span>
              <br />
              <span style={muted}>
                Transition {incident.transition ?? "none"} · opened {incident.opened_at} · updated{" "}
                {incident.updated_at} · age {incident.age_seconds}s
              </span>
              {incident.transitions.length > 0 && (
                <ul>
                  {incident.transitions.map((transition) => (
                    <li key={`${incident.incident_key}:${transition.kind}:${transition.occurred_at}`}>
                      Transition {transition.kind} · Reason code{" "}
                      {transition.reason_code ?? "not-provided"} · Qualification impact{" "}
                      {transition.qualification_impact ?? "not-provided"} · {transition.age_seconds}s
                    </li>
                  ))}
                </ul>
              )}
            </li>
          ))}
        </ol>
      )}
      <h3>Recent recovery actions</h3>
      {recoveryActions.length === 0 ? (
        <p style={muted}>No recent recovery actions returned.</p>
      ) : (
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #333" }}>
              <th style={cell}>Recovery action</th>
              <th style={cell}>Incident key</th>
              <th style={cell}>Target</th>
              <th style={cell}>State</th>
              <th style={cell}>Expected fences</th>
              <th style={cell}>Timeline</th>
              <th style={cell}>Worker</th>
            </tr>
          </thead>
          <tbody>
            {recoveryActions.map((action) => (
              <tr key={action.action_id} style={{ borderBottom: "1px solid #222" }}>
                <td style={cell}>
                  <strong>{action.action_type}</strong>
                  <br />
                  <code>{action.action_id}</code>
                </td>
                <td style={cell}>{action.incident_key ?? "unlinked"}</td>
                <td style={cell}>
                  {action.target_type}:{action.target_id}
                </td>
                <td style={cell}>
                  Raw state {action.raw_state}
                  <br />
                  Normalized state {action.state}
                  <br />
                  Result {actionResult(action)}
                </td>
                <td style={cell}>
                  controller {action.expected_controller_epoch}
                  <br />
                  attempt {action.expected_attempt_id}
                  <br />
                  lease {action.expected_lease_epoch}
                </td>
                <td style={cell}>
                  Requested {action.requested_at}
                  <br />
                  Started {action.started_at ?? "not-started"}
                  <br />
                  Finished {action.finished_at ?? "not-finished"}
                  <br />
                  Next allowed {action.next_allowed_at}
                </td>
                <td style={action.worker_id === null ? { ...cell, ...danger } : cell}>
                  Worker {action.worker_id ?? "unclaimed"} · epoch {action.worker_epoch}
                  <br />
                  lease expires {action.worker_lease_expires_at ?? "none"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
