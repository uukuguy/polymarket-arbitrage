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
          {incidents.map((incident) => {
            const actions = recoveryActions.filter(
              (action) => action.incident_key === incident.incident_key,
            );
            return (
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
                {actions.length === 0 ? (
                  <p style={danger}>Recovery action not linked to this incident.</p>
                ) : (
                  <ul>
                    {actions.map((action) => (
                      <li key={action.action_id}>
                        Recovery action {action.action_type} · Result {actionResult(action)} · target{" "}
                        {action.target_type}:{action.target_id} · expected attempt{" "}
                        {action.expected_attempt_id}
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            );
          })}
        </ol>
      )}
    </section>
  );
}
