export type RuntimeEvent = {
  incident_key: string;
  severity: string;
  summary: string;
  kind: "detected" | "recovered";
  occurred_at: string;
  detail: { failures: string[]; source?: string };
};

export type ActiveRuntimeIncident = {
  incident_key: string;
  severity: string;
  summary: string;
  opened_at: string;
  source: string;
  failures: string[];
};

export type ControlPlaneRead =
  | { status: "unavailable"; reason: string }
  | {
      status: "available";
      job_counts: Record<string, number>;
      open_incidents: Array<{ incident_key: string; component: string; severity: string; summary: string }>;
      runtime_watchdog: { current: ActiveRuntimeIncident | null; recent_events: RuntimeEvent[] };
      soak_evidence: { latest_run_id: string; latest_observed_at: string } | null;
      cloud_usage: { budget_day: string; used_bytes: number; daily_budget_bytes: number | null; threshold_percent: number; latest_observation: { source: string; operation: string; bytes_received: number; observed_at: string } | null };
    };

function validEvent(value: unknown): value is RuntimeEvent {
  if (!value || typeof value !== "object") return false;
  const event = value as Record<string, unknown>;
  const detail = event.detail as Record<string, unknown> | null;
  return (
    (event.kind === "detected" || event.kind === "recovered") &&
    typeof event.incident_key === "string" &&
    typeof event.severity === "string" &&
    typeof event.summary === "string" &&
    typeof event.occurred_at === "string" &&
    !!detail &&
    Array.isArray(detail.failures) &&
    detail.failures.every((item) => typeof item === "string") &&
    (detail.source === undefined || typeof detail.source === "string")
  );
}

function validActiveIncident(value: unknown): value is ActiveRuntimeIncident {
  if (!value || typeof value !== "object") return false;
  const incident = value as Record<string, unknown>;
  return (
    typeof incident.incident_key === "string" &&
    typeof incident.severity === "string" &&
    typeof incident.summary === "string" &&
    typeof incident.opened_at === "string" &&
    typeof incident.source === "string" &&
    Array.isArray(incident.failures) &&
    incident.failures.every((item) => typeof item === "string")
  );
}

export async function readControlPlane(): Promise<ControlPlaneRead> {
  const base = process.env.POLYARB_CONTROL_API_URL ?? "https://polyarb-control-api.fly.dev";
  try {
    const response = await fetch(`${base}/perception/control-plane`, { cache: "no-store" });
    const payload: unknown = await response.json();
    if (!response.ok || !payload || typeof payload !== "object") throw new Error("unavailable");
    const data = payload as Record<string, unknown>;
    const runtime = data.runtime_watchdog as Record<string, unknown> | undefined;
    const recent = runtime?.recent_events;
    const current = runtime?.current;
    const evidence = data.soak_evidence;
    const usage = data.cloud_usage as Record<string, unknown> | undefined;
    const validEvidence = evidence === null || (
      typeof evidence === "object" && evidence !== null &&
      typeof (evidence as Record<string, unknown>).latest_run_id === "string" &&
      typeof (evidence as Record<string, unknown>).latest_observed_at === "string"
    );
    if (data.status !== "available" || !runtime || !usage || typeof usage.used_bytes !== "number" || typeof usage.threshold_percent !== "number" || !Array.isArray(recent) || !recent.every(validEvent) || (current !== null && !validActiveIncident(current)) || !validEvidence) {
      throw new Error("invalid control-plane read model");
    }
    return data as ControlPlaneRead;
  } catch {
    return { status: "unavailable", reason: "control-plane-read-unavailable" };
  }
}
