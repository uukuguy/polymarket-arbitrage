export type RuntimeEvent = {
  kind: "detected" | "recovered";
  occurred_at: string;
  detail: { failures: string[] };
};

export type ControlPlaneRead =
  | { status: "unavailable"; reason: string }
  | {
      status: "available";
      job_counts: Record<string, number>;
      open_incidents: Array<{ incident_key: string; component: string; severity: string; summary: string }>;
      runtime_watchdog: { current: { summary: string; opened_at?: string } | null; recent_events: RuntimeEvent[] };
    };

function validEvent(value: unknown): value is RuntimeEvent {
  if (!value || typeof value !== "object") return false;
  const event = value as Record<string, unknown>;
  const detail = event.detail as Record<string, unknown> | null;
  return (
    (event.kind === "detected" || event.kind === "recovered") &&
    typeof event.occurred_at === "string" &&
    !!detail && Array.isArray(detail.failures) && detail.failures.every((item) => typeof item === "string")
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
    if (data.status !== "available" || !runtime || !Array.isArray(recent) || !recent.every(validEvent)) {
      throw new Error("invalid control-plane read model");
    }
    return data as ControlPlaneRead;
  } catch {
    return { status: "unavailable", reason: "control-plane-read-unavailable" };
  }
}
