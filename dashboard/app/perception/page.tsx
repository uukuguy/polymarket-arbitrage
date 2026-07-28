import { readPerceptionOverview } from "@/lib/perception";
import type { PerceptionGroupStatus } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const panel = {
  border: "1px solid #292929",
  borderRadius: 8,
  padding: 16,
  background: "#111",
} as const;

const muted = { color: "#888", fontSize: 13 } as const;

function fmtTime(ms: number | null | undefined): string {
  if (ms == null) return "not exposed";
  return new Date(ms).toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

function countStatus(
  statuses: PerceptionGroupStatus[],
  status: PerceptionGroupStatus,
): number {
  return statuses.filter((candidate) => candidate === status).length;
}

function NotExposed({ children }: { children?: string }) {
  return (
    <span style={{ color: "#d2a85a" }}>
      {children ?? "not exposed by the Task 6 public read model"}
    </span>
  );
}

export default async function PerceptionOverviewPage() {
  const overview = await readPerceptionOverview();

  if (overview.status === "unavailable") {
    return (
      <main style={{ padding: 24, maxWidth: 1280, margin: "0 auto" }}>
        <h1 style={{ fontSize: 26 }}>Perception overview</h1>
        <section style={{ ...panel, borderColor: "#6b4a10", background: "#302408" }}>
          <h2 style={{ marginTop: 0, color: "#ffd47a" }}>
            Perception unavailable
          </h2>
          <p>{overview.reason}</p>
          <strong>Unavailable is not zero opportunities.</strong>
          <p style={muted}>
            The bounded read failed or timed out; no market conclusion can be
            drawn from this render.
          </p>
        </section>
      </main>
    );
  }

  const { status, groups, discovery, reconciliation, incidents } = overview.data;
  const groupStatuses = groups.items.map((group) => group.status);
  const opportunities = status.opportunities;
  const openIncidents = incidents.items.filter(
    (incident) => incident.state !== "verified",
  );
  const validZero =
    opportunities.status === "available" && opportunities.count === 0;

  return (
    <main style={{ padding: 24, maxWidth: 1280, margin: "0 auto" }}>
      <h1 style={{ fontSize: 26, marginBottom: 6 }}>Perception overview</h1>
      <p style={{ ...muted, marginTop: 0 }}>
        Observer-only view of bounded Task 6 public GET contracts. Every unknown
        is labelled; it is never converted to zero.
      </p>

      <section
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
          gap: 12,
          margin: "20px 0",
        }}
      >
        <div style={panel}>
          <div style={muted}>watching</div>
          <NotExposed />
        </div>
        <div style={panel}>
          <div style={muted}>stale (returned bounded page)</div>
          <strong>{countStatus(groupStatuses, "stale")}</strong>
        </div>
        <div style={panel}>
          <div style={muted}>unavailable</div>
          <NotExposed>not exposed per group</NotExposed>
        </div>
        <div style={panel}>
          <div style={muted}>invalidated (returned bounded page)</div>
          <strong>{countStatus(groupStatuses, "invalidated")}</strong>
        </div>
      </section>

      <section style={{ ...panel, marginBottom: 12 }}>
        <h2 style={{ marginTop: 0 }}>Current opportunities</h2>
        {opportunities.status === "available" ? (
          <>
            <div style={{ fontSize: 30, fontWeight: 700 }}>
              {opportunities.count}
            </div>
            {validZero && (
              <p style={{ color: "#9bc79b" }}>No certified edge right now.</p>
            )}
          </>
        ) : (
          <p style={{ color: "#ffd47a" }}>
            unavailable — {opportunities.reason}. Unavailable is not zero
            opportunities.
          </p>
        )}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: 10,
          }}
        >
          {["Certified edge", "Capacity", "Structure age", "Quote age"].map(
            (label) => (
              <div key={label}>
                <div style={muted}>{label}</div>
                <NotExposed />
              </div>
            ),
          )}
        </div>
      </section>

      <section style={{ ...panel, marginBottom: 12 }}>
        <h2 style={{ marginTop: 0 }}>Coverage windows</h2>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #333" }}>
              <th style={{ padding: 8 }}>Window</th>
              <th style={{ padding: 8 }}>Raw coverage</th>
              <th style={{ padding: 8 }}>Weighted coverage</th>
            </tr>
          </thead>
          <tbody>
            {["15m", "30m", "60m"].map((window) => (
              <tr key={window} style={{ borderBottom: "1px solid #222" }}>
                <td style={{ padding: 8 }}>{window}</td>
                <td style={{ padding: 8 }}><NotExposed /></td>
                <td style={{ padding: 8 }}><NotExposed /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))",
          gap: 12,
          marginBottom: 12,
        }}
      >
        <section style={panel}>
          <h2 style={{ marginTop: 0 }}>Discovery</h2>
          {discovery.discovery ? (
            <>
              <p>completed: {String(discovery.discovery.completed)}</p>
              <p>groups seen: {discovery.discovery.groups_seen}</p>
              <p>promoted: {discovery.discovery.promoted_count}</p>
              <p>promotion queue: {discovery.discovery.promotion_queue_depth}</p>
              <p>last finish: {fmtTime(discovery.discovery.last_finished_at_ms)}</p>
            </>
          ) : (
            <p style={muted}>No discovery run has been recorded.</p>
          )}
        </section>
        <section style={panel}>
          <h2 style={{ marginTop: 0 }}>Reconciliation</h2>
          {reconciliation.reconciliation ? (
            <>
              <p>state: {reconciliation.reconciliation.status}</p>
              <p>pages: {reconciliation.reconciliation.pages_completed}</p>
              <p>events: {reconciliation.reconciliation.events_seen}</p>
              <p>rejected: {reconciliation.reconciliation.rejected_count}</p>
              <p>
                checkpoint: {fmtTime(reconciliation.reconciliation.checkpoint_at_ms)}
              </p>
            </>
          ) : (
            <p style={muted}>No reconciliation run has been recorded.</p>
          )}
        </section>
      </div>

      <section style={{ ...panel, marginBottom: 12 }}>
        <h2 style={{ marginTop: 0 }}>Resource mode</h2>
        <NotExposed />
      </section>

      <section style={panel}>
        <h2 style={{ marginTop: 0 }}>
          Open incidents ({status.open_incident_count})
        </h2>
        <p style={muted}>
          Latest incident states from a bounded page; verified terminal events
          are omitted.
        </p>
        {openIncidents.length === 0 ? (
          <p style={muted}>
            No open incident state was returned. The authoritative open count
            above may include an incident outside this bounded page.
          </p>
        ) : (
          openIncidents.map((incident) => (
            <div
              key={`${incident.incident_id}:${incident.sequence}`}
              style={{ borderTop: "1px solid #292929", padding: "10px 0" }}
            >
              <strong>{incident.kind}</strong> · {incident.state}
              <div style={muted}>
                {fmtTime(incident.occurred_at_ms)} · {incident.scope}
              </div>
            </div>
          ))
        )}
      </section>

      <section style={{ ...panel, marginTop: 12 }}>
        <h2 style={{ marginTop: 0 }}>Observed groups (returned bounded page)</h2>
        {groups.next_after !== null && (
          <p style={{ color: "#ffd47a" }}>
            More groups exist after this page; status counts shown above are
            not global totals.
          </p>
        )}
        {groups.items.map((group) => (
          <div
            key={`${group.group_id}:${group.revision}`}
            style={{ borderTop: "1px solid #292929", padding: "10px 0" }}
          >
            <a
              href={`/perception/${encodeURIComponent(group.group_id)}`}
              style={{ color: "#9ec5fe" }}
            >
              {group.group_id}
            </a>
            <span style={muted}>
              {" "}· {group.status} · revision {group.revision} · {group.leg_count} legs
            </span>
          </div>
        ))}
      </section>
    </main>
  );
}
