import { readPerceptionGroupHistory } from "@/lib/perception";
import type { PerceptionIncident } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 0;

type TimelineItem = {
  key: string;
  occurredAtMs: number;
  label: "Membership revision" | "Incident event";
  title: string;
  detail: string;
};

const panel = {
  border: "1px solid #292929",
  borderRadius: 8,
  padding: 16,
  background: "#111",
} as const;

const muted = { color: "#888", fontSize: 13 } as const;

function fmtTime(ms: number): string {
  return new Date(ms).toISOString().replace("T", " ").slice(0, 23) + " UTC";
}

function belongsToGroup(incident: PerceptionIncident, groupId: string): boolean {
  return (
    incident.scope === `candidate:${groupId}` ||
    incident.scope === `group:${groupId}` ||
    incident.evidence.group_id === groupId
  );
}

function decodeRouteGroupId(encodedGroupId: string): string {
  try {
    return decodeURIComponent(encodedGroupId);
  } catch {
    return encodedGroupId;
  }
}

export default async function PerceptionGroupPage({
  params,
}: {
  params: Promise<{ group_id: string }>;
}) {
  const { group_id: encodedGroupId } = await params;
  const groupId = decodeRouteGroupId(encodedGroupId);
  const detail = await readPerceptionGroupHistory(groupId);

  if (detail.status === "unavailable") {
    return (
      <main style={{ padding: 24, maxWidth: 1000, margin: "0 auto" }}>
        <a href="/perception" style={{ color: "#9ec5fe" }}>← Perception overview</a>
        <h1>Group timeline</h1>
        <section style={{ ...panel, borderColor: "#6b4a10", background: "#302408" }}>
          <h2 style={{ color: "#ffd47a" }}>Perception unavailable</h2>
          <p>{detail.reason}</p>
          <strong>Unavailable is not an empty group history.</strong>
        </section>
      </main>
    );
  }

  const membershipEvents: TimelineItem[] = detail.data.history.items.map(
    (revision) => ({
      key: `membership:${revision.revision}`,
      occurredAtMs: revision.observed_at_ms,
      label: "Membership revision",
      title: `Revision ${revision.revision} · ${revision.status}`,
      detail: `${revision.leg_count} legs · membership ${revision.membership_hash}`,
    }),
  );
  const incidentEvents: TimelineItem[] = detail.data.incidents.items
    .filter((incident) => belongsToGroup(incident, groupId))
    .map((incident) => ({
      key: `incident:${incident.incident_id}:${incident.sequence}`,
      occurredAtMs: incident.occurred_at_ms,
      label: "Incident event",
      title: `${incident.kind} · ${incident.state}`,
      detail: incident.scope,
    }));
  const timeline = [...membershipEvents, ...incidentEvents].sort(
    (left, right) => right.occurredAtMs - left.occurredAtMs,
  );

  return (
    <main style={{ padding: 24, maxWidth: 1000, margin: "0 auto" }}>
      <a href="/perception" style={{ color: "#9ec5fe" }}>← Perception overview</a>
      <h1 style={{ marginBottom: 6 }}>Group timeline</h1>
      <code style={{ color: "#aaa" }}>{groupId}</code>
      <p style={muted}>
        One descending, timestamped operator timeline assembled only from Task 6
        public GET data.
      </p>
      {detail.data.history.next_before_revision !== null && (
        <p style={{ color: "#ffd47a", fontSize: 13 }}>
          Older revisions exist before revision{" "}
          {detail.data.history.next_before_revision}; this bounded page does not
          auto-fetch them.
        </p>
      )}

      <section
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: 12,
          margin: "20px 0",
        }}
      >
        <div style={panel}>
          <strong>Membership revision</strong>
          <p style={muted}>{membershipEvents.length} recorded revisions</p>
        </div>
        <div style={panel}>
          <strong>Quote batch</strong>
          <p style={{ color: "#d2a85a" }}>
            not exposed by the Task 6 public read model
          </p>
        </div>
        <div style={panel}>
          <strong>Opportunity transition</strong>
          <p style={{ color: "#d2a85a" }}>
            not exposed by the Task 6 public read model
          </p>
        </div>
        <div style={panel}>
          <strong>Incident event</strong>
          <p style={muted}>{incidentEvents.length} matching events</p>
        </div>
      </section>

      <section style={panel}>
        {timeline.length === 0 ? (
          <p style={muted}>No membership or incident events were returned.</p>
        ) : (
          timeline.map((event) => (
            <article
              key={event.key}
              style={{ borderTop: "1px solid #292929", padding: "14px 0" }}
            >
              <div style={{ color: "#9ec5fe", fontSize: 12 }}>{event.label}</div>
              <strong>{event.title}</strong>
              <div style={muted}>{event.detail}</div>
              <time style={{ color: "#777", fontSize: 12 }}>
                {fmtTime(event.occurredAtMs)}
              </time>
            </article>
          ))
        )}
      </section>
    </main>
  );
}
