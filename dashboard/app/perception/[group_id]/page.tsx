import { readPerceptionGroupHistory } from "@/lib/perception";
import type { PerceptionGroupTimelineItem } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 0;

type TimelineItem = {
  key: string;
  occurredAtMs: number;
  classOrder: number;
  stableId: number;
  label:
    | "Membership revision"
    | "Quote batch"
    | "Opportunity transition"
    | "Incident event";
  title: string;
  detail: string;
  evidence?: string;
};

const panel = {
  border: "1px solid #292929",
  borderRadius: 8,
  padding: 16,
  background: "#111",
} as const;

const muted = { color: "#aaa", fontSize: 14, overflowWrap: "anywhere" } as const;
const classColors = {
  "Membership revision": "#9ec5fe",
  "Quote batch": "#c9a7ff",
  "Opportunity transition": "#74d99f",
  "Incident event": "#ffb36b",
} as const;

function fmtTime(ms: number): string {
  return new Date(ms).toISOString().replace("T", " ").slice(0, 23) + " UTC";
}

function decodeRouteGroupId(encodedGroupId: string): string {
  try {
    return decodeURIComponent(encodedGroupId);
  } catch {
    return encodedGroupId;
  }
}

function timelineItem(item: PerceptionGroupTimelineItem): TimelineItem {
  if (item.class === "membership_revision") {
    return {
      key: `membership:${item.stable_id}`,
      occurredAtMs: item.occurred_at_ms,
      classOrder: 0,
      stableId: item.stable_id,
      label: "Membership revision",
      title: `Revision ${item.revision} · ${item.status}`,
      detail: `${item.leg_count} legs · membership ${item.membership_hash}`,
    };
  }
  if (item.class === "quote_batch") {
    return {
      key: `quote:${item.stable_id}`,
      occurredAtMs: item.occurred_at_ms,
      classOrder: 1,
      stableId: item.stable_id,
      label: "Quote batch",
      title: `${item.quote_batch_id} · ${item.status}`,
      detail: `${item.leg_count} legs · ${item.duration_ms} ms${
        item.failure_reason === null ? "" : ` · ${item.failure_reason}`
      }`,
    };
  }
  if (item.class === "opportunity_transition") {
    const from = item.from === null
      ? "initial"
      : `${item.from.last_result}/${item.from.opportunity ? "edge" : "no edge"}`;
    const to = `${item.to.last_result}/${
      item.to.opportunity ? "edge" : "no edge"
    }`;
    return {
      key: `opportunity:${item.stable_id}`,
      occurredAtMs: item.occurred_at_ms,
      classOrder: 2,
      stableId: item.stable_id,
      label: "Opportunity transition",
      title: `${from} → ${to}`,
      detail: `edge ${
        item.gross_edge_bps === null ? "n/a" : `${item.gross_edge_bps} bps`
      } · ${item.reason ?? "successful quote"}`,
    };
  }
  return {
    key: `incident:${item.stable_id}`,
    occurredAtMs: item.occurred_at_ms,
    classOrder: 3,
    stableId: item.stable_id,
    label: "Incident event",
    title: `${item.kind} · ${item.state}`,
    detail: `${item.scope} · sequence ${item.sequence}`,
    evidence: JSON.stringify(item.evidence),
  };
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

  const timeline = detail.data.timeline.items.map(timelineItem).sort(
    (left, right) =>
      right.occurredAtMs - left.occurredAtMs ||
      left.classOrder - right.classOrder ||
      right.stableId - left.stableId,
  );
  const counts = {
    membership_revision: detail.data.timeline.items.filter(
      (item) => item.class === "membership_revision",
    ).length,
    quote_batch: detail.data.timeline.items.filter(
      (item) => item.class === "quote_batch",
    ).length,
    opportunity_transition: detail.data.timeline.items.filter(
      (item) => item.class === "opportunity_transition",
    ).length,
    incident_event: detail.data.timeline.items.filter(
      (item) => item.class === "incident_event",
    ).length,
  };

  return (
    <main style={{ padding: 24, maxWidth: 1000, margin: "0 auto" }}>
      <a href="/perception" style={{ color: "#9ec5fe" }}>← Perception overview</a>
      <h1 style={{ marginBottom: 6 }}>Group timeline</h1>
      <code style={{ color: "#aaa" }}>{groupId}</code>
      <p style={muted}>
        One authenticated, descending operations timeline from a single bounded
        read snapshot.
      </p>
      {detail.data.timeline.next_before !== null && (
        <p style={{ color: "#ffd47a", fontSize: 13 }}>
          Older evidence exists; this bounded page does not auto-fetch it.
        </p>
      )}
      <p style={muted}>
        History completeness — membership{" "}
        {detail.data.timeline.history_complete.membership ? "complete" : "bounded"},
        quote {detail.data.timeline.history_complete.quote ? "complete" : "bounded"},
        opportunity{" "}
        {detail.data.timeline.history_complete.opportunity ? "complete" : "bounded"},
        incident {detail.data.timeline.history_complete.incident ? "complete" : "bounded"}.
        {" "}history_floor: candidate sources are global/conservative; incident is
        exact group scope.
      </p>

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
          <p style={muted}>{counts.membership_revision} records on this page</p>
        </div>
        <div style={panel}>
          <strong>Quote batch</strong>
          <p style={muted}>{counts.quote_batch} records on this page</p>
        </div>
        <div style={panel}>
          <strong>Opportunity transition</strong>
          <p style={muted}>{counts.opportunity_transition} records on this page</p>
        </div>
        <div style={panel}>
          <strong>Incident event</strong>
          <p style={muted}>{counts.incident_event} records on this page</p>
        </div>
      </section>

      <section style={panel}>
        {timeline.length === 0 ? (
          <p style={muted}>No group operations evidence was returned.</p>
        ) : (
          timeline.map((event) => (
            <article
              key={event.key}
              style={{ borderTop: "1px solid #292929", padding: "14px 0" }}
            >
              <div style={{ color: classColors[event.label], fontSize: 14 }}>
                {event.label}
              </div>
              <strong>{event.title}</strong>
              <div style={muted}>{event.detail}</div>
              {event.evidence !== undefined && (
                <code style={{ ...muted, display: "block" }}>{event.evidence}</code>
              )}
              <time style={{ color: "#999", fontSize: 14 }}>
                {fmtTime(event.occurredAtMs)}
              </time>
            </article>
          ))
        )}
      </section>
    </main>
  );
}
