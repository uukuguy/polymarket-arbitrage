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

function fmtPercent(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function fmtOptionalCount(value: number | null): string {
  return value == null ? "pending" : String(value);
}

function fmtAge(serverTimeMs: number, observedAtMs: number): string {
  const seconds = Math.max(0, serverTimeMs - observedAtMs) / 1000;
  return seconds < 60
    ? `${seconds.toFixed(1)}s`
    : `${(seconds / 60).toFixed(1)}m`;
}

function fmtDurationMs(value: number): string {
  const seconds = value / 1000;
  return seconds < 60
    ? `${seconds.toFixed(1)}s`
    : `${(seconds / 60).toFixed(1)}m`;
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

  const {
    status,
    currentOpportunities,
    groups,
    discovery,
    reconciliation,
    incidents,
  } = overview.data;
  const groupStatuses = groups.items.map((group) => group.status);
  const opportunityStatus = status.opportunities;
  const openIncidents = incidents.items.filter(
    (incident) => incident.state !== "verified",
  );
  const validZero =
    opportunityStatus.status === "available" &&
    opportunityStatus.count === 0;

  return (
    <main style={{ padding: 24, maxWidth: 1280, margin: "0 auto" }}>
      <h1 style={{ fontSize: 26, marginBottom: 6 }}>Perception overview</h1>
      <p style={{ ...muted, marginTop: 0 }}>
        Observer-only view of bounded Task 6 public GET contracts. Every unknown
        is labelled; it is never converted to zero.
      </p>

      <section style={{ margin: "20px 0" }}>
        <h2>Global Candidate state</h2>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: 12,
          }}
        >
          <div style={panel}>
            <div style={muted}>watching</div>
            <strong>{status.candidate_state_counts.watching}</strong>
          </div>
          <div style={panel}>
            <div style={muted}>No edge</div>
            <strong>{status.candidate_state_counts["no-edge"]}</strong>
          </div>
          <div style={panel}>
            <div style={muted}>unavailable</div>
            <strong>{status.candidate_state_counts.unavailable}</strong>
          </div>
        </div>
        <h2>Bounded Structure page</h2>
        <p style={muted}>
          These Structure status counts cover only the returned groups page,
          never the full market universe.
        </p>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: 12,
          }}
        >
          <div style={panel}>
            <div style={muted}>stale</div>
            <strong>{countStatus(groupStatuses, "stale")}</strong>
          </div>
          <div style={panel}>
            <div style={muted}>invalidated</div>
            <strong>{countStatus(groupStatuses, "invalidated")}</strong>
          </div>
        </div>
      </section>

      <section style={{ ...panel, marginBottom: 12 }}>
        <h2 style={{ marginTop: 0 }}>Current opportunities</h2>
        {opportunityStatus.status === "available" ? (
          <>
            <div style={{ fontSize: 30, fontWeight: 700 }}>
              {opportunityStatus.count}
            </div>
            {validZero && (
              <p style={{ color: "#9bc79b" }}>No certified edge right now.</p>
            )}
          </>
        ) : (
          <p style={{ color: "#ffd47a" }}>
            unavailable — {opportunityStatus.reason}. Unavailable is not zero
            opportunities.
          </p>
        )}
        <p style={muted}>
          Showing {currentOpportunities.items.length} of{" "}
          {currentOpportunities.current_opportunity_count} authenticated current
          opportunities.
        </p>
        {currentOpportunities.next_after_group_id !== null && (
          <p style={{ color: "#ffd47a" }}>
            More current opportunities exist after this bounded page.
          </p>
        )}
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #333" }}>
              <th style={{ padding: 8 }}>Group</th>
              <th style={{ padding: 8 }}>Certified edge (bps)</th>
              <th style={{ padding: 8 }}>Bundle cost</th>
              <th style={{ padding: 8 }}>Max bundle size</th>
              <th style={{ padding: 8 }}>Structure age</th>
              <th style={{ padding: 8 }}>Quote age</th>
            </tr>
          </thead>
          <tbody>
            {currentOpportunities.items.map((item) => (
              <tr key={item.group_id} style={{ borderBottom: "1px solid #222" }}>
                <td style={{ padding: 8 }}>{item.group_id}</td>
                <td style={{ padding: 8 }}>
                  {item.gross_edge_bps.toFixed(1)}
                </td>
                <td style={{ padding: 8 }}>{item.bundle_cost.toFixed(4)}</td>
                <td style={{ padding: 8 }}>
                  {item.max_bundle_size.toFixed(2)}
                </td>
                <td style={{ padding: 8 }}>
                  {fmtAge(
                    status.server_time_ms,
                    item.structure_observed_at_ms,
                  )}
                </td>
                <td style={{ padding: 8 }}>
                  {fmtAge(status.server_time_ms, item.quote_quoted_at_ms)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {currentOpportunities.items.length === 0 && (
          <p style={muted}>No authenticated current opportunity rows.</p>
        )}
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
            {(
              [
                ["15", "15m"],
                ["30", "30m"],
                ["60", "60m"],
              ] as const
            ).map(([minutes, label]) => {
              const window = discovery.discovery?.coverage.by_minutes[minutes];
              return (
                <tr key={minutes} style={{ borderBottom: "1px solid #222" }}>
                  <td style={{ padding: 8 }}>{label}</td>
                  <td style={{ padding: 8 }}>
                    {window ? fmtPercent(window.raw_fraction) : "not recorded"}
                  </td>
                  <td style={{ padding: 8 }}>
                    {window
                      ? fmtPercent(window.liquidity_weighted_fraction)
                      : "not recorded"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {discovery.discovery && (
          <p style={muted}>
            Known groups: {discovery.discovery.coverage.known_groups} · total
            liquidity weight:{" "}
            {discovery.discovery.coverage.total_liquidity_weight.toFixed(2)}
          </p>
        )}
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
              <p>
                queue classes:{" "}
                {Object.entries(discovery.discovery.queue_depth_by_class)
                  .map(([priority, depth]) => `${priority}=${depth}`)
                  .join(", ") || "empty"}
              </p>
              <p>
                oldest visit age:{" "}
                {discovery.discovery.oldest_visit_age_ms == null
                  ? "none"
                  : `${discovery.discovery.oldest_visit_age_ms} ms`}
              </p>
              <p>
                load_state: {discovery.discovery.load_state.last_decision} ·{" "}
                {discovery.discovery.load_state.last_reason ?? "fresh"}
              </p>
              <p>
                admission_proof capacity:{" "}
                {discovery.discovery.admission_proof?.effective_capacity ??
                  "not configured"}
              </p>
              <p>
                candidate_attempt_start_count:{" "}
                {discovery.discovery.candidate_attempt_start_count} ·
                candidate_start_deadline_breach_count:{" "}
                {discovery.discovery.candidate_start_deadline_breach_count}
              </p>
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
              <p>duration_ms: {reconciliation.reconciliation.duration_ms}</p>
              <p>
                observations / baseline:{" "}
                {reconciliation.reconciliation.observations_count} /{" "}
                {reconciliation.reconciliation.baseline_count}
              </p>
              <p>
                added_count:{" "}
                {fmtOptionalCount(reconciliation.reconciliation.added_count)} ·
                changed_count:{" "}
                {fmtOptionalCount(reconciliation.reconciliation.changed_count)} ·
                closed_count:{" "}
                {fmtOptionalCount(reconciliation.reconciliation.closed_count)}
              </p>
              <p>
                unchanged_count:{" "}
                {fmtOptionalCount(reconciliation.reconciliation.unchanged_count)} ·
                applied_rejected_count:{" "}
                {fmtOptionalCount(
                  reconciliation.reconciliation.applied_rejected_count,
                )}
              </p>
              <p>
                checkpoint: {fmtTime(reconciliation.reconciliation.checkpoint_at_ms)}
              </p>
              <p style={muted}>
                Historical duration distribution is not tracked; duration_ms is
                the current validated window only.
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
          Open incidents ({incidents.open_count})
        </h2>
        <p style={muted}>
          Latest incident states from a bounded page; verified terminal events
          are omitted.
        </p>
        <p style={muted}>
          Notification delivery is not tracked. These rows prove durable
          lifecycle/operator state only; they do not claim that an alert
          reached any external channel.
        </p>
        {incidents.next_before !== null && (
          <p style={{ color: "#ffd47a" }}>
            More open incidents exist before this bounded page.
          </p>
        )}
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
              <div style={muted}>
                lifecycle {fmtDurationMs(incident.lifecycle_age_ms)} · action{" "}
                {incident.action ?? "not recorded"} · retries{" "}
                {incident.retry_count ?? "not recorded"}
              </div>
              <div style={muted}>
                next retry {fmtTime(incident.next_retry_at_ms)} · recovery{" "}
                {incident.recovery_occurred_at_ms === null
                  ? "not started"
                  : `started ${fmtTime(incident.recovery_occurred_at_ms)}`}
              </div>
              {incident.recovery_start_evidence !== null && (
                <pre
                  style={{
                    ...muted,
                    margin: "6px 0 0",
                    overflowWrap: "anywhere",
                    whiteSpace: "pre-wrap",
                  }}
                >
                  recovery-start evidence{" "}
                  {JSON.stringify(incident.recovery_start_evidence, null, 2)}
                </pre>
              )}
              {incident.history_floor !== null && (
                <div style={muted}>
                  history compacted through event{" "}
                  {incident.history_floor.through_event_id} (
                  {incident.history_floor.compacted_event_count} rows for scope)
                </div>
              )}
            </div>
          ))
        )}
      </section>

      <section style={{ ...panel, marginTop: 12 }}>
        <h2 style={{ marginTop: 0 }}>Observed groups (returned bounded page)</h2>
        {groups.next_after !== null && (
          <p style={{ color: "#ffd47a" }}>
            More groups exist after this page; the Structure stale/invalidated
            counts above are not global totals.
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
