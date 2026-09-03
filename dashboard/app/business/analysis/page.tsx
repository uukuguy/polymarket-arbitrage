import { readBusinessOverview } from "../../../lib/business-overview";
import { readBusinessResearchPage, type ResearchItem, type ResearchPage } from "../../../lib/business-research";
import { BusinessShell, Metric, ProductCard, ResearchIndexPending, Status, UnavailableBusiness } from "../business-ui";

const stateLabel: Record<string, string> = {
  "positive-edge": "Positive gross edge",
  "no-edge": "No gross edge",
  "incomplete-coverage": "Incomplete quote coverage",
  "expired-or-closed": "Expired or closed event",
  "context-unavailable": "Context unavailable",
};
const number = (value: unknown, digits = 0) => typeof value === "number" && Number.isFinite(value)
  ? value.toLocaleString(undefined, { maximumFractionDigits: digits }) : "—";
const money = (value: unknown) => typeof value === "number" && Number.isFinite(value)
  ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "—";
const record = (value: unknown): Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const formatEndTime = (value: unknown) => typeof value === "number" && Number.isFinite(value)
  ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "end unknown";

function CandidateTable({ page }: { page: ResearchPage }) {
  if (page.status !== "available") return <ResearchIndexPending product="Analysis candidate" />;
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginTop: 16 }}>
    <div style={{ padding: "14px 16px", borderBottom: "1px solid #303030" }}><strong>Group-level candidate facts</strong><span style={{ color: "#aeb8c8", marginLeft: 10 }}>{page.items.length} highest-priority groups shown</span><p style={{ color: "#aeb8c8", marginBottom: 0 }}>Positive candidates are ordered by theoretical gross profit. Fees, slippage, and simultaneous execution are not assessed; this is research evidence, not a certified opportunity.</p></div>
    <div style={{ overflowX: "auto" }}><table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}><thead><tr>{["State", "Event", "Bundle economics", "Coverage", "Group"].map((name) => <th key={name} style={{ padding: "10px 12px", borderBottom: "1px solid #303030", textAlign: "left" }}>{name}</th>)}</tr></thead><tbody>{page.items.map((item: ResearchItem) => { const event = record(item.event); const state = typeof item.candidate_state === "string" ? item.candidate_state : "context-unavailable"; return <tr key={String(item.group_id)}><td style={{ padding: "10px 12px", borderBottom: "1px solid #303030" }}><Status value={state === "positive-edge" ? "available" : "lagging"} /><br /><strong>{stateLabel[state] ?? state}</strong></td><td style={{ padding: "10px 12px", borderBottom: "1px solid #303030", minWidth: 220 }}><strong>{typeof event.title === "string" ? event.title : String(item.event_id ?? "Unknown event")}</strong><br /><span style={{ color: "#aeb8c8" }}>{event.is_open === true ? "Open" : "Closed/unknown"} · {formatEndTime(event.end_time_ms)}</span></td><td style={{ padding: "10px 12px", borderBottom: "1px solid #303030" }}><strong>{money(item.gross_profit_usd)} gross profit</strong><br /><span style={{ color: "#aeb8c8" }}>{money(item.capital_required_usd)} capital required · {number(item.gross_roi_bps, 0)} bps gross ROI</span><br /><span style={{ color: "#8d9aab" }}>{money(item.bundle_cost)} bundle cost · {number(item.max_bundle_size, 2)} bundles</span></td><td style={{ padding: "10px 12px", borderBottom: "1px solid #303030" }}><strong>{number(item.quoted_member_count)} / {number(item.expected_member_count)}</strong><br /><span style={{ color: "#aeb8c8" }}>{typeof item.quality === "string" ? item.quality : "quality unknown"}</span></td><td style={{ padding: "10px 12px", borderBottom: "1px solid #303030", fontFamily: "monospace", maxWidth: 220, overflowWrap: "anywhere" }}>{String(item.group_id)}</td></tr>; })}</tbody></table></div>
  </section>;
}

export default async function AnalysisPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  const counts = data.analysis.component_counts ?? {};
  const page = await readBusinessResearchPage("analysis");
  const summary = page && page.status === "available" && typeof page.summary === "object" && page.summary !== null ? page.summary as Record<string, unknown> : {};
  const stateCounts = typeof summary.state_counts === "object" && summary.state_counts !== null ? summary.state_counts as Record<string, unknown> : {};
  return <BusinessShell overview={data} title="Analysis funnel" subtitle="The decision funnel is intentionally explicit about what M1 has and has not yet persisted.">
    <ProductCard title="Lineage-bound research funnel" item={data.analysis}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 14, marginTop: 14 }}>
        <Metric label="Structure universe" value={(counts.structure_records ?? 0).toLocaleString()} note="published market-universe records" />
        <Metric label="Quote coverage" value={(counts.quote_records ?? 0).toLocaleString()} note="same-lineage quote records" />
        <Metric label="Certified opportunities" value={counts.certified_opportunities?.toLocaleString() ?? "not published"} note="shown only for the same quote generation" />
        <Metric label="Positive-edge candidates" value={number(summary.positive_edge_count)} note="group calculation; still not certified" />
      </div>
      <p>{data.analysis.status === "not-published" ? "No lineage-bound research funnel has been published yet." : "This funnel reports only published, same-lineage structure and quote facts."}</p>
      <p>Candidate analysis is not a certified opportunity. A positive gross edge must still pass the execution and certification gates before it appears in Certified opportunities.</p>
    </ProductCard>
    {page ? <><ProductCard title="Candidate-state funnel" item={{ status: page.status, reason_code: page.reason_code }}><div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 14, marginTop: 14 }}>{Object.entries(stateLabel).map(([state, label]) => <Metric key={state} label={label} value={number(stateCounts[state])} />)}</div></ProductCard><CandidateTable page={page} /></> : <ResearchIndexPending product="Analysis candidate" />}
  </BusinessShell>;
}
