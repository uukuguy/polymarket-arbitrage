import { readBusinessOverview } from "../../../lib/business-overview";
import { readQuoteCoveragePage, type QuoteCoverageItem, type QuoteCoveragePage } from "../../../lib/business-research";
import { BusinessShell, Metric, ProductCard, ResearchIndexPending, Status, UnavailableBusiness } from "../business-ui";

const cellStyle = { padding: "10px 12px", borderBottom: "1px solid #303030", textAlign: "left" as const, verticalAlign: "top" as const };
const number = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? value.toLocaleString() : "—";
const record = (value: unknown): Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const formatEndTime = (value: unknown) => typeof value === "number" && Number.isFinite(value)
  ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "end unknown";
const coverageLabel: Record<string, string> = {
  "coverage-gap": "Coverage gap",
  "analysis-ready": "Complete; review in Analysis",
  healthy: "Complete coverage",
  "needs-context": "Structure context needed",
};

function CoverageTable({ page }: { page: QuoteCoveragePage }) {
  if (page.status !== "available") return <ResearchIndexPending product="Quote coverage" />;
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginBottom: 16 }}>
    <div style={{ padding: "14px 16px", borderBottom: "1px solid #303030" }}>
      <strong>Group coverage health</strong><span style={{ color: "#aeb8c8", marginLeft: 10 }}>{page.items.length} active groups shown</span>
      <p style={{ color: "#aeb8c8", marginBottom: 0 }}>Ordered by actionable coverage defects, then current group readiness. Price extremity is intentionally not a signal on this page.</p>
    </div>
    <div style={{ overflowX: "auto" }}><table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}><thead><tr>{["Coverage state", "Event / group", "Quoted legs", "Structure quality", "Next action"].map((name) => <th style={cellStyle} key={name}>{name}</th>)}</tr></thead><tbody>{page.items.map((item: QuoteCoverageItem) => {
      const event = record(item.event);
      const available = item.coverage_state === "healthy" || item.coverage_state === "analysis-ready";
      return <tr key={item.group_id}><td style={cellStyle}><Status value={available ? "available" : "lagging"} /><br /><strong>{coverageLabel[item.coverage_state] ?? item.coverage_state}</strong></td><td style={{ ...cellStyle, minWidth: 260 }}><strong>{typeof event.title === "string" ? event.title : "Unnamed current event"}</strong><br /><span style={{ color: "#aeb8c8" }}>{formatEndTime(event.end_time_ms)} · <span style={{ fontFamily: "monospace" }}>{item.group_id}</span></span></td><td style={cellStyle}><strong>{number(item.quoted_member_count)} / {number(item.expected_member_count)} legs</strong><br /><span style={{ color: "#aeb8c8" }}>{number(item.missing_member_count)} missing</span></td><td style={cellStyle}>{typeof item.quality === "string" ? item.quality : "quality unknown"}</td><td style={{ ...cellStyle, minWidth: 200 }}>{item.action}</td></tr>;
    })}</tbody></table></div>
  </section>;
}

export default async function QuotesPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  const page = await readQuoteCoveragePage();
  const counts = page?.status === "available" && page.summary && typeof page.summary.visible_state_counts === "object" && page.summary.visible_state_counts !== null ? page.summary.visible_state_counts as Record<string, unknown> : {};
  return <BusinessShell overview={data} title="Quote coverage" subtitle="Whether active Structure groups have the quote legs required for reliable downstream analysis.">
    <ProductCard title="Current quote generation" item={data.quote}><p>Generation: {data.quote.generation_key ?? "not published"}</p><p>Parent Structure generation: {data.quote.parent_structure_generation_key ?? "not published"}</p><p>Quote records: {data.quote.record_count ?? "not published"}</p><div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 14, marginTop: 14 }}><Metric label="Coverage gaps shown" value={number(counts["coverage-gap"])} /><Metric label="Ready for Analysis" value={number(counts["analysis-ready"])} /><Metric label="Complete, no edge" value={number(counts.healthy)} /></div></ProductCard>
    {page ? <CoverageTable page={page} /> : <ResearchIndexPending product="Quote coverage" />}
    <p>Coverage health is not an investment ranking. A complete group may have no positive combined edge; only Analysis evaluates that separately.</p>
  </BusinessShell>;
}
