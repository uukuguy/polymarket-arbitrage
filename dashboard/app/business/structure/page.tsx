import { readBusinessOverview } from "../../../lib/business-overview";
import { readStructureIntelligencePage, readStructureIntelligenceSummary, type StructureIntelligenceItem, type StructureIntelligencePage } from "../../../lib/business-research";
import { BusinessShell, Metric, ProductCard, ResearchIndexPending, Status, UnavailableBusiness } from "../business-ui";

const tableStyle = { width: "100%", borderCollapse: "collapse" as const, fontSize: 13 };
const cellStyle = { padding: "10px 12px", borderBottom: "1px solid #303030", textAlign: "left" as const, verticalAlign: "top" as const };
const number = (value: unknown) => typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 0 }) : "—";
const text = (value: unknown) => typeof value === "string" && value ? value : "—";
const ending = (value: unknown) => typeof value === "number" ? new Date(value).toLocaleString() : "Unknown";

function EventTable({ page }: { page: StructureIntelligencePage }) {
  if (page.status !== "available") return <ResearchIndexPending product="Structure intelligence" />;
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginBottom: 16 }}><div style={{ padding: "14px 16px", borderBottom: "1px solid #303030" }}><strong>Event universe</strong><span style={{ color: "#aeb8c8", marginLeft: 10 }}>Active state, scheduled end, activity, tags and market breadth</span></div><div style={{ overflowX: "auto" }}><table style={tableStyle}><thead><tr>{["Event", "State / end", "Liquidity / volume", "Markets", "Tags", "Neg-risk evidence"].map((name) => <th style={cellStyle} key={name}>{name}</th>)}</tr></thead><tbody>{page.items.map((event: StructureIntelligenceItem) => <tr key={String(event.event_id)}><td style={{ ...cellStyle, minWidth: 240 }}><strong>{text(event.title)}</strong><br /><span style={{ color: "#9ba8b9" }}>{text(event.slug)}</span></td><td style={cellStyle}><Status value={event.is_open === true ? "available" : "not-published"} /><br /><span style={{ color: "#aeb8c8" }}>{ending(event.end_time_ms)}</span></td><td style={cellStyle}>${number(event.liquidity)}<br /><span style={{ color: "#aeb8c8" }}>${number(event.volume)}</span></td><td style={cellStyle}>{number(event.active_market_count)} active / {number(event.market_count)} total<br /><span style={{ color: "#aeb8c8" }}>{number(event.closed_market_count)} closed</span></td><td style={{ ...cellStyle, maxWidth: 190 }}>{Array.isArray(event.tags) && event.tags.length ? event.tags.join(" · ") : "—"}</td><td style={{ ...cellStyle, maxWidth: 220 }}><strong>{text(event.neg_risk_quality)}</strong><br /><span style={{ color: "#aeb8c8" }}>{text(event.neg_risk_reason)}</span></td></tr>)}</tbody></table></div>{page.next_after && <p style={{ padding: "0 16px", color: "#f6c85f" }}>Showing first 100 events; the API supports cursor pagination for deeper research.</p>}</section>;
}

function GroupTable({ page }: { page: StructureIntelligencePage }) {
  if (page.status !== "available" || page.items.length === 0) return null;
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginBottom: 16 }}><div style={{ padding: "14px 16px", borderBottom: "1px solid #303030" }}><strong>Structural risk queue</strong><span style={{ color: "#aeb8c8", marginLeft: 10 }}>Neg-risk groups requiring evidence review</span></div><div style={{ overflowX: "auto" }}><table style={tableStyle}><thead><tr>{["Group", "Event", "Type", "Named / expected", "Quality", "Reason"].map((name) => <th style={cellStyle} key={name}>{name}</th>)}</tr></thead><tbody>{page.items.map((group) => <tr key={String(group.group_id)}><td style={{ ...cellStyle, maxWidth: 220, overflowWrap: "anywhere" }}>{text(group.group_id)}</td><td style={cellStyle}>{text(group.event_id)}</td><td style={cellStyle}>{text(group.neg_risk_type)}</td><td style={cellStyle}>{number(group.active_named_count)} / {number(group.expected_member_count)}</td><td style={cellStyle}><strong>{text(group.quality)}</strong></td><td style={cellStyle}>{text(group.reason)}</td></tr>)}</tbody></table></div></section>;
}

export default async function StructurePage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const [summary, events, groups] = await Promise.all([readStructureIntelligenceSummary(), readStructureIntelligencePage("events"), readStructureIntelligencePage("groups")]);
  const { data } = overview;
  return <BusinessShell overview={data} title="Structure research" subtitle="Research the market universe as business evidence: what is active, when it ends, where activity is concentrated, and which neg-risk relationships need review.">
    <ProductCard title="Current structure generation" item={data.structure}><p>Generation: {data.structure.generation_key ?? "not published"}</p><p>Published source records: {data.structure.record_count?.toLocaleString() ?? "not published"}</p></ProductCard>
    {summary?.status === "available" ? <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12, padding: "16px 0", marginBottom: 4 }}><Metric label="Events" value={number(summary.event_count)} note="published market themes" /><Metric label="Open now" value={number(summary.open_event_count)} note="active and not closed" /><Metric label="Markets" value={number(summary.market_count)} note="derived event membership" /><Metric label="Projection size" value={`${((summary.projection_octets ?? 0) / 1_000_000).toFixed(1)} MB`} note="bounded Postgres business index" /></section> : <ResearchIndexPending product="Structure intelligence" />}
    {events ? <EventTable page={events} /> : <ResearchIndexPending product="Structure event universe" />}
    {groups && <GroupTable page={groups} />}
    <p style={{ color: "#aeb8c8", fontSize: 13 }}>This view is a bounded research projection, not a full raw-data mirror. Missing values are shown as unknown rather than zero; full source artifacts remain authenticated in R2.</p>
  </BusinessShell>;
}
