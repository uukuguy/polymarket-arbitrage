import { readBusinessOverview } from "../../../lib/business-overview";
import { readStructureIntelligencePage, readStructureIntelligenceSummary, type StructureIntelligenceItem } from "../../../lib/business-research";
import { BusinessShell, Metric, ResearchIndexPending, UnavailableBusiness } from "../business-ui";

const text = (v: unknown) => typeof v === "string" && v ? v : "Unknown";
const number = (v: unknown) => typeof v === "number" ? v.toLocaleString() : "Unknown";
const eventLinkStyle = { color: "#c8dcff", outline: "3px solid transparent", outlineOffset: 3 };
export default async function StructurePage() {
  const overview = await readBusinessOverview(); if (overview.status === "unavailable") return <UnavailableBusiness />;
  const [summary, events] = await Promise.all([readStructureIntelligenceSummary(), readStructureIntelligencePage("events", { openOnly: true })]);
  return <BusinessShell overview={overview.data} title="Structure research" subtitle="Current market structures worth researching before price and execution analysis."><style>{`.focusVisible:focus-visible { outline: 3px solid #8ab4f8 !important; outline-offset: 3px; }`}</style>
    {summary?.status === "available" && <section style={{ display:"flex", gap:20, flexWrap:"wrap" }}><Metric label="Open events" value={number(summary.open_event_count)} /><Metric label="Markets" value={number(summary.market_count)} /></section>}
    {!events || events.status !== "available" ? <ResearchIndexPending product="Structure intelligence" /> : <section><h2>Event universe</h2><table style={{ width:"100%", borderCollapse:"collapse" }}><thead><tr><th>Event</th><th>End</th><th>Liquidity</th><th>Markets</th><th>Neg-risk</th></tr></thead><tbody>{events.items.map((event: StructureIntelligenceItem) => <tr key={String(event.event_id)}><td><a className="focusVisible" style={eventLinkStyle} title="Open event research from Structure" href={`/business/events/${encodeURIComponent(String(event.event_id))}?from=structure`}>{text(event.title)}</a><br /><small>{text(event.slug)}</small></td><td>{typeof event.end_time_ms === "number" ? new Date(event.end_time_ms).toLocaleString() : "Unknown"}</td><td>${number(event.liquidity)}</td><td>{number(event.active_market_count)} active / {number(event.market_count)}</td><td>{text(event.neg_risk_quality)}<br /><small>{text(event.neg_risk_reason)}</small></td></tr>)}</tbody></table></section>}
  </BusinessShell>;
}
