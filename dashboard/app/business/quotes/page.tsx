import { readBusinessOverview } from "../../../lib/business-overview";
import { readBusinessResearchPage, type ResearchItem, type ResearchPage } from "../../../lib/business-research";
import { BusinessShell, ProductCard, ResearchIndexPending, Status, UnavailableBusiness } from "../business-ui";

const tableStyle = { width: "100%", borderCollapse: "collapse" as const, fontSize: 13 };
const cellStyle = { padding: "10px 12px", borderBottom: "1px solid #303030", textAlign: "left" as const, verticalAlign: "top" as const };
const text = (value: unknown) => typeof value === "string" && value ? value : "—";
const price = (value: unknown) => typeof value === "number" ? value.toFixed(3) : "—";
const depth = (value: unknown) => typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "—";
const money = (value: unknown) => typeof value === "number" ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "—";
const record = (value: unknown): Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const shortId = (value: unknown) => typeof value === "string" && value ? `${value.slice(0, 10)}…${value.slice(-6)}` : "No group";
const formatEndTime = (value: unknown) => typeof value === "number" && Number.isFinite(value)
  ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value))
  : "end unknown";
const reasonLabel = (value: unknown) => {
  const reasons = Array.isArray(value) ? value : [];
  if (reasons.includes("meaningful-executable-depth") && reasons.includes("non-neutral-yes-price")) return "Deep, non-neutral YES";
  if (reasons.includes("meaningful-executable-depth")) return "Executable depth";
  if (reasons.includes("non-neutral-yes-price")) return "Non-neutral YES";
  if (reasons.includes("missing-or-invalid-quote")) return "Quote needs review";
  if (reasons.includes("not-executable")) return "Not executable";
  return "Low-priority evidence";
};

function QuoteEvidenceTable({ page }: { page: ResearchPage }) {
  if (page.status !== "available") return <ResearchIndexPending product="Quote" />;
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginBottom: 16 }}><div style={{ padding: "14px 16px", borderBottom: "1px solid #303030" }}><strong>Research leads</strong><span style={{ color: "#aeb8c8", marginLeft: 10 }}>{page.items.length} loaded from the current fenced generation</span><p style={{ color: "#aeb8c8", marginBottom: 0 }}>Ordered by executable depth and non-neutral YES price; research priority, not a certified opportunity.</p></div><div style={{ overflowX: "auto" }}><table style={tableStyle}><thead><tr>{["Research signal", "Market", "Executable quote", "Event", "Neg-risk context", "Data quality"].map((name) => <th style={cellStyle} key={name}>{name}</th>)}</tr></thead><tbody>{page.items.map((quote: ResearchItem) => { const discovery = record(quote.discovery); const event = record(quote.event_context); const group = record(quote.neg_risk_context); return <tr key={String(quote.token_id)}><td style={{ ...cellStyle, minWidth: 180 }}><strong>{reasonLabel(discovery.reasons)}</strong><br /><span style={{ color: "#aeb8c8" }}>Score {typeof discovery.score === "number" ? discovery.score.toFixed(0) : "—"} · {typeof discovery.price_extremity_bps === "number" ? discovery.price_extremity_bps.toFixed(0) : "—"} bps from neutral</span></td><td style={{ ...cellStyle, minWidth: 250 }}><strong>{text(quote.slug)}</strong><br /><span style={{ color: "#9ba8b9" }}>Market {text(quote.market_id)}</span></td><td style={cellStyle}><strong>{money(discovery.executable_notional_usd)} executable notional</strong><br /><span style={{ color: "#aeb8c8" }}>{price(quote.best_ask_price)} YES ask · {depth(quote.best_ask_size)} contracts</span></td><td style={{ ...cellStyle, minWidth: 180 }}><strong>{event.status === "available" ? text(event.title) : text(quote.event_id)}</strong><br /><span style={{ color: "#aeb8c8" }}>{event.status === "available" ? `${event.is_open === true ? "Open" : "Closed/unknown"} · ${formatEndTime(event.end_time_ms)}` : "Context not indexed"}</span></td><td style={{ ...cellStyle, minWidth: 170 }}><strong>{group.status === "available" ? shortId(group.group_id) : "Context not indexed"}</strong><br /><span style={{ color: "#aeb8c8" }}>{group.status === "available" ? `${text(group.quality)} · ${group.expected_member_count ?? "—"} members` : text(quote.neg_risk_market_id)}</span></td><td style={cellStyle}><Status value={quote.terminal_state === "executable" ? "available" : "unavailable"} /><br /><span style={{ color: "#aeb8c8" }}>{text(quote.terminal_state)}</span></td></tr>; })}</tbody></table></div>{page.next_after && <p style={{ padding: "0 16px", color: "#f6c85f" }}>Showing the first discovery page; cursor pagination continues the same global ranking.</p>}</section>;
}

export default async function QuotesPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  const page = await readBusinessResearchPage("quotes");
  return <BusinessShell overview={data} title="Quote coverage" subtitle="Current pricing input coverage and its exact Structure parent; lagging never means silently current.">
    <ProductCard title="Current quote generation" item={data.quote}><p>Generation: {data.quote.generation_key ?? "not published"}</p><p>Parent Structure generation: {data.quote.parent_structure_generation_key ?? "not published"}</p><p>Quote records: {data.quote.record_count ?? "not published"}</p></ProductCard>
    {page ? <QuoteEvidenceTable page={page} /> : <ResearchIndexPending product="Quote" />}
    <p>A LAGGING quote generation was built from an older Structure generation. It remains inspectable evidence, but it is not a valid basis for calling downstream opportunities current.</p>
  </BusinessShell>;
}
