import { readBusinessOverview } from "../../../lib/business-overview";
import { readBusinessResearchPage, type ResearchItem, type ResearchPage } from "../../../lib/business-research";
import { BusinessShell, ProductCard, ResearchIndexPending, Status, UnavailableBusiness } from "../business-ui";

const tableStyle = { width: "100%", borderCollapse: "collapse" as const, fontSize: 13 };
const cellStyle = { padding: "10px 12px", borderBottom: "1px solid #303030", textAlign: "left" as const, verticalAlign: "top" as const };
const text = (value: unknown) => typeof value === "string" && value ? value : "—";
const price = (value: unknown) => typeof value === "number" ? value.toFixed(3) : "—";
const depth = (value: unknown) => typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "—";

function QuoteEvidenceTable({ page }: { page: ResearchPage }) {
  if (page.status !== "available") return <ResearchIndexPending product="Quote" />;
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginBottom: 16 }}><div style={{ padding: "14px 16px", borderBottom: "1px solid #303030" }}><strong>Executable quote evidence</strong><span style={{ color: "#aeb8c8", marginLeft: 10 }}>{page.items.length} loaded from the current fenced generation</span></div><div style={{ overflowX: "auto" }}><table style={tableStyle}><thead><tr>{["Market", "YES ask / depth", "Execution", "Event", "Neg-risk group"].map((name) => <th style={cellStyle} key={name}>{name}</th>)}</tr></thead><tbody>{page.items.map((quote: ResearchItem) => <tr key={String(quote.token_id)}><td style={{ ...cellStyle, minWidth: 280 }}><strong>{text(quote.slug)}</strong><br /><span style={{ color: "#9ba8b9" }}>Market {text(quote.market_id)}</span></td><td style={cellStyle}><strong>{price(quote.best_ask_price)}</strong><br /><span style={{ color: "#aeb8c8" }}>{depth(quote.best_ask_size)} contracts at ask</span></td><td style={cellStyle}><Status value={quote.terminal_state === "executable" ? "available" : "unavailable"} /><br /><span style={{ color: "#aeb8c8" }}>{text(quote.terminal_state)}</span></td><td style={cellStyle}>{text(quote.event_id)}</td><td style={{ ...cellStyle, maxWidth: 250, overflowWrap: "anywhere" }}>{text(quote.neg_risk_market_id)}</td></tr>)}</tbody></table></div>{page.next_after && <p style={{ padding: "0 16px", color: "#f6c85f" }}>Showing first 100 rows; cursor pagination is available through the API.</p>}</section>;
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
