import type { BusinessOverview, Product, ProductStatus } from "../../lib/business-overview";
import type { ResearchItem, ResearchPage } from "../../lib/business-research";

const routes = [["Overview", "/business"], ["Structure", "/business/structure"], ["Quotes", "/business/quotes"], ["Analysis", "/business/analysis"], ["Opportunities", "/business/opportunities"], ["Runtime", "/control-plane"]] as const;
const tone: Record<ProductStatus, string> = { available: "#55d68a", lagging: "#f6c85f", "not-published": "#b7b7b7", stale: "#ff8b8b", unavailable: "#ff8b8b" };

export function BusinessShell({ children, overview, title, subtitle }: { children: React.ReactNode; overview: BusinessOverview; title: string; subtitle: string }) {
  return <main style={{ padding: "28px 24px 56px", maxWidth: 1440, margin: "0 auto" }}><header style={{ marginBottom: 18, borderBottom: "1px solid #242a35", paddingBottom: 18 }}><div style={{ display: "flex", justifyContent: "space-between", gap: 20, alignItems: "start", flexWrap: "wrap" }}><div><p style={{ color: "#8ab4f8", margin: "0 0 7px", fontSize: 12, fontWeight: 700, letterSpacing: ".09em" }}>M1 BUSINESS INTELLIGENCE · SNAPSHOT {overview.observed_at}</p><h1 style={{ margin: 0, fontSize: 30, letterSpacing: "-.03em" }}>{title}</h1><p style={{ color: "#aeb8c8", maxWidth: 760, marginBottom: 0 }}>{subtitle}</p></div><div style={{ minWidth: 240, background: "#111827", border: "1px solid #283548", padding: "10px 13px", borderRadius: 8, fontSize: 13 }}><Status value={overview.eligibility.state === "ready" ? "available" : "lagging"} /> Qualification <strong>{overview.eligibility.state}</strong><br /><span style={{ color: "#aeb8c8" }}>{overview.eligibility.reason_code ?? "no published blocker"}</span></div></div></header><nav aria-label="Business research" style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 20 }}>{routes.map(([label, href]) => <a key={href} href={href} style={{ color: "#c8dcff", border: "1px solid #2d3b50", borderRadius: 999, padding: "6px 10px", fontSize: 13, textDecoration: "none" }}>{label}</a>)}</nav>{children}</main>;
}

export function Status({ value }: { value: ProductStatus }) { return <span style={{ color: tone[value], fontWeight: 700 }}>{value.toUpperCase()}</span>; }

export function ProductCard({ title, item, children }: { title: string; item: Product; children?: React.ReactNode }) { return <section style={{ border: "1px solid #2c3442", background: "#10151e", borderRadius: 9, padding: 16, marginBottom: 14 }}><div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}><h2 style={{ margin: 0, fontSize: 16 }}>{title}</h2><Status value={item.status} /></div>{item.reason_code && <p style={{ color: "#f6c85f", fontSize: 13, margin: "9px 0 0" }}>Reason: {item.reason_code}</p>}{children}</section>; }

export function Metric({ label, value, note }: { label: string; value: React.ReactNode; note?: React.ReactNode }) { return <div style={{ minWidth: 0, borderLeft: "2px solid #31598a", padding: "3px 0 3px 10px" }}><div style={{ color: "#9ba8b9", fontSize: 11, letterSpacing: ".06em", textTransform: "uppercase" }}>{label}</div><div style={{ color: "#f2f6ff", fontWeight: 750, fontSize: 20, lineHeight: 1.25, overflowWrap: "anywhere" }}>{value}</div>{note && <div style={{ color: "#8d9aab", fontSize: 12, marginTop: 3 }}>{note}</div>}</div>; }

export function UnavailableBusiness() { return <main style={{ padding: 24 }}><h1>Business research unavailable</h1><p>The atomic business snapshot is unavailable; this is not zero opportunities.</p></main>; }

export function ResearchIndexPending({ product }: { product: string }) { return <section style={{ border: "1px solid #f6c85f", borderRadius: 8, padding: 16, marginBottom: 16 }}><h2 style={{ marginTop: 0 }}>Detailed {product} index unavailable</h2><p>The published summary above remains valid. Its bounded detail index is either not deployed yet or has not been materialized for this generation; this is not a zero-row result.</p></section>; }

const tableStyle = { width: "100%", borderCollapse: "collapse" as const, fontSize: 13 };
const cellStyle = { padding: "10px 12px", borderBottom: "1px solid #303030", textAlign: "left" as const, verticalAlign: "top" as const };
function compact(value: unknown) { if (value === null || value === undefined) return "—"; if (typeof value === "object") return JSON.stringify(value).slice(0, 140); return String(value); }

export function ResearchTable({ page, product }: { page: ResearchPage; product: "structure" | "quotes" }) {
  if (page.status !== "available") return <ProductCard title={`${product} research index`} item={{ status: page.status, reason_code: page.reason_code }}><p>{page.reason_code ?? "This product has not published a current generation."}</p></ProductCard>;
  const identity = product === "structure" ? "entity_id" : "token_id";
  const fields = product === "structure" ? ["component", "source_cursor", "row"] : ["market_id", "condition_id", "best_bid", "best_ask", "updated_at"];
  return <section style={{ border: "1px solid #303030", borderRadius: 8, overflow: "hidden", marginBottom: 16 }}>
    <div style={{ padding: "14px 16px", display: "flex", justifyContent: "space-between", gap: 16, borderBottom: "1px solid #303030" }}><span><Status value="available" /> {page.items.length.toLocaleString()} loaded / {(page.indexed_record_count ?? page.items.length).toLocaleString()} indexed {page.source_record_count === undefined ? "" : `from ${page.source_record_count.toLocaleString()} source records`}</span><code style={{ color: "#b7b7b7", overflowWrap: "anywhere" }}>{page.generation_key}</code></div>
    <div style={{ overflowX: "auto" }}><table style={tableStyle}><thead><tr>{[identity, ...fields].map((field) => <th style={cellStyle} key={field}>{field}</th>)}</tr></thead><tbody>{page.items.map((item: ResearchItem) => <tr key={String(item[identity])}>{[identity, ...fields].map((field) => <td style={{ ...cellStyle, maxWidth: field === "row" ? 420 : 240, overflowWrap: "anywhere" }} key={field}>{compact(item[field])}</td>)}</tr>)}</tbody></table></div>
    {page.next_after && <p style={{ padding: "0 16px", color: "#f6c85f" }}>More rows exist. Cursor pagination is available through the API; this dashboard intentionally renders the first 100 rows.</p>}
  </section>;
}
