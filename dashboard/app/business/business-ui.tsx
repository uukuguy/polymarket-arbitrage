import type { BusinessOverview, Product, ProductStatus } from "../../lib/business-overview";
import type { ResearchItem, ResearchPage } from "../../lib/business-research";

const routes = [["Overview", "/business"], ["Structure", "/business/structure"], ["Quotes", "/business/quotes"], ["Analysis", "/business/analysis"], ["Opportunities", "/business/opportunities"]] as const;
const tone: Record<ProductStatus, string> = { available: "#55d68a", lagging: "#f6c85f", "not-published": "#b7b7b7", stale: "#ff8b8b", unavailable: "#ff8b8b" };

export function BusinessShell({ children, overview, title, subtitle }: { children: React.ReactNode; overview: BusinessOverview; title: string; subtitle: string }) {
  return <main style={{ padding: "32px 24px", maxWidth: 1120, margin: "0 auto" }}><header style={{ marginBottom: 28 }}><p style={{ color: "#8ab4f8", margin: "0 0 8px", fontSize: 13 }}>M1 BUSINESS RESEARCH · AS OF {overview.observed_at}</p><h1 style={{ margin: 0 }}>{title}</h1><p style={{ color: "#b7b7b7" }}>{subtitle}</p><p><Status value={overview.eligibility.state === "ready" ? "available" : "lagging"} /> Qualification: {overview.eligibility.state} · {overview.eligibility.reason_code ?? "none"}</p></header><nav aria-label="Business research" style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 28 }}>{routes.map(([label, href]) => <a key={href} href={href} style={{ color: "#9ec5fe" }}>{label}</a>)}</nav>{children}</main>;
}

export function Status({ value }: { value: ProductStatus }) { return <span style={{ color: tone[value], fontWeight: 700 }}>{value.toUpperCase()}</span>; }

export function ProductCard({ title, item, children }: { title: string; item: Product; children?: React.ReactNode }) { return <section style={{ border: "1px solid #303030", borderRadius: 8, padding: 20, marginBottom: 16 }}><h2 style={{ marginTop: 0 }}>{title} <Status value={item.status} /></h2>{item.reason_code && <p>Reason: {item.reason_code}</p>}{children}</section>; }

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
    <div style={{ padding: "14px 16px", display: "flex", justifyContent: "space-between", gap: 16, borderBottom: "1px solid #303030" }}><span><Status value="available" /> {page.items.length.toLocaleString()} materialized rows</span><code style={{ color: "#b7b7b7", overflowWrap: "anywhere" }}>{page.generation_key}</code></div>
    <div style={{ overflowX: "auto" }}><table style={tableStyle}><thead><tr>{[identity, ...fields].map((field) => <th style={cellStyle} key={field}>{field}</th>)}</tr></thead><tbody>{page.items.map((item: ResearchItem) => <tr key={String(item[identity])}>{[identity, ...fields].map((field) => <td style={{ ...cellStyle, maxWidth: field === "row" ? 420 : 240, overflowWrap: "anywhere" }} key={field}>{compact(item[field])}</td>)}</tr>)}</tbody></table></div>
    {page.next_after && <p style={{ padding: "0 16px", color: "#f6c85f" }}>More rows exist. Cursor pagination is available through the API; this dashboard intentionally renders the first 100 rows.</p>}
  </section>;
}
