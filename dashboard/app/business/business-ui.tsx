import type { BusinessOverview, Product, ProductStatus } from "../../lib/business-overview";

const routes = [["Overview", "/business"], ["Structure", "/business/structure"], ["Quotes", "/business/quotes"], ["Analysis", "/business/analysis"], ["Opportunities", "/business/opportunities"]] as const;
const tone: Record<ProductStatus, string> = { available: "#55d68a", lagging: "#f6c85f", "not-published": "#b7b7b7", stale: "#ff8b8b", unavailable: "#ff8b8b" };

export function BusinessShell({ children, overview, title, subtitle }: { children: React.ReactNode; overview: BusinessOverview; title: string; subtitle: string }) {
  return <main style={{ padding: "32px 24px", maxWidth: 1120, margin: "0 auto" }}><header style={{ marginBottom: 28 }}><p style={{ color: "#8ab4f8", margin: "0 0 8px", fontSize: 13 }}>M1 BUSINESS RESEARCH · AS OF {overview.observed_at}</p><h1 style={{ margin: 0 }}>{title}</h1><p style={{ color: "#b7b7b7" }}>{subtitle}</p><p><Status value={overview.eligibility.state === "ready" ? "available" : "lagging"} /> Qualification: {overview.eligibility.state} · {overview.eligibility.reason_code ?? "none"}</p></header><nav aria-label="Business research" style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 28 }}>{routes.map(([label, href]) => <a key={href} href={href} style={{ color: "#9ec5fe" }}>{label}</a>)}</nav>{children}</main>;
}

export function Status({ value }: { value: ProductStatus }) { return <span style={{ color: tone[value], fontWeight: 700 }}>{value.toUpperCase()}</span>; }

export function ProductCard({ title, item, children }: { title: string; item: Product; children?: React.ReactNode }) { return <section style={{ border: "1px solid #303030", borderRadius: 8, padding: 20, marginBottom: 16 }}><h2 style={{ marginTop: 0 }}>{title} <Status value={item.status} /></h2>{item.reason_code && <p>Reason: {item.reason_code}</p>}{children}</section>; }

export function UnavailableBusiness() { return <main style={{ padding: 24 }}><h1>Business research unavailable</h1><p>The atomic business snapshot is unavailable; this is not zero opportunities.</p></main>; }
