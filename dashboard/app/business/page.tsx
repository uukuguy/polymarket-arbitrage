import { readBusinessOverview } from "../../lib/business-overview";
import { BusinessShell, ProductCard, Status, UnavailableBusiness } from "./business-ui";

export default async function BusinessPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  return <BusinessShell overview={data} title="Business research" subtitle="A lineage-consistent view of what M1 has published, what is ready to use, and what is still not observable.">
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 16 }}>
      <ProductCard title="Structure" item={data.structure}><p>{data.structure.record_count ?? "—"} records across the published market structure.</p><a href="/business/structure">Inspect structure →</a></ProductCard>
      <ProductCard title="Quotes" item={data.quote}><p>{data.quote.record_count ?? "—"} current quote records.</p><a href="/business/quotes">Inspect quote coverage →</a></ProductCard>
      <ProductCard title="Analysis" item={data.analysis}><p>Decision funnel: {data.analysis.status === "not-published" ? "not yet projected" : "published"}.</p><a href="/business/analysis">Inspect analysis truth →</a></ProductCard>
      <ProductCard title="Certified opportunities" item={data.opportunities}><p><strong>{data.opportunities.count ?? "—"}</strong> current certified opportunities.</p><a href="/business/opportunities">Inspect opportunities →</a></ProductCard>
    </div>
    <section style={{ marginTop: 24 }}><h2>What to trust now</h2><p><Status value={data.opportunities.status} /> opportunities are a real zero only when status is AVAILABLE and count is 0. Any other status means the current result is not publishable business truth.</p></section>
    <section><h2>Business blockers</h2>{data.blockers.length ? data.blockers.map((item) => <p key={`${item.scope}:${item.code}`}>{item.scope} / {item.code} — {item.impact}</p>) : <p>No published blockers. Qualification can still be paused while its durable evidence is being built.</p>}</section>
  </BusinessShell>;
}
