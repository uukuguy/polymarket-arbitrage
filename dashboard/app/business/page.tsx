import { readBusinessOverview } from "../../lib/business-overview";
import { BusinessShell, Metric, ProductCard, Status, UnavailableBusiness } from "./business-ui";

export default async function BusinessPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  return <BusinessShell overview={data} title="Business research" subtitle="A lineage-consistent view of what M1 has published, what is ready to use, and what is still not observable.">
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(255px, 1fr))", gap: 12 }}>
      <ProductCard title="Structure universe" item={data.structure}><div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginTop: 14 }}><Metric label="Published records" value={(data.structure.record_count ?? 0).toLocaleString()} /><Metric label="Browsable index" value={(data.structure.indexed_record_count ?? 0).toLocaleString()} note="compact research index" /></div><p><a href="/business/structure">Inspect structure →</a></p></ProductCard>
      <ProductCard title="Quote coverage" item={data.quote}><div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginTop: 14 }}><Metric label="Published quotes" value={(data.quote.record_count ?? 0).toLocaleString()} /><Metric label="Browsable quotes" value={(data.quote.indexed_record_count ?? 0).toLocaleString()} note="current generation only" /></div><p><a href="/business/quotes">Inspect quote coverage →</a></p></ProductCard>
      <ProductCard title="Analysis funnel" item={data.analysis}><Metric label="Decision projection" value={data.analysis.status === "not-published" ? "Not published" : "Published"} note="candidate, reject and certification stages" /><p><a href="/business/analysis">Inspect analysis truth →</a></p></ProductCard>
      <ProductCard title="Certified opportunities" item={data.opportunities}><Metric label="Current result" value={data.opportunities.count ?? "—"} note="zero is meaningful only for AVAILABLE" /><p><a href="/business/opportunities">Inspect opportunities →</a></p></ProductCard>
    </div>
    <section style={{ marginTop: 24 }}><h2>What to trust now</h2><p><Status value={data.opportunities.status} /> opportunities are a real zero only when status is AVAILABLE and count is 0. Any other status means the current result is not publishable business truth.</p></section>
    <section><h2>Business blockers</h2>{data.blockers.length ? data.blockers.map((item) => <p key={`${item.scope}:${item.code}`}>{item.scope} / {item.code} — {item.impact}</p>) : <p>No published blockers. Qualification can still be paused while its durable evidence is being built.</p>}</section>
  </BusinessShell>;
}
