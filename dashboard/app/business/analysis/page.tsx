import { readBusinessOverview } from "../../../lib/business-overview";
import { BusinessShell, Metric, ProductCard, UnavailableBusiness } from "../business-ui";

export default async function AnalysisPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  const counts = data.analysis.component_counts ?? {};
  return <BusinessShell overview={data} title="Analysis funnel" subtitle="The decision funnel is intentionally explicit about what M1 has and has not yet persisted.">
    <ProductCard title="Lineage-bound research funnel" item={data.analysis}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 14, marginTop: 14 }}>
        <Metric label="Structure universe" value={(counts.structure_records ?? 0).toLocaleString()} note="published market-universe records" />
        <Metric label="Quote coverage" value={(counts.quote_records ?? 0).toLocaleString()} note="same-lineage quote records" />
        <Metric label="Certified opportunities" value={counts.certified_opportunities?.toLocaleString() ?? "not published"} note="shown only for the same quote generation" />
      </div>
      <p>{data.analysis.status === "not-published" ? "No lineage-bound research funnel has been published yet." : "This funnel reports only published structure, quote, and certified-opportunity facts."}</p>
      <p>Candidate and reject detail is not yet persisted; opportunity count alone cannot reconstruct no-edge or rejected candidates.</p>
    </ProductCard>
  </BusinessShell>;
}
