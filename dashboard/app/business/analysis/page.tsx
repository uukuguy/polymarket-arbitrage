import { readBusinessOverview } from "../../../lib/business-overview";
import { BusinessShell, ProductCard, UnavailableBusiness } from "../business-ui";

export default async function AnalysisPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  return <BusinessShell overview={data} title="Analysis funnel" subtitle="The decision funnel is intentionally explicit about what M1 has and has not yet persisted.">
    <ProductCard title="Persisted analysis" item={data.analysis}><p>{data.analysis.status === "not-published" ? "No durable candidate / reject / certification funnel has been published yet." : "A durable analysis projection is published."}</p><p>Opportunity count alone cannot reconstruct no-edge or rejected candidates. Those counts will appear here only after they are written as a versioned analysis projection.</p></ProductCard>
  </BusinessShell>;
}
