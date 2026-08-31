import { readBusinessOverview } from "../../../lib/business-overview";
import { BusinessShell, ProductCard, UnavailableBusiness } from "../business-ui";

export default async function OpportunitiesPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  return <BusinessShell overview={data} title="Certified opportunities" subtitle="The final, intentionally small output of M1 — read together with the upstream coverage and lineage pages.">
    <ProductCard title="Current opportunity projection" item={data.opportunities}><p>Certified opportunity count: {data.opportunities.count ?? "not published"}</p><p>Quote generation: {data.opportunities.quote_generation_key ?? "not published"}</p><p>Parent Structure generation: {data.opportunities.parent_structure_generation_key ?? "not published"}</p></ProductCard>
    <p>{data.opportunities.status === "available" && data.opportunities.count === 0 ? "This is a real current zero: the published projection contains no certified opportunities." : "Do not interpret this as a zero unless status is AVAILABLE and the count is exactly 0."}</p>
  </BusinessShell>;
}
