import { readBusinessOverview } from "../../../lib/business-overview";
import { readBusinessResearchPage } from "../../../lib/business-research";
import { BusinessShell, ProductCard, ResearchIndexPending, ResearchTable, UnavailableBusiness } from "../business-ui";

export default async function QuotesPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  const page = await readBusinessResearchPage("quotes");
  return <BusinessShell overview={data} title="Quote coverage" subtitle="Current pricing input coverage and its exact Structure parent; lagging never means silently current.">
    <ProductCard title="Current quote generation" item={data.quote}><p>Generation: {data.quote.generation_key ?? "not published"}</p><p>Parent Structure generation: {data.quote.parent_structure_generation_key ?? "not published"}</p><p>Quote records: {data.quote.record_count ?? "not published"}</p></ProductCard>
    {page ? <ResearchTable page={page} product="quotes" /> : <ResearchIndexPending product="Quote" />}
    <p>A LAGGING quote generation was built from an older Structure generation. It remains inspectable evidence, but it is not a valid basis for calling downstream opportunities current.</p>
  </BusinessShell>;
}
