import { readBusinessOverview } from "../../../lib/business-overview";
import { readBusinessResearchPage } from "../../../lib/business-research";
import { BusinessShell, ProductCard, ResearchIndexPending, ResearchTable, UnavailableBusiness } from "../business-ui";

export default async function StructurePage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  const page = await readBusinessResearchPage("structure");
  return <BusinessShell overview={data} title="Structure research" subtitle="The published market universe that all downstream M1 products must be able to trace back to.">
    <ProductCard title="Current structure generation" item={data.structure}>
      <p>Generation: {data.structure.generation_key ?? "not published"}</p><p>Published records: {data.structure.record_count ?? "not published"}</p><h3>Component coverage</h3>
      {data.structure.component_counts ? <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 8 }}>{Object.entries(data.structure.component_counts).sort().map(([name, count]) => <div key={name} style={{ border: "1px solid #303030", padding: 10 }}><strong>{count.toLocaleString()}</strong><br /><span style={{ color: "#b7b7b7" }}>{name}</span></div>)}</div> : <p>Component counts are not published.</p>}
    </ProductCard>
    {page ? <ResearchTable page={page} product="structure" /> : <ResearchIndexPending product="Structure" />}
    <p>These are component records, not a claim that every number is a tradable market. Use the generation key to compare any downstream quote or opportunity lineage.</p>
  </BusinessShell>;
}
