import { readBusinessOverview } from "../../../lib/business-overview";
import { BusinessShell, ProductCard, UnavailableBusiness } from "../business-ui";

export default async function StructurePage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const { data } = overview;
  return <BusinessShell overview={data} title="Structure research" subtitle="The published market universe that all downstream M1 products must be able to trace back to.">
    <ProductCard title="Current structure generation" item={data.structure}>
      <p>Generation: {data.structure.generation_key ?? "not published"}</p><p>Published records: {data.structure.record_count ?? "not published"}</p><h3>Component coverage</h3>
      {data.structure.component_counts ? <ul>{Object.entries(data.structure.component_counts).sort().map(([name, count]) => <li key={name}>{name}: {count.toLocaleString()}</li>)}</ul> : <p>Component counts are not published.</p>}
    </ProductCard>
    <p>These are component records, not a claim that every number is a tradable market. Use the generation key to compare any downstream quote or opportunity lineage.</p>
  </BusinessShell>;
}
