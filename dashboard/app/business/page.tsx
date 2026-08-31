import { readBusinessOverview } from "../../lib/business-overview";

export default async function BusinessPage() {
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <main style={{ padding: 24 }}><h1>Business research unavailable</h1><p>{overview.reason}; this is not zero opportunities.</p></main>;
  const { data } = overview;
  const product = (name: string, item: { status: string; generation_key?: string }) => <section><h2>{name}</h2><p>Status: {item.status}</p><p>Generation: {item.generation_key ?? "not provided"}</p></section>;
  return <main style={{ padding: 24, maxWidth: 960 }}><h1>Business research</h1><p>As of: {data.observed_at}</p><p>Eligibility: {data.eligibility.state} ({data.eligibility.reason_code ?? "none"})</p>{product("Structure", data.structure)}{product("Quote", data.quote)}{product("Analysis", data.analysis)}<section><h2>Certified opportunities</h2><p>Status: {data.opportunities.status}</p><p>Count: {data.opportunities.count ?? "not provided"}</p></section><section><h2>Business blockers</h2>{data.blockers.length ? data.blockers.map((item) => <p key={`${item.scope}:${item.code}`}>{item.scope} / {item.code} — {item.impact}</p>) : <p>None</p>}</section></main>;
}
