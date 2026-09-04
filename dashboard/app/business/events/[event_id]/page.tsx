import { readBusinessOverview } from "../../../../lib/business-overview";
import { readEventResearchDetail } from "../../../../lib/business-research";
import { BusinessShell, ResearchIndexPending, UnavailableBusiness } from "../../business-ui";

const record = (value: unknown): Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const money = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "Unknown";
export default async function EventResearchPage({ params, searchParams }: { params: Promise<{ event_id: string }>; searchParams: Promise<{ focus_group_id?: string; observed_generation?: string }> }) {
  const [{ event_id }, query] = await Promise.all([params, searchParams]); const overview = await readBusinessOverview(); if (overview.status === "unavailable") return <UnavailableBusiness />;
  const detail = await readEventResearchDetail(event_id, query.focus_group_id, query.observed_generation); if (!detail || detail.status !== "available") return <BusinessShell overview={overview.data} title="Event research" subtitle="Current lineage-bound investment research evidence."><ResearchIndexPending product="Event research" /></BusinessShell>;
  const event = record(detail.event); return <BusinessShell overview={overview.data} title={typeof event.title === "string" ? event.title : event_id} subtitle="Research evidence only — not a Certified opportunity or trading authorization.">
    <section><h2>Structure evidence</h2><p>Open event · {typeof event.end_time_ms === "number" ? new Date(event.end_time_ms).toLocaleString() : "end unknown"}</p><p>Liquidity {money(event.liquidity)} · Volume {money(event.volume)} · {typeof event.market_count === "number" ? event.market_count : "Unknown"} markets</p></section>
    <section><h2>Quote coverage</h2><p>{detail.state_counts?.["incomplete-coverage"] ?? 0} coverage gaps · {detail.state_counts?.["no-edge"] ?? 0} complete/no-edge groups</p></section>
    <section><h2>Analysis</h2><p>Positive groups are theoretical gross evidence; fees, slippage, and simultaneous execution are not assessed.</p>{detail.groups.map((value) => { const group=record(value); const candidate=record(group.candidate); return <article key={String(group.group_id)} style={{ borderTop:"1px solid #303030", padding:"12px 0" }}><strong>{String(group.group_id)}</strong><br />{String(group.candidate_state)} · Gross profit {money(candidate.gross_profit_usd)} · Capital required {money(candidate.capital_required_usd)} · Gross ROI {typeof candidate.gross_roi_bps === "number" ? `${candidate.gross_roi_bps.toFixed(0)} bps` : "Unknown"}</article>; })}</section>
    <details><summary>Lineage and limitations</summary><pre>{JSON.stringify(detail.anchor, null, 2)}</pre><p>{detail.cautions?.join(" ")}</p></details>
  </BusinessShell>;
}
