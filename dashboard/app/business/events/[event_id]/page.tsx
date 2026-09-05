import { readBusinessOverview } from "../../../../lib/business-overview";
import { readEventResearchDetail, type EventResearchDetail, type EventResearchGroup } from "../../../../lib/business-research";
import { BusinessShell, Metric, UnavailableBusiness } from "../../business-ui";

const SOURCE_FOCUS = new Set(["structure", "quotes", "analysis"]);
const record = (value: unknown): Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const text = (value: unknown) => typeof value === "string" && value ? value : "Unknown";
const number = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? value.toLocaleString() : "Unknown";
const money = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "Unknown";
const endTime = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? new Date(value).toLocaleString() : "Unknown";
const sectionStyle = { border: "1px solid #2c3442", background: "#10151e", borderRadius: 9, padding: 16, marginBottom: 14 };
const focusStyle = { outline: "3px solid #8ab4f8", outlineOffset: 3 };
const groupIdStyle = { overflowWrap: "anywhere", wordBreak: "break-word" } as const;

function count(value: Record<string, unknown>, key: string) { return value[key] === null || value[key] === undefined ? "Unknown" : number(value[key]); }
function unavailableCopy(detail: EventResearchDetail | null) {
  if (detail?.status === "not-published") return "This event’s current research detail has not been published yet; it is not a zero-result event.";
  if (detail?.status === "unavailable") return "This event is unavailable because it is closed, expired, or no longer on the current lineage; it is not a zero-result event.";
  return "The current event research detail could not be read. This is not a zero-result event.";
}

export default async function EventResearchPage({ params, searchParams }: {
  params: Promise<{ event_id: string }>;
  searchParams: Promise<{ from?: string; focus_group_id?: string; observed_generation?: string }>;
}) {
  const [{ event_id }, query] = await Promise.all([params, searchParams]);
  const overview = await readBusinessOverview();
  if (overview.status === "unavailable") return <UnavailableBusiness />;
  const source = SOURCE_FOCUS.has(query.from ?? "") ? query.from as "structure" | "quotes" | "analysis" : "structure";
  const detail = await readEventResearchDetail(event_id, {
    focusGroupId: query.focus_group_id,
    observedGeneration: query.observed_generation,
  });
  if (!detail || detail.status !== "available") return <BusinessShell overview={overview.data} title="Event research" subtitle="Current lineage-bound research evidence.">
    <section style={sectionStyle}><h2 style={{ marginTop: 0 }}>Event research {detail?.status ?? "unavailable"}</h2><p>{unavailableCopy(detail)}</p></section>
  </BusinessShell>;

  const event = record(detail.event);
  const totals = record(detail.quote_coverage);
  const analysis = record(detail.analysis);
  const focus = detail.focused_group ?? null;
  const blockers = Array.isArray(detail.blockers) ? detail.blockers as Array<{ code: string; count: number }> : [];
  const cautions = detail.cautions ?? [];
  const groups = focus ? [focus, ...detail.groups.filter((group) => group.group_id !== focus.group_id)] : detail.groups;
  const positives = groups.filter((group) => group.candidate_state === "positive-edge");

  return <BusinessShell overview={overview.data} title={text(event.title)} subtitle="Research evidence only — not a certified opportunity or trading authorization.">
    <section style={sectionStyle}><p style={{ marginTop: 0, color: "#8ab4f8", fontWeight: 700 }}>CURRENT EVENT RESEARCH · INITIAL FOCUS: {source.toUpperCase()}</p><div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
      <Metric label="Status" value="Open / current lineage" /><Metric label="End" value={endTime(event.end_time_ms)} /><Metric label="Liquidity" value={money(event.liquidity)} /><Metric label="Volume" value={money(event.volume)} /><Metric label="Market breadth" value={`${number(event.active_market_count)} active / ${number(event.market_count)} total`} /><Metric label="Research stage" value={text(detail.research_stage)} />
    </div><p style={{ marginBottom: 0 }}>Blockers: {blockers.length ? blockers.map((blocker) => `${text(blocker.code)} (${number(blocker.count)})`).join(", ") : "None explicitly published"}</p></section>

    <section id="structure" style={{ ...sectionStyle, ...(source === "structure" ? focusStyle : {}) }} aria-labelledby="structure-heading"><h2 id="structure-heading" style={{ marginTop: 0 }}>Structure evidence</h2><p>{count(record(detail.structure), "group_count")} bounded groups in the current structure generation.</p>{groups.map((group) => { const structure = group.structure; return <article key={group.group_id} style={{ borderTop: "1px solid #303030", padding: "12px 0" }}><strong style={groupIdStyle}>{group.group_id}{focus?.group_id === group.group_id ? " · focused group" : ""}</strong><br />Quality {text(structure.neg_risk_quality)} · Reason {text(structure.neg_risk_reason)} · Expected members {count(structure, "expected_member_count")}</article>; })}</section>

    <section id="quotes" style={{ ...sectionStyle, ...(source === "quotes" ? focusStyle : {}) }} aria-labelledby="quotes-heading"><h2 id="quotes-heading" style={{ marginTop: 0 }}>Quote coverage</h2><p>Expected {count(totals, "expected")} · Observed {count(totals, "observed")} · Executable {count(totals, "executable")} · Non-executable {count(totals, "non_executable")} · Missing {count(totals, "missing")}</p>{groups.map((group) => { const coverage = group.quote_coverage; return <article key={group.group_id} style={{ borderTop: "1px solid #303030", padding: "12px 0" }}><strong style={groupIdStyle}>{group.group_id}</strong><br />{text(coverage.coverage_state)} · Missing {count(coverage, "missing")} · Observed {count(coverage, "observed")} / Expected {count(coverage, "expected")}</article>; })}</section>

    <section id="analysis" style={{ ...sectionStyle, ...(source === "analysis" ? focusStyle : {}) }} aria-labelledby="analysis-heading"><h2 id="analysis-heading" style={{ marginTop: 0 }}>Analysis</h2><p>Theoretical gross evidence only. Fees, slippage, simultaneous execution, resolution, and settlement delay are not assessed.</p>{positives.length ? positives.map((group) => { const candidate = group.candidate; return <article key={group.group_id} style={{ borderTop: "1px solid #303030", padding: "12px 0" }}><strong style={groupIdStyle}>{group.group_id}</strong><br />Capital required {money(candidate.capital_required_usd)} · Gross profit {money(candidate.gross_profit_usd)} · Gross ROI {typeof candidate.gross_roi_bps === "number" ? `${candidate.gross_roi_bps.toFixed(2)} bps` : "Unknown"} · research-only</article>; }) : <p>No positive group edge is published for this event.</p>}<p>Published analysis states: {Object.entries(record(analysis.state_counts)).map(([state, value]) => `${state} (${number(value)})`).join(", ") || "Unknown"}</p></section>

    <section style={{ ...sectionStyle, borderColor: "#f6c85f" }} aria-labelledby="risks-heading"><h2 id="risks-heading" style={{ marginTop: 0 }}>Risks and unknowns</h2><p>Execution quality, fees, slippage, oracle resolution, and settlement delay are not assessed. Missing source fields are shown as Unknown, not zero.</p></section>
    <details style={sectionStyle}><summary>Lineage and provenance</summary><pre style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{JSON.stringify(detail.anchor, null, 2)}</pre><p>{cautions.join(" ")}</p></details>
  </BusinessShell>;
}
