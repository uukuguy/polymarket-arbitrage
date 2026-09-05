"""Source contracts for the atomic M1 business research page."""

import subprocess
from pathlib import Path


def test_business_page_uses_only_the_atomic_business_overview_reader() -> None:
    page = Path("dashboard/app/business/page.tsx").read_text()
    unavailable = Path("dashboard/app/business/business-ui.tsx").read_text()
    reader = Path("dashboard/lib/business-overview.ts").read_text()

    assert "readBusinessOverview" in page
    assert "this is not zero opportunities" in unavailable
    assert "/perception/opportunities" not in page
    assert "/perception/control-plane" not in page
    assert "m1.business-overview.v1" in reader
    assert "business-overview-unavailable" in reader


def test_business_overview_reader_retries_one_transient_authority_read() -> None:
    reader = Path("dashboard/lib/business-overview.ts").read_text()

    assert "const BUSINESS_OVERVIEW_READ_ATTEMPTS = 2;" in reader
    assert "for (let attempt = 0; attempt < BUSINESS_OVERVIEW_READ_ATTEMPTS; attempt += 1)" in reader
    assert "if (response.ok && data) return { status: \"available\", data };" in reader


def test_business_page_exposes_quote_and_opportunity_lineage() -> None:
    quote_page = Path("dashboard/app/business/quotes/page.tsx").read_text()
    opportunity_page = Path("dashboard/app/business/opportunities/page.tsx").read_text()

    assert "parent_structure_generation_key" in quote_page
    assert "quote_generation_key" in opportunity_page


def test_business_research_routes_are_separated_by_truth_layer() -> None:
    root = Path("dashboard/app/business")

    assert (root / "structure/page.tsx").is_file()
    assert (root / "quotes/page.tsx").is_file()
    assert (root / "analysis/page.tsx").is_file()
    assert (root / "opportunities/page.tsx").is_file()
    analysis_page = (root / "analysis/page.tsx").read_text()
    assert "readBusinessResearchPage(\"analysis\")" in analysis_page
    assert "positive-edge" in analysis_page
    assert "Candidate analysis is not a certified opportunity" in analysis_page
    assert "structure_records" in analysis_page
    assert "quote_records" in analysis_page
    assert "certified_opportunities" in analysis_page
    assert "gross profit" in analysis_page
    assert "capital required" in analysis_page
    assert "gross ROI" in analysis_page
    assert "executable economic value" not in analysis_page
    assert "real current zero" in (root / "opportunities/page.tsx").read_text()


def test_structure_default_reads_only_open_unexpired_events() -> None:
    page = Path("dashboard/app/business/structure/page.tsx").read_text()

    assert 'readStructureIntelligencePage("events", { openOnly: true })' in page


def test_quote_coverage_renders_health_not_price_discovery() -> None:
    page = Path("dashboard/app/business/quotes/page.tsx").read_text()

    assert "readQuoteCoveragePage" in page
    assert "Group coverage health" in page
    assert "Coverage gap" in page
    assert "Active coverage gaps" in page
    assert "Price extremity is intentionally not a signal" in page
    assert "price_extremity_bps" not in page
    assert "formatEndTime" in page


def test_quote_coverage_decoder_checks_health_contract() -> None:
    reader = Path("dashboard/lib/business-research.ts").read_text()

    assert "m1.quote-coverage-page.v1" in reader
    assert "coverage-gap" in reader
    assert "readQuoteCoveragePage" in reader


def test_event_workbench_has_one_strict_detail_decoder() -> None:
    reader = Path("dashboard/lib/business-research.ts").read_text()

    assert "decodeEventResearchDetail" in reader
    assert "m1.event-research-detail.v1" in reader
    assert "validEventResearchGroup" in reader
    assert "validEventResearchAnchor" in reader
    assert "Number.isFinite" in reader
    assert "nonNegativeInteger" in reader
    assert "focusGroupId" in reader
    assert "focus_group_id" in reader
    assert "value.groups.length !== 0" in reader


def test_event_workbench_rejects_untrusted_detail_shapes() -> None:
    reader = Path("dashboard/lib/business-research.ts").read_text()

    # The decoder must fence malformed lineage, non-finite economics, negative
    # counts, foreign focus IDs, and unavailable envelopes carrying facts.
    for contract in (
        "validEventResearchAnchor",
        "finiteNonNegative",
        "nonNegativeInteger",
        "focused_group",
        "value.status !== \"available\"",
    ):
        assert contract in reader


def test_event_workbench_decoder_requires_the_requested_focused_group() -> None:
    """A requested focus is an authority constraint, not a presentation hint."""
    script = r'''
const fs = require("fs");
const ts = require("./dashboard/node_modules/typescript");
const source = fs.readFileSync("dashboard/lib/business-research.ts", "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
const module = { exports: {} };
new Function("exports", "module", "require", compiled)(module.exports, module, require);
const group = (group_id) => ({ group_id, structure: {}, candidate_state: "no-edge", candidate: {}, quote_coverage: { expected: 0, observed: 0, executable: 0, non_executable: 0, missing: 0, coverage_state: "complete-executable" } });
const focused = group("group:requested");
const base = { schema_version: "m1.event-research-detail.v1", status: "available", event_id: "event:one", event: {}, anchor: { quote_generation_key: "quote:one", structure_generation_key: "structure:one", changed_since_entry: false, materialized_at: "2026-09-06T00:00:00Z" }, state_counts: {}, structure: { group_count: 2 }, quote_coverage: { expected: 0, observed: 0, executable: 0, non_executable: 0, missing: 0 }, analysis: { research_only: true }, blockers: [], cautions: [], groups: [focused, group("group:other")] };
const decode = module.exports.decodeEventResearchDetail;
const result = {
  matching: decode({ ...base, focused_group: focused }, "group:requested") !== null,
  missing: decode({ ...base, focused_group: null }, "group:requested") === null,
  mismatched: decode({ ...base, focused_group: group("group:other") }, "group:requested") === null,
};
process.stdout.write(JSON.stringify(result));
'''
    result = subprocess.run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        cwd=Path.cwd(),
        text=True,
    )

    assert result.stdout == '{"matching":true,"missing":true,"mismatched":true}'


def test_event_workbench_accepts_null_missing_event_metrics_but_rejects_invalid_values() -> None:
    """The authority uses null (not zero) when an event metric is unavailable."""
    script = r'''
const fs = require("fs");
const ts = require("./dashboard/node_modules/typescript");
const source = fs.readFileSync("dashboard/lib/business-research.ts", "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
const module = { exports: {} };
new Function("exports", "module", "require", compiled)(module.exports, module, require);
const base = { schema_version: "m1.event-research-detail.v1", status: "available", event_id: "event:one", event: {}, anchor: { quote_generation_key: "quote:one", structure_generation_key: "structure:one", changed_since_entry: false, materialized_at: "2026-09-06T00:00:00Z" }, state_counts: {}, structure: { group_count: 0 }, quote_coverage: { expected: 0, observed: 0, executable: 0, non_executable: 0, missing: 0 }, analysis: { research_only: true }, blockers: [], cautions: [], groups: [], focused_group: null };
const decode = module.exports.decodeEventResearchDetail;
const result = {
  nullMissing: decode({ ...base, event: { liquidity: null, volume: null, missing_fields: ["liquidity", "volume"] } }) !== null,
  undefinedMissing: decode({ ...base, event: { missing_fields: ["liquidity", "volume"] } }) !== null,
  negative: decode({ ...base, event: { volume: -1 } }) === null,
  nonFinite: decode({ ...base, event: { liquidity: Number.NaN } }) === null,
  wrongType: decode({ ...base, event: { volume: "unknown" } }) === null,
};
process.stdout.write(JSON.stringify(result));
'''
    result = subprocess.run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        cwd=Path.cwd(),
        text=True,
    )

    assert result.stdout == '{"nullMissing":true,"undefinedMissing":true,"negative":true,"nonFinite":true,"wrongType":true}'


def test_event_workbench_renders_one_authority_with_contextual_focus() -> None:
    detail = Path("dashboard/app/business/events/[event_id]/page.tsx").read_text()

    assert "readEventResearchDetail(event_id, {" in detail
    assert "from?: string" in detail
    assert "SOURCE_FOCUS" in detail
    assert "focus_group_id" in detail
    assert "Structure evidence" in detail
    assert "Quote coverage" in detail
    assert "Gross profit" in detail
    assert "not assessed" in detail
    assert "Lineage and provenance" in detail
    assert "readStructure" not in detail
    assert "readQuoteCoveragePage" not in detail
    assert "readBusinessResearchPage" not in detail


def test_all_business_main_tables_link_events_to_one_workbench() -> None:
    for page in ("structure", "quotes", "analysis"):
        source = Path(f"dashboard/app/business/{page}/page.tsx").read_text()
        assert "/business/events/" in source
        assert "focusVisible" in source

    quote_page = Path("dashboard/app/business/quotes/page.tsx").read_text()
    analysis_page = Path("dashboard/app/business/analysis/page.tsx").read_text()
    assert "focus_group_id" in quote_page
    assert "observed_generation" in quote_page
    assert "focus_group_id" in analysis_page
    assert "observed_generation" in analysis_page
