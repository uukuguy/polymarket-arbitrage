"""Source contracts for the atomic M1 business research page."""

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
    assert "not yet persisted" in analysis_page
    assert "structure_records" in analysis_page
    assert "quote_records" in analysis_page
    assert "certified_opportunities" in analysis_page
    assert "real current zero" in (root / "opportunities/page.tsx").read_text()


def test_quote_coverage_renders_discovery_evidence_not_an_opportunity_claim() -> None:
    page = Path("dashboard/app/business/quotes/page.tsx").read_text()

    assert "Research leads" in page
    assert "executable notional" in page
    assert "research priority, not a certified opportunity" in page
    assert "event_context" in page
    assert "neg_risk_context" in page
    assert "discovery" in page
    assert "formatEndTime" in page


def test_business_research_decoder_checks_quote_discovery_contract() -> None:
    reader = Path("dashboard/lib/business-research.ts").read_text()

    assert "price_extremity_bps" in reader
    assert "executable_notional_usd" in reader
    assert "missing-or-invalid-quote" in reader
