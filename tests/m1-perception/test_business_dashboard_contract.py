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
    assert "readBusinessResearchPage(\"analysis\")" in analysis_page
    assert "positive-edge" in analysis_page
    assert "Candidate analysis is not a certified opportunity" in analysis_page
    assert "structure_records" in analysis_page
    assert "quote_records" in analysis_page
    assert "certified_opportunities" in analysis_page
    assert "executable economic value" in analysis_page
    assert "real current zero" in (root / "opportunities/page.tsx").read_text()


def test_structure_default_reads_only_open_unexpired_events() -> None:
    page = Path("dashboard/app/business/structure/page.tsx").read_text()

    assert 'readStructureIntelligencePage("events", { openOnly: true })' in page


def test_quote_coverage_renders_health_not_price_discovery() -> None:
    page = Path("dashboard/app/business/quotes/page.tsx").read_text()

    assert "readQuoteCoveragePage" in page
    assert "Group coverage health" in page
    assert "Coverage gap" in page
    assert "Price extremity is intentionally not a signal" in page
    assert "price_extremity_bps" not in page
    assert "formatEndTime" in page


def test_quote_coverage_decoder_checks_health_contract() -> None:
    reader = Path("dashboard/lib/business-research.ts").read_text()

    assert "m1.quote-coverage-page.v1" in reader
    assert "coverage-gap" in reader
    assert "readQuoteCoveragePage" in reader
