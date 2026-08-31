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
    assert "not yet persisted" in (root / "analysis/page.tsx").read_text()
    assert "real current zero" in (root / "opportunities/page.tsx").read_text()
