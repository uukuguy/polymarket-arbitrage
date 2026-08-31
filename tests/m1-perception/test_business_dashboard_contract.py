"""Source contracts for the atomic M1 business research page."""

from pathlib import Path


def test_business_page_uses_only_the_atomic_business_overview_reader() -> None:
    page = Path("dashboard/app/business/page.tsx").read_text()
    reader = Path("dashboard/lib/business-overview.ts").read_text()

    assert "readBusinessOverview" in page
    assert "this is not zero opportunities" in page
    assert "/perception/opportunities" not in page
    assert "/perception/control-plane" not in page
    assert "m1.business-overview.v1" in reader
    assert "business-overview-unavailable" in reader


def test_business_page_exposes_quote_and_opportunity_lineage() -> None:
    page = Path("dashboard/app/business/page.tsx").read_text()

    assert "parent_structure_generation_key" in page
    assert "quote_generation_key" in page
