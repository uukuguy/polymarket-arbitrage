"""Contract for fenced Quote business-index staging."""

from pathlib import Path


MIGRATION = Path("alembic/versions/045_m1_quote_research_staging.py")


def test_revision_045_declares_generation_bound_quote_staging() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "045"' in text
    assert 'down_revision = "044"' in text
    assert "m1_business_quote_staging_rows" in text
    assert "generation_key" in text
    assert "token_id" in text
    assert "payload" in text
    assert "m1_business_quote_staging_rows_page" in text
