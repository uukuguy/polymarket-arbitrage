"""Contracts for generation-bound M1 business research indexes."""

from pathlib import Path


MIGRATION = Path("alembic/versions/040_m1_business_research_indexes.py")


def test_revision_040_declares_generation_bound_read_indexes() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "040"' in text
    assert 'down_revision = "039"' in text
    assert "m1_business_structure_rows" in text
    assert "m1_business_quote_rows" in text
    assert "generation_key" in text
    assert "m1_business_structure_rows_page" in text
    assert "m1_business_quote_rows_page" in text
    assert "GRANT SELECT" in text
    assert "GRANT INSERT" not in text
    assert "GRANT UPDATE" not in text
    assert "GRANT DELETE" not in text
