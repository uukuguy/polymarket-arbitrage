"""Regression contract for bounded candidate-source lookup performance."""

from pathlib import Path


def test_candidate_source_index_covers_quote_group_lookup() -> None:
    migration = Path("alembic/versions/048_m1_analysis_candidate_lookup_index.py").read_text()

    assert 'revision = "048"' in migration
    assert 'down_revision = "047"' in migration
    assert "m1_business_quote_rows_candidate_group" in migration
    assert "neg_risk_market_id" in migration
