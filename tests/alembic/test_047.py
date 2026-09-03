"""Contract tests for bounded M1 analysis candidate storage."""

from pathlib import Path


def test_analysis_candidate_migration_is_bounded_and_runtime_scoped() -> None:
    migration = Path("alembic/versions/047_m1_analysis_candidates.py").read_text()

    assert 'revision = "047"' in migration
    assert 'down_revision = "046"' in migration
    assert '"record_count BETWEEN 0 AND 20000"' in migration
    assert '"payload_octets BETWEEN 2 AND 2048"' in migration
    assert "gross_edge_bps DESC NULLS LAST" in migration
    assert "GRANT SELECT, INSERT, DELETE, TRUNCATE" in migration
