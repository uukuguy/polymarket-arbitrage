"""Alembic 008 contract tests for bounded global L2 freshness reads."""

from __future__ import annotations

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/008_l2_tob_global_freshness_index.py")


def test_008_chains_to_007_and_builds_online() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    compact = " ".join(text.replace('"', "").split())
    assert 'revision = "008"' in text
    assert 'down_revision = "007"' in text
    assert "autocommit_block()" in text
    assert (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_l2_tob_ts_desc "
        "ON l2_top_of_book (ts DESC)"
    ) in compact


def test_008_downgrade_is_online_and_symmetric() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    downgrade = text[text.index("def downgrade(") :]
    assert "DROP INDEX CONCURRENTLY IF EXISTS idx_l2_tob_ts_desc" in downgrade
