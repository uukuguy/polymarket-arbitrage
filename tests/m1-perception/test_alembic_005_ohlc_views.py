"""Wave 0 RED tests for Alembic migration 005 (Phase 05 Plan 01).

These are pure file-content lint tests — they read the migration source as
text and assert substrings. They run in CI with no Supabase DSN required.
Live forward+reverse roundtrip is exercised manually via
``make supabase-migrate-test`` (Task 3) against a developer-pointed test DB.

Schema contract (from 05-01-PLAN.md and 05-RESEARCH.md Example 1):

- ``revision == "005"``, ``down_revision == "004"`` (Phase 04 D-07 shipped 004)
- OHLC views use Postgres core ``date_trunc`` — NEVER TimescaleDB
  ``time_bucket`` (Supabase PG17 deprecation, see RESEARCH Pitfall 1)
- ``l2_book_levels`` table with composite UNIQUE (asset_id, ts, side, level)
  + RLS anon SELECT policy + BRIN(ts) index
- ``l2_candidates.l3_promoted_at_ts`` nullable column added (D-08 dashboard
  surface; Pitfall 8 Option C)
- Three OHLC views (1m / 5m / 1h) explicitly ``GRANT SELECT ... TO anon`` —
  views don't inherit base-table RLS the same way (RESEARCH §Architecture)
- ``downgrade()`` reverses in the correct order: drop views BEFORE table
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# Locate the migration file by absolute path so the test is independent of
# pytest's cwd. parents[2] = project root (tests/m1-perception/<file> →
# project root).
_MIGRATION_PATH: Path = (
    Path(__file__).resolve().parents[2] / "alembic" / "versions" / "005_l2_book_levels_and_ohlc.py"
)


def _read_migration_text() -> str:
    """Read the migration file fresh on each test call.

    Will raise FileNotFoundError (which pytest reports as ERROR) until
    Task 2 creates the file — that is the intended RED state.
    """
    return _MIGRATION_PATH.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: Revision metadata (down_revision chain to 004)
# ─────────────────────────────────────────────────────────────────────────────


def test_005_revision_metadata_correct() -> None:
    """revision == "005" and down_revision == "004" (Phase 04 D-07 shipped 004)."""
    text = _read_migration_text()
    assert 'revision = "005"' in text, (
        'Migration must declare revision = "005" (got file but no such line)'
    )
    assert 'down_revision = "004"' in text, (
        'Migration must declare down_revision = "004" — Phase 04 D-07 shipped '
        "004_add_yes_token_id.py; chaining to 003 would skip that revision."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Pitfall 1 anti-regression — date_trunc only, NEVER time_bucket
# ─────────────────────────────────────────────────────────────────────────────


def test_005_uses_date_trunc_not_time_bucket() -> None:
    """OHLC views must use Postgres core date_trunc, never TimescaleDB time_bucket.

    Supabase Postgres 17 deprecated TimescaleDB — time_bucket would fail
    at runtime with `function time_bucket(...) does not exist`. This test
    is the anti-regression guard for RESEARCH Pitfall 1 (D-03 revised).
    """
    text = _read_migration_text()
    # Expect at least 3 occurrences of date_trunc (one per OHLC view —
    # 1m view uses date_trunc('minute', ...), 5m uses floor() but 1h
    # uses date_trunc('hour', ...); RESEARCH Example 1 also uses
    # date_trunc inside GROUP BY for 1m and 1h). We use >= 3 as a
    # conservative lower bound that matches the canonical example.
    date_trunc_count = text.count("date_trunc")
    assert date_trunc_count >= 3, (
        f"Expected at least 3 occurrences of date_trunc (one per OHLC view "
        f"per RESEARCH Example 1); got {date_trunc_count}. "
        "1m and 1h views must use date_trunc; 5m uses floor()."
    )
    # Case-insensitive scan for time_bucket — must be absent entirely.
    assert not re.search(r"time_bucket", text, flags=re.IGNORECASE), (
        "time_bucket is TimescaleDB-only and unavailable on Supabase PG17. "
        "Use date_trunc + floor() instead (RESEARCH Pitfall 1)."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: l2_book_levels DDL shape (BRIN, RLS, UNIQUE)
# ─────────────────────────────────────────────────────────────────────────────


def test_005_book_levels_ddl_shape() -> None:
    """l2_book_levels table must include BRIN index, RLS policy, and composite UNIQUE."""
    text = _read_migration_text()
    assert "idx_l2_book_levels_ts_brin" in text, (
        "Missing BRIN index name idx_l2_book_levels_ts_brin "
        "(append-only time-series convention, mirrors 003_l2_tables.py)"
    )
    assert "USING BRIN (ts)" in text, (
        "Missing BRIN(ts) index DDL — required for time-series scans on "
        "append-only l2_book_levels (RESEARCH Pitfall 6)"
    )
    assert "ENABLE ROW LEVEL SECURITY" in text, (
        "Missing ALTER TABLE ... ENABLE ROW LEVEL SECURITY for l2_book_levels"
    )
    assert "CREATE POLICY anon_read ON l2_book_levels" in text, (
        "Missing RLS anon_read policy on l2_book_levels — anon SELECT "
        "convention from 003_l2_tables.py"
    )
    # Composite UNIQUE constraint — either via SQLAlchemy UniqueConstraint
    # helper or via raw SQL UNIQUE (asset_id, ts, side, level)
    has_unique = "UniqueConstraint" in text or "UNIQUE (asset_id, ts, side, level)" in text
    assert has_unique, (
        "Missing composite UNIQUE on (asset_id, ts, side, level) — required "
        "to prevent duplicate rows from the same WS book frame"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Views explicitly GRANT SELECT to anon (views don't inherit RLS)
# ─────────────────────────────────────────────────────────────────────────────


def test_005_grants_views_to_anon() -> None:
    """Each OHLC view must explicitly GRANT SELECT to anon role.

    Views don't inherit base-table RLS policies the same way as tables —
    Phase 02 D-19 + RESEARCH §Architecture: explicit GRANT keeps surface
    whitelisted for the anon role used by the dashboard.
    """
    text = _read_migration_text()
    assert "GRANT SELECT ON l2_ohlc_1m TO anon" in text, (
        "Missing GRANT SELECT ON l2_ohlc_1m TO anon"
    )
    assert "GRANT SELECT ON l2_ohlc_5m TO anon" in text, (
        "Missing GRANT SELECT ON l2_ohlc_5m TO anon"
    )
    assert "GRANT SELECT ON l2_ohlc_1h TO anon" in text, (
        "Missing GRANT SELECT ON l2_ohlc_1h TO anon"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: l2_candidates.l3_promoted_at_ts column (D-08 dashboard surface)
# ─────────────────────────────────────────────────────────────────────────────


def test_005_adds_l3_promoted_at_ts_column() -> None:
    """Must add nullable l3_promoted_at_ts column to l2_candidates (D-08 / Pitfall 8 Option C)."""
    text = _read_migration_text()
    assert "l3_promoted_at_ts" in text, (
        "Missing l3_promoted_at_ts identifier — required for D-08 dashboard surface"
    )
    assert "l2_candidates" in text, (
        "Missing l2_candidates reference — l3_promoted_at_ts must be added to l2_candidates"
    )
    # The column must be nullable (add-only discipline, pre-existing rows
    # cannot be backfilled). Either SQLAlchemy ``nullable=True`` or raw
    # SQL ``NULL`` declaration is accepted.
    nullable_ok = "nullable=True" in text or "NULL" in text
    assert nullable_ok, (
        "l3_promoted_at_ts must be NULLABLE — add-only discipline requires "
        "existing l2_candidates rows to remain valid (they receive NULL)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: downgrade() reverses in the correct order (views before table)
# ─────────────────────────────────────────────────────────────────────────────


def test_005_downgrade_reverses_in_correct_order() -> None:
    """downgrade() must drop views BEFORE the underlying l2_top_of_book / l2_book_levels.

    Views depend on the base table they SELECT from. Dropping the table
    first would either fail (if FK-like dependency tracking catches it)
    or leave invalid view definitions. RESEARCH Example 1 establishes
    the canonical order: 1h → 5m → 1m views, then l2_candidates index,
    then l2_candidates column, then l2_book_levels table.
    """
    text = _read_migration_text()
    # Find positions of relevant operations.
    drop_view_1h_pos = text.find("DROP VIEW IF EXISTS l2_ohlc_1h")
    # The table-drop reference: match either ``op.drop_table("l2_book_levels"``
    # or ``DROP TABLE ... l2_book_levels``. Use regex to be tolerant.
    drop_table_match = re.search(
        r"drop_table\s*\(\s*[\"']l2_book_levels[\"']",
        text,
    )
    assert drop_view_1h_pos >= 0, "downgrade() must contain 'DROP VIEW IF EXISTS l2_ohlc_1h'"
    assert drop_table_match is not None, 'downgrade() must contain op.drop_table("l2_book_levels")'
    drop_table_pos = drop_table_match.start()
    assert drop_view_1h_pos < drop_table_pos, (
        "Views must be dropped before the l2_book_levels table — "
        f"DROP VIEW found at position {drop_view_1h_pos}, "
        f"drop_table at position {drop_table_pos}. "
        "Reverse the order in downgrade()."
    )


# Plan-aware safety: when migration file is missing, FileNotFoundError
# manifests as the test's ERROR signal — the RED state for Task 2.
# We don't suppress that; pytest's default reporting is the right surface.
if __name__ == "__main__":  # pragma: no cover — manual debug only
    pytest.main([__file__, "-v"])
