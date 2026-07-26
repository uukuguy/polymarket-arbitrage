"""Tests for polyarb.observation.l2_temp_db (Phase 04 Plan 02 Task 1).

D-02 named-temp-file adapter — built from Supabase markets_latest narrow rows.

Coverage:
- test_build_temp_db_schema: full markets DDL + auxiliary tables present
- test_build_temp_db_is_real_file_not_memory: a SEPARATE sqlite3 connection
    reads the inserted rows (proves :memory: pitfall avoided — RESEARCH Pitfall 1)
- test_null_filled_column_warns: warn_null_filled_recipe_columns logs WARNING
- test_ghost_suspicious_empty_validation_issues: scanner can run a recipe
    that subqueries validation_issues (empty, but present) — no SQL error
- test_not_null_columns_filled_with_sentinel: narrow row without
    condition_id / fetched_at_ms inserts via sentinel (no NOT NULL violation)
- test_snapshot_fk_does_not_block_insert: FK on snapshot_id does not block
    insert when no snapshots row exists (PRAGMA foreign_keys=OFF in temp DB)
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


def _narrow_row(market_id: str = "m1", **overrides) -> dict:
    """A typical markets_latest narrow row (11 cols incl. yes_token_id from D-07)."""
    base = {
        "market_id": market_id,
        "question": "Will it rain tomorrow?",
        "slug": "will-it-rain-tomorrow",
        "event_slug": "weather-2026",
        "mid_price": 0.5,
        "liquidity_usd": 100000.0,
        "volume_usd": 50000.0,
        "end_time_ms": 1_800_000_000_000,
        "snapshot_id": 1,
        "question_zh": None,
        "yes_token_id": f"YES-{market_id}",
        "no_token_id": f"NO-{market_id}",
    }
    base.update(overrides)
    return base


def test_build_temp_db_schema(tmp_path):
    """Temp DB markets table must contain every DDL column + aux tables present."""
    from polyarb.observation.l2_temp_db import build_temp_db

    tmp = build_temp_db([_narrow_row("m1")])
    try:
        con = sqlite3.connect(tmp)
        cur = con.execute("PRAGMA table_info(markets)")
        cols = {row[1] for row in cur.fetchall()}
        # Spot-check critical columns (full 23 DDL cols)
        for expected in (
            "market_id",
            "condition_id",
            "yes_token_id",
            "no_token_id",
            "mid_price",
            "liquidity_usd",
            "volume_usd",
            "best_bid_price",
            "best_bid_size",
            "best_ask_price",
            "best_ask_size",
            "end_time_ms",
            "active",
            "closed",
            "neg_risk",
            "neg_risk_market_id",
            "fetched_at_ms",
            "page_fetched_at_ms",
            "snapshot_id",
            "incomplete",
            "event_id",
        ):
            assert expected in cols, f"DDL column {expected!r} missing"
        # All auxiliary tables that scanner recipes may reference must exist.
        for table in (
            "question_translations",
            "validation_issues",
            "event_tags",
            "events",
            "snapshots",
        ):
            cur2 = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            assert cur2.fetchone() is not None, f"auxiliary table {table!r} missing"
        con.close()
    finally:
        os.unlink(tmp)


def test_build_temp_db_is_real_file_not_memory(tmp_path):
    """A SEPARATE sqlite3 connection must read the inserted rows.

    RESEARCH Pitfall 1: two ``:memory:`` connections are independent empty DBs;
    scanner.run_recipe opens its own connection via ``file:{path}?mode=ro``.
    A named temp file is shared across processes/connections.
    """
    from polyarb.observation.l2_temp_db import build_temp_db

    tmp_path_returned = build_temp_db([_narrow_row("m1"), _narrow_row("m2")])
    try:
        assert tmp_path_returned.exists(), "build_temp_db must return a real path"
        assert tmp_path_returned != Path(":memory:"), ":memory: is the pitfall"
        # Open with a NEW connection (mimics scanner.run_recipe).
        uri = f"file:{tmp_path_returned}?mode=ro"
        con2 = sqlite3.connect(uri, uri=True)
        try:
            rows = con2.execute("SELECT market_id FROM markets ORDER BY market_id").fetchall()
            assert [r[0] for r in rows] == ["m1", "m2"]
        finally:
            con2.close()
    finally:
        os.unlink(tmp_path_returned)


def test_build_temp_db_preserves_token_pair() -> None:
    from polyarb.observation.l2_temp_db import build_temp_db

    tmp = build_temp_db([_narrow_row("m1", no_token_id="NO-custom")])
    try:
        with sqlite3.connect(tmp) as con:
            pair = con.execute(
                "SELECT yes_token_id, no_token_id FROM markets WHERE market_id='m1'"
            ).fetchone()
        assert pair == ("YES-m1", "NO-custom")
    finally:
        os.unlink(tmp)


def test_null_filled_column_warns(caplog):
    """warn_null_filled_recipe_columns logs WARNING (does NOT raise) for
    a recipe referencing NULL-filled columns (e.g. best_bid_price)."""
    from polyarb.observation.l2_temp_db import warn_null_filled_recipe_columns
    from polyarb.observation.recipes import Recipe

    recipe = Recipe(
        name="thick-but-slippery",
        description="test",
        where="best_bid_price > 0",
        order_by="liquidity_usd DESC",
        limit=10,
    )
    # loguru → propagate via stdlib? caplog captures only the std logging tree by
    # default. We instead capture via a sink registered to loguru.
    captured: list[str] = []
    from loguru import logger as _logger

    sink_id = _logger.add(lambda msg: captured.append(str(msg)), level="WARNING")
    try:
        warn_null_filled_recipe_columns(recipe)  # must NOT raise
    finally:
        _logger.remove(sink_id)
    assert any("NULL-filled" in m for m in captured), (
        f"expected NULL-filled warning, got: {captured}"
    )


def test_ghost_suspicious_empty_validation_issues(tmp_path):
    """Scanner can SELECT from validation_issues even when empty — no SQL error."""
    from polyarb.observation.l2_temp_db import build_temp_db

    tmp = build_temp_db([_narrow_row("m1")])
    try:
        uri = f"file:{tmp}?mode=ro"
        con = sqlite3.connect(uri, uri=True)
        try:
            # Same shape as ghost-suspicious recipe subquery.
            rows = con.execute(
                "SELECT m.market_id FROM markets m "
                "WHERE m.market_id IN (SELECT market_id FROM validation_issues)"
            ).fetchall()
            assert rows == []  # empty validation_issues → 0 markets, NOT an error.
        finally:
            con.close()
    finally:
        os.unlink(tmp)


def test_not_null_columns_filled_with_sentinel():
    """A narrow row missing condition_id / fetched_at_ms / snapshot_id inserts
    successfully via sentinel — no NOT NULL constraint violation."""
    from polyarb.observation.l2_temp_db import build_temp_db

    # markets_latest narrow projection LACKS condition_id, fetched_at_ms, incomplete.
    # snapshot_id is in the narrow projection but may be missing for some rows.
    minimal = {
        "market_id": "m1",
        "question": "Q?",
        "slug": "s",
        "event_slug": "e",
        "mid_price": 0.5,
        "liquidity_usd": 100.0,
        "volume_usd": 10.0,
        "end_time_ms": 1_800_000_000_000,
        # snapshot_id intentionally absent
        "yes_token_id": "YES-1",
    }
    tmp = build_temp_db([minimal])
    try:
        con = sqlite3.connect(tmp)
        try:
            r = con.execute(
                "SELECT condition_id, fetched_at_ms, snapshot_id, incomplete "
                "FROM markets WHERE market_id='m1'"
            ).fetchone()
            assert r == ("", 0, 0, 0), f"sentinel-fill mismatch: {r}"
        finally:
            con.close()
    finally:
        os.unlink(tmp)


def test_snapshot_fk_does_not_block_insert():
    """schemas.DDL declares snapshot_id REFERENCES snapshots(id) AND PRAGMA
    foreign_keys=ON. The temp DB MUST disable FK enforcement (Option A) so
    sentinel snapshot_id=0 inserts cleanly without seeding snapshots."""
    from polyarb.observation.l2_temp_db import build_temp_db

    row = _narrow_row("m1", snapshot_id=99999)  # 99999 not in snapshots
    tmp = build_temp_db([row])
    try:
        con = sqlite3.connect(tmp)
        try:
            r = con.execute(
                "SELECT market_id, snapshot_id FROM markets WHERE market_id='m1'"
            ).fetchone()
            assert r == ("m1", 99999), f"expected (m1, 99999), got {r}"
        finally:
            con.close()
    finally:
        os.unlink(tmp)
