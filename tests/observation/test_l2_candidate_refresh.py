"""Tests for polyarb.observation.l2_candidate_refresh.

Plan 03-05 Task 3 — compute_candidates (union) + diff_candidate_sets +
on_snapshot_complete (debounce + cap + ws_consumer mutation).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Fixtures ──────────────────────────────────────────────────────────────


def _create_minimal_sqlite(db_path: Path, markets: list[dict]) -> None:
    """Create SQLite with the markets columns the recipe scanner needs.

    Recipes JOIN question_translations (LEFT) so it must exist too.
    We seed `liquidity_usd` since the default scanner recipes ORDER BY it.
    """
    con = sqlite3.connect(db_path)
    con.executescript(
        """
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY,
            condition_id TEXT,
            slug TEXT,
            question TEXT,
            yes_token_id TEXT,
            no_token_id TEXT,
            mid_price REAL,
            liquidity_usd REAL,
            volume_usd REAL,
            best_bid_price REAL,
            best_bid_size REAL,
            best_ask_price REAL,
            best_ask_size REAL,
            end_time_ms INTEGER,
            active INTEGER,
            closed INTEGER,
            neg_risk INTEGER,
            neg_risk_market_id TEXT,
            fetched_at_ms INTEGER,
            page_fetched_at_ms INTEGER,
            snapshot_id INTEGER,
            incomplete INTEGER DEFAULT 0,
            event_id TEXT
        );
        CREATE TABLE question_translations (
            question_hash TEXT PRIMARY KEY,
            question_en TEXT,
            question_zh TEXT,
            translator_model TEXT,
            translated_at_ms INTEGER,
            token_cost INTEGER,
            retry_count INTEGER DEFAULT 0,
            is_dead INTEGER DEFAULT 0
        );
        CREATE TABLE validation_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER,
            layer INTEGER,
            category TEXT,
            market_id TEXT,
            detail TEXT,
            raw_payload TEXT
        );
        """
    )
    for m in markets:
        cols = ",".join(m.keys())
        placeholders = ",".join("?" * len(m))
        con.execute(f"INSERT INTO markets ({cols}) VALUES ({placeholders})", list(m.values()))
    con.commit()
    con.close()


@pytest.fixture
def settings_with_db(tmp_path):
    """Settings with db_path pointing at the tmp_path SQLite."""
    db_path = tmp_path / "state.db"
    from polyarb.config import Settings

    s = Settings(db_path=db_path, parquet_root=tmp_path / "snapshots")
    return s, db_path


@pytest.fixture(autouse=True)
def _reset_debounce_state():
    """Reset module-level debounce state between tests."""
    import polyarb.observation.l2_candidate_refresh as mod
    mod._last_refresh_at_s = 0.0
    yield
    mod._last_refresh_at_s = 0.0


def _seed_markets(n: int, prefix: str = "M", *, liquidity_base: float = 200_000.0) -> list[dict]:
    return [
        {
            "market_id": f"{prefix}{i}",
            "condition_id": f"COND-{prefix}{i}",
            "slug": f"slug-{prefix.lower()}-{i}",
            "question": f"Q{i}?",
            "yes_token_id": f"YES-{prefix}{i}",
            "no_token_id": f"NO-{prefix}{i}",
            "liquidity_usd": liquidity_base + i,
            "best_bid_price": 0.40,
            "best_ask_price": 0.42,
            "fetched_at_ms": 1_000_000 + i,
            "snapshot_id": 1,
        }
        for i in range(n)
    ]


# ── Test 1: compute_candidates union ──────────────────────────────────────


def test_compute_candidates_union(settings_with_db, tmp_path):
    """5 from a recipe + 3 watchlist with 1 overlap → 7 unique asset_ids."""
    from polyarb.observation.l2_candidate_refresh import compute_candidates

    settings, db_path = settings_with_db
    markets = _seed_markets(5, "R")
    # one watchlist slug matches an existing market slug -> overlap
    markets.extend(_seed_markets(3, "W"))
    _create_minimal_sqlite(db_path, markets)

    # Scanner yaml: pick a recipe that returns R-markets.
    # Use builtin "thick-but-slippery"? requires spread>0.10 — our test markets
    # have spread=0.02. Instead define a yaml recipe with where=1=1.
    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  all-markets:\n"
        "    description: test recipe\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 100\n"
    )

    watchlist_yaml = tmp_path / "watchlist.yaml"
    # 3 watchlist entries: 1 overlaps with R0, 2 new (W0, W1)
    watchlist_yaml.write_text(
        "- slug: slug-r-0\n"
        "  reason: overlap\n"
        "  added: 2026-05-24\n"
        "- slug: slug-w-0\n"
        "  reason: new\n"
        "  added: 2026-05-24\n"
        "- slug: slug-w-1\n"
        "  reason: new\n"
        "  added: 2026-05-24\n"
    )

    rows = compute_candidates(settings, scanner_yaml, watchlist_yaml)
    asset_ids = {r.asset_id for r in rows}
    # 5 R yes_token_ids + 2 NEW W yes_token_ids (W0,W1) +
    # R0 (overlapped, watchlist version overwrites recipe entry) = 5+2 = 7 unique
    assert len(asset_ids) == 7


def test_compute_candidates_watchlist_overrides_recipe(settings_with_db, tmp_path):
    """Overlap between recipe + watchlist → final CandidateRow.source='watchlist'."""
    from polyarb.observation.l2_candidate_refresh import compute_candidates

    settings, db_path = settings_with_db
    markets = _seed_markets(2, "R")
    _create_minimal_sqlite(db_path, markets)

    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  all-r:\n"
        "    description: test\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 100\n"
    )
    watchlist_yaml = tmp_path / "watchlist.yaml"
    watchlist_yaml.write_text(
        "- slug: slug-r-0\n  reason: overlap\n  added: 2026-05-24\n"
    )

    rows = compute_candidates(settings, scanner_yaml, watchlist_yaml)
    # find the R0 (yes_token_id=YES-R0) row
    r0 = [r for r in rows if r.asset_id == "YES-R0"]
    assert len(r0) == 1
    assert r0[0].source == "watchlist"


def test_compute_candidates_empty_scanner_yaml(settings_with_db, tmp_path):
    """scanner_yaml=None → only watchlist results, no exception."""
    from polyarb.observation.l2_candidate_refresh import compute_candidates

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(2, "W"))
    watchlist_yaml = tmp_path / "watchlist.yaml"
    watchlist_yaml.write_text(
        "- slug: slug-w-0\n  reason: only\n  added: 2026-05-24\n"
    )

    rows = compute_candidates(settings, None, watchlist_yaml)
    asset_ids = {r.asset_id for r in rows}
    assert "YES-W0" in asset_ids


def test_compute_candidates_empty_watchlist(settings_with_db, tmp_path):
    """watchlist_yaml=None → only recipes, no exception."""
    from polyarb.observation.l2_candidate_refresh import compute_candidates

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(3, "R"))
    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  r-only:\n"
        "    description: x\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 100\n"
    )
    rows = compute_candidates(settings, scanner_yaml, None)
    assert len(rows) == 3


def test_diff_candidate_sets_added_removed():
    """old={A,B,C}, new=[D,E,A] → removed={B,C}, added rows D,E."""
    from polyarb.observation.l2_candidate_refresh import CandidateRow, diff_candidate_sets

    new_rows = [
        CandidateRow(asset_id="D", market_id=None, event_id=None,
                     recipe_name="r", source="recipe", ranking_score=None),
        CandidateRow(asset_id="E", market_id=None, event_id=None,
                     recipe_name="r", source="recipe", ranking_score=None),
        CandidateRow(asset_id="A", market_id=None, event_id=None,
                     recipe_name="r", source="recipe", ranking_score=None),
    ]
    removed, added = diff_candidate_sets({"A", "B", "C"}, new_rows)
    assert removed == {"B", "C"}
    assert {r.asset_id for r in added} == {"D", "E"}


def test_diff_candidate_sets_no_change():
    from polyarb.observation.l2_candidate_refresh import CandidateRow, diff_candidate_sets

    new_rows = [
        CandidateRow(asset_id="A", market_id=None, event_id=None,
                     recipe_name="r", source="recipe", ranking_score=None),
        CandidateRow(asset_id="B", market_id=None, event_id=None,
                     recipe_name="r", source="recipe", ranking_score=None),
    ]
    removed, added = diff_candidate_sets({"A", "B"}, new_rows)
    assert removed == set()
    assert added == []


def test_candidate_set_cap_500(settings_with_db, tmp_path):
    """1000 recipe candidates → cap enforced at 500; watchlist always retained."""
    from polyarb.observation.l2_candidate_refresh import MAX_CANDIDATES, compute_candidates

    settings, db_path = settings_with_db
    # 600 R markets, 10 W markets in watchlist
    markets = _seed_markets(600, "R")
    markets.extend(_seed_markets(10, "W"))
    _create_minimal_sqlite(db_path, markets)

    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  big:\n"
        "    description: x\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 10000\n"
    )
    watchlist_yaml = tmp_path / "watchlist.yaml"
    watchlist_yaml.write_text(
        "".join(
            f"- slug: slug-w-{i}\n  reason: keep\n  added: 2026-05-24\n"
            for i in range(10)
        )
    )

    rows = compute_candidates(settings, scanner_yaml, watchlist_yaml)
    assert len(rows) <= MAX_CANDIDATES == 500
    asset_ids = {r.asset_id for r in rows}
    # All 10 watchlist assets present
    for i in range(10):
        assert f"YES-W{i}" in asset_ids, f"watchlist YES-W{i} dropped under cap"


@pytest.mark.asyncio
async def test_refresh_debounce_60s(settings_with_db, monkeypatch, tmp_path):
    """Two calls within 60s → second is no-op; third after 61s triggers."""
    import polyarb.observation.l2_candidate_refresh as mod

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(1, "R"))
    # We don't even need yaml — debounce is checked BEFORE compute_candidates
    settings.candidate_scanner_yaml = None
    settings.candidate_watchlist_yaml = None

    fake_ws = MagicMock()
    fake_ws.subscribed_assets = []
    fake_ws._subscribed_assets = []

    times = iter([100.0, 130.0, 200.0])
    monkeypatch.setattr(mod.time, "monotonic", lambda: next(times))

    compute_calls = {"n": 0}
    real_compute = mod.compute_candidates

    def _spy(*a, **kw):
        compute_calls["n"] += 1
        return real_compute(*a, **kw)

    monkeypatch.setattr(mod, "compute_candidates", _spy)

    ok1 = await mod.on_snapshot_complete(
        {"snapshot_id": 1, "taken_at_ms": 1}, ws_consumer=fake_ws, settings=settings
    )
    ok2 = await mod.on_snapshot_complete(
        {"snapshot_id": 2, "taken_at_ms": 2}, ws_consumer=fake_ws, settings=settings
    )
    ok3 = await mod.on_snapshot_complete(
        {"snapshot_id": 3, "taken_at_ms": 3}, ws_consumer=fake_ws, settings=settings
    )

    assert ok1 is True
    assert ok2 is False, "second call within 60s must debounce"
    assert ok3 is True, "third call after 60s must run"
    assert compute_calls["n"] == 2, "compute_candidates must only run when not debounced"


@pytest.mark.asyncio
async def test_on_snapshot_complete_mutates_ws_consumer(settings_with_db, tmp_path):
    """Handler writes the new asset_id list into ws_consumer._subscribed_assets."""
    import polyarb.observation.l2_candidate_refresh as mod

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(5, "R"))
    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  five:\n"
        "    description: x\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 10\n"
    )
    settings.candidate_scanner_yaml = scanner_yaml
    settings.candidate_watchlist_yaml = None

    fake_ws = MagicMock()
    fake_ws.subscribed_assets = []
    fake_ws._subscribed_assets = []

    ok = await mod.on_snapshot_complete(
        {"snapshot_id": 1, "taken_at_ms": 1}, ws_consumer=fake_ws, settings=settings
    )
    assert ok is True
    assert len(fake_ws._subscribed_assets) == 5
    assert all(a.startswith("YES-R") for a in fake_ws._subscribed_assets)


def test_compute_candidates_recipe_failure_continues(settings_with_db, tmp_path, monkeypatch):
    """If one recipe raises, others still run; no exception propagates."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.observation import scanner as scanner_mod

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(3, "R"))
    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  ok-one:\n"
        "    description: x\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 10\n"
        "  break-me:\n"
        "    description: x\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 10\n"
    )

    # Make `run_recipe` fail for recipes named "break-me"
    real_run_recipe = scanner_mod.run_recipe

    def _maybe_fail(db_path, recipe):
        if recipe.name == "break-me":
            raise RuntimeError("simulated recipe failure")
        return real_run_recipe(db_path, recipe)

    monkeypatch.setattr(mod, "run_recipe", _maybe_fail)

    # Must not raise
    rows = mod.compute_candidates(settings, scanner_yaml, None)
    asset_ids = {r.asset_id for r in rows}
    # ok-one populated all 3 markets
    assert len(asset_ids) == 3
