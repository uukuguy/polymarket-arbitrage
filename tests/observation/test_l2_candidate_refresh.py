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


@pytest.fixture(autouse=True)
def _isolate_supabase_env(monkeypatch):
    """Phase 04 Plan 02 — D-01 isolation: ensure tests don't pick up live Supabase
    credentials from .env. Without this, the Phase 04 fetch path in
    `on_snapshot_complete` would make real network calls during unit tests and
    consume monotonic ticks via httpx internals (breaking time-patched tests
    like test_refresh_debounce_60s).

    Individual tests that need to exercise the fetch path mock create_client +
    _fetch_all_markets_latest explicitly; they should NOT rely on real env.
    """
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    for var in (
        "POLYARB_SUPABASE_URL",
        "POLYARB_SUPABASE_SERVICE_KEY",
        "POLYARB_SUPABASE_DB_DSN",
        "POLYARB_L2_MIRROR_ENABLED",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def settings_with_db(tmp_path):
    """Settings with db_path pointing at the tmp_path SQLite.

    Phase 04 Plan 02 — D-01 isolation: explicitly empty supabase_url + service_key
    so unit tests never hit the live fetch path (which would consume httpx
    internals' monotonic ticks and break time-patched tests).
    """
    from pydantic import SecretStr

    db_path = tmp_path / "state.db"
    from polyarb.config import Settings

    s = Settings(
        db_path=db_path,
        parquet_root=tmp_path / "snapshots",
        supabase_url="",
        supabase_service_key=SecretStr(""),
    )
    return s, db_path


@pytest.fixture(autouse=True)
def _reset_debounce_state():
    """Reset module-level debounce + Phase-04 D-01 fail-soft state between tests."""
    import polyarb.observation.l2_candidate_refresh as mod
    mod._last_refresh_at_s = 0.0
    # Phase 04 Plan 02 — D-01 fail-soft state.
    if hasattr(mod, "_last_known_markets_rows"):
        mod._last_known_markets_rows = None
    if hasattr(mod, "_last_fetch_success_at_s"):
        mod._last_fetch_success_at_s = None
    yield
    mod._last_refresh_at_s = 0.0
    if hasattr(mod, "_last_known_markets_rows"):
        mod._last_known_markets_rows = None
    if hasattr(mod, "_last_fetch_success_at_s"):
        mod._last_fetch_success_at_s = None


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

    # Each on_snapshot_complete call invokes time.monotonic() once (line:
    # now = time.monotonic()). Three calls → three values.
    times = [100.0, 130.0, 200.0]
    idx = {"i": 0}

    def _mono():
        v = times[idx["i"]]
        if idx["i"] < len(times) - 1:
            idx["i"] += 1
        return v

    monkeypatch.setattr(mod.time, "monotonic", _mono)

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


# ── Plan 06: mirror upsert wired into on_snapshot_complete tail ─────────


@pytest.mark.asyncio
async def test_on_snapshot_complete_calls_mirror_upsert_when_provided(
    settings_with_db, tmp_path
):
    """When `mirror=<L2SupabaseMirror-like>` is passed, the tail calls
    mirror.upsert_candidates(added) AND mirror.mark_candidates_removed(removed).
    """
    import polyarb.observation.l2_candidate_refresh as mod

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(3, "R"))
    scanner_yaml = tmp_path / "recipes.yaml"
    scanner_yaml.write_text(
        "recipes:\n"
        "  r-only:\n"
        "    description: x\n"
        "    where: market_id LIKE 'R%'\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 10\n"
    )
    settings.candidate_scanner_yaml = scanner_yaml
    settings.candidate_watchlist_yaml = None

    fake_ws = MagicMock()
    # Pre-populate ws with 2 'old' assets so 2 should be removed
    fake_ws.subscribed_assets = ["OLD-A", "OLD-B"]
    fake_ws._subscribed_assets = ["OLD-A", "OLD-B"]

    fake_mirror = MagicMock()
    fake_mirror.upsert_candidates.return_value = True
    fake_mirror.mark_candidates_removed.return_value = True

    ok = await mod.on_snapshot_complete(
        {"snapshot_id": 42, "taken_at_ms": 1},
        ws_consumer=fake_ws,
        settings=settings,
        mirror=fake_mirror,
    )
    assert ok is True

    # 3 new R-markets added
    fake_mirror.upsert_candidates.assert_called_once()
    added_arg = fake_mirror.upsert_candidates.call_args[0][0]
    assert len(added_arg) == 3, f"expected 3 added rows, got {len(added_arg)}"
    # Each upsert row must carry snapshot_id from payload
    assert all(r["snapshot_id"] == 42 for r in added_arg)

    # 2 old assets removed
    fake_mirror.mark_candidates_removed.assert_called_once()
    removed_arg = fake_mirror.mark_candidates_removed.call_args[0][0]
    assert set(removed_arg) == {"OLD-A", "OLD-B"}


@pytest.mark.asyncio
async def test_on_snapshot_complete_no_mirror_call_when_none(
    settings_with_db, tmp_path
):
    """mirror=None (config-disabled) → no mirror method calls; refresh still runs."""
    import polyarb.observation.l2_candidate_refresh as mod

    settings, db_path = settings_with_db
    _create_minimal_sqlite(db_path, _seed_markets(2, "R"))
    settings.candidate_scanner_yaml = None
    settings.candidate_watchlist_yaml = None

    fake_ws = MagicMock()
    fake_ws.subscribed_assets = []
    fake_ws._subscribed_assets = []

    ok = await mod.on_snapshot_complete(
        {"snapshot_id": 1, "taken_at_ms": 1},
        ws_consumer=fake_ws,
        settings=settings,
        mirror=None,
    )
    assert ok is True  # refresh ran (debounce not blocking)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 04 Plan 02 — D-01 Supabase fetch + D-03 cap + D-04 fallback tests
# ─────────────────────────────────────────────────────────────────────────────


def _make_supabase_mock_with_pages(pages: list[list[dict]]) -> MagicMock:
    """Build a mock supabase client whose .range(...).execute() pops pages."""
    mock_client = MagicMock()
    page_iter = iter(pages)

    def _execute() -> MagicMock:
        m = MagicMock()
        try:
            m.data = next(page_iter)
        except StopIteration:
            m.data = []
        return m

    mock_client.table.return_value.select.return_value.range.return_value.execute = _execute
    return mock_client


def test_fetch_pagination():
    """_fetch_all_markets_latest paginates past 1000-row PostgREST cap."""
    from polyarb.observation.l2_candidate_refresh import _fetch_all_markets_latest

    page1 = [{"market_id": f"m{i}"} for i in range(1000)]
    page2 = [{"market_id": f"m{i}"} for i in range(1000, 1500)]
    mock_client = _make_supabase_mock_with_pages([page1, page2])

    rows = _fetch_all_markets_latest(mock_client)
    assert len(rows) == 1500, "must paginate past 1000-row cap (RESEARCH Pitfall 2)"
    assert rows[0]["market_id"] == "m0"
    assert rows[1000]["market_id"] == "m1000"


def test_fetch_single_page_under_1000():
    """Loop exits when a batch is shorter than page_size."""
    from polyarb.observation.l2_candidate_refresh import _fetch_all_markets_latest

    page = [{"market_id": f"m{i}"} for i in range(300)]
    mock_client = _make_supabase_mock_with_pages([page])

    rows = _fetch_all_markets_latest(mock_client)
    assert len(rows) == 300


def _supabase_settings(db_path: Path):
    """Settings with Supabase URL + key + a real (but minimal) db_path fallback."""
    from pydantic import SecretStr

    from polyarb.config import Settings

    return Settings(
        db_path=db_path,
        parquet_root=db_path.parent / "snapshots",
        supabase_url="https://x.supabase.co",
        supabase_service_key=SecretStr("test-key"),
    )


def _narrow_for_near_end(market_id: str = "m1") -> dict:
    """A narrow row that satisfies the near-end builtin recipe.

    Builtin near-end WHERE:
      end_time_ms BETWEEN strftime('%s','now')*1000 AND strftime('%s','now','+72 hours')*1000
      AND liquidity_usd > 1000

    Use end_time ~24h from now + liquidity 100k to comfortably satisfy.
    """
    import time

    return {
        "market_id": market_id,
        "question": "Q?",
        "slug": f"slug-{market_id}",
        "event_slug": "ev",
        "mid_price": 0.5,
        "liquidity_usd": 100_000.0,
        "volume_usd": 10_000.0,
        # 24h from now — well within the 72h near-end window.
        "end_time_ms": int((time.time() + 24 * 3600) * 1000),
        "snapshot_id": 1,
        "yes_token_id": f"YES-{market_id}",
        "question_zh": None,
    }


def test_compute_candidates_uses_temp_db_when_markets_rows(tmp_path):
    """When markets_rows provided, build temp DB; temp file unlinked after."""
    from polyarb.observation.l2_candidate_refresh import compute_candidates

    settings = _supabase_settings(tmp_path / "state.db")
    # Track build_temp_db return so we can verify cleanup.
    built_paths: list[Path] = []
    import polyarb.observation.l2_candidate_refresh as mod
    real_build = mod.build_temp_db

    def _spy(rows):
        p = real_build(rows)
        built_paths.append(p)
        return p

    import unittest.mock as um

    with um.patch.object(mod, "build_temp_db", side_effect=_spy):
        rows = compute_candidates(settings, markets_rows=[_narrow_for_near_end("m1")])

    # near-end default recipe should produce at least our row.
    assert any(r.asset_id == "YES-m1" for r in rows), (
        f"expected YES-m1 in candidates, got {[r.asset_id for r in rows]}"
    )
    # Cleanup verified: built temp path unlinked.
    assert built_paths, "build_temp_db spy should have been invoked"
    for p in built_paths:
        assert not p.exists(), f"temp file {p} not cleaned up (/tmp leak)"


def test_compute_candidates_fallback_to_db_path_when_no_rows(settings_with_db, tmp_path):
    """markets_rows=None → falls back to Path(settings.db_path) (D-04)."""
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
        "    limit: 10\n"
    )
    # markets_rows omitted — should fall back to settings.db_path.
    rows = compute_candidates(settings, scanner_yaml, None)
    assert len(rows) == 3, f"D-04 fallback failed; got {len(rows)} rows"


@pytest.mark.asyncio
async def test_supabase_fetch_fail_uses_last_known(tmp_path):
    """on_snapshot_complete with create_client raising → no exception, fail-soft."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.observation.l2_candidate_refresh import on_snapshot_complete

    # Seed last-known rows so fail-soft has something to fall back on.
    mod._last_known_markets_rows = [_narrow_for_near_end("m1")]

    settings = _supabase_settings(tmp_path / "state.db")
    settings.candidate_scanner_yaml = None
    settings.candidate_watchlist_yaml = None

    with patch("polyarb.observation.l2_candidate_refresh.create_client") as mock_create:
        mock_create.side_effect = RuntimeError("network down")
        fake_ws = MagicMock()
        fake_ws.subscribed_assets = []
        fake_ws._subscribed_assets = []
        # MUST NOT raise.
        ok = await on_snapshot_complete(
            {"snapshot_id": 99, "taken_at_ms": 1},
            ws_consumer=fake_ws,
            settings=settings,
        )
    assert ok is True, "refresh should still run (fail-soft) on fetch failure"


def test_cap_500_with_supabase_rows(tmp_path):
    """600 narrow rows from Supabase → candidate set capped at 500 (D-03)."""
    from polyarb.observation.l2_candidate_refresh import MAX_CANDIDATES, compute_candidates

    settings = _supabase_settings(tmp_path / "state.db")
    rows = [_narrow_for_near_end(f"m{i}") for i in range(600)]
    out = compute_candidates(settings, markets_rows=rows)
    assert len(out) <= MAX_CANDIDATES == 500, f"cap violated: {len(out)} > 500"


@pytest.mark.asyncio
async def test_on_snapshot_complete_records_fetch_success_on_supabase_call(tmp_path):
    """Successful supabase fetch updates _last_fetch_success_at_s (chain-truth)."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.observation.l2_candidate_refresh import on_snapshot_complete

    settings = _supabase_settings(tmp_path / "state.db")
    settings.candidate_scanner_yaml = None
    settings.candidate_watchlist_yaml = None

    mock_client = _make_supabase_mock_with_pages([[_narrow_for_near_end("m1")]])
    with patch("polyarb.observation.l2_candidate_refresh.create_client", return_value=mock_client):
        fake_ws = MagicMock()
        fake_ws.subscribed_assets = []
        fake_ws._subscribed_assets = []
        ok = await on_snapshot_complete(
            {"snapshot_id": 1, "taken_at_ms": 1},
            ws_consumer=fake_ws,
            settings=settings,
        )
    assert ok is True
    assert mod._last_fetch_success_at_s is not None, "fetch success timestamp not recorded"
    assert len(mod._last_known_markets_rows or []) == 1
    assert (mod._last_known_markets_rows or [{}])[0]["market_id"] == "m1"
