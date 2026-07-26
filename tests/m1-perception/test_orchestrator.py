"""End-to-end orchestrator tests with mocked Gamma + CLOB clients.

These exercise ``run_snapshot`` against the recorded fixtures (no live API
calls). They validate:

  T6.1  Full pipeline produces SQLite snapshot row + Parquet file
  T6.2  Layer 1 mismatch flips is_valid=False
  T6.3  Ghost-book detection appears in validation_issues
  T6.4  F-1 unparseable price → Issue rather than crash
  T6.5  fetched_at_ms is stamped on every row written to SQLite

Test isolation:
  - tmp_path-based db_path / parquet_root (escapes project root via
    ``POLYARB_ALLOW_EXTERNAL_PATHS=1`` environment escape hatch — see config.py F-3).
  - GammaClient.fetch_all_active_markets and ClobReaderClient.get_books /
    get_prices_buy_sell are patched at the symbol used by the orchestrator
    (``polyarb.snapshot.orchestrator.GammaClient`` etc).
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

# F-3 escape hatch: tmp_path is outside project root by design. Set BEFORE any
# Settings instantiation so the path validator allows external paths.
os.environ["POLYARB_ALLOW_EXTERNAL_PATHS"] = "1"

from polyarb.config import Settings  # noqa: E402
from polyarb.snapshot.orchestrator import (  # noqa: E402
    _include_in_snapshot,
    run_snapshot,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_subset_always_keeps_low_liquidity_neg_risk_sibling() -> None:
    market = {"liquidity_usd": 1.0, "neg_risk_market_id": "group-1"}

    assert _include_in_snapshot("subset", market, threshold=1000.0) is True


def _load_gamma_fixture() -> list[dict]:
    """Load the recorded Gamma /markets fixture (5 real markets)."""
    return json.loads((FIXTURES_DIR / "gamma_sample.json").read_text())


def _load_clob_fixture() -> dict:
    """Load the recorded CLOB books + prices fixture."""
    return json.loads((FIXTURES_DIR / "clob_sample.json").read_text())


def _make_settings(tmp_path: Path) -> Settings:
    """Build a Settings instance pointing at tmp_path for db + parquet."""
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        # Lower threshold so subset mode keeps multiple fixture markets.
        liquidity_threshold_usd=100.0,
    )


def test_unit_settings_never_inherit_cloud_credentials(tmp_path: Path) -> None:
    """Mocked snapshot tests must never auto-enable production cloud adapters."""
    settings = _make_settings(tmp_path)

    assert settings.supabase_mirror_enabled is False
    assert settings.l2_mirror_enabled is False
    assert settings.r2_enabled is False


def _books_as_objects(book_dicts: list[dict]) -> list[SimpleNamespace]:
    """Wrap dicts as ``SimpleNamespace`` so the orchestrator's
    ``hasattr(b, '__dict__')`` indexing path is exercised (matches what the real
    py-clob-client SDK returns: dataclass-like objects with ``.asset_id``)."""
    return [SimpleNamespace(**bd) for bd in book_dicts]


def _make_fake_gamma(markets: list[dict], events: list[dict] | None = None) -> AsyncMock:
    """Build a fake_gamma with both /markets and /events return values configured.

    Phase 1.1 Amendment 01: orchestrator phase 1 fetches /events first then
    /markets. Tests that don't care about events can pass events=None (default
    empty list) — markets still get event_id=None which is acceptable.
    """
    fake = AsyncMock()
    fake.fetch_all_active_markets.return_value = markets
    fake.fetch_all_active_events.return_value = events if events is not None else []

    # Plan 02-09 (D-23): orchestrator now consumes iter_active_markets (async
    # generator). AsyncMock returns a coroutine by default, not iterable —
    # supply a real async-generator function bound on the mock instance.
    def _make_iter(items):
        async def _iter(coverage):
            for item in items:
                yield item
            coverage.result = type(coverage.result)(len(items), 1, True, None)

        return _iter

    fake.iter_active_markets = _make_iter(markets)
    fake.iter_active_events = _make_iter(events if events is not None else [])

    async def _fetch_market_states(market_ids):
        return {market_id: {"active": True, "closed": False} for market_id in market_ids}

    fake.fetch_market_states = AsyncMock(side_effect=_fetch_market_states)

    async def _fetch_market_parent_states(market_groups):
        return {
            market_id: {
                "event_id": f"parent-{market_id}",
                "active": True,
                "closed": False,
                "archived": False,
            }
            for market_id in market_groups
        }

    fake.fetch_market_parent_states = AsyncMock(side_effect=_fetch_market_parent_states)
    fake.aclose = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    return fake


def _events_for_markets(markets: list[dict]) -> list[dict]:
    """Synthesize one event per market so each market gets a populated event_id."""
    return [
        {
            "id": f"EV-{m['id']}",
            "slug": f"event-{m['id']}",
            "title": f"Event for {m['id']}",
            "ticker": "TKR",
            "active": True,
            "closed": False,
            "liquidity": 1000.0,
            "volume": 5000.0,
            "endDate": "2026-12-31T00:00:00Z",
            "tags": [{"id": "120", "label": "Test", "slug": "test"}],
            "markets": [{"id": m["id"]}],
        }
        for m in markets
    ]


def _standard_neg_risk_event(markets: list[dict]) -> dict:
    """Build one authoritative standard neg-risk event for the supplied markets."""
    return {
        "id": "EV-neg-risk",
        "slug": "event-neg-risk",
        "title": "Neg-risk event",
        "ticker": "NEG",
        "active": True,
        "closed": False,
        "liquidity": 1000.0,
        "volume": 5000.0,
        "endDate": "2026-12-31T00:00:00Z",
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "group-neg-risk",
        "markets": [
            {
                "id": market["id"],
                "active": True,
                "closed": False,
                "negRiskOther": False,
                "groupItemTitle": f"Outcome {index}",
            }
            for index, market in enumerate(markets)
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# T6.1 — Full pipeline produces SQLite + Parquet with mocks
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_pipeline_writes_sqlite_and_parquet(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # SnapshotResult sanity
    assert result.market_count == len(gamma_data) == 5
    assert result.mode == "subset"
    assert result.is_valid is True  # no Layer-1 issue (count matches)
    assert result.parquet_path.exists(), f"Parquet missing: {result.parquet_path}"
    assert result.snapshot_id >= 1

    # SQLite contents
    con = sqlite3.connect(settings.db_path)
    snapshot_count = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    market_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    con.close()
    assert snapshot_count == 1
    assert market_count == 5  # all 5 fixture markets persisted (mark-don't-drop)


# ─────────────────────────────────────────────────────────────────────────────
# T6.2 — Layer 1 mismatch flips is_valid=False
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_layer1_count_mismatch_flips_is_valid_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If normalize_market drops a row, gamma_count_reported != len(markets)
    after normalize, which is exactly what Layer 1 catches.

    Force-drop one market by patching normalize_market to return None for the
    last row. The unmodified Gamma fetch reports the original count.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    # Real normalize for first 4, None for the 5th.
    from polyarb.snapshot import orchestrator as orch_mod

    real_normalize = orch_mod.normalize_market
    seen = {"count": 0}

    def fake_normalize(raw: dict, market_to_event_map=None):
        seen["count"] += 1
        if seen["count"] >= 5:
            return None  # drop the 5th — orchestrator now sees 4/5 → Layer 1 fires
        return real_normalize(raw, market_to_event_map)

    monkeypatch.setattr(orch_mod, "normalize_market", fake_normalize)

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert result.is_valid is False, "Layer 1 mismatch must flip is_valid"
    assert "api_jitter" in result.issue_categories, (
        f"expected api_jitter in {result.issue_categories}"
    )
    # Confirm the flag is also persisted in SQLite (D-D3).
    con = sqlite3.connect(settings.db_path)
    is_valid_row = con.execute("SELECT is_valid FROM snapshots LIMIT 1").fetchone()[0]
    con.close()
    assert is_valid_row == 0, "is_valid must be persisted as 0 when validation fails"


# ─────────────────────────────────────────────────────────────────────────────
# T6.3 — Ghost-book detection appears in validation_issues
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ghost_book_detected_in_validation_issues(tmp_path: Path) -> None:
    """Construct a book whose top-of-book looks dead (ask=0.99, bid=0.01) but
    /price disagrees with the ask by more than 0.05 → Layer 4 GHOST_BOOK Issue.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()

    # Inject a synthetic book for market[0]'s yes_token_id whose top-of-book
    # screams "dead" but whose /price is divergent → exactly the ghost-book signal.
    yes_tid = json.loads(gamma_data[0]["clobTokenIds"])[0]
    ghost_book = {
        "market": gamma_data[0]["conditionId"],
        "asset_id": yes_tid,
        "timestamp": "1777448920617",
        "bids": [{"price": "0.01", "size": "1.0"}],
        "asks": [{"price": "0.99", "size": "1.0"}],
    }

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects([ghost_book]))
        # /price says 0.55 — far from book ask of 0.99 → ghost-book signal
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={
                "buy": {yes_tid: {"BUY": "0.55"}},
                "sell": {yes_tid: {"SELL": "0.56"}},
            }
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert "ghost_book" in result.issue_categories, (
        f"expected ghost_book Issue, got categories: {result.issue_categories}"
    )

    # And the ghost_book row is queryable from SQLite (the operator path).
    con = sqlite3.connect(settings.db_path)
    rows = con.execute(
        "SELECT layer, category FROM validation_issues WHERE category = 'ghost_book'"
    ).fetchall()
    con.close()
    assert rows, "ghost_book row must be persisted to validation_issues"
    assert all(r[0] == 4 for r in rows), "ghost_book is a Layer 4 finding"


# ─────────────────────────────────────────────────────────────────────────────
# T6.4 — F-1: unparseable book price → Issue not crash
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_f1_unparseable_price_does_not_crash(tmp_path: Path) -> None:
    """Inject a book whose top-of-book ``size`` is the literal string "NaN-bad"
    (unparseable). The orchestrator must NOT raise; instead it must record an
    Issue(layer=4, category=UNKNOWN) and the snapshot must still persist.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    yes_tid = json.loads(gamma_data[0]["clobTokenIds"])[0]
    bad_book = {
        "market": gamma_data[0]["conditionId"],
        "asset_id": yes_tid,
        "timestamp": "1777448920617",
        "bids": [{"price": "0.42", "size": "100.0"}],
        # Unparseable price string — orchestrator's float() must catch (F-1).
        "asks": [{"price": "NaN-bad", "size": "1.0"}],
    }

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects([bad_book]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": {yes_tid: {"BUY": "0.55"}}, "sell": {yes_tid: {"SELL": "0.56"}}}
        )

        # Must NOT raise — F-1 mandates "log Issue and continue."
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # Snapshot still persisted (D-D3).
    assert result.market_count == 5
    assert result.parquet_path.exists()

    # An UNKNOWN-layer-4 issue must be present for the unparseable ask.
    con = sqlite3.connect(settings.db_path)
    rows = con.execute(
        "SELECT layer, category, detail FROM validation_issues "
        "WHERE category = 'unknown' AND layer = 4"
    ).fetchall()
    con.close()
    assert any("unparseable ask" in (r[2] or "") for r in rows), (
        f"expected 'unparseable ask' Issue, got rows: {rows}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# T6.5 — fetched_at_ms is stamped on rows in DB
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetched_at_ms_stamped_on_db_rows(tmp_path: Path) -> None:
    """Every market row in the SQLite ``markets`` table must have a non-null
    ``fetched_at_ms`` populated by the orchestrator (Pitfall 6).
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    rows = con.execute("SELECT market_id, fetched_at_ms FROM markets").fetchall()
    con.close()
    assert rows, "markets table must have rows"
    for market_id, fetched_at_ms in rows:
        assert fetched_at_ms is not None, f"market_id={market_id} has NULL fetched_at_ms"
        assert fetched_at_ms > 0, f"market_id={market_id} has invalid fetched_at_ms"

    # SnapshotResult.finished_at_ms must be >= taken_at_ms (sanity).
    assert result.finished_at_ms >= result.taken_at_ms


# ─────────────────────────────────────────────────────────────────────────────
# Bonus: CLOB unreachable handled gracefully (D-E2 / D-D3)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_clob_unreachable_records_issue_but_persists_snapshot(tmp_path: Path) -> None:
    """If CLOB blows up, orchestrator records an API_UNREACHABLE Layer 4 issue
    and STILL writes the snapshot — snapshot row is queryable (D-D3 + D-E2)."""
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(side_effect=RuntimeError("simulated CLOB outage"))
        clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert result.market_count == 5
    assert "api_unreachable" in result.issue_categories
    assert "clob_missing" not in result.issue_categories
    clob_inst.get_books.assert_awaited_once()
    assert clob_inst.get_books.await_args.kwargs["projection"] == "top"
    # Snapshot row exists despite CLOB failure
    con = sqlite3.connect(settings.db_path)
    n = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    con.close()
    assert n == 1


# =============================================================================
# Plan 01-5 T3 — additional orchestrator coverage (extends Wave 3 unit tests)
# =============================================================================
#
# These tests use the conftest-provided ``mocked_gamma_orchestrator`` and
# ``mocked_clob`` fixtures (added in Plan 01-5 T1) so behavior is consistent
# across orchestrator + cli + integration layers. Existing Wave-3 tests above
# remain unchanged; the new tests below extend coverage.


@pytest.mark.asyncio
async def test_full_mode_uses_all_markets(
    settings_for_test, mocked_gamma_orchestrator, mocked_clob
) -> None:
    """``mode="full"`` must fetch CLOB books for every market regardless of
    the liquidity_threshold_usd subset filter."""
    result = await run_snapshot(settings_for_test, mode="full", now_ms=1_714_435_200_000)
    assert result.mode == "full"
    # CLOB was called at least once (full mode always exercises CLOB if there
    # are tokens, and the fixture has 5 markets * 2 tokens = 10 token ids).
    assert mocked_clob["books"].call_count >= 1


@pytest.mark.asyncio
async def test_subset_mode_persists_correct_mode_column(
    settings_for_test, mocked_gamma_orchestrator, mocked_clob
) -> None:
    """SQLite ``snapshots.mode`` column must reflect the requested mode."""
    result = await run_snapshot(settings_for_test, mode="subset", now_ms=1_714_435_200_000)
    con = sqlite3.connect(settings_for_test.db_path)
    mode_row = con.execute(
        "SELECT mode FROM snapshots WHERE id = ?", (result.snapshot_id,)
    ).fetchone()
    is_valid_row = con.execute(
        "SELECT is_valid FROM snapshots WHERE id = ?", (result.snapshot_id,)
    ).fetchone()
    con.close()
    assert mode_row[0] == "subset"
    assert is_valid_row[0] == int(result.is_valid)


@pytest.mark.asyncio
async def test_writes_parquet_with_string_token_id_schema(
    settings_for_test, mocked_gamma_orchestrator, mocked_clob
) -> None:
    """Parquet schema preserves ``yes_token_id`` as string (Pitfall 3 end-to-end).

    Polymarket's uint256 token IDs have 70+ decimal digits and overflow int64.
    pyarrow must write them as ``pa.string()`` — anything else risks silent
    corruption when round-tripping through Parquet.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    result = await run_snapshot(settings_for_test, mode="subset", now_ms=1_714_435_200_000)
    table = pq.read_table(result.parquet_path)
    assert "yes_token_id" in table.column_names
    assert table.schema.field("yes_token_id").type == pa.string()
    assert "no_token_id" in table.column_names
    assert table.schema.field("no_token_id").type == pa.string()
    assert table.num_rows == result.market_count


@pytest.mark.asyncio
async def test_per_row_fetched_at_ms_set(
    settings_for_test, mocked_gamma_orchestrator, mocked_clob
) -> None:
    """All rows share exactly one fetched_at_ms (CLOB-completion time)."""
    result = await run_snapshot(settings_for_test, mode="subset", now_ms=1_714_435_200_000)
    con = sqlite3.connect(settings_for_test.db_path)
    distinct_ts = con.execute(
        "SELECT DISTINCT fetched_at_ms FROM markets WHERE snapshot_id = ?",
        (result.snapshot_id,),
    ).fetchall()
    con.close()
    assert len(distinct_ts) == 1, (
        f"All rows should share one fetched_at_ms, got {len(distinct_ts)} distinct values"
    )
    (single_ts,) = distinct_ts[0]
    assert single_ts is not None and single_ts > 0


@pytest.mark.asyncio
async def test_validation_issues_have_non_empty_categories(
    settings_for_test, mocked_gamma_orchestrator, mocked_clob
) -> None:
    """D-D4: every persisted Issue row must have a non-empty category string."""
    await run_snapshot(settings_for_test, mode="subset", now_ms=1_714_435_200_000)
    con = sqlite3.connect(settings_for_test.db_path)
    cats = con.execute("SELECT DISTINCT category FROM validation_issues").fetchall()
    con.close()
    # If there are no issues at all, the assertion is vacuously true (some
    # subset runs are clean). If there ARE rows, none may have empty category.
    for (cat,) in cats:
        assert cat and isinstance(cat, str) and len(cat) > 0, (
            f"empty category found in validation_issues row: {cat!r}"
        )


@pytest.mark.asyncio
async def test_subset_filter_excludes_high_threshold(
    mocked_gamma_orchestrator, mocked_clob, tmp_db_path: Path, tmp_parquet_root: Path
) -> None:
    """With an impossibly-high liquidity_threshold_usd, no fixture market
    passes the filter and CLOB.get_books is called with an empty token list."""
    settings = Settings(
        db_path=tmp_db_path,
        parquet_root=tmp_parquet_root,
        retry_attempts=2,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=999_999_999.0,  # nothing passes
    )
    await run_snapshot(settings, mode="subset", now_ms=1_714_435_200_000)
    # CLOB.get_books was called once; the call's first positional arg is the
    # token-id list which must be empty (subset filter caught everything).
    assert mocked_clob["books"].await_count == 1
    call_args = mocked_clob["books"].call_args
    if call_args.args:
        token_arg = call_args.args[0]
    else:
        token_arg = call_args.kwargs.get("token_ids", [])
    assert token_arg == []


@pytest.mark.asyncio
async def test_invalid_mode_raises_value_error(
    settings_for_test, mocked_gamma_orchestrator, mocked_clob
) -> None:
    """run_snapshot enforces mode in ('subset', 'full') — anything else raises
    a ValueError before any I/O happens.
    """
    with pytest.raises(ValueError, match="invalid mode"):
        await run_snapshot(settings_for_test, mode="weekly")


# ─────────────────────────────────────────────────────────────────────────────
# T6.6 (post-live-run regression) — Gamma duplicate market_id is deduped
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_gamma_duplicate_market_id_deduped(tmp_path: Path) -> None:
    """Live empirical (2026-04-29): Gamma /markets returns ~4% duplicate market_ids
    across pagination boundaries (1,960 dups in 48,985 rows). Without dedupe,
    SQLite UNIQUE constraint on markets.market_id rolls back the entire snapshot.
    The orchestrator MUST dedupe by market_id (keep first) before persist.
    """
    settings = _make_settings(tmp_path)
    raw = _load_gamma_fixture()
    # Synthesize a duplicate: append the first market again with drifted liquidity.
    duplicated = raw + [{**raw[0], "liquidityNum": float(raw[0].get("liquidityNum", 0)) + 100}]

    fake_gamma = _make_fake_gamma(duplicated, _events_for_markets(duplicated))

    clob_data = _load_clob_fixture()
    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # Despite Gamma returning 6 rows (5 originals + 1 dup), persisted count must be 5.
    assert result.market_count == 5, (
        f"Dedupe failed — expected 5 unique markets persisted, got {result.market_count}"
    )

    con = sqlite3.connect(settings.db_path)
    db_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    distinct = con.execute("SELECT COUNT(DISTINCT market_id) FROM markets").fetchone()[0]
    con.close()
    assert db_count == 5
    assert distinct == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conflicting_fields",
    [
        {"active": False},
        {"closed": True},
    ],
)
async def test_ordinary_market_duplicate_state_conflict_blocks_publication(
    tmp_path: Path,
    conflicting_fields: dict[str, object],
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    valid = {
        **_load_gamma_fixture()[0],
        "active": True,
        "closed": False,
        "negRisk": False,
        "negRiskMarketID": None,
    }
    conflicting_duplicate = {**valid, **conflicting_fields}
    fake_gamma = _make_fake_gamma(
        [valid, conflicting_duplicate],
        _events_for_markets([valid]),
    )
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "duplicate-market-truth-conflict" in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("conflicting_fields", "reason_fragment"),
    [
        ({"negRiskMarketID": "conflicting-group"}, "group-id"),
        ({"active": False}, "active"),
    ],
)
async def test_conflicting_duplicate_market_truth_blocks_publication(
    tmp_path: Path,
    conflicting_fields: dict[str, object],
    reason_fragment: str,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    valid = {
        **_load_gamma_fixture()[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    conflicting_duplicate = {**valid, **conflicting_fields}
    fake_gamma = _make_fake_gamma(
        [valid, conflicting_duplicate],
        [_standard_neg_risk_event([valid])],
    )
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert reason_fragment in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


# ─────────────────────────────────────────────────────────────────────────────
# T6.7 (post-live-run regression) — only target_markets persisted in subset
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subset_persists_only_target_markets(tmp_path: Path) -> None:
    """Live empirical (2026-04-29): orchestrator was persisting ALL normalized
    markets (48,985) instead of just target_markets (subset = 17,955). This
    regressed validation_issues by flooding 91k+ L4 phantom warnings on tokens
    we never even fetched from CLOB. Subset persist scope must equal target_markets.
    """
    settings = _make_settings(tmp_path)
    # Set high threshold so only one fixture market passes subset.
    settings = Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        liquidity_threshold_usd=10_000_000.0,  # only the highest-liquidity fixture passes (or none)
    )
    gamma_data = _load_gamma_fixture()

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    clob_data = _load_clob_fixture()
    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # All 5 fixture markets are below 10M liquidity → 0 persisted (subset is empty).
    assert result.market_count == 0
    con = sqlite3.connect(settings.db_path)
    db_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    con.close()
    assert db_count == 0, f"Persist scope leaked: SQLite has {db_count} rows for empty subset"


# ─────────────────────────────────────────────────────────────────────────────
# Phase timing visibility — every phase emits a 'done in <duration>' line
# and the overall run prints a 'Snapshot complete in <duration>' summary.
#
# Backstory: LIVE-RUN-003/004 hung silently because logs only showed the phase
# label, never per-phase timings. This pins the contract that every phase
# emits a measurable duration so future stalls localize to a specific phase.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_phase_timing_lines_emitted(tmp_path: Path) -> None:
    from loguru import logger

    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    captured: list[str] = []
    sink_id = logger.add(lambda msg: captured.append(msg.record["message"]), level="INFO")

    try:
        with (
            patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
            patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        ):
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
            clob_inst.get_prices_buy_sell = AsyncMock(
                return_value={
                    "buy": clob_data["prices_buy"],
                    "sell": clob_data["prices_sell"],
                }
            )
            await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)
    finally:
        logger.remove(sink_id)

    # Each phase must emit one 'start' and one 'done in <duration>' line
    for phase_num in range(1, 8):
        phase_messages = [m for m in captured if f"Phase {phase_num}/7" in m]
        starts = [m for m in phase_messages if "start" in m]
        dones = [m for m in phase_messages if "done in" in m]
        assert len(starts) == 1, (
            f"phase {phase_num}/7 missing 'start' (or duplicated): {phase_messages}"
        )
        assert len(dones) == 1, (
            f"phase {phase_num}/7 missing 'done in' (or duplicated): {phase_messages}"
        )

    # Overall completion summary must be present
    overall = [m for m in captured if "Snapshot complete in" in m]
    assert len(overall) == 1, f"missing overall summary line, got: {captured}"


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1.1 Amendment 01 — events / event_tags / event_id flow through pipeline
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_amendment_01_events_persisted_to_sqlite(tmp_path: Path) -> None:
    """End-to-end: /events response → SQLite events + event_tags tables.

    Verifies the new Phase 1 sub-step (events fetch) produces persisted rows
    that carry the snapshot_id FK and downstream queries can join on it.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    events_data = _events_for_markets(gamma_data)

    fake_gamma = _make_fake_gamma(gamma_data, events_data)

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    try:
        events_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        event_tags_count = con.execute("SELECT COUNT(*) FROM event_tags").fetchone()[0]
        # Events linked to this snapshot
        ev_for_snap = con.execute(
            "SELECT COUNT(*) FROM events WHERE snapshot_id = ?",
            (result.snapshot_id,),
        ).fetchone()[0]
    finally:
        con.close()

    assert events_count == 5, f"expected 5 events, got {events_count}"
    # 5 events × 1 tag each = 5 event_tags rows
    assert event_tags_count == 5, f"expected 5 event_tags, got {event_tags_count}"
    assert ev_for_snap == 5


@pytest.mark.asyncio
async def test_amendment_01_event_id_populated_on_markets(tmp_path: Path) -> None:
    """Each persisted market row must carry an event_id flowing from /events."""
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    events_data = _events_for_markets(gamma_data)

    fake_gamma = _make_fake_gamma(gamma_data, events_data)

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    try:
        rows = con.execute("SELECT market_id, event_id FROM markets ORDER BY market_id").fetchall()
        # JOIN check — every market_row's event_id must exist in events table.
        joined = con.execute(
            "SELECT COUNT(*) FROM markets m INNER JOIN events e ON m.event_id = e.id"
        ).fetchone()[0]
    finally:
        con.close()

    assert len(rows) == 5
    # Every market gets event_id (the synthetic events fixture maps 1:1).
    for market_id, event_id in rows:
        assert event_id == f"EV-{market_id}", (
            f"market {market_id} got event_id={event_id}, expected EV-{market_id}"
        )
    # JOIN sanity: all 5 markets join to events.
    assert joined == 5


@pytest.mark.asyncio
async def test_amendment_01_non_neg_risk_orphan_market_is_published(
    tmp_path: Path,
) -> None:
    """Ordinary markets may outlive the active event catalogue."""
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    # /events fixture mentions ONLY the first market — markets 1-4 are orphans
    partial_events = _events_for_markets([gamma_data[0]])

    fake_gamma = _make_fake_gamma(gamma_data, partial_events)

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    try:
        market_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
    finally:
        con.close()

    assert result.is_valid is True
    assert "api_unreachable" not in result.issue_categories
    assert market_count == 5
    assert coverage == (1, None, None)
    assert publish_mock.await_count == 1

    with sqlite3.connect(settings.db_path) as con:
        orphan_rows = con.execute(
            "SELECT event_id, neg_risk, neg_risk_market_id FROM markets WHERE event_id IS NULL"
        ).fetchall()
    assert orphan_rows == [(None, 0, None)] * 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("neg_risk", "group_id", "drop_neg_risk"),
    [
        (True, None, False),
        (False, "unverified-group", False),
        ("false", None, False),
        (False, None, True),
    ],
)
async def test_orphan_market_requires_canonical_non_neg_risk_truth(
    tmp_path: Path,
    neg_risk: object,
    group_id: str | None,
    drop_neg_risk: bool,
) -> None:
    """Only an explicit canonical false/no-group row gets the event exemption."""
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "negRisk": neg_risk,
        "negRiskMarketID": group_id,
    }
    if drop_neg_risk:
        market.pop("negRisk")
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma([market], [])

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        market_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert market_count == 0
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_orphan_neg_risk_market_with_inactive_parent_is_quarantined(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "negRisk": True,
        "negRiskMarketID": "stale-group",
    }
    fake_gamma = _make_fake_gamma([market], [])
    fake_gamma.fetch_market_parent_states.side_effect = None
    fake_gamma.fetch_market_parent_states.return_value = {
        market["id"]: {
            "event_id": "inactive-parent",
            "active": False,
            "closed": False,
            "archived": True,
        }
    }
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        market_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        membership_count = con.execute(
            "SELECT COUNT(*) FROM event_market_memberships WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()[0]
        group_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_group_truth WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()[0]
        layer1 = con.execute(
            "SELECT category,detail FROM validation_issues WHERE snapshot_id=? AND layer=1",
            (result.snapshot_id,),
        ).fetchall()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert market_count == 0
    assert membership_count == 0
    assert group_count == 0
    assert layer1 == [
        (
            "api_jitter",
            f"Gamma stale neg-risk market quarantined: {market['id']}",
        )
    ]
    fake_gamma.fetch_market_parent_states.assert_awaited_once_with({market["id"]: "stale-group"})
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_neg_risk_markets_without_group_identity_are_quarantined_in_full_mode(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    template = _load_gamma_fixture()
    group_less_markets = [
        {
            **template[index],
            "id": f"group-less-{index}",
            "conditionId": f"group-less-condition-{index}",
            "clobTokenIds": json.dumps([f"group-less-yes-{index}", f"group-less-no-{index}"]),
            "negRisk": True,
            "negRiskMarketID": None,
        }
        for index in range(2)
    ]
    ordinary = {
        **template[2],
        "id": "ordinary-market",
        "conditionId": "ordinary-condition",
        "clobTokenIds": json.dumps(["ordinary-yes", "ordinary-no"]),
        "negRisk": False,
        "negRiskMarketID": None,
    }
    group_less_event = {
        "id": "EV-group-less",
        "slug": "event-group-less",
        "title": "Source anomaly without neg-risk group identity",
        "ticker": "GROUPLESS",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "markets": [
            {
                "id": market["id"],
                "active": True,
                "closed": False,
                "negRiskOther": False,
            }
            for market in group_less_markets
        ],
    }
    fake_gamma = _make_fake_gamma(
        [*group_less_markets, ordinary],
        [group_less_event, *_events_for_markets([ordinary])],
    )

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=[])
        clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})
        result = await run_snapshot(settings, mode="full", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        persisted_ids = con.execute(
            "SELECT market_id FROM markets WHERE snapshot_id=? ORDER BY market_id",
            (result.snapshot_id,),
        ).fetchall()
        membership_count = con.execute(
            "SELECT COUNT(*) FROM event_market_memberships WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()[0]
        group_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_group_truth WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()[0]
        layer1 = con.execute(
            "SELECT category,detail FROM validation_issues "
            "WHERE snapshot_id=? AND layer=1 ORDER BY id",
            (result.snapshot_id,),
        ).fetchall()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert persisted_ids == [("ordinary-market",)]
    assert membership_count == 0
    assert group_count == 0
    assert layer1 == [
        (
            "api_jitter",
            "Gamma neg-risk market missing group identity quarantined: group-less-0,group-less-1",
        )
    ]
    assert clob_inst.get_books.await_args.args[0] == ["ordinary-yes", "ordinary-no"]
    assert clob_inst.get_prices_buy_sell.await_args.args[0] == [
        "ordinary-yes",
        "ordinary-no",
    ]
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_137_stale_orphan_neg_risk_markets_are_quarantined(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    template = _load_gamma_fixture()[0]
    markets = [
        {
            **template,
            "id": f"orphan-{index:03d}",
            "conditionId": f"condition-{index:03d}",
            "clobTokenIds": json.dumps([f"yes-token-{index:03d}", f"no-token-{index:03d}"]),
            "negRisk": True,
            "negRiskMarketID": f"stale-group-{index:03d}",
        }
        for index in range(137)
    ]
    fake_gamma = _make_fake_gamma(markets, [])
    fake_gamma.fetch_market_parent_states.side_effect = None
    fake_gamma.fetch_market_parent_states.return_value = {
        market["id"]: {
            "event_id": f"inactive-parent-{market['id']}",
            "active": False,
            "closed": False,
            "archived": True,
        }
        for market in markets
    }

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=[])
        clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        market_count = con.execute(
            "SELECT COUNT(*) FROM markets WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()[0]

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert market_count == 0
    fake_gamma.fetch_market_parent_states.assert_awaited_once_with(
        {market["id"]: market["negRiskMarketID"] for market in markets}
    )
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_orphan_neg_risk_parent_lookup_failure_blocks_publication(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "negRisk": True,
        "negRiskMarketID": "unknown-group",
    }
    fake_gamma = _make_fake_gamma([market], [])
    fake_gamma.fetch_market_parent_states.side_effect = RuntimeError("parent unavailable")
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert coverage[2] == "orphan-parent-state-lookup-failed:RuntimeError"
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_orphan_neg_risk_market_with_active_parent_blocks_publication(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "negRisk": True,
        "negRiskMarketID": "live-group",
    }
    fake_gamma = _make_fake_gamma([market], [])
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert coverage[2] == f"orphan-neg-risk-parent-active:{market['id']}"
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_incomplete_source_group_truth_blocks_publication(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    gamma_data = _load_gamma_fixture()[:1]
    clob_data = _load_clob_fixture()
    event = _standard_neg_risk_event(gamma_data)
    event.pop("enableNegRisk")
    fake_gamma = _make_fake_gamma(gamma_data, [event])

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        assert con.execute(
            "SELECT quality FROM neg_risk_group_truth WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone() == ("incomplete-source",)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "incomplete-source" in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_missing_authoritative_event_member_blocks_publication(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    active_market = {
        **all_markets[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    all_markets[0] = active_market
    observed_markets = [active_market]
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma(
        observed_markets,
        [_standard_neg_risk_event(all_markets)],
    )

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "missing-market" in coverage[2]
    assert all_markets[1]["id"] in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_inactive_event_member_missing_from_active_stream_does_not_block_publication(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    active_market = {
        **all_markets[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    all_markets[0] = active_market
    event = _standard_neg_risk_event(all_markets)
    event["markets"][1]["active"] = False
    fake_gamma = _make_fake_gamma([active_market], [event])
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        persisted_members = con.execute(
            "SELECT market_id,member_kind,active,closed FROM event_market_memberships "
            "WHERE snapshot_id=? ORDER BY market_id",
            (result.snapshot_id,),
        ).fetchall()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert persisted_members == [
        (all_markets[0]["id"], "named", 1, 0),
        (all_markets[1]["id"], "inactive-reserved", 0, 0),
    ]
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_closed_ordinary_mapped_market_missing_from_active_stream_is_reconciled(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    observed_market = all_markets[0]
    missing_market_id = all_markets[1]["id"]
    fake_gamma = _make_fake_gamma(
        [observed_market],
        _events_for_markets(all_markets),
    )
    fake_gamma.fetch_market_states.side_effect = None
    fake_gamma.fetch_market_states.return_value = {
        missing_market_id: {"active": True, "closed": True}
    }
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    fake_gamma.fetch_market_states.assert_awaited_once_with([missing_market_id])
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_ordinary_mapped_market_that_closes_during_snapshot_is_reconciled(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    observed_market = all_markets[0]
    missing_market_id = all_markets[1]["id"]
    fake_gamma = _make_fake_gamma(
        [observed_market],
        _events_for_markets(all_markets),
    )
    fake_gamma.fetch_market_states.side_effect = [
        {missing_market_id: {"active": True, "closed": False}},
        {missing_market_id: {"active": True, "closed": True}},
    ]
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert fake_gamma.fetch_market_states.await_count == 2
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_closed_point_truth_reconciles_stale_open_event_member(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    active_market = {
        **all_markets[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    all_markets[0] = active_market
    event = _standard_neg_risk_event(all_markets)
    missing_market_id = all_markets[1]["id"]
    fake_gamma = _make_fake_gamma([active_market], [event])
    fake_gamma.fetch_market_states.side_effect = None
    fake_gamma.fetch_market_states.return_value = {
        missing_market_id: {"active": True, "closed": True}
    }
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        disagreement = con.execute(
            "SELECT category,detail FROM validation_issues WHERE snapshot_id=? AND layer=1",
            (result.snapshot_id,),
        ).fetchall()
        reconciled_member = con.execute(
            "SELECT member_kind,active,closed FROM event_market_memberships "
            "WHERE snapshot_id=? AND market_id=?",
            (result.snapshot_id, missing_market_id),
        ).fetchone()
        reconciled_group = con.execute(
            "SELECT expected_member_count,quality,reason FROM neg_risk_group_truth "
            "WHERE snapshot_id=? AND neg_risk_market_id='group-neg-risk'",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert disagreement == [
        (
            "api_jitter",
            f"Gamma event/member status disagreement: point truth non-open for {missing_market_id}",
        )
    ]
    assert reconciled_member == ("named", 1, 1)
    assert reconciled_group == (
        2,
        "complete-unsupported",
        "standard-neg-risk-has-non-tradable-members",
    )
    fake_gamma.fetch_market_states.assert_awaited_once_with([missing_market_id])
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_missing_member_that_closes_during_long_snapshot_is_reconciled(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    active_market = {
        **all_markets[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    event = _standard_neg_risk_event([active_market, all_markets[1]])
    missing_market_id = all_markets[1]["id"]
    fake_gamma = _make_fake_gamma([active_market], [event])
    fake_gamma.fetch_market_states.side_effect = [
        {missing_market_id: {"active": True, "closed": False}},
        {missing_market_id: {"active": True, "closed": True}},
    ]
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
        layer1 = con.execute(
            "SELECT category,detail FROM validation_issues "
            "WHERE snapshot_id=? AND layer=1 ORDER BY id",
            (result.snapshot_id,),
        ).fetchall()
        reconciled_member = con.execute(
            "SELECT active,closed FROM event_market_memberships "
            "WHERE snapshot_id=? AND market_id=?",
            (result.snapshot_id, missing_market_id),
        ).fetchone()

    assert result.is_valid is True
    assert coverage == (1, None, None)
    assert layer1 == [
        (
            "api_jitter",
            f"Gamma event/member state changed during snapshot: non-open for {missing_market_id}",
        )
    ]
    assert reconciled_member == (1, 1)
    assert fake_gamma.fetch_market_states.await_args_list == [
        (([missing_market_id],),),
        (([missing_market_id],),),
    ]
    assert publish_mock.await_count == 1


@pytest.mark.asyncio
async def test_missing_member_final_state_lookup_failure_blocks_publication(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    active_market = {
        **all_markets[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    event = _standard_neg_risk_event([active_market, all_markets[1]])
    missing_market_id = all_markets[1]["id"]
    fake_gamma = _make_fake_gamma([active_market], [event])
    fake_gamma.fetch_market_states.side_effect = [
        {missing_market_id: {"active": True, "closed": False}},
        RuntimeError("final lookup unavailable"),
    ]
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed,failure_source,failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage == (
        0,
        "events",
        "event-member-final-state-lookup-failed:RuntimeError",
    )
    assert fake_gamma.fetch_market_states.await_count == 2
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_missing_member_point_lookup_failure_blocks_publication(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    all_markets = _load_gamma_fixture()[:2]
    active_market = {
        **all_markets[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    event = _standard_neg_risk_event([active_market, all_markets[1]])
    fake_gamma = _make_fake_gamma([active_market], [event])
    fake_gamma.fetch_market_states.side_effect = RuntimeError("point lookup unavailable")
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert coverage[2] == "event-member-state-lookup-failed:RuntimeError"
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_market_group_semantic_mismatch_blocks_publication(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "market-side-wrong-group",
    }
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma(
        [market],
        [_standard_neg_risk_event([market])],
    )

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "group-id" in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_duplicate_event_truth_drift_marks_event_coverage_incomplete(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-neg-risk",
    }
    first_event = _standard_neg_risk_event([market])
    conflicting_event = {
        **first_event,
        "negRiskMarketID": "group-conflict",
    }
    fake_gamma = _make_fake_gamma(
        [market],
        [first_event, conflicting_event],
    )
    clob_data = _load_clob_fixture()

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "duplicate-event-truth-conflict" in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_market_side_neg_risk_group_without_truth_blocks_publication(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "orphan-market-side-group",
    }
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma([market], _events_for_markets([market]))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "neg-risk-without-truth" in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("group_id", [None, "   "])
async def test_low_liquidity_orphan_neg_risk_is_checked_before_subset_filter(
    tmp_path: Path,
    group_id: object,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    market = {
        **_load_gamma_fixture()[0],
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": group_id,
        "liquidityNum": 0.0,
        "liquidity": "0",
    }
    fake_gamma = _make_fake_gamma([market], _events_for_markets([market]))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=[])
        clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    with sqlite3.connect(settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone() == (0,)
        coverage = con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()

    assert result.market_count == 0
    assert result.is_valid is False
    assert coverage[:2] == (0, "events")
    assert "neg-risk-without-truth" in coverage[2]
    assert len(coverage[2]) <= 200
    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_amendment_01_events_failure_does_not_kill_snapshot(tmp_path: Path) -> None:
    """If /events fetch fails, the diagnostic snapshot completes without publication.

    Event membership is authoritative market truth. An events outage is recorded
    as an Issue and source-coverage row, but cannot create a partial current view.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.fetch_all_active_events.side_effect = RuntimeError("simulated /events outage")

    # Plan 02-09: provide iter_active_markets async generator.
    # iter_active_events is also stubbed but the test patches the wrapped
    # fetch_all_active_events to raise — for the streaming consumer we need
    # iter_active_events to raise the same RuntimeError.
    async def _iter_markets(coverage):
        for m in gamma_data:
            yield m
        coverage.result = type(coverage.result)(len(gamma_data), 1, True, None)

    async def _iter_events_raise(_coverage):
        raise RuntimeError("simulated /events outage")
        yield  # unreachable but keeps this as an async generator

    fake_gamma.iter_active_markets = _iter_markets
    fake_gamma.iter_active_events = _iter_events_raise

    fake_gamma.aclose = AsyncMock()
    fake_gamma.__aenter__.return_value = fake_gamma
    fake_gamma.__aexit__.return_value = None

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # Diagnostic snapshot completed; api_unreachable Issue recorded for /events.
    assert result.market_count == 5
    assert result.is_valid is False
    assert "api_unreachable" in result.issue_categories

    con = sqlite3.connect(settings.db_path)
    try:
        events_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        market_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        coverage = con.execute(
            "SELECT completed, failure_source FROM snapshot_source_coverage WHERE snapshot_id=?",
            (result.snapshot_id,),
        ).fetchone()
    finally:
        con.close()

    assert events_count == 0
    assert market_count == 0
    assert coverage == (0, "events")


@pytest.mark.asyncio
async def test_incomplete_event_source_preserves_last_complete_market_truth(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    complete_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))
    incomplete_gamma = AsyncMock()

    async def _iter_markets(coverage):
        for market in gamma_data[:1]:
            yield market
        coverage.result = type(coverage.result)(1, 1, True, None)

    async def _iter_events_raise(_coverage):
        raise RuntimeError("events stopped after a partial traversal")
        yield  # pragma: no cover

    incomplete_gamma.iter_active_markets = _iter_markets
    incomplete_gamma.iter_active_events = _iter_events_raise
    incomplete_gamma.aclose = AsyncMock()
    incomplete_gamma.__aenter__.return_value = incomplete_gamma
    incomplete_gamma.__aexit__.return_value = None

    with (
        patch(
            "polyarb.snapshot.orchestrator.GammaClient",
            side_effect=[complete_gamma, incomplete_gamma],
        ),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
            return_value=True,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        first = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)
        second = await run_snapshot(settings, mode="subset", now_ms=1_777_448_001_000)

    assert first.is_valid is True
    assert second.is_valid is False
    assert publish_mock.await_count == 1

    with sqlite3.connect(settings.db_path) as con:
        current_markets = con.execute(
            "SELECT market_id, snapshot_id FROM markets ORDER BY market_id"
        ).fetchall()
        incomplete_coverage = con.execute(
            "SELECT completed, failure_source FROM snapshot_source_coverage WHERE snapshot_id=?",
            (second.snapshot_id,),
        ).fetchone()

    assert current_markets == sorted([(market["id"], first.snapshot_id) for market in gamma_data])
    assert incomplete_coverage == (0, "events")


@pytest.mark.asyncio
async def test_amendment_01_event_tags_writeable_with_multiple_tags(tmp_path: Path) -> None:
    """Verify event_tags table can hold multiple tags per event (many-to-many)."""
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    # Synthesize events with 3 tags each (Finance/Crypto/Tech) instead of just 1
    events_data = []
    for m in gamma_data:
        ev = {
            "id": f"EV-{m['id']}",
            "slug": f"event-{m['id']}",
            "title": f"Event for {m['id']}",
            "ticker": "TKR",
            "active": True,
            "closed": False,
            "liquidity": 1000.0,
            "volume": 5000.0,
            "endDate": "2026-12-31T00:00:00Z",
            "tags": [
                {"id": "120", "label": "Finance", "slug": "finance"},
                {"id": "121", "label": "Crypto", "slug": "crypto"},
                {"id": "122", "label": "Tech", "slug": "tech"},
            ],
            "markets": [{"id": m["id"]}],
        }
        events_data.append(ev)

    fake_gamma = _make_fake_gamma(gamma_data, events_data)

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    try:
        # 5 events × 3 tags = 15 event_tags rows
        et_count = con.execute("SELECT COUNT(*) FROM event_tags").fetchone()[0]
        # Verify "Finance" tag is searchable via the index
        finance_count = con.execute(
            "SELECT COUNT(*) FROM event_tags WHERE tag_label = 'Finance'"
        ).fetchone()[0]
        # Distinct tag labels
        distinct_labels = con.execute(
            "SELECT COUNT(DISTINCT tag_label) FROM event_tags"
        ).fetchone()[0]
    finally:
        con.close()

    assert et_count == 15
    assert finance_count == 5
    assert distinct_labels == 3


# ─────────────────────────────────────────────────────────────────────────────
# Plan 03-05 — Step 7.7: event bus NOTIFY fan-out (feature-flagged)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_step_7_7_emits_snapshot_complete_when_enabled(tmp_path: Path) -> None:
    """When event_bus_enabled=True, step 7.7 calls publish_snapshot_complete once."""
    settings = _make_settings(tmp_path)
    # Plan 05: enable event bus on this settings instance only (B1 default is False)
    settings.event_bus_enabled = True

    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
            return_value=True,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert result.is_valid is True
    assert publish_mock.await_count == 1
    args, kwargs = publish_mock.await_args
    assert kwargs.get("snapshot_id") == result.snapshot_id
    assert kwargs.get("taken_at_ms") == result.taken_at_ms


@pytest.mark.asyncio
async def test_step_7_7_failsoft_when_publish_raises(tmp_path: Path) -> None:
    """publish_snapshot_complete raising must NOT block snapshot completion."""
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = True

    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
            side_effect=RuntimeError("simulated NOTIFY failure"),
        ),
        patch("polyarb.snapshot.orchestrator.sentry_sdk.add_breadcrumb") as breadcrumb_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        # Must NOT raise — snapshot completes
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert result.snapshot_id >= 1
    # At least one breadcrumb with category="event-bus" level="warning"
    bus_breadcrumbs = [
        c for c in breadcrumb_mock.call_args_list if c.kwargs.get("category") == "event-bus"
    ]
    assert bus_breadcrumbs, "fail-soft step 7.7 must emit event-bus breadcrumb"


@pytest.mark.asyncio
async def test_step_7_7_skipped_when_event_bus_disabled(tmp_path: Path) -> None:
    """event_bus_enabled=False → publish_snapshot_complete NOT called."""
    settings = _make_settings(tmp_path)
    settings.event_bus_enabled = False

    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert publish_mock.await_count == 0


@pytest.mark.asyncio
async def test_step_7_7_skipped_by_default(tmp_path: Path, monkeypatch) -> None:
    """B1 invariant: with POLYARB_EVENT_BUS_ENABLED unset, default is False — no publish."""
    monkeypatch.delenv("POLYARB_EVENT_BUS_ENABLED", raising=False)
    settings = _make_settings(tmp_path)
    # Must be False by default
    assert settings.event_bus_enabled is False

    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
        patch(
            "polyarb.snapshot.orchestrator.publish_snapshot_complete",
            new_callable=AsyncMock,
        ) as publish_mock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert publish_mock.await_count == 0
