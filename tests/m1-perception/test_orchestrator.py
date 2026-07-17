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


def _books_as_objects(book_dicts: list[dict]) -> list[SimpleNamespace]:
    """Wrap dicts as ``SimpleNamespace`` so the orchestrator's
    ``hasattr(b, '__dict__')`` indexing path is exercised (matches what the real
    py-clob-client SDK returns: dataclass-like objects with ``.asset_id``)."""
    return [SimpleNamespace(**bd) for bd in book_dicts]


def _make_fake_gamma(
    markets: list[dict], events: list[dict] | None = None
) -> AsyncMock:
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
        async def _iter():
            for item in items:
                yield item
        return _iter

    fake.iter_active_markets = _make_iter(markets)
    fake.iter_active_events = _make_iter(events if events is not None else [])

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


# ─────────────────────────────────────────────────────────────────────────────
# T6.1 — Full pipeline produces SQLite + Parquet with mocks
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_pipeline_writes_sqlite_and_parquet(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = _make_fake_gamma(gamma_data, _events_for_markets(gamma_data))

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    rows = con.execute(
        "SELECT market_id, fetched_at_ms FROM markets"
    ).fetchall()
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(side_effect=RuntimeError("simulated CLOB outage"))
        clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert result.market_count == 5
    assert "api_unreachable" in result.issue_categories
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
    cats = con.execute(
        "SELECT DISTINCT category FROM validation_issues"
    ).fetchall()
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
    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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
    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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
        with patch(
            "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
        ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(
                return_value=_books_as_objects(clob_data["books"])
            )
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
        starts = [m for m in captured if f"Phase {phase_num}/7" in m and "start" in m]
        dones = [m for m in captured if f"Phase {phase_num}/7" in m and "done in" in m]
        assert len(starts) == 1, (
            f"phase {phase_num}/7 missing 'start' (or duplicated): {[m for m in captured if f'Phase {phase_num}/7' in m]}"
        )
        assert len(dones) == 1, (
            f"phase {phase_num}/7 missing 'done in' (or duplicated): {[m for m in captured if f'Phase {phase_num}/7' in m]}"
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    try:
        rows = con.execute(
            "SELECT market_id, event_id FROM markets ORDER BY market_id"
        ).fetchall()
        # JOIN check — every market_row's event_id must exist in events table.
        joined = con.execute(
            "SELECT COUNT(*) FROM markets m "
            "INNER JOIN events e ON m.event_id = e.id"
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
async def test_amendment_01_orphan_market_event_id_is_null(tmp_path: Path) -> None:
    """If /events doesn't list a market, its event_id stays None (orphan tolerated).

    Real Gamma data has /markets entries that aren't in /events at all (closed
    parents, archived events). The pipeline must accept event_id=NULL rather
    than dropping the market.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()
    # /events fixture mentions ONLY the first market — markets 1-4 are orphans
    partial_events = _events_for_markets([gamma_data[0]])

    fake_gamma = _make_fake_gamma(gamma_data, partial_events)

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    con = sqlite3.connect(settings.db_path)
    try:
        with_event = con.execute(
            "SELECT COUNT(*) FROM markets WHERE event_id IS NOT NULL"
        ).fetchone()[0]
        without_event = con.execute(
            "SELECT COUNT(*) FROM markets WHERE event_id IS NULL"
        ).fetchone()[0]
    finally:
        con.close()

    # 1 market in /events fixture → 1 with event_id; 4 orphans → 4 NULL
    assert with_event == 1
    assert without_event == 4


@pytest.mark.asyncio
async def test_amendment_01_events_failure_does_not_kill_snapshot(tmp_path: Path) -> None:
    """If /events fetch fails, snapshot still completes — markets get event_id NULL.

    /events is the secondary source (category/tags); /markets is the mainline.
    A /events outage is recorded as Issue but doesn't block the run.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.fetch_all_active_events.side_effect = RuntimeError(
        "simulated /events outage"
    )

    # Plan 02-09: provide iter_active_markets async generator.
    # iter_active_events is also stubbed but the test patches the wrapped
    # fetch_all_active_events to raise — for the streaming consumer we need
    # iter_active_events to raise the same RuntimeError.
    async def _iter_markets():
        for m in gamma_data:
            yield m

    async def _iter_events_raise():
        raise RuntimeError("simulated /events outage")
        yield  # unreachable but keeps this as an async generator

    fake_gamma.iter_active_markets = _iter_markets
    fake_gamma.iter_active_events = _iter_events_raise

    fake_gamma.aclose = AsyncMock()
    fake_gamma.__aenter__.return_value = fake_gamma
    fake_gamma.__aexit__.return_value = None

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # Snapshot completed; api_unreachable Issue recorded for /events.
    assert result.market_count == 5
    assert "api_unreachable" in result.issue_categories

    con = sqlite3.connect(settings.db_path)
    try:
        events_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        without_event = con.execute(
            "SELECT COUNT(*) FROM markets WHERE event_id IS NULL"
        ).fetchone()[0]
    finally:
        con.close()

    assert events_count == 0
    assert without_event == 5  # all markets event_id NULL when /events fails


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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock, patch(
        "polyarb.snapshot.orchestrator.publish_snapshot_complete",
        new_callable=AsyncMock,
        return_value=True,
    ) as publish_mock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock, patch(
        "polyarb.snapshot.orchestrator.publish_snapshot_complete",
        new_callable=AsyncMock,
        side_effect=RuntimeError("simulated NOTIFY failure"),
    ), patch(
        "polyarb.snapshot.orchestrator.sentry_sdk.add_breadcrumb"
    ) as breadcrumb_mock:
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
        c for c in breadcrumb_mock.call_args_list
        if c.kwargs.get("category") == "event-bus"
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock, patch(
        "polyarb.snapshot.orchestrator.publish_snapshot_complete",
        new_callable=AsyncMock,
    ) as publish_mock:
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

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock, patch(
        "polyarb.snapshot.orchestrator.publish_snapshot_complete",
        new_callable=AsyncMock,
    ) as publish_mock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
        )

        await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert publish_mock.await_count == 0
