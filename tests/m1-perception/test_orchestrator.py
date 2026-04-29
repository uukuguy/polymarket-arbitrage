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

import asyncio
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
from polyarb.snapshot.orchestrator import run_snapshot  # noqa: E402

FIXTURES_DIR = Path(__file__).parent / "fixtures"


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


# ─────────────────────────────────────────────────────────────────────────────
# T6.1 — Full pipeline produces SQLite + Parquet with mocks
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_pipeline_writes_sqlite_and_parquet(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.aclose = AsyncMock()
    # Async context manager protocol: __aenter__ returns the mock itself
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

    def fake_normalize(raw: dict):
        seen["count"] += 1
        if seen["count"] >= 5:
            return None  # drop the 5th — orchestrator now sees 4/5 → Layer 1 fires
        return real_normalize(raw)

    monkeypatch.setattr(orch_mod, "normalize_market", fake_normalize)

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
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

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.__aenter__.return_value = fake_gamma
    fake_gamma.__aexit__.return_value = None

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

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.__aenter__.return_value = fake_gamma
    fake_gamma.__aexit__.return_value = None

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

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
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

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.__aenter__.return_value = fake_gamma
    fake_gamma.__aexit__.return_value = None

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
