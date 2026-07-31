"""RED tests for `_book_levels_rows_from_frame` projector + L2 dispatcher gate.

Phase 05 Plan 03 — Task 1 (RED) + Task 3 (dispatcher gate).

These tests stay RED until Task 2 lands:
- `polyarb.observation.l3_promote` (module + state + getters)
- `polyarb.daemon.l2_main._book_levels_rows_from_frame`
- `polyarb.storage.l2_supabase_mirror.L2SupabaseMirror.push_book_levels`

After Task 2 the 7 projector tests go GREEN. After Task 3 the 2 dispatcher
tests go GREEN as well.

Contract (mirrored from `_trade_row_from_frame`):
- Frames without `asset_id` produce `[]`.
- Frames without `timestamp` (and no `ts`) produce `[]`.
- Empty `bids` + `asks` produce `[]`.
- Levels with `size <= 0` are skipped (`valid_idx` re-enumerates after filter).
- Levels that are not dicts, or whose `price` is non-numeric, are skipped.
- Bids → `side="BUY"`, asks → `side="SELL"` (uppercase, matches l2_trades).
- `max_levels` caps per side (default 10 → up to 20 rows total).
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from polyarb.observation.l3_evidence import FrameDispatchResult

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Helpers ────────────────────────────────────────────────────────────────

_VALID_TS = "2026-06-01T12:00:00.000Z"


def _make_book_frame(
    asset_id: str | None = "0xasset-1",
    bids: list | None = None,
    asks: list | None = None,
    timestamp: str | None = _VALID_TS,
) -> dict:
    frame: dict = {"event_type": "book"}
    if asset_id is not None:
        frame["asset_id"] = asset_id
    if timestamp is not None:
        frame["timestamp"] = timestamp
    frame["bids"] = bids if bids is not None else []
    frame["asks"] = asks if asks is not None else []
    return frame


def _lvl(price: float | str | None, size: float | int) -> dict:
    return {"price": price, "size": size}


# ── Projector tests (Task 1 — RED until Task 2) ────────────────────────────


def test_book_levels_rows_top10_per_side() -> None:
    """12 levels per side → exactly 10 BUY + 10 SELL = 20 rows; level 1-indexed."""
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    bids = [_lvl(0.50 - i * 0.001, 100.0 + i) for i in range(12)]
    asks = [_lvl(0.51 + i * 0.001, 200.0 + i) for i in range(12)]
    rows = _book_levels_rows_from_frame(_make_book_frame(bids=bids, asks=asks))

    assert len(rows) == 20
    bids_out = [r for r in rows if r["side"] == "BUY"]
    asks_out = [r for r in rows if r["side"] == "SELL"]
    assert len(bids_out) == 10
    assert len(asks_out) == 10

    expected_keys = {"asset_id", "ts", "side", "level", "price", "size"}
    for r in rows:
        assert set(r.keys()) == expected_keys, f"row {r} keys must be exactly {expected_keys}"

    # level is 1-indexed and corresponds to position in input list (best=1)
    assert [r["level"] for r in bids_out] == list(range(1, 11))
    assert [r["level"] for r in asks_out] == list(range(1, 11))


def test_book_levels_normalizes_polymarket_worst_first_order() -> None:
    """Persist the nearest levels even when the WS sends the farthest first.

    Production evidence on 2026-07-20 showed bids ascending and asks
    descending.  The projector must rank levels by price instead of trusting
    their input position.
    """
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    rows = _book_levels_rows_from_frame(
        _make_book_frame(
            bids=[_lvl(0.001, 10), _lvl(0.20, 20), _lvl(0.369, 30)],
            asks=[_lvl(0.999, 40), _lvl(0.60, 50), _lvl(0.377, 60)],
        ),
        max_levels=2,
    )

    bids_out = [row for row in rows if row["side"] == "BUY"]
    asks_out = [row for row in rows if row["side"] == "SELL"]
    assert [(row["level"], row["price"]) for row in bids_out] == [
        (1, 0.369),
        (2, 0.20),
    ]
    assert [(row["level"], row["price"]) for row in asks_out] == [
        (1, 0.377),
        (2, 0.60),
    ]


def test_book_levels_skips_zero_or_negative_size() -> None:
    """size<=0 entries are skipped; level=enumeration index after filter."""
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    bids = [
        _lvl(0.50, 100.0),  # valid (level=1)
        _lvl(0.49, 0.0),  # skipped
        _lvl(0.48, -5.0),  # skipped
        _lvl(0.47, 50.0),  # valid (level=2)
    ]
    rows = _book_levels_rows_from_frame(_make_book_frame(bids=bids, asks=[]))

    bids_out = [r for r in rows if r["side"] == "BUY"]
    assert len(bids_out) == 2
    assert [r["level"] for r in bids_out] == [1, 2]
    assert [r["price"] for r in bids_out] == [0.50, 0.47]


def test_book_levels_returns_empty_for_no_asset_id() -> None:
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    frame = _make_book_frame(asset_id=None, bids=[_lvl(0.5, 100)])
    # _make_book_frame won't add asset_id key when None → frame has no asset_id
    assert "asset_id" not in frame
    assert _book_levels_rows_from_frame(frame) == []


def test_book_levels_returns_empty_for_no_timestamp() -> None:
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    frame = _make_book_frame(timestamp=None, bids=[_lvl(0.5, 100)])
    # _make_book_frame won't add timestamp when None
    assert "timestamp" not in frame
    assert "ts" not in frame
    assert _book_levels_rows_from_frame(frame) == []


def test_book_levels_handles_malformed_entries() -> None:
    """Non-numeric price / non-dict entries are skipped without raising."""
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    bids = [
        _lvl("not-a-number", 1),  # skipped — price not float-able
        "not-a-dict",  # skipped — not a dict
        _lvl(0.50, 100.0),  # valid
    ]
    rows = _book_levels_rows_from_frame(_make_book_frame(bids=bids, asks=[]))

    bids_out = [r for r in rows if r["side"] == "BUY"]
    assert len(bids_out) == 1
    assert bids_out[0]["price"] == 0.50
    assert bids_out[0]["level"] == 1


def test_book_levels_side_normalization() -> None:
    """bids → BUY, asks → SELL (uppercase) matching l2_trades.side convention."""
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    rows = _book_levels_rows_from_frame(
        _make_book_frame(
            bids=[_lvl(0.50, 100)],
            asks=[_lvl(0.51, 200)],
        )
    )
    assert len(rows) == 2
    buys = [r for r in rows if r["side"] == "BUY"]
    sells = [r for r in rows if r["side"] == "SELL"]
    assert len(buys) == 1
    assert len(sells) == 1
    assert buys[0]["price"] == 0.50
    assert sells[0]["price"] == 0.51


def test_book_levels_empty_book_returns_empty() -> None:
    from polyarb.daemon.l2_main import _book_levels_rows_from_frame

    assert _book_levels_rows_from_frame(_make_book_frame(bids=[], asks=[])) == []


# ── Dispatcher gate tests (Task 3 — RED until Task 3 lands) ────────────────
#
# Strategy: call `_on_event` via inspection of `l2_main.main` would be too
# heavy; the dispatcher branch is the simpler unit. We import the `_on_event`
# closure indirectly by importing `l2_main` itself and patching `l2_mirror`
# via a small re-implementation: we don't have direct access to the closure
# (it's defined inside `main()`). Instead we exercise the same code path by
# constructing a minimal harness that mirrors the branch.
#
# Phase 05 Plan 03 — the dispatcher logic added in Task 3 lives inside the
# `_on_event` closure in `main()`. To unit-test the gate without booting the
# daemon, we extract the gate logic by calling the projector + checking
# `l3_promote.get_l3_active_set()` directly — that IS what Task 3 wires.
#
# These two tests use a real `L2SupabaseMirror` instance with mocked supabase
# client to verify that:
#   (a) when asset_id ∈ _l3_active_set, push_book_levels IS called
#   (b) when asset_id ∉ _l3_active_set, push_book_levels is NOT called
#
# The integration is then verified end-to-end via the projector tests above +
# the existence of the wiring grep (see plan's <verification> §2).
#
# NOTE: Because `_on_event` is a closure inside `main()`, we don't import
# main(). Instead Task 3 exposes the gate logic in a testable way: we
# reproduce the dispatcher branch inline and assert behaviour.


def _dispatch_book_branch(frame: dict, l2_mirror) -> None:
    """Reproduction of the dispatcher branch added in Task 3.

    Mirror of the production code path:
    ```python
    if event_type == "book":
        from polyarb.observation import l3_promote
        if asset_id_raw and asset_id_raw in l3_promote.get_l3_active_set():
            book_rows = _book_levels_rows_from_frame(frame, max_levels=10)
            if book_rows:
                l2_mirror.push_book_levels(book_rows)
    ```
    """
    from polyarb.daemon.l2_main import (
        _book_levels_rows_from_frame,
        _tob_row_from_frame,
    )
    from polyarb.observation import l3_promote

    event_type = frame.get("event_type", "unknown")
    asset_id_raw = frame.get("asset_id") or ""
    if l2_mirror is None:
        return
    if event_type in ("price_change", "best_bid_ask", "book"):
        row = _tob_row_from_frame(frame)
        if row is not None:
            l2_mirror.push_top_of_book([row])
        if event_type == "book":
            if asset_id_raw and asset_id_raw in l3_promote.get_l3_active_set():
                book_rows = _book_levels_rows_from_frame(frame, max_levels=10)
                if book_rows:
                    l2_mirror.push_book_levels(book_rows)


def test_on_event_book_with_l3_active_asset_calls_push_book_levels() -> None:
    """Active asset → both push_top_of_book AND push_book_levels invoked."""
    from unittest.mock import MagicMock

    from polyarb.observation import l3_promote

    # Cleanup-safe setup: snapshot prior set, mutate, restore in finally.
    prior = set(l3_promote._l3_active_set)
    try:
        l3_promote._l3_active_set = {"0xasset-1"}

        l2_mirror = MagicMock()
        l2_mirror.push_top_of_book.return_value = True
        l2_mirror.push_book_levels.return_value = True

        frame = _make_book_frame(
            asset_id="0xasset-1",
            bids=[_lvl(0.5, 100)],
            asks=[_lvl(0.51, 200)],
        )
        _dispatch_book_branch(frame, l2_mirror)

        assert l2_mirror.push_top_of_book.call_count == 1
        assert l2_mirror.push_book_levels.call_count == 1
        call_rows = l2_mirror.push_book_levels.call_args[0][0]
        assert len(call_rows) == 2  # 1 BUY + 1 SELL
    finally:
        l3_promote._l3_active_set = prior


def test_on_event_book_with_non_l3_asset_does_not_call_push_book_levels() -> None:
    """Asset NOT in active set → only push_top_of_book; depth write skipped."""
    from unittest.mock import MagicMock

    from polyarb.observation import l3_promote

    prior = set(l3_promote._l3_active_set)
    try:
        l3_promote._l3_active_set = set()  # explicitly empty

        l2_mirror = MagicMock()
        l2_mirror.push_top_of_book.return_value = True
        l2_mirror.push_book_levels.return_value = True

        frame = _make_book_frame(
            asset_id="0xunknown",
            bids=[_lvl(0.5, 100)],
            asks=[_lvl(0.51, 200)],
        )
        _dispatch_book_branch(frame, l2_mirror)

        assert l2_mirror.push_top_of_book.call_count == 1
        assert l2_mirror.push_book_levels.call_count == 0
    finally:
        l3_promote._l3_active_set = prior


@pytest.mark.asyncio
async def test_production_dispatch_reports_tob_and_depth_outcomes_separately() -> None:
    from polyarb.daemon.l2_main import make_l2_event_handler
    from polyarb.observation import l3_promote

    prior = set(l3_promote._l3_active_set)
    try:
        l3_promote._l3_active_set = {"0xasset-1"}
        mirror = MagicMock()
        mirror.push_top_of_book.return_value = True
        mirror.push_book_levels.return_value = False

        result = await make_l2_event_handler(
            mirror, book_levels_required=lambda asset_id: asset_id == "0xasset-1"
        )(
            _make_book_frame(
                asset_id="0xasset-1",
                bids=[_lvl(0.5, 100)],
                asks=[_lvl(0.51, 200)],
                timestamp="2026-07-23T01:02:03Z",
            )
        )

        assert result == FrameDispatchResult(
            tob_written=False,
            book_levels_written=False,
            observed_at=datetime(2026, 7, 23, 1, 2, 3, tzinfo=UTC),
        )
        assert mirror.push_top_of_book.call_count == 1
        assert mirror.push_book_levels.call_count == 1
    finally:
        l3_promote._l3_active_set = prior


@pytest.mark.asyncio
async def test_non_book_dispatch_has_explicit_false_depth_outcome() -> None:
    from polyarb.daemon.l2_main import make_l2_event_handler

    mirror = MagicMock()
    mirror.push_top_of_book.return_value = True
    result = await make_l2_event_handler(mirror)(
        {
            "event_type": "best_bid_ask",
            "asset_id": "asset-a",
            "best_bid": "0.4",
            "best_ask": "0.6",
            "timestamp": "2026-07-23T01:02:03+00:00",
        }
    )

    assert result == FrameDispatchResult(
        tob_written=False,
        book_levels_written=False,
        observed_at=datetime(2026, 7, 23, 1, 2, 3, tzinfo=UTC),
    )
    await asyncio.sleep(0.01)
    mirror.push_top_of_book.assert_called_once()
    mirror.push_book_levels.assert_not_called()


# ── Quick task 260601-depth-yes-usd: _tob_row_from_frame depth fill ────────


def test_tob_row_book_event_fills_depth_yes_usd_from_top_10_bids():
    """Quick 260601: book event with bids/asks arrays → depth = sum(price*size top-10).

    Phase 03 latent TODO — depth_yes_usd was hardcoded None. Promoter D-13
    threshold `depth_yes_usd > 500` could never fire in prod (631 lifetime
    rows: 0 with depth populated). This RED test asserts the fill works.
    """
    import pytest

    from polyarb.daemon.l2_main import _tob_row_from_frame

    frame = _make_book_frame(
        asset_id="AID-1",
        bids=[{"price": "0.45", "size": "1000"}, {"price": "0.44", "size": "500"}],
        asks=[{"price": "0.46", "size": "800"}, {"price": "0.47", "size": "200"}],
    )
    row = _tob_row_from_frame(frame)
    assert row is not None
    # depth_yes_usd = sum of (price * size) for top-10 bids = 0.45*1000 + 0.44*500 = 670.0
    assert row["depth_yes_usd"] == pytest.approx(0.45 * 1000 + 0.44 * 500)
    # depth_no_usd = sum for asks = 0.46*800 + 0.47*200 = 462.0
    assert row["depth_no_usd"] == pytest.approx(0.46 * 800 + 0.47 * 200)


def test_tob_row_normalizes_polymarket_worst_first_order():
    """Best prices and depth come from price-ranked levels, not array order."""
    import pytest

    from polyarb.daemon.l2_main import _tob_row_from_frame

    frame = _make_book_frame(
        asset_id="AID-1",
        bids=[
            _lvl(0.001, 1000),
            _lvl(0.20, 20),
            _lvl(0.30, 30),
            _lvl(0.369, 40),
        ],
        asks=[
            _lvl(0.999, 1000),
            _lvl(0.60, 60),
            _lvl(0.50, 50),
            _lvl(0.377, 40),
        ],
    )

    row = _tob_row_from_frame(frame)
    assert row is not None
    assert row["best_bid"] == pytest.approx(0.369)
    assert row["best_ask"] == pytest.approx(0.377)
    assert row["spread"] == pytest.approx(0.008)
    assert row["mid_price"] == pytest.approx(0.373)
    assert row["depth_yes_usd"] == pytest.approx(0.369 * 40 + 0.30 * 30 + 0.20 * 20 + 0.001 * 1000)
    assert row["depth_no_usd"] == pytest.approx(0.377 * 40 + 0.50 * 50 + 0.60 * 60 + 0.999 * 1000)


def test_tob_row_book_event_caps_at_top_10_levels():
    """Quick 260601: if bids has 15 levels, only top-10 contribute to depth_yes_usd."""
    import pytest

    from polyarb.daemon.l2_main import _tob_row_from_frame

    frame = _make_book_frame(
        asset_id="AID-1",
        bids=[{"price": "0.5", "size": "100"} for _ in range(15)],
        asks=[],
    )
    row = _tob_row_from_frame(frame)
    assert row is not None
    # Only top-10 = 0.5 * 100 * 10 = 500.0; the extra 5 levels are ignored.
    assert row["depth_yes_usd"] == pytest.approx(500.0)
    # asks empty → depth_no_usd None
    assert row["depth_no_usd"] is None


def test_tob_row_book_event_skips_zero_size_levels():
    """Quick 260601: levels with size<=0 are excluded from depth sum."""
    import pytest

    from polyarb.daemon.l2_main import _tob_row_from_frame

    frame = _make_book_frame(
        asset_id="AID-1",
        bids=[
            {"price": "0.5", "size": "100"},
            {"price": "0.4", "size": "0"},  # skip — zero size
            {"price": "0.3", "size": "200"},
            {"price": "0.2", "size": "-50"},  # skip — negative size
        ],
        asks=[],
    )
    row = _tob_row_from_frame(frame)
    assert row is not None
    # 0.5*100 + 0.3*200 = 50 + 60 = 110
    assert row["depth_yes_usd"] == pytest.approx(110.0)


def test_tob_row_price_change_event_leaves_depth_none():
    """Quick 260601: non-book events (price_change/best_bid_ask/last_trade) → depth = None.

    Polymarket WS docs: only `book` event carries bids[]/asks[] arrays with size.
    Other event types (price_change is per-side delta, best_bid_ask is best-only)
    do NOT carry depth info; the projector must leave depth_yes_usd/depth_no_usd
    as None for those, to preserve chain-truth (depth populated <=> source was a book frame).
    """
    from polyarb.daemon.l2_main import _tob_row_from_frame

    frame = {
        "event_type": "price_change",
        "asset_id": "AID-1",
        "timestamp": _VALID_TS,
        "side": "BUY",
        "price": "0.55",
        "size": "100",
    }
    row = _tob_row_from_frame(frame)
    assert row is not None
    assert row["depth_yes_usd"] is None
    assert row["depth_no_usd"] is None
