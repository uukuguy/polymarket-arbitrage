"""Tests for L2SupabaseMirror success-path freshness cache write — Phase 03.1 Plan 01 Task 2.

GAP-3 mechanical fix (with GAP-2 from Task 1): mirror success path writes
`last_mirror_at_s` to the local SQLite singleton so /health has a freshness
anchor. The store is INJECTED via constructor (optional kwarg, defaults None
for backwards-compat).

Contracts asserted:
1. Constructor accepts optional `store=` (defaults None) — legacy callers unchanged
2. push_top_of_book success → calls store.upsert_l2_tob_mirror_state(int wall-clock)
3. push_top_of_book FAILURE → does NOT call store.upsert (failure path remains pure)
4. push_trades success → ALSO calls store.upsert_l2_tob_mirror_state (any successful write refreshes)
5. store=None at construction → success path is silent no-op (no AttributeError)
"""
from __future__ import annotations

import os
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Helpers ────────────────────────────────────────────────────────────────


def _make_supabase_mock(raise_on_insert: bool = False) -> MagicMock:
    client = MagicMock()

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.insert.return_value = tbl
        tbl.delete.return_value = tbl
        tbl.update.return_value = tbl
        tbl.select.return_value = tbl
        tbl.in_.return_value = tbl
        tbl.is_.return_value = tbl
        if raise_on_insert:
            tbl.execute.side_effect = RuntimeError("simulated supabase failure")
        else:
            tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client.table.side_effect = _table_mock
    return client


def _tob_rows(n: int) -> list[dict]:
    return [
        {
            "asset_id": f"asset-{i}",
            "ts": "2026-05-26T00:00:00Z",
            "best_bid": 0.5,
            "best_ask": 0.55,
            "spread": 0.05,
            "mid_price": 0.525,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 800.0,
            "source_event": "price_change",
        }
        for i in range(n)
    ]


def _trade_rows(n: int) -> list[dict]:
    return [
        {
            "asset_id": f"asset-{i}",
            "market_id": f"mkt-{i}",
            "ts": "2026-05-26T00:00:00Z",
            "price": 0.5,
            "size": 10.0,
            "side": "BUY",
            "taker_address": f"0x{i:040x}",
            "trade_hash": f"0xhash-{i}",
            "source": "ws",
        }
        for i in range(n)
    ]


# ── Tests ──────────────────────────────────────────────────────────────────


def test_constructor_accepts_optional_store_kwarg() -> None:
    """L2SupabaseMirror(url, key) without store kwarg works — backwards-compat."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=mock
    ):
        # No store kwarg — must not raise.
        m = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        # And with store kwarg — must accept.
        m2 = L2SupabaseMirror(
            url="https://x.supabase.co", service_key="key", store=MagicMock()
        )
        assert m is not None and m2 is not None


def test_push_top_of_book_success_writes_freshness_cache() -> None:
    """Success path must call store.upsert_l2_tob_mirror_state with current wall-clock."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock()
    store_mock = MagicMock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(
            url="https://x.supabase.co", service_key="key", store=store_mock
        )

        before = int(time.time())
        result = mirror.push_top_of_book(_tob_rows(3))
        after = int(time.time())

        assert result is True
        assert store_mock.upsert_l2_tob_mirror_state.call_count == 1
        called_arg = store_mock.upsert_l2_tob_mirror_state.call_args[0][0]
        assert isinstance(called_arg, int)
        assert before <= called_arg <= after, (
            f"freshness timestamp {called_arg} outside [{before}, {after}]"
        )


def test_push_top_of_book_failure_does_not_write_cache() -> None:
    """Failure path must NOT call store.upsert — only successful writes refresh freshness."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock(raise_on_insert=True)
    store_mock = MagicMock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(
            url="https://x.supabase.co", service_key="key", store=store_mock
        )
        result = mirror.push_top_of_book(_tob_rows(3))

        # Fail-soft envelope returns False but does NOT raise.
        assert result is False
        # And critically — must NOT have touched the freshness cache.
        store_mock.upsert_l2_tob_mirror_state.assert_not_called()


def test_push_trades_success_writes_freshness_cache() -> None:
    """push_trades success ALSO refreshes the freshness anchor."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock()
    store_mock = MagicMock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(
            url="https://x.supabase.co", service_key="key", store=store_mock
        )
        before = int(time.time())
        result = mirror.push_trades(_trade_rows(2))
        after = int(time.time())

        assert result is True
        assert store_mock.upsert_l2_tob_mirror_state.call_count == 1
        called_arg = store_mock.upsert_l2_tob_mirror_state.call_args[0][0]
        assert isinstance(called_arg, int)
        assert before <= called_arg <= after


def test_store_none_success_path_is_silent_noop() -> None:
    """When store=None (legacy callers), success path must NOT raise."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        # No store= kwarg.
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        # Must complete cleanly with True; no AttributeError on self._store.upsert(...).
        assert mirror.push_top_of_book(_tob_rows(1)) is True
        assert mirror.push_trades(_trade_rows(1)) is True
