"""Tests for polyarb.storage.l2_supabase_mirror — Plan 06 Task 2 (Wave 0 RED).

Mirror contract:
- L2SupabaseMirror(url, service_key) — single supabase client constructed at init
- push_top_of_book(rows: list[dict]) -> bool — chunked .insert() at _CHUNK_SIZE=1000
- push_trades(rows: list[dict]) -> bool — .upsert(rows, on_conflict='trade_hash')
- upsert_candidates(rows: list[dict]) -> bool — upsert into l2_candidates
- mark_candidates_removed(asset_ids: list[str]) -> bool — UPDATE removed_at_ts
- Fail-soft envelope: any exception → log warning + breadcrumb + return False
- Dual-anchor breadcrumb (Phase 02.2 preemptive — Open Q 9):
    success path → add_breadcrumb(category='l2-mirror', level='info', ...)
    failure path → add_breadcrumb(category='l2-mirror', level='warning', ...)
- category='l2-mirror' (NOT 'mirror' — distinct from L1 for Sentry filter)

W6 invariant: constructor receives REST URL (https://...supabase.co), NOT DSN.
"""
from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Helpers ────────────────────────────────────────────────────────────────


def _make_supabase_mock() -> MagicMock:
    client = MagicMock()

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.insert.return_value = tbl
        tbl.delete.return_value = tbl
        tbl.update.return_value = tbl
        tbl.select.return_value = tbl
        tbl.neq.return_value = tbl
        tbl.eq.return_value = tbl
        tbl.in_.return_value = tbl
        tbl.is_.return_value = tbl
        tbl.order.return_value = tbl
        tbl.limit.return_value = tbl
        tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client.table.side_effect = _table_mock
    return client


def _tob_rows(n: int) -> list[dict]:
    return [
        {
            "asset_id": f"asset-{i}",
            "ts": "2026-05-24T00:00:00Z",
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
            "ts": "2026-05-24T00:00:00Z",
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


def test_init_creates_single_client() -> None:
    """L2SupabaseMirror constructor must call create_client exactly once."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=mock
    ) as cc:
        L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        assert cc.call_count == 1
        # Constructor must accept REST URL (https://), NOT a DSN (postgresql://)
        args, _ = cc.call_args
        assert args[0].startswith("https://"), (
            f"L2SupabaseMirror constructor expects REST URL, got {args[0]!r}"
        )


def test_push_top_of_book_chunks_at_1000() -> None:
    """2500 rows must produce 3 .insert() chunked calls (1000+1000+500)."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    captured_inserts: list[int] = []

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()

        def _insert(rows):
            captured_inserts.append(len(rows))
            return tbl

        tbl.insert.side_effect = _insert
        tbl.execute.return_value = MagicMock(data=[])
        return tbl

    mock.table.side_effect = _table_mock

    with patch("polyarb.storage.l2_supabase_mirror.create_client", return_value=mock):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        ok = mirror.push_top_of_book(_tob_rows(2500))

    assert ok is True
    assert captured_inserts == [1000, 1000, 500], (
        f"expected chunks [1000,1000,500]; got {captured_inserts}"
    )


def test_push_trades_uses_on_conflict_trade_hash() -> None:
    """push_trades must call .upsert(rows, on_conflict='trade_hash') — idempotent backfill."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    captured = {}

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()

        def _upsert(rows, **kwargs):
            captured["rows"] = rows
            captured["kwargs"] = kwargs
            return tbl

        tbl.upsert.side_effect = _upsert
        tbl.execute.return_value = MagicMock(data=[])
        return tbl

    mock.table.side_effect = _table_mock

    with patch("polyarb.storage.l2_supabase_mirror.create_client", return_value=mock):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        ok = mirror.push_trades(_trade_rows(3))

    assert ok is True
    assert captured.get("kwargs", {}).get("on_conflict") == "trade_hash", (
        f"expected on_conflict='trade_hash'; got {captured.get('kwargs')!r}"
    )


def test_push_top_of_book_failsoft() -> None:
    """Mirror exception → returns False (does NOT raise) + breadcrumb warning."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()

    def _explode(name: str):
        raise RuntimeError("supabase down")

    mock.table.side_effect = _explode
    breadcrumbs: list[dict] = []

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=mock
    ), patch(
        "polyarb.storage.l2_supabase_mirror.sentry_sdk.add_breadcrumb",
        side_effect=lambda **kw: breadcrumbs.append(kw),
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        ok = mirror.push_top_of_book(_tob_rows(3))

    assert ok is False, "fail-soft must return False (not raise)"
    warning_breadcrumbs = [
        bc for bc in breadcrumbs
        if bc.get("category") == "l2-mirror" and bc.get("level") == "warning"
    ]
    assert warning_breadcrumbs, (
        f"expected at least one warning breadcrumb category='l2-mirror'; "
        f"got {breadcrumbs!r}"
    )


def test_push_top_of_book_success_emits_info_breadcrumb() -> None:
    """Phase 02.2 preemptive — success path emits category='l2-mirror' level='info'."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    breadcrumbs: list[dict] = []

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=mock
    ), patch(
        "polyarb.storage.l2_supabase_mirror.sentry_sdk.add_breadcrumb",
        side_effect=lambda **kw: breadcrumbs.append(kw),
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        ok = mirror.push_top_of_book(_tob_rows(5))

    assert ok is True
    info_bc = [
        bc for bc in breadcrumbs
        if bc.get("category") == "l2-mirror" and bc.get("level") == "info"
    ]
    assert info_bc, (
        f"Phase 02.2 preemptive: success path must emit info breadcrumb; "
        f"got {breadcrumbs!r}"
    )


def test_upsert_candidates_inserts_new_rows() -> None:
    """upsert_candidates writes new candidate rows to l2_candidates."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    tables_used: list[str] = []

    def _table_mock(name: str):
        tables_used.append(name)
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.insert.return_value = tbl
        tbl.execute.return_value = MagicMock(data=[])
        return tbl

    mock.table.side_effect = _table_mock

    with patch("polyarb.storage.l2_supabase_mirror.create_client", return_value=mock):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        ok = mirror.upsert_candidates([
            {
                "snapshot_id": 1,
                "recipe_name": "high_liquidity",
                "asset_id": "asset-A",
                "market_id": "mkt-A",
                "event_id": "evt-A",
                "source": "recipe",
                "ranking_score": {"liquidity": 1000.0},
            },
        ])

    assert ok is True
    assert "l2_candidates" in tables_used


def test_category_l2_mirror_not_plain_mirror() -> None:
    """Sentry breadcrumb category must be 'l2-mirror' (NOT 'mirror' — L1 namespace)."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    mock = _make_supabase_mock()
    breadcrumbs: list[dict] = []

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=mock
    ), patch(
        "polyarb.storage.l2_supabase_mirror.sentry_sdk.add_breadcrumb",
        side_effect=lambda **kw: breadcrumbs.append(kw),
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        mirror.push_top_of_book(_tob_rows(1))
        mirror.push_trades(_trade_rows(1))

    cats = {bc.get("category") for bc in breadcrumbs}
    assert "l2-mirror" in cats, f"missing 'l2-mirror' breadcrumb; got {cats!r}"
    assert "mirror" not in cats, (
        f"L1's 'mirror' category must not appear in L2 mirror code; got {cats!r}"
    )
