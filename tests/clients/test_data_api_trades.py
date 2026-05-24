"""Tests for polyarb.clients.data_api_client — Plan 06 Task 3 (Wave 0 RED).

Backfill contract (post-Open Q 2 resolution, 2026-05-24):
- DATA_API_BASE = "https://data-api.polymarket.com"
- _LIMITER = AsyncLimiter(150, 10)  — 25% headroom under 200/10s
- MAX_OFFSET = 1000 (conservative; live probe showed 3000 OK but 4000 -> 400)
- PAGE_SIZE = 500
- TIME_FILTER_PARAM not used — Data API /trades does NOT support server-side
  time filtering. Backfill paginates the GLOBAL feed + client-side filters
  by `trade["asset"] == asset_id` + stops when trade["timestamp"] < cutoff_ts.
- 429 response → log warning + asyncio.sleep(10) + tenacity retries
- httpx.AsyncClient init: follow_redirects=False (Phase 02 F-2)
- Trade fields (per probe): timestamp (unix seconds), transactionHash (hash),
  asset (token id), price, size, side, proxyWallet (taker)
"""
from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


def _trade(idx: int, *, asset: str, ts: int) -> dict:
    return {
        "asset": asset,
        "proxyWallet": f"0x{idx:040x}",
        "side": "BUY",
        "size": 10.0,
        "price": 0.5,
        "timestamp": ts,
        "transactionHash": f"0xhash-{idx:08x}",
        "conditionId": f"cond-{idx}",
        "slug": f"slug-{idx}",
        "eventSlug": f"evt-{idx}",
        "outcome": "Up",
        "outcomeIndex": 0,
        "title": f"trade-{idx}",
        "icon": "",
        "name": "", "pseudonym": "", "bio": "",
        "profileImage": "", "profileImageOptimized": "",
    }


# ── Tests ────────────────────────────────────────────────────────────────


async def test_pagination_0_500_1000() -> None:
    """Backfill must paginate offset=0,500,1000 then stop at MAX_OFFSET."""
    from polyarb.clients.data_api_client import (
        DATA_API_BASE,
        backfill_trades_for_asset,
    )

    asset = "asset-A"
    now = int(time.time())
    # Three full pages (500 each), all rows belong to target asset, all within cutoff
    pages = [
        [_trade(i, asset=asset, ts=now - i) for i in range(500)],
        [_trade(500 + i, asset=asset, ts=now - 500 - i) for i in range(500)],
        [_trade(1000 + i, asset=asset, ts=now - 1000 - i) for i in range(500)],
    ]
    call_offsets: list[int] = []

    with respx.mock(base_url=DATA_API_BASE) as router:
        def _handler(request):
            off = int(request.url.params.get("offset", "0"))
            call_offsets.append(off)
            idx = off // 500
            if idx >= len(pages):
                return httpx.Response(200, json=[])
            return httpx.Response(200, json=pages[idx])

        router.get("/trades").mock(side_effect=_handler)
        out = []
        async for t in backfill_trades_for_asset(asset_id=asset, days=7):
            out.append(t)

    assert call_offsets[:3] == [0, 500, 1000], (
        f"expected offsets 0,500,1000; got {call_offsets}"
    )
    # All 1500 rows belong to target asset → all yielded
    assert len(out) == 1500, f"expected 1500 trades; got {len(out)}"


async def test_client_side_asset_filter() -> None:
    """Backfill must filter trades whose 'asset' != asset_id (Open Q 2 resolution).

    Polymarket Data API /trades does NOT support server-side asset filter,
    so backfill_trades_for_asset MUST filter client-side.
    """
    from polyarb.clients.data_api_client import (
        DATA_API_BASE,
        backfill_trades_for_asset,
    )

    target = "asset-target"
    now = int(time.time())
    page = [_trade(i, asset=target if i % 2 == 0 else "asset-other", ts=now - i) for i in range(10)]

    with respx.mock(base_url=DATA_API_BASE) as router:
        router.get("/trades").mock(
            side_effect=[httpx.Response(200, json=page), httpx.Response(200, json=[])]
        )
        out = []
        async for t in backfill_trades_for_asset(asset_id=target, days=7):
            out.append(t)

    assert all(t["asset"] == target for t in out), "non-target asset leaked"
    assert len(out) == 5


async def test_cutoff_at_7_days() -> None:
    """When a trade with timestamp < cutoff_ts is encountered, iteration ends."""
    from polyarb.clients.data_api_client import (
        DATA_API_BASE,
        backfill_trades_for_asset,
    )

    asset = "asset-A"
    now = int(time.time())
    cutoff = now - 7 * 86400
    # First 3 inside cutoff, then 1 OLDER than cutoff (should break)
    page = [
        _trade(0, asset=asset, ts=now - 100),
        _trade(1, asset=asset, ts=now - 200),
        _trade(2, asset=asset, ts=now - 300),
        _trade(3, asset=asset, ts=cutoff - 100),  # older than cutoff
        _trade(4, asset=asset, ts=cutoff - 200),  # should never be reached
    ]
    call_count = 0

    with respx.mock(base_url=DATA_API_BASE) as router:
        def _handler(request):
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json=page)

        router.get("/trades").mock(side_effect=_handler)
        out = []
        async for t in backfill_trades_for_asset(asset_id=asset, days=7):
            out.append(t)

    assert call_count == 1, f"expected exactly 1 page request after cutoff hit; got {call_count}"
    assert len(out) == 3, f"expected 3 trades before cutoff; got {len(out)}"


async def test_trade_hash_dedup() -> None:
    """Duplicate transactionHash in subsequent pages must NOT be yielded twice.

    Use full-page chunks (page_size=2 with 2-row pages) so the iterator
    advances to a second page instead of short-page-terminating after page1.
    """
    from polyarb.clients.data_api_client import (
        DATA_API_BASE,
        backfill_trades_for_asset,
    )

    asset = "asset-A"
    now = int(time.time())
    # Both pages have exactly page_size=2 rows so pagination advances.
    page1 = [_trade(i, asset=asset, ts=now - i) for i in range(2)]   # hashes 0,1
    page2 = [_trade(i, asset=asset, ts=now - i) for i in [1, 2]]      # hash 1 dupe, 2 new
    page3: list[dict] = []  # short page → terminate

    with respx.mock(base_url=DATA_API_BASE) as router:
        router.get("/trades").mock(
            side_effect=[
                httpx.Response(200, json=page1),
                httpx.Response(200, json=page2),
                httpx.Response(200, json=page3),
            ]
        )
        out = []
        async for t in backfill_trades_for_asset(asset_id=asset, days=7, page_size=2):
            out.append(t)

    hashes = [t["transactionHash"] for t in out]
    assert len(hashes) == len(set(hashes)), f"dedup failed: {hashes}"
    assert set(hashes) == {"0xhash-00000000", "0xhash-00000001", "0xhash-00000002"}, (
        f"expected {{0,1,2}}; got {hashes}"
    )


async def test_429_retry() -> None:
    """A 429 response triggers asyncio.sleep(10) + retry; eventual 200 succeeds."""
    from polyarb.clients.data_api_client import (
        DATA_API_BASE,
        backfill_trades_for_asset,
    )

    asset = "asset-A"
    now = int(time.time())
    page = [_trade(i, asset=asset, ts=now - i) for i in range(2)]

    sleep_calls: list[float] = []
    real_sleep = asyncio.sleep

    async def _fake_sleep(secs):
        sleep_calls.append(secs)
        # Fall through to real (very short) wait so tenacity can retry promptly
        await real_sleep(0.001)

    with respx.mock(base_url=DATA_API_BASE) as router:
        router.get("/trades").mock(
            side_effect=[
                httpx.Response(429, json={"error": "rate"}),
                httpx.Response(200, json=page),
                httpx.Response(200, json=[]),
            ]
        )
        with patch("polyarb.clients.data_api_client.asyncio.sleep", new=_fake_sleep):
            out = []
            async for t in backfill_trades_for_asset(asset_id=asset, days=7):
                out.append(t)

    # The 429 path must have invoked sleep(10) at least once
    assert any(s == 10 for s in sleep_calls), (
        f"expected asyncio.sleep(10) on 429; got sleeps {sleep_calls}"
    )
    assert len(out) == 2


async def test_negative_size_filtered() -> None:
    """Defensive: size <= 0 must NOT be yielded (T-03-06-04)."""
    from polyarb.clients.data_api_client import (
        DATA_API_BASE,
        backfill_trades_for_asset,
    )

    asset = "asset-A"
    now = int(time.time())
    page = [
        _trade(0, asset=asset, ts=now - 1),
        _trade(1, asset=asset, ts=now - 2) | {"size": -5.0},
        _trade(2, asset=asset, ts=now - 3) | {"size": 0.0},
        _trade(3, asset=asset, ts=now - 4),
    ]

    with respx.mock(base_url=DATA_API_BASE) as router:
        router.get("/trades").mock(
            side_effect=[httpx.Response(200, json=page), httpx.Response(200, json=[])]
        )
        out = []
        async for t in backfill_trades_for_asset(asset_id=asset, days=7):
            out.append(t)

    sizes = [t["size"] for t in out]
    assert all(s > 0 for s in sizes), f"non-positive size leaked: {sizes}"
    assert len(out) == 2


async def test_rate_limiter_150_per_10s() -> None:
    """Module-level AsyncLimiter must be constructed with (150, 10)."""
    import polyarb.clients.data_api_client as mod

    assert hasattr(mod, "_LIMITER"), "_LIMITER must exist at module level"
    # AsyncLimiter exposes .max_rate and .time_period
    assert mod._LIMITER.max_rate == 150, f"max_rate={mod._LIMITER.max_rate}"
    assert mod._LIMITER.time_period == 10, f"time_period={mod._LIMITER.time_period}"


async def test_max_offset_constant_is_1000() -> None:
    """MAX_OFFSET must be 1000 (NOT Gamma's 10000)."""
    import polyarb.clients.data_api_client as mod

    assert getattr(mod, "MAX_OFFSET", None) == 1000, (
        f"MAX_OFFSET must be 1000; got {getattr(mod, 'MAX_OFFSET', None)}"
    )
