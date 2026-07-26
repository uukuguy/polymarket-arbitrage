"""Unit tests for GammaClient — pagination + retry semantics, mocked via respx.

asyncio_mode=auto is set in pyproject [tool.pytest.ini_options], so plain
``async def test_*`` is auto-collected.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from polyarb.clients.gamma_client import GammaClient, _NonRetryableHTTPError
from polyarb.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"


def _fast_settings() -> Settings:
    """Settings with retry waits compressed to ~ms so tests stay fast."""
    return Settings(retry_min_wait_s=0.001, retry_max_wait_s=0.01)


def _make_market_dict(idx: int) -> dict:
    """Lightweight stand-in for a Polymarket market dict (only fields the
    client touches: pagination doesn't read fields, but we want realistic
    enough payloads)."""
    return {"id": str(540000 + idx), "question": f"market {idx}", "active": True}


@pytest.fixture
def real_gamma_sample() -> list[dict]:
    """The recorded T1 fixture (5 real markets)."""
    with open(FIXTURES / "gamma_sample.json") as f:
        data = json.load(f)
    assert isinstance(data, list) and len(data) >= 1
    return data


# ---------------------------------------------------------------------------
# Test 1: pagination terminates on short page; offset increments by PAGE_LIMIT
# ---------------------------------------------------------------------------
async def test_fetch_all_paginates_until_short_page() -> None:
    settings = _fast_settings()
    page0 = [_make_market_dict(i) for i in range(100)]
    page1 = [_make_market_dict(100 + i) for i in range(100)]
    page2 = [_make_market_dict(200 + i) for i in range(42)]  # short page → terminate

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets").mock(
            side_effect=[
                httpx.Response(200, json=page0),
                httpx.Response(200, json=page1),
                httpx.Response(200, json=page2),
            ]
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_markets()
        finally:
            await client.aclose()

    assert len(out) == 242
    assert route.call_count == 3
    # Verify offset incremented by 100 each call.
    offsets = [int(call.request.url.params.get("offset", "0")) for call in route.calls]
    assert offsets == [0, 100, 200]
    # Verify limit param sent on every call.
    limits = [int(call.request.url.params.get("limit", "0")) for call in route.calls]
    assert limits == [100, 100, 100]


# ---------------------------------------------------------------------------
# Test 2: single short page (== T1 fixture: 5 markets) → one GET, terminate
# ---------------------------------------------------------------------------
async def test_fetch_all_single_page_terminates_immediately(real_gamma_sample: list[dict]) -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets").mock(
            return_value=httpx.Response(200, json=real_gamma_sample)
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_markets()
        finally:
            await client.aclose()

    assert len(out) == 5
    assert route.call_count == 1
    # Sanity: returned dicts preserve the JSON-string clobTokenIds field (Pitfall 2).
    assert "clobTokenIds" in out[0]
    assert isinstance(out[0]["clobTokenIds"], str)


# ---------------------------------------------------------------------------
# Test 3: 5xx → retry succeeds on 3rd attempt
# ---------------------------------------------------------------------------
async def test_retry_on_500_then_succeeds() -> None:
    settings = _fast_settings()  # retry_attempts=3 (default)
    page = [_make_market_dict(i) for i in range(5)]  # short → terminate

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets").mock(
            side_effect=[
                httpx.Response(500, json={"error": "boom"}),
                httpx.Response(500, json={"error": "boom"}),
                httpx.Response(200, json=page),
            ]
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_markets()
        finally:
            await client.aclose()

    assert len(out) == 5
    assert route.call_count == 3


# ---------------------------------------------------------------------------
# Test 4: 5xx → retry exhausts → raises (with reraise=True)
# ---------------------------------------------------------------------------
async def test_retry_exhausts_then_raises() -> None:
    settings = _fast_settings()  # retry_attempts=3
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets").mock(
            return_value=httpx.Response(500, json={"error": "boom"})
        )
        client = GammaClient(settings)
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await client.fetch_all_active_markets()
        finally:
            await client.aclose()
    # After exhausting the 3 retry attempts, no further calls.
    assert route.call_count == 3


# ---------------------------------------------------------------------------
# Test 5: 404 → NO retry (4xx is non-retryable). Exactly 1 call.
# ---------------------------------------------------------------------------
async def test_no_retry_on_404() -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets").mock(
            return_value=httpx.Response(404, json={"error": "not found"})
        )
        client = GammaClient(settings)
        try:
            with pytest.raises(_NonRetryableHTTPError):
                await client.fetch_all_active_markets()
        finally:
            await client.aclose()
    # Critical assertion: 4xx → exactly 1 call, NOT retry_attempts.
    assert route.call_count == 1


# ---------------------------------------------------------------------------
# Test 6: aclose closes the underlying httpx client
# ---------------------------------------------------------------------------
async def test_aclose_closes_http_client() -> None:
    settings = _fast_settings()
    client = GammaClient(settings)
    assert client._http.is_closed is False
    await client.aclose()
    assert client._http.is_closed is True


# ---------------------------------------------------------------------------
# Test 7: progress logging — periodic INFO emit during paginated fetch.
#
# Backstory: LIVE-RUN-003 hung at 15min with zero output because the original
# fetch_all_active_markets() only logged a single line *after* all 490 pages
# completed. This test pins the new contract: a 'starting' line, the 1st page
# emits, then every 50th page, plus a 'final' line.
# ---------------------------------------------------------------------------
async def test_fetch_all_emits_periodic_progress() -> None:
    """Pagination over 110 pages must emit progress lines, not silent for ~3min."""
    from loguru import logger

    settings = _fast_settings()
    # 110 full pages + 1 short page → 111 calls, terminates on the short one.
    full_pages = [[_make_market_dict(i + 100 * p) for i in range(100)] for p in range(110)]
    short_page = [_make_market_dict(11000 + i) for i in range(7)]

    captured: list[str] = []
    sink_id = logger.add(lambda msg: captured.append(msg.record["message"]), level="INFO")

    try:
        with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
            router.get("/markets").mock(
                side_effect=[httpx.Response(200, json=p) for p in full_pages]
                + [httpx.Response(200, json=short_page)]
            )
            client = GammaClient(settings)
            try:
                out = await client.fetch_all_active_markets()
            finally:
                await client.aclose()
    finally:
        logger.remove(sink_id)

    assert len(out) == 110 * 100 + 7

    # Contract assertions — order matters but exact phrasing is intentionally
    # loose so future tweaks to wording don't break the test.
    starting = [m for m in captured if "starting streaming fetch" in m]
    page_1 = [m for m in captured if "page 1 fetched" in m]
    page_50 = [m for m in captured if "page 50 fetched" in m]
    page_100 = [m for m in captured if "page 100 fetched" in m]
    final = [m for m in captured if "final" in m]

    assert len(starting) == 1, f"expected 1 'starting' line, got {captured}"
    assert len(page_1) == 1, f"expected page-1 progress, got {captured}"
    assert len(page_50) == 1, f"expected page-50 progress, got {captured}"
    assert len(page_100) == 1, f"expected page-100 progress, got {captured}"
    assert len(final) == 1, f"expected final summary line, got {captured}"


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1.1 Amendment 01: fetch_all_active_events()
# ─────────────────────────────────────────────────────────────────────────────


def _make_event_dict(idx: int, n_markets: int = 2, n_tags: int = 3) -> dict:
    """Lightweight stand-in for a Polymarket /events row.

    Real shape: {id, slug, title, ticker, liquidity, volume, endDate,
    tags: [{id, label, slug, ...}], markets: [{id, ...}]}
    """
    return {
        "id": str(16000 + idx),
        "slug": f"event-{idx}",
        "title": f"Event {idx}",
        "ticker": f"TKR-{idx}",
        "active": True,
        "closed": False,
        "liquidity": 12345.6,
        "volume": 78901.2,
        "endDate": "2026-12-31T00:00:00Z",
        "tags": [
            {"id": str(100 + j), "label": f"Tag{j}", "slug": f"tag{j}"} for j in range(n_tags)
        ],
        "markets": [{"id": str(540000 + idx * 10 + k)} for k in range(n_markets)],
    }


async def test_fetch_events_paginates_until_short_page() -> None:
    """fetch_all_active_events follows the same pagination pattern as /markets."""
    settings = _fast_settings()
    page0 = [_make_event_dict(i) for i in range(100)]
    page1 = [_make_event_dict(100 + i) for i in range(100)]
    page2 = [_make_event_dict(200 + i) for i in range(31)]  # short page → terminate

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/events").mock(
            side_effect=[
                httpx.Response(200, json=page0),
                httpx.Response(200, json=page1),
                httpx.Response(200, json=page2),
            ]
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_events()
        finally:
            await client.aclose()

    assert len(out) == 231
    assert route.call_count == 3
    offsets = [int(call.request.url.params.get("offset", "0")) for call in route.calls]
    assert offsets == [0, 100, 200]
    # Verify the events filter params (active=true, closed=false), no archived param.
    for call in route.calls:
        assert call.request.url.params.get("active") == "true"
        assert call.request.url.params.get("closed") == "false"


async def test_fetch_events_single_short_page() -> None:
    """Single-page response terminates immediately (1 GET call)."""
    settings = _fast_settings()
    events = [_make_event_dict(i) for i in range(7)]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/events").mock(return_value=httpx.Response(200, json=events))
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_events()
        finally:
            await client.aclose()

    assert len(out) == 7
    assert route.call_count == 1
    # Sanity: nested tags + markets preserved verbatim (RAW dict contract).
    assert isinstance(out[0]["tags"], list)
    assert isinstance(out[0]["markets"], list)
    assert "id" in out[0]["tags"][0]
    assert "label" in out[0]["tags"][0]


async def test_fetch_events_500_then_succeeds() -> None:
    """5xx on /events triggers retry like /markets does."""
    settings = _fast_settings()
    page = [_make_event_dict(i) for i in range(3)]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/events").mock(
            side_effect=[
                httpx.Response(500, json={"error": "boom"}),
                httpx.Response(200, json=page),
            ]
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_events()
        finally:
            await client.aclose()

    assert len(out) == 3
    assert route.call_count == 2


async def test_fetch_events_404_no_retry() -> None:
    """4xx on /events is non-retryable (same policy as /markets)."""
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/events").mock(
            return_value=httpx.Response(404, json={"error": "not found"})
        )
        client = GammaClient(settings)
        try:
            with pytest.raises(_NonRetryableHTTPError):
                await client.fetch_all_active_events()
        finally:
            await client.aclose()
    assert route.call_count == 1


# ─────────────────────────────────────────────────────────────────────────────
# Plan 02-09 (D-23): streaming iterator API tests
# ─────────────────────────────────────────────────────────────────────────────


async def test_iter_active_markets_yields_one_at_a_time() -> None:
    """3-page mock; iter_active_markets yields each market individually,
    stamps `_page_fetched_at_ms`, and strips to `_MARKET_KEEP` fields."""
    settings = _fast_settings()
    page0 = [
        {
            "id": str(540000 + i),
            "conditionId": f"0x{i:064d}",
            "question": f"q {i}",
            "active": True,
            "closed": False,
            "extra_garbage_field": "should_be_stripped",
            "another_unused": [1, 2, 3],
        }
        for i in range(100)
    ]
    page1 = [
        {
            "id": str(540100 + i),
            "conditionId": f"0x{i:064d}",
            "question": f"q {i}",
            "active": True,
            "closed": False,
            "extra_garbage_field": "should_be_stripped",
        }
        for i in range(100)
    ]
    page2 = [
        {
            "id": str(540200 + i),
            "conditionId": f"0x{i:064d}",
            "question": f"q {i}",
            "active": True,
            "closed": False,
        }
        for i in range(42)
    ]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets").mock(
            side_effect=[
                httpx.Response(200, json=page0),
                httpx.Response(200, json=page1),
                httpx.Response(200, json=page2),
            ]
        )
        client = GammaClient(settings)
        try:
            collected: list[dict] = []
            async for m in client.iter_active_markets():
                collected.append(m)
        finally:
            await client.aclose()

    assert len(collected) == 242
    # Every yielded dict has _page_fetched_at_ms stamped
    assert all("_page_fetched_at_ms" in m for m in collected)
    # Stripping: extra_garbage_field MUST be gone after _MARKET_KEEP filter
    assert all("extra_garbage_field" not in m for m in collected)
    assert all("another_unused" not in m for m in collected)
    # Surviving keys ⊆ _MARKET_KEEP
    keep = GammaClient._MARKET_KEEP
    for m in collected:
        assert set(m.keys()) <= keep, f"unexpected keys: {set(m.keys()) - keep}"


async def test_iter_active_markets_paginate_is_async_gen() -> None:
    """Structural check: _paginate is now an async generator function, not coroutine."""
    import inspect

    assert inspect.isasyncgenfunction(GammaClient._paginate)
    assert inspect.isasyncgenfunction(GammaClient.iter_active_markets)
    assert inspect.isasyncgenfunction(GammaClient.iter_active_events)


async def test_iter_active_events_trims_nested_markets() -> None:
    """iter_active_events yields events whose `markets` is trimmed to `[{"id": ...}]`."""
    settings = _fast_settings()
    raw_events = [
        {
            "id": str(16000 + i),
            "slug": f"event-{i}",
            "title": f"Event {i}",
            "active": True,
            "closed": False,
            "markets": [
                {"id": str(540000 + i * 10 + k), "extra": "junk", "more": [1, 2]} for k in range(3)
            ],
        }
        for i in range(5)
    ]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/events").mock(return_value=httpx.Response(200, json=raw_events))
        client = GammaClient(settings)
        try:
            collected: list[dict] = []
            async for e in client.iter_active_events():
                collected.append(e)
        finally:
            await client.aclose()

    assert len(collected) == 5
    for ev in collected:
        assert isinstance(ev["markets"], list)
        for m in ev["markets"]:
            assert set(m.keys()) == {"id"}, f"nested markets not trimmed: {m}"


async def test_fetch_all_active_markets_still_returns_list() -> None:
    """Backward-compat: fetch_all_active_markets returns a list (not iterator).

    Content equality with iterator-collected version.
    """
    settings = _fast_settings()
    page = [_make_market_dict(i) for i in range(50)]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=False) as router:
        router.get("/markets").mock(return_value=httpx.Response(200, json=page))
        client = GammaClient(settings)
        try:
            via_list = await client.fetch_all_active_markets()
        finally:
            await client.aclose()
        assert isinstance(via_list, list)
        assert len(via_list) == 50

        # Reset respx and call iterator path
        router.get("/markets").mock(return_value=httpx.Response(200, json=page))
        client2 = GammaClient(settings)
        try:
            via_iter: list[dict] = []
            async for m in client2.iter_active_markets():
                via_iter.append(m)
        finally:
            await client2.aclose()

    # Content matches (both apply the same _MARKET_KEEP filter)
    assert [m["id"] for m in via_list] == [m["id"] for m in via_iter]


async def test_iter_active_markets_422_yields_partial() -> None:
    """422 mid-pagination → iterator stops cleanly with items yielded so far."""
    settings = _fast_settings()
    page0 = [_make_market_dict(i) for i in range(100)]
    page1 = [_make_market_dict(100 + i) for i in range(100)]
    # Third call returns 422 (Polymarket offset>10000 cap)
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets").mock(
            side_effect=[
                httpx.Response(200, json=page0),
                httpx.Response(200, json=page1),
                httpx.Response(422, json={"error": "offset cap"}),
            ]
        )
        client = GammaClient(settings)
        try:
            collected: list[dict] = []
            async for m in client.iter_active_markets():
                collected.append(m)
        finally:
            await client.aclose()
    # 200 items yielded; iterator stops without raising
    assert len(collected) == 200


async def test_iter_active_markets_max_pages_runaway_raises() -> None:
    """If the API returns MAX_PAGES full pages, the iterator must raise mid-stream."""
    settings = _fast_settings()
    # Build PAGE_LIMIT entries per page so the loop keeps requesting.
    full_page = [_make_market_dict(i) for i in range(100)]

    # Monkey-patch MAX_PAGES to a tiny number to keep the test fast.
    orig_max = GammaClient.MAX_PAGES
    try:
        GammaClient.MAX_PAGES = 3  # type: ignore[assignment]

        with respx.mock(base_url=settings.gamma_url, assert_all_called=False) as router:
            # Return a full page every time → never terminates organically.
            router.get("/markets").mock(return_value=httpx.Response(200, json=full_page))
            client = GammaClient(settings)
            try:
                with pytest.raises(RuntimeError, match="exceeded"):
                    async for _ in client.iter_active_markets():
                        pass
            finally:
                await client.aclose()
    finally:
        GammaClient.MAX_PAGES = orig_max  # type: ignore[assignment]
