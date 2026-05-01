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
    full_pages = [
        [_make_market_dict(i + 100 * p) for i in range(100)] for p in range(110)
    ]
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
    starting = [m for m in captured if "starting paginated fetch" in m]
    page_1 = [m for m in captured if "page 1 fetched" in m]
    page_50 = [m for m in captured if "page 50 fetched" in m]
    page_100 = [m for m in captured if "page 100 fetched" in m]
    final = [m for m in captured if "final" in m]

    assert len(starting) == 1, f"expected 1 'starting' line, got {captured}"
    assert len(page_1) == 1, f"expected page-1 progress, got {captured}"
    assert len(page_50) == 1, f"expected page-50 progress, got {captured}"
    assert len(page_100) == 1, f"expected page-100 progress, got {captured}"
    assert len(final) == 1, f"expected final summary line, got {captured}"
