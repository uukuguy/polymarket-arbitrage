"""Inj L2-5 dry-run: replay 429 fixture against backfill code locally.

This is NOT a live chaos test — it's a unit test that validates the
backfill code path correctly handles a 429 response (logs warning + sleeps
+ retries via tenacity, doesn't crash). The real Inj L2-5 (live 429 from
Polymarket) is deferred per 03.1-CONTEXT.md "实际触发时再验"; this plan
delivers fixture infrastructure only.

Coverage:
- Fixture file is loadable and has plausible shape (status 429 + Retry-After)
- _fetch_page invokes tenacity retry on 429 (verified by counting requests)
- _fetch_page eventually raises HTTPStatusError after stop_after_attempt(5)
  exhausts (we mock the sleep to avoid waiting 10s × 5 attempts in CI)
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "data_api_429.json"


# ── Fixture sanity ───────────────────────────────────────────────────────────


def test_fixture_loadable():
    """data_api_429.json is well-formed + has Retry-After header."""
    assert FIXTURE.exists(), f"fixture missing: {FIXTURE}"
    with FIXTURE.open() as f:
        data = json.load(f)
    assert data["_meta"]["status_code"] == 429
    assert "Retry-After" in data["_meta"]["headers"]
    assert data["_meta"]["headers"]["Retry-After"] == "30"
    # Body shape is intentionally loose — Polymarket hasn't published the schema
    assert "body" in data


# ── Replay fixture against the actual backfill code path ────────────────────


@pytest.mark.asyncio
async def test_fetch_page_retries_on_429(monkeypatch):
    """_fetch_page sees 429 → logs + sleeps + tenacity retries.

    Uses httpx.MockTransport seeded with the fixture's 429 response on every
    request. After 5 tenacity attempts (stop_after_attempt), it should raise
    HTTPStatusError(status_code=429). Asserts:
      - Request was actually attempted multiple times (retry loop fired)
      - Sleep was called (429 backoff applied)
      - Final raise is the expected HTTPStatusError
    """
    from polyarb.clients import data_api_client

    with FIXTURE.open() as f:
        fixture = json.load(f)

    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            429,
            headers=fixture["_meta"]["headers"],
            json=fixture["body"],
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url=data_api_client.DATA_API_BASE)

    # Mock asyncio.sleep so the test doesn't actually wait 10s × N attempts
    mock_sleep = AsyncMock()
    monkeypatch.setattr("polyarb.clients.data_api_client.asyncio.sleep", mock_sleep)

    # Also short-circuit the rate limiter to keep the test sub-second
    class _NoopLimiter:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr("polyarb.clients.data_api_client._LIMITER", _NoopLimiter())

    try:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await data_api_client._fetch_page(client, params={"limit": 10, "offset": 0})
        assert excinfo.value.response.status_code == 429
        # Tenacity stop_after_attempt(5) ⇒ exactly 5 attempts before reraise
        assert call_count == 5, f"expected 5 retry attempts, got {call_count}"
        # 429 backoff sleep called at least once
        assert mock_sleep.await_count >= 1, "asyncio.sleep should have been called for 429 backoff"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_fetch_page_succeeds_when_429_clears(monkeypatch):
    """When the first request is 429 but subsequent retries succeed, _fetch_page
    returns the eventual 200 payload — proving the retry loop unwinds correctly.
    """
    from polyarb.clients import data_api_client

    with FIXTURE.open() as f:
        fixture = json.load(f)

    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:  # first two attempts 429, third succeeds
            return httpx.Response(
                429,
                headers=fixture["_meta"]["headers"],
                json=fixture["body"],
            )
        return httpx.Response(200, json=[{"asset": "0xabc", "size": 1.0, "timestamp": 1700000000}])

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url=data_api_client.DATA_API_BASE)

    monkeypatch.setattr("polyarb.clients.data_api_client.asyncio.sleep", AsyncMock())

    class _NoopLimiter:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr("polyarb.clients.data_api_client._LIMITER", _NoopLimiter())

    try:
        result = await data_api_client._fetch_page(client, params={"limit": 10, "offset": 0})
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["asset"] == "0xabc"
        assert call_count == 3, f"expected 3 attempts (2x 429 + 1x 200), got {call_count}"
    finally:
        await client.aclose()
