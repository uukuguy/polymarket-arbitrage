"""Unit tests for GammaClient — pagination + retry semantics, mocked via respx.

asyncio_mode=auto is set in pyproject [tool.pytest.ini_options], so plain
``async def test_*`` is auto-collected.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import call as mock_call

import httpx
import pytest
import respx

from polyarb.clients.gamma_client import (
    GammaClient,
    PaginationCoverage,
    PaginationCursorRejectedError,
    PaginationIntegrityError,
    PaginationResult,
    _NonRetryableHTTPError,
)
from polyarb.config import Settings
from polyarb.perception.market_truth import INVALID_EVENT_MEMBER_REASON
from polyarb.snapshot.normalizer import normalize_events

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.asyncio
async def test_gamma_cancelled_context_exit_bounds_hung_http_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded Structure page cancellation must not hang during client cleanup."""
    from polyarb.clients import gamma_client as gamma_module

    client = GammaClient(Settings(scan_shared_secret="test"))
    closed = asyncio.Event()

    async def hung_close() -> None:
        await closed.wait()

    monkeypatch.setattr(client._http, "aclose", hung_close)
    monkeypatch.setattr(gamma_module, "GAMMA_CANCELLED_CLOSE_TIMEOUT_S", 0.001)

    await client.__aexit__(asyncio.CancelledError, None, None)


def _fast_settings() -> Settings:
    """Settings with retry waits compressed to ~ms so tests stay fast."""
    return Settings(retry_min_wait_s=0.001, retry_max_wait_s=0.01)


def _make_market_dict(idx: int) -> dict:
    """Lightweight stand-in for a Polymarket market dict (only fields the
    client touches: pagination doesn't read fields, but we want realistic
    enough payloads)."""
    return {"id": str(540000 + idx), "question": f"market {idx}", "active": True}


def _market_page(markets: list[dict], next_cursor: str | None = None) -> dict:
    return {"markets": markets, "next_cursor": next_cursor}


def _event_page(events: list[dict], next_cursor: str | None = None) -> dict:
    return {"events": events, "next_cursor": next_cursor}


async def test_markets_use_keyset_until_missing_next_cursor() -> None:
    settings = _fast_settings()
    page_1 = {"markets": [_make_market_dict(i) for i in range(100)], "next_cursor": "c1"}
    page_2 = {"markets": [_make_market_dict(100)], "next_cursor": None}
    coverage = PaginationCoverage(source="markets")
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets/keyset").mock(
            side_effect=[httpx.Response(200, json=page_1), httpx.Response(200, json=page_2)]
        )
        async with GammaClient(settings) as client:
            rows = [row async for row in client.iter_active_markets(coverage)]
    assert len(rows) == 101
    assert coverage.result == PaginationResult(101, 2, True, None)
    assert route.calls[0].request.url.params.get("after_cursor") is None
    assert route.calls[1].request.url.params["after_cursor"] == "c1"


async def test_repeated_keyset_cursor_is_not_complete() -> None:
    settings = _fast_settings()
    coverage = PaginationCoverage(source="markets")
    page = {"markets": [_make_market_dict(1)], "next_cursor": "same"}
    with respx.mock(base_url=settings.gamma_url) as router:
        router.get("/markets/keyset").mock(return_value=httpx.Response(200, json=page))
        async with GammaClient(settings) as client:
            with pytest.raises(PaginationIntegrityError, match="repeated cursor"):
                _ = [row async for row in client.iter_active_markets(coverage)]
    assert coverage.result.completed is False


async def test_keyset_http_error_never_becomes_successful_short_page() -> None:
    settings = _fast_settings()
    coverage = PaginationCoverage(source="markets")
    with respx.mock(base_url=settings.gamma_url) as router:
        router.get("/markets/keyset").mock(return_value=httpx.Response(422, json={"error": "cap"}))
        async with GammaClient(settings) as client:
            with pytest.raises(_NonRetryableHTTPError):
                _ = [row async for row in client.iter_active_markets(coverage)]
    assert coverage.result.completed is False


async def test_bounded_page_classifies_rejected_continuation_cursor() -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets/keyset").mock(
            return_value=httpx.Response(403, json={"error": "cursor expired"})
        )
        async with GammaClient(settings) as client:
            with pytest.raises(PaginationCursorRejectedError) as raised:
                await client.fetch_active_market_page("expired-cursor", 100)

    assert raised.value.source == "markets"
    assert raised.value.status_code == 403
    assert route.call_count == 1


@pytest.fixture
def real_gamma_sample() -> list[dict]:
    """The recorded T1 fixture (5 real markets)."""
    with open(FIXTURES / "gamma_sample.json") as f:
        data = json.load(f)
    assert isinstance(data, list) and len(data) >= 1
    return data


# ---------------------------------------------------------------------------
# Test 1: pagination terminates only when next_cursor is missing
# ---------------------------------------------------------------------------
async def test_fetch_all_paginates_until_short_page() -> None:
    settings = _fast_settings()
    page0 = [_make_market_dict(i) for i in range(100)]
    page1 = [_make_market_dict(100 + i) for i in range(100)]
    page2 = [_make_market_dict(200 + i) for i in range(42)]  # short page → terminate

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets/keyset").mock(
            side_effect=[
                httpx.Response(200, json=_market_page(page0, "c1")),
                httpx.Response(200, json=_market_page(page1, "c2")),
                httpx.Response(200, json=_market_page(page2)),
            ]
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_markets()
        finally:
            await client.aclose()

    assert len(out) == 242
    assert route.call_count == 3
    cursors = [call.request.url.params.get("after_cursor") for call in route.calls]
    assert cursors == [None, "c1", "c2"]
    # Verify limit param sent on every call.
    limits = [int(call.request.url.params.get("limit", "0")) for call in route.calls]
    assert limits == [100, 100, 100]


# ---------------------------------------------------------------------------
# Test 2: single short page (== T1 fixture: 5 markets) → one GET, terminate
# ---------------------------------------------------------------------------
async def test_fetch_all_single_page_terminates_immediately(real_gamma_sample: list[dict]) -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets/keyset").mock(
            return_value=httpx.Response(200, json=_market_page(real_gamma_sample))
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
        route = router.get("/markets/keyset").mock(
            side_effect=[
                httpx.Response(500, json={"error": "boom"}),
                httpx.Response(500, json={"error": "boom"}),
                httpx.Response(200, json=_market_page(page)),
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
        route = router.get("/markets/keyset").mock(
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
        route = router.get("/markets/keyset").mock(
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


async def test_fetch_market_states_deduplicates_and_preserves_closed_truth() -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        first = router.get("/markets/market-1").mock(
            return_value=httpx.Response(
                200,
                json={"id": "market-1", "active": True, "closed": True},
            )
        )
        second = router.get("/markets/market-2").mock(
            return_value=httpx.Response(
                200,
                json={"id": "market-2", "active": False, "closed": False},
            )
        )
        async with GammaClient(settings) as client:
            states = await client.fetch_market_states(["market-2", "market-1", "market-1"])

    assert states == {
        "market-1": {"active": True, "closed": True},
        "market-2": {"active": False, "closed": False},
    }
    assert first.call_count == 1
    assert second.call_count == 1


async def test_fetch_market_states_falls_back_from_point_404_to_exact_list() -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        point = router.get("/markets/market-1").mock(
            return_value=httpx.Response(
                404,
                json={"type": "not found error", "error": "id not found"},
            )
        )
        exact = router.get(
            "/markets",
            params={"id": "market-1", "limit": "1"},
        ).mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "market-1", "active": True, "closed": False}],
            )
        )
        async with GammaClient(settings) as client:
            states = await client.fetch_market_states(["market-1"])

    assert states == {"market-1": {"active": True, "closed": False}}
    assert point.call_count == 1
    assert exact.call_count == 1


@pytest.mark.parametrize(
    "payload",
    [
        [],
        [{"id": "wrong-market", "active": True, "closed": False}],
        [{"id": "market-1", "active": 1, "closed": False}],
        [
            {"id": "market-1", "active": True, "closed": False},
            {"id": "market-1", "active": True, "closed": False},
        ],
    ],
)
async def test_fetch_market_states_point_404_fallback_fails_closed(
    payload: list[dict],
) -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets/market-1").mock(
            return_value=httpx.Response(404, json={"error": "id not found"})
        )
        router.get(
            "/markets",
            params={"id": "market-1", "limit": "1"},
        ).mock(return_value=httpx.Response(200, json=payload))
        async with GammaClient(settings) as client:
            with pytest.raises(PaginationIntegrityError, match="fallback"):
                await client.fetch_market_states(["market-1"])


async def test_fetch_market_states_non_404_does_not_fallback() -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        point = router.get("/markets/market-1").mock(
            return_value=httpx.Response(422, json={"error": "invalid"})
        )
        async with GammaClient(settings) as client:
            with pytest.raises(_NonRetryableHTTPError):
                await client.fetch_market_states(["market-1"])

    assert point.call_count == 1


async def test_fetch_market_states_fails_closed_on_malformed_identity() -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets/market-1").mock(
            return_value=httpx.Response(
                200,
                json={"id": "wrong-market", "active": True, "closed": True},
            )
        )
        async with GammaClient(settings) as client:
            with pytest.raises(PaginationIntegrityError, match="identity mismatch"):
                await client.fetch_market_states(["market-1"])


async def test_fetch_market_states_rejects_unbounded_lookup_set() -> None:
    settings = _fast_settings()
    async with GammaClient(settings) as client:
        with pytest.raises(PaginationIntegrityError, match="lookup limit"):
            await client.fetch_market_states(
                [f"market-{index}" for index in range(client.MAX_MARKET_STATE_LOOKUPS + 1)]
            )


async def test_fetch_market_states_batches_large_exact_id_set() -> None:
    settings = _fast_settings()
    # Production 2026-08-01 exposed 545 active/open event members that were
    # absent from the active-market keyset.  That is still one bounded race
    # window and must fit below the client's hard fan-out ceiling.
    market_ids = [f"market-{index:03d}" for index in range(545)]
    chunks = [market_ids[start : start + 25] for start in range(0, len(market_ids), 25)]

    def payload(ids: list[str]) -> list[dict]:
        return [
            {"id": market_id, "active": False, "closed": True}
            for market_id in reversed(ids)
        ]

    async with GammaClient(settings) as client:
        client._get = AsyncMock(side_effect=[payload(ids) for ids in chunks])
        states = await client.fetch_market_states(market_ids)

    assert set(states) == set(market_ids)
    assert all(state == {"active": False, "closed": True} for state in states.values())
    assert client._get.await_args_list == [
        mock_call(
            "/markets",
            [("id", market_id) for market_id in ids] + [("limit", str(len(ids)))],
        )
        for ids in chunks
    ]


async def test_fetch_market_states_marks_batch_ids_absent_from_exact_catalog() -> None:
    settings = _fast_settings()
    market_ids = [f"market-{index:03d}" for index in range(101)]

    async with GammaClient(settings) as client:
        client._get = AsyncMock(return_value=[])
        states = await client.fetch_market_states(market_ids)

    assert set(states) == set(market_ids)
    assert all(
        state == {"active": False, "closed": True, "source_absent": True}
        for state in states.values()
    )


async def test_fetch_market_parent_states_returns_inactive_parent_truth() -> None:
    settings = _fast_settings()
    payload = {
        "id": "market-1",
        "negRisk": True,
        "negRiskMarketID": "group-1",
        "events": [
            {
                "id": "event-1",
                "active": False,
                "closed": False,
                "archived": True,
            }
        ],
    }
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets", params={"id": "market-1"}).mock(
            return_value=httpx.Response(200, json=[payload])
        )
        async with GammaClient(settings) as client:
            states = await client.fetch_market_parent_states({"market-1": "group-1"})

    assert states == {
        "market-1": {
            "event_id": "event-1",
            "active": False,
            "closed": False,
            "archived": True,
        }
    }
    assert route.call_count == 1


async def test_fetch_market_parent_states_batches_large_exact_id_set() -> None:
    settings = _fast_settings()
    market_groups = {
        f"market-{index:03d}": f"group-{index:03d}"
        for index in range(GammaClient.MARKET_PARENT_LOOKUP_BATCH_SIZE * 2 + 2)
    }

    def _payload(ids: list[str]) -> list[dict]:
        return [
            {
                "id": market_id,
                "negRisk": True,
                "negRiskMarketID": market_groups[market_id],
                "events": [
                    {
                        "id": f"event-{market_id}",
                        "active": False,
                        "closed": False,
                        "archived": True,
                    }
                ],
            }
            for market_id in reversed(ids)
        ]

    sorted_ids = sorted(market_groups)
    chunks = [
        sorted_ids[start : start + GammaClient.MARKET_PARENT_LOOKUP_BATCH_SIZE]
        for start in range(0, len(sorted_ids), GammaClient.MARKET_PARENT_LOOKUP_BATCH_SIZE)
    ]
    async with GammaClient(settings) as client:
        client._get = AsyncMock(side_effect=[_payload(ids) for ids in chunks])
        states = await client.fetch_market_parent_states(market_groups)

    assert set(states) == set(market_groups)
    assert client._get.await_args_list == [
        mock_call(
            "/markets",
            [("id", market_id) for market_id in ids] + [("limit", str(len(ids)))],
        )
        for ids in chunks
    ]


async def test_fetch_market_parent_states_bounds_parallel_batch_lookups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow Gamma batch cannot serially consume the whole Structure deadline."""
    settings = _fast_settings()
    monkeypatch.setattr(GammaClient, "MARKET_PARENT_LOOKUP_BATCH_SIZE", 1)
    market_groups = {f"market-{index}": f"group-{index}" for index in range(6)}
    active = 0
    peak = 0

    async def get_batch(_path, params):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0)
            market_id = params[0][1]
            return [
                {
                    "id": market_id,
                    "negRisk": True,
                    "negRiskMarketID": market_groups[market_id],
                    "events": [
                        {
                            "id": f"event-{market_id}",
                            "active": False,
                            "closed": False,
                            "archived": True,
                        }
                    ],
                }
            ]
        finally:
            active -= 1

    async with GammaClient(settings) as client:
        client._get = get_batch
        states = await client.fetch_market_parent_states(market_groups)

    assert set(states) == set(market_groups)
    assert peak == GammaClient.MAX_CONCURRENT_PARENT_LOOKUPS


@pytest.mark.parametrize("response_kind", ["missing", "extra", "duplicate"])
async def test_fetch_market_parent_states_requires_exact_response_identity_set(
    response_kind: str,
) -> None:
    settings = _fast_settings()
    market_groups = {"market-1": "group-1", "market-2": "group-2"}

    def _market(market_id: str) -> dict:
        return {
            "id": market_id,
            "negRisk": True,
            "negRiskMarketID": market_groups.get(market_id, "group-extra"),
            "events": [
                {
                    "id": f"event-{market_id}",
                    "active": False,
                    "closed": False,
                    "archived": True,
                }
            ],
        }

    payloads = {
        "missing": [_market("market-1")],
        "extra": [_market("market-1"), _market("market-2"), _market("market-extra")],
        "duplicate": [_market("market-1"), _market("market-1")],
    }
    async with GammaClient(settings) as client:
        client._get = AsyncMock(return_value=payloads[response_kind])
        with pytest.raises(
            PaginationIntegrityError,
            match="identity|invalid shape",
        ):
            await client.fetch_market_parent_states(market_groups)


async def test_fetch_market_parent_states_quarantines_missing_non_open_market() -> None:
    settings = _fast_settings()
    market_groups = {"market-1": "group-1", "market-2": "group-2"}
    parent_payload = [
        {
            "id": "market-1",
            "negRisk": True,
            "negRiskMarketID": "group-1",
            "events": [
                {
                    "id": "event-1",
                    "active": False,
                    "closed": False,
                    "archived": True,
                }
            ],
        }
    ]

    async with GammaClient(settings) as client:
        client._get = AsyncMock(
            side_effect=[
                parent_payload,
                {"id": "market-2", "active": False, "closed": True},
            ]
        )
        states = await client.fetch_market_parent_states(market_groups)

    assert states["market-1"]["event_id"] == "event-1"
    assert states["market-2"] == {
        "event_id": None,
        "active": False,
        "closed": True,
        "archived": True,
        "source_absent": False,
    }


async def test_fetch_market_parent_states_propagates_chunk_failure_and_stops() -> None:
    settings = _fast_settings()
    market_groups = {
        f"market-{index:03d}": f"group-{index:03d}"
        for index in range(GammaClient.MARKET_PARENT_LOOKUP_BATCH_SIZE + 1)
    }
    first_ids = sorted(market_groups)[: GammaClient.MARKET_PARENT_LOOKUP_BATCH_SIZE]
    first_payload = [
        {
            "id": market_id,
            "negRisk": True,
            "negRiskMarketID": market_groups[market_id],
            "events": [
                {
                    "id": f"event-{market_id}",
                    "active": False,
                    "closed": False,
                    "archived": True,
                }
            ],
        }
        for market_id in first_ids
    ]

    async with GammaClient(settings) as client:
        client._get = AsyncMock(side_effect=[first_payload, httpx.ConnectError("chunk failed")])
        with pytest.raises(httpx.ConnectError, match="chunk failed"):
            await client.fetch_market_parent_states(market_groups)

    assert client._get.await_count == 2


@pytest.mark.parametrize(
    "payload",
    [
        [],
        [{"id": "wrong", "negRisk": True, "negRiskMarketID": "group-1", "events": []}],
        [
            {
                "id": "market-1",
                "negRisk": True,
                "negRiskMarketID": "wrong-group",
                "events": [],
            }
        ],
        [
            {
                "id": "market-1",
                "negRisk": True,
                "negRiskMarketID": "group-1",
                "events": [],
            }
        ],
        [
            {
                "id": "market-1",
                "negRisk": True,
                "negRiskMarketID": "group-1",
                "events": [
                    {
                        "id": "event-1",
                        "active": False,
                        "closed": False,
                        "archived": False,
                    },
                    {
                        "id": "event-2",
                        "active": False,
                        "closed": False,
                        "archived": False,
                    },
                ],
            }
        ],
        [
            {
                "id": "market-1",
                "negRisk": True,
                "negRiskMarketID": "group-1",
                "events": [
                    {
                        "id": "event-1",
                        "active": "false",
                        "closed": False,
                        "archived": False,
                    }
                ],
            }
        ],
    ],
)
async def test_fetch_market_parent_states_fails_closed_on_malformed_truth(
    payload: list[dict],
) -> None:
    settings = _fast_settings()
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets", params={"id": "market-1"}).mock(
            return_value=httpx.Response(200, json=payload)
        )
        if not payload:
            router.get("/markets/market-1").mock(
                return_value=httpx.Response(200, json=[])
            )
        async with GammaClient(settings) as client:
            with pytest.raises(PaginationIntegrityError):
                await client.fetch_market_parent_states({"market-1": "group-1"})


async def test_fetch_market_parent_states_rejects_unbounded_lookup_set() -> None:
    settings = _fast_settings()
    async with GammaClient(settings) as client:
        with pytest.raises(PaginationIntegrityError, match="lookup limit"):
            await client.fetch_market_parent_states(
                {
                    f"market-{index}": f"group-{index}"
                    for index in range(client.MAX_MARKET_PARENT_LOOKUPS + 1)
                }
            )


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
# Test 7: long keyset traversal remains memory-bounded and completes.
# ---------------------------------------------------------------------------
async def test_fetch_all_handles_many_keyset_pages() -> None:
    from loguru import logger

    settings = _fast_settings()
    full_pages = [[_make_market_dict(i + 100 * p) for i in range(100)] for p in range(110)]
    short_page = [_make_market_dict(11000 + i) for i in range(7)]

    responses = [
        httpx.Response(200, json=_market_page(page, f"c{index + 1}"))
        for index, page in enumerate(full_pages)
    ]
    responses.append(httpx.Response(200, json=_market_page(short_page)))

    captured: list[str] = []
    sink_id = logger.add(lambda msg: captured.append(msg.record["message"]), level="INFO")
    try:
        with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
            route = router.get("/markets/keyset").mock(side_effect=responses)
            client = GammaClient(settings)
            try:
                out = await client.fetch_all_active_markets()
            finally:
                await client.aclose()
    finally:
        logger.remove(sink_id)

    assert len(out) == 110 * 100 + 7
    assert route.call_count == 111
    assert route.calls[-1].request.url.params["after_cursor"] == "c110"
    assert len([message for message in captured if "starting streaming fetch" in message]) == 1
    assert len([message for message in captured if "page 1 fetched" in message]) == 1
    assert len([message for message in captured if "page 50 fetched" in message]) == 1
    assert len([message for message in captured if "page 100 fetched" in message]) == 1
    assert len([message for message in captured if "final" in message]) == 1


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
        route = router.get("/events/keyset").mock(
            side_effect=[
                httpx.Response(200, json=_event_page(page0, "c1")),
                httpx.Response(200, json=_event_page(page1, "c2")),
                httpx.Response(200, json=_event_page(page2)),
            ]
        )
        client = GammaClient(settings)
        try:
            out = await client.fetch_all_active_events()
        finally:
            await client.aclose()

    assert len(out) == 231
    assert route.call_count == 3
    cursors = [call.request.url.params.get("after_cursor") for call in route.calls]
    assert cursors == [None, "c1", "c2"]
    # Verify the events filter params (active=true, closed=false), no archived param.
    for call in route.calls:
        assert call.request.url.params.get("active") == "true"
        assert call.request.url.params.get("closed") == "false"


async def test_fetch_events_single_short_page() -> None:
    """Single-page response terminates immediately (1 GET call)."""
    settings = _fast_settings()
    events = [_make_event_dict(i) for i in range(7)]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/events/keyset").mock(
            return_value=httpx.Response(200, json=_event_page(events))
        )
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
        route = router.get("/events/keyset").mock(
            side_effect=[
                httpx.Response(500, json={"error": "boom"}),
                httpx.Response(200, json=_event_page(page)),
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
        route = router.get("/events/keyset").mock(
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

    coverage = PaginationCoverage(source="markets")
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets/keyset").mock(
            side_effect=[
                httpx.Response(200, json=_market_page(page0, "c1")),
                httpx.Response(200, json=_market_page(page1, "c2")),
                httpx.Response(200, json=_market_page(page2)),
            ]
        )
        client = GammaClient(settings)
        try:
            collected: list[dict] = []
            async for m in client.iter_active_markets(coverage):
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
    assert coverage.result == PaginationResult(242, 3, True, None)


async def test_iter_active_markets_paginate_is_async_gen() -> None:
    """Structural check: keyset paginator is an async generator function."""
    import inspect

    assert inspect.isasyncgenfunction(GammaClient._paginate_keyset)
    assert inspect.isasyncgenfunction(GammaClient.iter_active_markets)
    assert inspect.isasyncgenfunction(GammaClient.iter_active_events)


async def test_iter_active_events_trims_nested_markets() -> None:
    """Event projection preserves membership truth fields and strips unrelated data."""
    settings = _fast_settings()
    raw_events = [
        {
            "id": str(16000 + i),
            "slug": f"event-{i}",
            "title": f"Event {i}",
            "active": True,
            "closed": False,
            "negRisk": True,
            "enableNegRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": f"group-{i}",
            "markets": [
                {
                    "id": str(540000 + i * 10 + k),
                    "active": True,
                    "closed": False,
                    "negRiskOther": False,
                    "groupItemTitle": f"Candidate {k}",
                    "extra": "junk",
                    "more": [1, 2],
                }
                for k in range(3)
            ],
        }
        for i in range(5)
    ]

    coverage = PaginationCoverage(source="events")
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/events/keyset").mock(
            return_value=httpx.Response(200, json=_event_page(raw_events))
        )
        client = GammaClient(settings)
        try:
            collected: list[dict] = []
            async for e in client.iter_active_events(coverage):
                collected.append(e)
        finally:
            await client.aclose()

    assert len(collected) == 5
    for ev in collected:
        assert ev["negRisk"] is True
        assert ev["enableNegRisk"] is True
        assert ev["negRiskAugmented"] is False
        assert ev["negRiskMarketID"].startswith("group-")
        assert isinstance(ev["markets"], list)
        for m in ev["markets"]:
            assert set(m.keys()) == {
                "id",
                "active",
                "closed",
                "negRiskOther",
                "groupItemTitle",
            }, f"nested markets not trimmed: {m}"
    assert coverage.result == PaginationResult(5, 1, True, None)


async def test_event_projection_preserves_invalid_membership_evidence() -> None:
    settings = _fast_settings()
    raw_event = _make_event_dict(0, n_markets=0)
    raw_event["negRisk"] = True
    raw_event["enableNegRisk"] = True
    raw_event["negRiskAugmented"] = False
    raw_event["negRiskMarketID"] = "group-invalid"
    raw_event["markets"] = [
        "not-a-dict",
        {
            "id": "market-a",
            "active": True,
            "closed": False,
            "negRiskOther": False,
            "unused": "stripped",
        },
        {
            "id": "market-b",
            "active": True,
            "closed": False,
            "negRiskOther": False,
        },
    ]
    coverage = PaginationCoverage(source="events")
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/events/keyset").mock(
            return_value=httpx.Response(200, json=_event_page([raw_event]))
        )
        async with GammaClient(settings) as client:
            projected = [event async for event in client.iter_active_events(coverage)][0]

    assert projected["markets"][0] == "not-a-dict"
    assert projected["markets"][1] == {
        "id": "market-a",
        "active": True,
        "closed": False,
        "negRiskOther": False,
        "groupItemTitle": None,
    }
    _, _, _, members, groups = normalize_events([projected])
    assert [member.market_id for member in members] == ["market-a", "market-b"]
    assert groups[0].quality == "incomplete-source"
    assert groups[0].reason == INVALID_EVENT_MEMBER_REASON


async def test_fetch_all_active_markets_still_returns_list() -> None:
    """Backward-compat: fetch_all_active_markets returns a list (not iterator).

    Content equality with iterator-collected version.
    """
    settings = _fast_settings()
    page = [_make_market_dict(i) for i in range(50)]

    with respx.mock(base_url=settings.gamma_url, assert_all_called=False) as router:
        router.get("/markets/keyset").mock(
            return_value=httpx.Response(200, json=_market_page(page))
        )
        client = GammaClient(settings)
        try:
            via_list = await client.fetch_all_active_markets()
        finally:
            await client.aclose()
        assert isinstance(via_list, list)
        assert len(via_list) == 50

        # Reset respx and call iterator path
        router.get("/markets/keyset").mock(
            return_value=httpx.Response(200, json=_market_page(page))
        )
        client2 = GammaClient(settings)
        try:
            via_iter: list[dict] = []
            coverage = PaginationCoverage(source="markets")
            async for m in client2.iter_active_markets(coverage):
                via_iter.append(m)
        finally:
            await client2.aclose()

    # Content matches (both apply the same _MARKET_KEEP filter)
    assert [m["id"] for m in via_list] == [m["id"] for m in via_iter]


async def test_iter_active_markets_422_marks_partial_and_raises() -> None:
    """422 mid-pagination must not convert partial results into success."""
    settings = _fast_settings()
    page0 = [_make_market_dict(i) for i in range(100)]
    page1 = [_make_market_dict(100 + i) for i in range(100)]
    coverage = PaginationCoverage(source="markets")
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        router.get("/markets/keyset").mock(
            side_effect=[
                httpx.Response(200, json=_market_page(page0, "c1")),
                httpx.Response(200, json=_market_page(page1, "c2")),
                httpx.Response(422, json={"error": "offset cap"}),
            ]
        )
        client = GammaClient(settings)
        try:
            collected: list[dict] = []
            with pytest.raises(_NonRetryableHTTPError):
                async for m in client.iter_active_markets(coverage):
                    collected.append(m)
        finally:
            await client.aclose()
    assert len(collected) == 200
    assert coverage.result.completed is False


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
            call_number = 0

            def next_page(_request: httpx.Request) -> httpx.Response:
                nonlocal call_number
                call_number += 1
                return httpx.Response(200, json=_market_page(full_page, f"cursor-{call_number}"))

            router.get("/markets/keyset").mock(side_effect=next_page)
            client = GammaClient(settings)
            coverage = PaginationCoverage(source="markets")
            try:
                with pytest.raises(PaginationIntegrityError, match="exceeded"):
                    async for _ in client.iter_active_markets(coverage):
                        pass
            finally:
                await client.aclose()
            assert coverage.result == PaginationResult(300, 3, False, "cursor-3")
    finally:
        GammaClient.MAX_PAGES = orig_max  # type: ignore[assignment]
