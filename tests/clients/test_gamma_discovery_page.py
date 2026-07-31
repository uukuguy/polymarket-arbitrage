from __future__ import annotations

import httpx
import pytest

from polyarb.clients.gamma_client import GammaClient, PaginationIntegrityError
from polyarb.config import Settings


def _gamma() -> GammaClient:
    return GammaClient(Settings(scan_shared_secret="test"))


@pytest.mark.asyncio
async def test_fetch_event_page_returns_durable_opaque_next_cursor(
    respx_mock,
) -> None:
    route = respx_mock.get("https://gamma-api.polymarket.com/events/keyset").mock(
        return_value=httpx.Response(
            200,
            json={
                "events": [
                    {
                        "id": "e-1",
                        "active": True,
                        "closed": False,
                        "markets": [],
                    }
                ],
                "next_cursor": "opaque/c-2?x=1",
            },
        )
    )
    gamma = _gamma()
    try:
        page = await gamma.fetch_active_event_page(
            cursor="opaque/c-1?x=1",
            limit=37,
        )
    finally:
        await gamma.aclose()

    assert page.event_ids == ("e-1",)
    assert page.requested_cursor == "opaque/c-1?x=1"
    assert page.next_cursor == "opaque/c-2?x=1"
    assert page.completed is False
    assert page.started_at_ms <= page.finished_at_ms
    assert route.calls[0].request.url.params["after_cursor"] == "opaque/c-1?x=1"
    assert route.calls[0].request.url.params["limit"] == "37"


@pytest.mark.asyncio
async def test_fetch_market_page_returns_one_bounded_durable_page(
    respx_mock,
) -> None:
    route = respx_mock.get("https://gamma-api.polymarket.com/markets/keyset").mock(
        return_value=httpx.Response(
            200,
            json={
                "markets": [
                    {
                        "id": "m-1",
                        "conditionId": "condition-1",
                        "active": True,
                        "closed": False,
                    }
                ],
                "next_cursor": "opaque/m-2?x=1",
            },
        )
    )
    gamma = _gamma()
    try:
        page = await gamma.fetch_active_market_page(
            cursor="opaque/m-1?x=1",
            limit=37,
        )
    finally:
        await gamma.aclose()

    assert tuple(market["id"] for market in page.markets) == ("m-1",)
    assert page.requested_cursor == "opaque/m-1?x=1"
    assert page.next_cursor == "opaque/m-2?x=1"
    assert page.completed is False
    assert route.calls[0].request.url.params["after_cursor"] == "opaque/m-1?x=1"
    assert route.calls[0].request.url.params["limit"] == "37"


@pytest.mark.asyncio
async def test_fetch_event_page_rejects_repeated_cursor(
    respx_mock,
) -> None:
    respx_mock.get("https://gamma-api.polymarket.com/events/keyset").mock(
        return_value=httpx.Response(
            200,
            json={"events": [], "next_cursor": "same"},
        )
    )
    gamma = _gamma()
    try:
        with pytest.raises(PaginationIntegrityError, match="cursor"):
            await gamma.fetch_active_event_page(cursor="same", limit=1)
    finally:
        await gamma.aclose()


@pytest.mark.asyncio
async def test_fetch_event_page_rejects_unbounded_limit() -> None:
    gamma = _gamma()
    try:
        with pytest.raises(PaginationIntegrityError, match="limit"):
            await gamma.fetch_active_event_page(cursor=None, limit=101)
    finally:
        await gamma.aclose()


@pytest.mark.asyncio
async def test_fetch_event_page_marks_terminal_page_without_inventing_cursor(
    respx_mock,
) -> None:
    respx_mock.get("https://gamma-api.polymarket.com/events/keyset").mock(
        return_value=httpx.Response(
            200,
            json={"events": [], "next_cursor": None},
        )
    )
    gamma = _gamma()
    try:
        page = await gamma.fetch_active_event_page(cursor=None, limit=1)
    finally:
        await gamma.aclose()

    assert page.events == ()
    assert page.next_cursor is None
    assert page.completed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "member",
    [
        "not-a-dict",
        {"active": True, "closed": False},
        {"id": "e-1", "active": "true", "closed": False},
        {"id": "e-1", "active": True, "closed": True},
    ],
)
async def test_fetch_event_page_rejects_malformed_members(
    respx_mock,
    member,
) -> None:
    respx_mock.get("https://gamma-api.polymarket.com/events/keyset").mock(
        return_value=httpx.Response(
            200,
            json={"events": [member], "next_cursor": "c-2"},
        )
    )
    gamma = _gamma()
    try:
        with pytest.raises(PaginationIntegrityError, match="member"):
            await gamma.fetch_active_event_page(cursor="c-1", limit=1)
    finally:
        await gamma.aclose()
