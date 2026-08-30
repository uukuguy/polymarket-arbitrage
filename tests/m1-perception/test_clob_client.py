"""Unit tests for ClobReaderClient — chunking + side fanout, mocked via patch.object.

py-clob-client is sync, so respx (httpx mock) doesn't help here. We patch the
SDK methods directly on the instance (``client._client.get_order_books`` /
``get_prices``) and inspect ``call_args_list`` to verify chunking + side
correctness.
"""

from __future__ import annotations

import asyncio
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from py_clob_client.clob_types import BookParams
from py_clob_client.exceptions import PolyApiException

from polyarb.clients.clob_client import ClobRateLimitError, ClobReaderClient
from polyarb.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"


def test_clob_sdk_transport_uses_the_declared_http_timeout(monkeypatch) -> None:
    from polyarb.clients import clob_client

    observed = []

    class FakeHttpClient:
        def __init__(self, **kwargs):
            observed.append(kwargs)

        def close(self) -> None:
            pass

    previous = clob_client.clob_http_helpers._http_client
    monkeypatch.setattr(clob_client.httpx, "Client", FakeHttpClient)
    monkeypatch.setattr(ClobReaderClient, "_transport_configured", False)

    try:
        ClobReaderClient(Settings(http_timeout_s=13.5, clob_batch_max_concurrency=4))
    finally:
        clob_client.clob_http_helpers._http_client = previous

    assert observed[0]["http2"] is False
    assert observed[0]["timeout"] == 13.5
    assert observed[0]["limits"] == clob_client.httpx.Limits(max_connections=4)


@pytest.fixture
def real_clob_sample() -> dict:
    """The recorded T1 fixture (2 books + prices_buy + prices_sell + token_ids)."""
    with open(FIXTURES / "clob_sample.json") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Test 1: single chunk (10 token IDs into batch_size=500)
# ---------------------------------------------------------------------------
async def test_get_books_single_chunk(real_clob_sample: dict) -> None:
    client = ClobReaderClient(Settings(clob_batch_size=500))
    fixture_books = real_clob_sample["books"]  # length 2 (T1 fixture)
    token_ids = [f"tok_{i}" for i in range(10)]

    with patch.object(client._client, "get_order_books", return_value=fixture_books) as m:
        out = await client.get_books(token_ids)

    # Returned exactly the SDK's response (length 2, from fixture).
    assert out == fixture_books
    assert len(out) == 2

    # Exactly 1 call (10 < batch_size=500 → no chunking).
    assert m.call_count == 1

    # Inspect what BookParams were passed: positional arg 0 is the list.
    call = m.call_args_list[0]
    params_list = call.args[0]
    assert len(params_list) == 10
    assert all(isinstance(p, BookParams) for p in params_list)
    # Token ids preserved.
    assert [p.token_id for p in params_list] == token_ids
    # No side specified for get_books (default empty string per SDK signature).
    assert all(p.side == "" for p in params_list)


# ---------------------------------------------------------------------------
# Test 2: multiple chunks (7 token IDs / batch_size=3 → 3 calls of [3,3,1])
# ---------------------------------------------------------------------------
async def test_get_books_multiple_chunks() -> None:
    client = ClobReaderClient(Settings(clob_batch_size=3))
    token_ids = [f"tok_{i}" for i in range(7)]

    # Each call returns a 1-element stub list (we're testing chunking math, not content).
    with patch.object(client._client, "get_order_books", return_value=[{"asset_id": "stub"}]) as m:
        out = await client.get_books(token_ids)

    assert m.call_count == math.ceil(7 / 3)  # == 3
    assert m.call_count == 3

    # Verify chunk sizes: [3, 3, 1]
    chunk_sizes = [len(call.args[0]) for call in m.call_args_list]
    assert chunk_sizes == [3, 3, 1]

    # Verify token ids preserved across chunks (in order).
    flat = [p.token_id for call in m.call_args_list for p in call.args[0]]
    assert flat == token_ids

    # Out is concat of 3 stub returns.
    assert len(out) == 3


async def test_get_books_fetches_chunks_with_bounded_concurrency() -> None:
    """A full Quote run must not spend its entire deadline on serial batches."""
    client = ClobReaderClient(Settings(clob_batch_size=2, clob_batch_max_concurrency=2))
    active = 0
    peak_active = 0
    lock = threading.Lock()

    def slow_fetch(params):
        nonlocal active, peak_active
        with lock:
            active += 1
            peak_active = max(peak_active, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return [{"asset_id": param.token_id, "asks": [], "bids": []} for param in params]

    token_ids = [f"tok_{i}" for i in range(8)]
    with patch.object(client._client, "get_order_books", side_effect=slow_fetch) as mocked:
        books = await client.get_books(token_ids, projection="top")

    assert peak_active == 2
    assert mocked.call_count == 4
    assert [book["asset_id"] for book in books] == token_ids


async def test_get_books_does_not_return_partial_books_when_one_chunk_fails() -> None:
    client = ClobReaderClient(Settings(clob_batch_size=2, clob_batch_max_concurrency=2))

    def fetch(params):
        if params[0].token_id == "tok_2":
            raise RuntimeError("upstream-failed")
        return [{"asset_id": param.token_id, "asks": [], "bids": []} for param in params]

    with patch.object(client._client, "get_order_books", side_effect=fetch):
        with pytest.raises(RuntimeError, match="upstream-failed"):
            await client.get_books(["tok_0", "tok_1", "tok_2", "tok_3"], projection="top")


# ---------------------------------------------------------------------------
# Test 3: empty token_ids → no SDK call, returns []
# ---------------------------------------------------------------------------
async def test_get_books_empty_token_ids() -> None:
    client = ClobReaderClient(Settings())
    with patch.object(client._client, "get_order_books", return_value=[]) as m:
        out = await client.get_books([])

    assert out == []
    assert m.call_count == 0  # crucial: short-circuit, no network


# ---------------------------------------------------------------------------
# Test 4: get_prices_buy_sell fans out to 2 sides, BookParams.side correct
# ---------------------------------------------------------------------------
async def test_get_prices_buy_sell_uses_correct_side(real_clob_sample: dict) -> None:
    client = ClobReaderClient(Settings(clob_batch_size=500))
    token_ids = ["t1", "t2"]

    # Mock returns side-specific dicts (mimicking real CLOB shape from fixture).
    buy_response = {"t1": {"BUY": "0.46"}, "t2": {"BUY": "0.53"}}
    sell_response = {"t1": {"SELL": "0.47"}, "t2": {"SELL": "0.54"}}

    with patch.object(client._client, "get_prices", side_effect=[buy_response, sell_response]) as m:
        out = await client.get_prices_buy_sell(token_ids)

    # 2 calls: one for BUY, one for SELL.
    assert m.call_count == 2

    # First call: all sides == "BUY"
    first_params = m.call_args_list[0].args[0]
    assert len(first_params) == 2
    assert all(p.side == "BUY" for p in first_params)
    assert [p.token_id for p in first_params] == token_ids

    # Second call: all sides == "SELL"
    second_params = m.call_args_list[1].args[0]
    assert all(p.side == "SELL" for p in second_params)
    assert [p.token_id for p in second_params] == token_ids

    # Output shape matches fixture (nested per-token dict, prices are strings).
    assert out == {"buy": buy_response, "sell": sell_response}
    assert out["buy"]["t1"]["BUY"] == "0.46"  # str, not float
    assert isinstance(out["buy"]["t1"]["BUY"], str)


# ---------------------------------------------------------------------------
# Test 5: SDK exceptions propagate (no swallowing)
# ---------------------------------------------------------------------------
async def test_propagates_sdk_exceptions() -> None:
    client = ClobReaderClient(Settings())
    with patch.object(client._client, "get_order_books", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="boom"):
            await client.get_books(["t1"])


async def test_get_books_translates_429_without_retaining_provider_body() -> None:
    client = ClobReaderClient(Settings())
    response = httpx.Response(
        429,
        json={"error": "secret-provider-body"},
        request=httpx.Request("GET", "https://clob.invalid/books?token=secret-token"),
    )

    with patch.object(
        client._client,
        "get_order_books",
        side_effect=PolyApiException(resp=response),
    ):
        with pytest.raises(ClobRateLimitError) as raised:
            await client.get_books(["secret-token"])

    assert raised.value.status_code == 429
    assert isinstance(raised.value.__cause__, PolyApiException)
    assert "secret" not in str(raised.value)


async def test_logs_sdk_transport_cause_without_exposing_request_content() -> None:
    client = ClobReaderClient(Settings())

    def wrapped_transport_failure(_params):
        try:
            raise httpx.ReadTimeout("upstream read timed out")
        except httpx.RequestError:
            raise PolyApiException(error_msg="Request exception!")

    with (
        patch.object(client._client, "get_order_books", side_effect=wrapped_transport_failure),
        patch("polyarb.clients.clob_client.logger.warning") as warning,
        pytest.raises(PolyApiException, match="Request exception"),
    ):
        await client.get_books(["secret-token-id"])

    message = warning.call_args.args[0]
    assert "transport_kind=ReadTimeout" in message
    assert "secret-token-id" not in message
    assert "upstream read timed out" not in message


async def test_injected_executor_owns_sync_sdk_call() -> None:
    executor = ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="candidate-dedicated-clob",
    )
    try:
        client = ClobReaderClient(Settings(), executor=executor)
        thread_names: list[str] = []

        def fetch(_params):
            thread_names.append(threading.current_thread().name)
            return []

        with patch.object(client._client, "get_order_books", side_effect=fetch):
            await client.get_books(["t1"])

        assert thread_names
        assert thread_names[0].startswith("candidate-dedicated-clob")
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


async def test_get_books_top_projection_discards_full_depth_per_chunk() -> None:
    """Snapshot callers retain the true top levels from worst-first CLOB rows."""
    client = ClobReaderClient(Settings(clob_batch_size=2))
    token_ids = [f"tok_{i}" for i in range(5)]

    def full_depth(params):
        return [
            SimpleNamespace(
                asset_id=param.token_id,
                asks=[
                    SimpleNamespace(price="0.99", size="1"),
                    SimpleNamespace(price="0.70", size="2"),
                    SimpleNamespace(price="0.51", size="3"),
                ],
                bids=[
                    SimpleNamespace(price="0.01", size="4"),
                    SimpleNamespace(price="0.30", size="5"),
                    SimpleNamespace(price="0.49", size="6"),
                ],
                hash="raw-book-field-must-not-be-retained",
            )
            for param in params
        ]

    with patch.object(client._client, "get_order_books", side_effect=full_depth) as mocked:
        out = await client.get_books(token_ids, projection="top")

    assert mocked.call_count == 3
    assert len(out) == len(token_ids)
    assert all(set(book) == {"asset_id", "asks", "bids"} for book in out)
    assert all(len(book["asks"]) == 1 and len(book["bids"]) == 1 for book in out)
    assert out[0]["asks"] == [{"price": "0.51", "size": "3"}]
    assert out[0]["bids"] == [{"price": "0.49", "size": "6"}]


async def test_get_books_top_projection_does_not_block_event_loop_during_compaction() -> None:
    client = ClobReaderClient(Settings(clob_batch_size=4))
    books = [{"asset_id": f"t{i}", "asks": [], "bids": []} for i in range(4)]

    def slow_compaction(book):
        time.sleep(0.05)
        return book

    ticker = asyncio.Event()
    asyncio.get_running_loop().call_later(0.02, ticker.set)
    with (
        patch.object(client._client, "get_order_books", return_value=books),
        patch(
            "polyarb.clients.clob_client._compact_book_top",
            side_effect=slow_compaction,
        ),
    ):
        task = asyncio.create_task(client.get_books(["t0", "t1", "t2", "t3"], projection="top"))
        await asyncio.wait_for(ticker.wait(), timeout=0.1)
        assert not task.done()
        await task
