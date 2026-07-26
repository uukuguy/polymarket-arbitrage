"""Unit tests for ClobReaderClient — chunking + side fanout, mocked via patch.object.

py-clob-client is sync, so respx (httpx mock) doesn't help here. We patch the
SDK methods directly on the instance (``client._client.get_order_books`` /
``get_prices``) and inspect ``call_args_list`` to verify chunking + side
correctness.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from unittest.mock import patch

import pytest
from py_clob_client.clob_types import BookParams

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"


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
