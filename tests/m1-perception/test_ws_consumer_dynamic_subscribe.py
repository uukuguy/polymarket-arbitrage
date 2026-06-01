"""RED → GREEN tests for WsConsumer.add_subscriptions / remove_subscriptions.

Phase 05 Plan 02 Task 1 (revision 1). 9 tests covering:

1. add_subscriptions: no live ws → returns False, mutates _l3_active_set
2. add_subscriptions: live ws → sends payload, returns True
3. add_subscriptions: empty list → noop, returns True
4. add_subscriptions: send failure → returns False, _l3_active_set NOT mutated
   (Warning #12 deterministic spec)
5. remove_subscriptions: live ws → sends unsubscribe, _l3_active_set updated
6. remove_subscriptions: no live ws → returns False, fallback mutation
7. Lint: add_subscriptions does NOT acquire a Lock (websockets 15+ safe)
8. _compute_active_assets returns union of _candidate_set and _l3_active_set
9. add_subscriptions accepts a 10-token Yes+No payload (D-05 N=5)

The 9 tests are RED until Task 2 refactors WsConsumer.
"""
from __future__ import annotations

import inspect
import json
import os
from unittest.mock import AsyncMock, MagicMock

# Allow tests to use POLYARB_ALLOW_EXTERNAL_PATHS for fixtures that touch
# tmp_path (matches conftest.py convention).
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def _make_consumer(initial_assets: list[str] | None = None):
    """Construct a WsConsumer with mock settings + watchdog for unit tests."""
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=initial_assets,
    )
    return consumer


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — add_subscriptions: no live ws → returns False, but mutates pending
# ─────────────────────────────────────────────────────────────────────────────


async def test_add_subscriptions_no_live_ws_returns_false_but_mutates_pending() -> None:
    """No live ws → return False; new tokens land in _l3_active_set fallback."""
    consumer = _make_consumer(initial_assets=["a"])
    assert consumer._current_ws is None

    result = await consumer.add_subscriptions(["b", "c"])

    assert result is False, "add_subscriptions must return False when ws is None"
    assert set(consumer.subscribed_assets) == {"a", "b", "c"}, (
        f"subscribed_assets must reflect fallback mutation; got {consumer.subscribed_assets}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — add_subscriptions: live ws → sends payload, returns True
# ─────────────────────────────────────────────────────────────────────────────


async def test_add_subscriptions_with_live_ws_sends_payload_and_returns_true() -> None:
    """Live ws → send subscribe payload + return True; subscribed_assets is union."""
    consumer = _make_consumer(initial_assets=["a"])
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock()
    consumer._current_ws = mock_ws

    result = await consumer.add_subscriptions(["b", "c"])

    assert result is True, "add_subscriptions must return True on successful send"
    assert mock_ws.send.call_count == 1, (
        f"send must be called exactly once; got {mock_ws.send.call_count}"
    )
    sent_arg = mock_ws.send.call_args[0][0]
    payload = json.loads(sent_arg)
    assert payload == {
        "operation": "subscribe",
        "assets_ids": ["b", "c"],
        "initial_dump": True,
    }, f"payload schema mismatch; got {payload}"
    assert set(consumer.subscribed_assets) == {"a", "b", "c"}


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — add_subscriptions: empty list → noop
# ─────────────────────────────────────────────────────────────────────────────


async def test_add_subscriptions_empty_list_is_noop() -> None:
    """Empty asset_ids → True, no send, no mutation."""
    consumer = _make_consumer(initial_assets=["a"])
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock()
    consumer._current_ws = mock_ws

    initial_assets = set(consumer.subscribed_assets)

    result = await consumer.add_subscriptions([])

    assert result is True, "empty add_subscriptions must return True"
    assert mock_ws.send.call_count == 0, "empty add must not call send"
    assert set(consumer.subscribed_assets) == initial_assets, "no mutation on empty"


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — add_subscriptions: send failure → returns False, NO mutation
#          (Warning #12 deterministic spec)
# ─────────────────────────────────────────────────────────────────────────────


async def test_add_subscriptions_send_failure_returns_false_and_does_not_pollute_active_sets() -> None:
    """On send failure: return False; _l3_active_set unchanged; "b" not in subscribed_assets."""
    consumer = _make_consumer(initial_assets=["a"])
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock(side_effect=RuntimeError("connection closed"))
    consumer._current_ws = mock_ws
    pre_l3 = set(consumer._l3_active_set)

    result = await consumer.add_subscriptions(["b"])

    assert result is False, "send failure must return False"
    assert "b" not in consumer._l3_active_set, "_l3_active_set must NOT be polluted on send failure"
    assert "b" not in consumer.subscribed_assets, (
        "subscribed_assets must NOT contain failed token (Warning #12 deterministic spec)"
    )
    assert consumer._l3_active_set == pre_l3, (
        f"_l3_active_set must be unchanged; pre={pre_l3} post={consumer._l3_active_set}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — remove_subscriptions: live ws → sends unsubscribe
# ─────────────────────────────────────────────────────────────────────────────


async def test_remove_subscriptions_with_live_ws_sends_unsubscribe() -> None:
    """Live ws + "b" in _l3_active_set → unsubscribe payload + remove from set."""
    consumer = _make_consumer(initial_assets=["a", "c"])
    consumer._l3_active_set = {"b"}
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock()
    consumer._current_ws = mock_ws

    result = await consumer.remove_subscriptions(["b"])

    assert result is True
    assert mock_ws.send.call_count == 1
    sent_arg = mock_ws.send.call_args[0][0]
    payload = json.loads(sent_arg)
    assert payload == {"operation": "unsubscribe", "assets_ids": ["b"]}, (
        f"unsubscribe payload schema mismatch; got {payload}"
    )
    assert "b" not in consumer._l3_active_set
    assert "b" not in consumer.subscribed_assets


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — remove_subscriptions: no live ws → fallback mutation only
# ─────────────────────────────────────────────────────────────────────────────


async def test_remove_subscriptions_no_live_ws_mutates_pending_only() -> None:
    """No live ws → return False; remove from _l3_active_set (fallback)."""
    consumer = _make_consumer(initial_assets=["a"])
    consumer._l3_active_set = {"b"}
    assert consumer._current_ws is None

    result = await consumer.remove_subscriptions(["b"])

    assert result is False, "no live ws → return False"
    assert "b" not in consumer.subscribed_assets, "fallback must drop b from union"


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — add_subscriptions does NOT acquire a Lock (websockets 15+ contract)
# ─────────────────────────────────────────────────────────────────────────────


def test_add_subscriptions_concurrent_with_recv_loop_safe() -> None:
    """Lint: source of add_subscriptions does NOT acquire a Lock.

    websockets 15+ supports concurrent send + recv from different tasks; a Lock
    would be both unnecessary and a deadlock risk (recv loop already holds the
    library-internal recv lock).
    """
    from polyarb.daemon.ws_consumer import WsConsumer

    src = inspect.getsource(WsConsumer.add_subscriptions)
    assert "Lock" not in src, (
        "add_subscriptions must NOT acquire any Lock (asyncio.Lock / threading.Lock); "
        "websockets 15+ supports concurrent send + recv"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — _compute_active_assets returns union
# ─────────────────────────────────────────────────────────────────────────────


def test_compute_active_assets_returns_union_of_candidate_and_l3_sets() -> None:
    """_candidate_set ∪ _l3_active_set is exposed via subscribed_assets."""
    consumer = _make_consumer()
    consumer._candidate_set = {"a", "b"}
    consumer._l3_active_set = {"b", "c"}

    assert set(consumer.subscribed_assets) == {"a", "b", "c"}


# ─────────────────────────────────────────────────────────────────────────────
# Test 9 — Yes+No double token: 10-token payload (Warning #13, D-05)
# ─────────────────────────────────────────────────────────────────────────────


async def test_add_subscriptions_with_yes_no_double_token() -> None:
    """Plan 04 promoter expands 5 markets × Yes+No = 10 tokens. API must accept all 10."""
    consumer = _make_consumer()
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock()
    consumer._current_ws = mock_ws

    ten_tokens = [
        "yes1", "no1", "yes2", "no2", "yes3",
        "no3", "yes4", "no4", "yes5", "no5",
    ]
    result = await consumer.add_subscriptions(ten_tokens)

    assert result is True
    assert mock_ws.send.call_count == 1
    sent_arg = mock_ws.send.call_args[0][0]
    payload = json.loads(sent_arg)
    assert len(payload["assets_ids"]) == 10, (
        f"expected 10 tokens in payload; got {len(payload['assets_ids'])}"
    )
    # No de-dup / no truncation
    assert set(payload["assets_ids"]) == set(ten_tokens)
