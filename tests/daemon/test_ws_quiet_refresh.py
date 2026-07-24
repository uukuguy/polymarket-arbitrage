"""Truthful quiet-market book refresh contracts for ``WsConsumer``."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from loguru import logger

from polyarb.daemon import ws_consumer as ws_consumer_module
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog

BASE_S = 1_000.0


def test_full_l3_dump_barrier_fits_inside_one_sampling_slot() -> None:
    assert 20.0 <= ws_consumer_module._BOOK_EVIDENCE_TIMEOUT_S < 30.0


async def _wait_until(predicate, *, timeout: float = 0.2) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


def _make_consumer() -> tuple[WsConsumer, WsWatchdog, MagicMock]:
    """Build real consumer/watchdog state and mock only the live transport."""
    watchdog = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=watchdog,
        on_event=lambda event: None,
        initial_assets=None,
    )
    consumer._candidate_set = {"candidate-b", "candidate-a"}
    consumer.set_l3_desired(["candidate-b", "l3-c"])
    consumer._l3_committed_set = {"candidate-b", "l3-c"}
    consumer._last_event_at_s = BASE_S
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws

    async def _send_with_evidence(payload: str) -> None:
        if json.loads(payload).get("operation") == "subscribe":
            for asset_id in ("candidate-b", "l3-c"):
                consumer.record_book_evidence(
                    asset_id=asset_id,
                    generation=consumer._connection_generation,
                    book_levels_succeeded=True,
                    observed_at=datetime(2026, 7, 23, tzinfo=UTC),
                )

    ws.send.side_effect = _send_with_evidence
    return consumer, watchdog, ws


@pytest.mark.asyncio
async def test_request_book_refresh_sends_sorted_union_without_mutating_truth() -> None:
    consumer, watchdog, ws = _make_consumer()
    candidates_before = set(consumer._candidate_set)
    l3_before = consumer.l3_membership_snapshot().desired
    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s

    result = await consumer.request_book_refresh()

    assert result is True
    assert [json.loads(call.args[0]) for call in ws.send.await_args_list] == [
        {"operation": "unsubscribe", "assets_ids": ["candidate-a", "candidate-b", "l3-c"]},
        {
            "operation": "subscribe",
            "assets_ids": ["candidate-a", "candidate-b", "l3-c"],
            "initial_dump": True,
        },
    ]
    assert consumer._candidate_set == candidates_before
    assert consumer.l3_membership_snapshot().desired == l3_before
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before


@pytest.mark.asyncio
async def test_refresh_waits_for_every_required_l3_token_in_same_generation() -> None:
    consumer, _watchdog, ws = _make_consumer()
    required = frozenset(f"l3-{index}" for index in range(10))
    consumer.set_l3_desired(required)
    consumer._l3_committed_set = set(required)
    ws.send.side_effect = None

    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count == 2)

    observed_at = datetime(2026, 7, 23, tzinfo=UTC)
    for asset_id in sorted(required)[:9]:
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=consumer._connection_generation,
            book_levels_succeeded=True,
            observed_at=observed_at,
        )
    await asyncio.sleep(0.01)
    assert task.done() is False

    consumer.record_book_evidence(
        asset_id=sorted(required)[9],
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    assert await asyncio.wait_for(task, timeout=0.2) is True


@pytest.mark.asyncio
async def test_refresh_rejects_failed_depth_and_old_generation_evidence() -> None:
    consumer, _watchdog, ws = _make_consumer()
    consumer.set_l3_desired(["l3-required"])
    consumer._l3_committed_set = {"l3-required"}
    ws.send.side_effect = None

    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count == 2)
    observed_at = datetime(2026, 7, 23, tzinfo=UTC)
    consumer.record_book_evidence(
        asset_id="l3-required",
        generation=consumer._connection_generation - 1,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    consumer.record_book_evidence(
        asset_id="l3-required",
        generation=consumer._connection_generation,
        book_levels_succeeded=False,
        observed_at=observed_at,
    )
    await asyncio.sleep(0)
    assert task.done() is False

    consumer.record_book_evidence(
        asset_id="l3-required",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    assert await asyncio.wait_for(task, timeout=0.2) is True


@pytest.mark.asyncio
async def test_explicit_required_set_sends_union_but_waits_only_for_required() -> None:
    consumer, _watchdog, ws = _make_consumer()
    ws.send.side_effect = None

    task = asyncio.create_task(
        consumer.request_book_refresh(required_asset_ids=frozenset({"l3-c"}))
    )
    await _wait_until(lambda: ws.send.await_count == 2)
    payloads = [json.loads(call.args[0]) for call in ws.send.await_args_list]
    assert payloads[0]["assets_ids"] == ["candidate-a", "candidate-b", "l3-c"]

    consumer.record_book_evidence(
        asset_id="l3-c",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=datetime(2026, 7, 23, tzinfo=UTC),
    )
    assert await asyncio.wait_for(task, timeout=0.2) is True


@pytest.mark.asyncio
async def test_no_l3_desired_falls_back_to_every_active_candidate() -> None:
    consumer, _watchdog, ws = _make_consumer()
    consumer.set_l3_desired([])
    consumer._l3_committed_set.clear()
    consumer._candidate_set = {"candidate-a", "candidate-b"}
    ws.send.side_effect = None

    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count == 2)
    observed_at = datetime(2026, 7, 23, tzinfo=UTC)
    consumer.record_book_evidence(
        asset_id="candidate-a",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    await asyncio.sleep(0)
    assert task.done() is False
    consumer.record_book_evidence(
        asset_id="candidate-b",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    assert await asyncio.wait_for(task, timeout=0.2) is True


@pytest.mark.asyncio
async def test_evidence_timeout_keeps_missing_identities_in_state_not_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, _watchdog, ws = _make_consumer()
    consumer.set_l3_desired(["l3-a", "l3-b"])
    consumer._l3_committed_set = {"l3-a", "l3-b"}
    ws.send.side_effect = None
    monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_TIMEOUT_S", 0.01)
    messages: list[str] = []
    sink_id = logger.add(lambda message: messages.append(str(message)), level="WARNING")
    try:
        task = asyncio.create_task(consumer.request_book_refresh())
        await _wait_until(lambda: ws.send.await_count == 2)
        consumer.record_book_evidence(
            asset_id="l3-a",
            generation=consumer._connection_generation,
            book_levels_succeeded=True,
            observed_at=datetime(2026, 7, 23, tzinfo=UTC),
        )
        assert await asyncio.wait_for(task, timeout=0.2) is False
    finally:
        logger.remove(sink_id)

    ws.close.assert_awaited_once()
    assert consumer._book_evidence_waiters == {}
    assert consumer.last_quiet_refresh_missing_assets == frozenset({"l3-b"})
    warning = next(message for message in messages if "ws quiet refresh failed" in message)
    assert "reason=evidence_timeout" in warning
    assert "error_type=TimeoutError" in warning
    assert "generation=0" in warning
    assert "total_count=4" in warning
    assert "required_count=2" in warning
    assert "missing_count=1" in warning
    assert "l3-a" not in warning
    assert "l3-b" not in warning


@pytest.mark.asyncio
async def test_refresh_if_quiet_obeys_quiet_boundary_and_retry_cooldown() -> None:
    consumer, _watchdog, ws = _make_consumer()
    observed_at = datetime.fromtimestamp(BASE_S, tz=UTC)
    for asset_id in ("candidate-b", "l3-c"):
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=consumer._connection_generation,
            book_levels_succeeded=True,
            observed_at=observed_at,
        )

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 59) is None
    assert consumer._last_quiet_refresh_attempt_at_s == 0.0

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 60) is True
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert ws.send.await_count == 2

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 89) is None
    assert ws.send.await_count == 2

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 90) is None
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert ws.send.await_count == 2


@pytest.mark.asyncio
async def test_new_business_frame_suppresses_refresh_after_cooldown() -> None:
    consumer, _watchdog, ws = _make_consumer()

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 60) is True
    consumer._last_event_at_s = BASE_S + 85

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 100) is None
    assert ws.send.await_count == 2


@pytest.mark.asyncio
async def test_unrelated_candidate_frame_does_not_mask_stale_l3_evidence() -> None:
    consumer, _watchdog, ws = _make_consumer()
    stale_at = datetime.fromtimestamp(BASE_S, tz=UTC)
    for asset_id in ("candidate-b", "l3-c"):
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=consumer._connection_generation,
            book_levels_succeeded=True,
            observed_at=stale_at,
        )
    consumer._last_event_at_s = BASE_S + 85

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 100) is True
    assert ws.send.await_count == 2


@pytest.mark.asyncio
async def test_fresh_l3_evidence_suppresses_refresh_when_global_stream_is_quiet() -> None:
    consumer, _watchdog, ws = _make_consumer()
    fresh_at = datetime.fromtimestamp(BASE_S + 85, tz=UTC)
    for asset_id in ("candidate-b", "l3-c"):
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=consumer._connection_generation,
            book_levels_succeeded=True,
            observed_at=fresh_at,
        )
    consumer._last_event_at_s = BASE_S

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 100) is None
    assert ws.send.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["empty", "no-ws", "closed"])
async def test_due_refresh_failures_do_not_forge_freshness(failure: str) -> None:
    consumer, watchdog, ws = _make_consumer()
    if failure == "empty":
        consumer._candidate_set.clear()
        consumer.set_l3_desired([])
        consumer._l3_committed_set.clear()
    elif failure == "no-ws":
        consumer._current_ws = None
    else:
        ws.send = AsyncMock(side_effect=RuntimeError("closed"))

    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s

    result = await consumer.refresh_if_quiet(now_s=BASE_S + 60)

    assert result is False
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before

    # A failed attempt still owns the retry cooldown. In particular, the
    # closed transport must not be hammered once per scheduler tick.
    assert await consumer.refresh_if_quiet(now_s=BASE_S + 89) is None
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert ws.send.await_count == (1 if failure == "closed" else 0)
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before

    # The exact cooldown boundary permits another attempt. Compensation has
    # already unpublished the failed socket, so no path retries that same
    # ambiguous generation.
    assert await consumer.refresh_if_quiet(now_s=BASE_S + 90) is False
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 90
    assert ws.send.await_count == (1 if failure == "closed" else 0)
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before


@pytest.mark.asyncio
async def test_run_quiet_refresh_sends_once_then_stops_cleanly() -> None:
    consumer, watchdog, ws = _make_consumer()
    stop_event = asyncio.Event()
    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s

    async def _send_then_stop(_payload: str) -> None:
        if json.loads(_payload).get("operation") == "subscribe":
            for asset_id in ("candidate-b", "l3-c"):
                consumer.record_book_evidence(
                    asset_id=asset_id,
                    generation=consumer._connection_generation,
                    book_levels_succeeded=True,
                    observed_at=datetime(2026, 7, 23, tzinfo=UTC),
                )
            stop_event.set()

    ws.send.side_effect = _send_then_stop

    await asyncio.wait_for(
        consumer.run_quiet_refresh(
            stop_event,
            quiet_after_s=0,
            retry_s=30,
            check_interval_s=0.01,
        ),
        timeout=0.2,
    )

    assert ws.send.await_count == 2
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before


@pytest.mark.asyncio
async def test_run_quiet_refresh_propagates_cancellation() -> None:
    consumer, _watchdog, _ws = _make_consumer()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        consumer.run_quiet_refresh(
            stop_event,
            quiet_after_s=10_000,
            retry_s=30,
            check_interval_s=30,
        )
    )
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_request_book_refresh_bounds_hung_send_without_forging_freshness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, watchdog, ws = _make_consumer()
    never_returns = asyncio.Event()
    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s
    messages: list[str] = []

    async def _hung_send(_payload: str) -> None:
        await never_returns.wait()

    ws.send.side_effect = _hung_send
    monkeypatch.setattr(ws_consumer_module, "_QUIET_REFRESH_SEND_TIMEOUT_S", 0.01, raising=False)
    sink_id = logger.add(lambda message: messages.append(str(message)), level="INFO")
    try:
        result = await asyncio.wait_for(consumer.request_book_refresh(), timeout=0.2)
    finally:
        logger.remove(sink_id)

    assert result is False
    assert any("ws quiet refresh: sending assets=3" in msg for msg in messages)
    warning = next(msg for msg in messages if "ws quiet refresh failed" in msg)
    assert "reason=unsubscribe_failed" in warning
    assert "error_type=RuntimeError" in warning
    assert "total_count=3" in warning
    assert "required_count=2" in warning
    assert "missing_count=2" in warning
    assert "candidate-a" not in warning
    assert "candidate-b" not in warning
    assert "l3-c" not in warning
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before
