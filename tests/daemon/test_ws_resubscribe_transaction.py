"""Production amendment: subscription control is one fenced transaction."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from polyarb.daemon import ws_consumer as ws_consumer_module
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog


def _consumer() -> tuple[WsConsumer, MagicMock]:
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        initial_assets=["a", "b"],
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._connection_generation = 7
    return consumer, ws


async def _wait_until(predicate, *, timeout: float = 0.2) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_quiet_refresh_requires_exact_pair_and_matching_mirror_evidence() -> None:
    consumer, ws = _consumer()

    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count >= 2)

    payloads = [json.loads(call.args[0]) for call in ws.send.await_args_list]
    assert payloads == [
        {"operation": "unsubscribe", "assets_ids": ["a", "b"]},
        {
            "operation": "subscribe",
            "assets_ids": ["a", "b"],
            "initial_dump": True,
        },
    ]
    assert task.done() is False

    observed_at = datetime(2026, 7, 23, tzinfo=UTC)
    consumer.record_book_evidence(
        asset_id="a",
        generation=6,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    consumer.record_book_evidence(
        asset_id="a",
        generation=7,
        book_levels_succeeded=False,
        observed_at=observed_at,
    )
    await asyncio.sleep(0)
    assert task.done() is False

    consumer.record_book_evidence(
        asset_id="a",
        generation=7,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    await asyncio.sleep(0)
    assert task.done() is False
    consumer.record_book_evidence(
        asset_id="b",
        generation=7,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    assert await task is True


@pytest.mark.asyncio
async def test_quiet_refresh_evidence_timeout_closes_only_captured_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, old_ws = _consumer()
    new_ws = MagicMock()
    new_ws.close = AsyncMock(return_value=None)
    monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_TIMEOUT_S", 0.01, raising=False)

    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: old_ws.send.await_count >= 2)
    consumer._current_ws = new_ws
    consumer._connection_generation = 8

    assert await task is False
    old_ws.close.assert_awaited_once()
    new_ws.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_replacement_commits_only_after_serialized_control_pair() -> None:
    consumer, ws = _consumer()

    assert await consumer.replace_candidate_set(["b", "c"]) is True

    assert [json.loads(call.args[0]) for call in ws.send.await_args_list] == [
        {
            "operation": "subscribe",
            "assets_ids": ["c"],
            "initial_dump": True,
        },
        {"operation": "unsubscribe", "assets_ids": ["a"]},
    ]
    assert consumer._candidate_set == {"b", "c"}


@pytest.mark.asyncio
async def test_candidate_partial_send_failure_does_not_commit_and_compensates() -> None:
    consumer, ws = _consumer()
    ws.send.side_effect = [None, RuntimeError("ambiguous transport")]

    assert await consumer.replace_candidate_set(["b", "c"]) is False

    assert consumer._candidate_set == {"a", "b"}
    ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_candidate_replacement_without_socket_publishes_reconnect_desire() -> None:
    consumer, _ws = _consumer()
    consumer._current_ws = None

    assert await consumer.replace_candidate_set(["cold-start"]) is False
    assert consumer._candidate_set == {"cold-start"}
    assert consumer._compute_active_assets() == ["cold-start"]


@pytest.mark.asyncio
async def test_control_lock_prevents_candidate_pair_interleaving_with_refresh() -> None:
    consumer, ws = _consumer()
    first_send_entered = asyncio.Event()
    release_first_send = asyncio.Event()

    async def _send(payload: str) -> None:
        if ws.send.await_count == 1:
            first_send_entered.set()
            await release_first_send.wait()

    ws.send.side_effect = _send
    quiet = asyncio.create_task(consumer.request_book_refresh())
    await asyncio.wait_for(first_send_entered.wait(), timeout=0.2)
    candidate = asyncio.create_task(consumer.replace_candidate_set(["c"]))
    await asyncio.sleep(0)
    assert ws.send.await_count == 1

    release_first_send.set()
    await _wait_until(lambda: ws.send.await_count >= 2)
    consumer.record_book_evidence(
        asset_id="a",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=datetime(2026, 7, 23, tzinfo=UTC),
    )
    consumer.record_book_evidence(
        asset_id="b",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=datetime(2026, 7, 23, tzinfo=UTC),
    )
    assert await asyncio.wait_for(quiet, timeout=0.2) is True
    assert await asyncio.wait_for(candidate, timeout=0.2) is True

    operations = [json.loads(call.args[0])["operation"] for call in ws.send.await_args_list]
    assert operations == ["unsubscribe", "subscribe", "subscribe", "unsubscribe"]


@pytest.mark.asyncio
async def test_refresh_cancellation_compensates_then_propagates() -> None:
    consumer, ws = _consumer()
    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count >= 2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)
    ws.close.assert_awaited_once()
    assert len(consumer._watchdog._reconnect_timestamps) == 1
    assert consumer._book_evidence_waiters == {}


@pytest.mark.asyncio
async def test_compensation_is_reserved_once_per_generation() -> None:
    consumer, ws = _consumer()

    await asyncio.wait_for(consumer._compensate_generation(ws, 7), timeout=0.2)
    await asyncio.wait_for(consumer._compensate_generation(ws, 7), timeout=0.2)

    ws.close.assert_awaited_once()
    assert len(consumer._watchdog._reconnect_timestamps) == 1


@pytest.mark.asyncio
async def test_public_compensation_closes_only_current_snapshot() -> None:
    consumer, ws = _consumer()

    await consumer.compensate_current_generation(reason_code="promote_append_failed")

    ws.close.assert_awaited_once()
    assert consumer._current_ws is None
    assert consumer._connection_generation == 7


@pytest.mark.asyncio
async def test_initial_subscription_failure_closes_candidate_before_publication() -> None:
    consumer, old_ws = _consumer()
    candidate = MagicMock()
    candidate.send = AsyncMock(side_effect=RuntimeError("initial send failed"))
    candidate.close = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="initial WS subscription failed"):
        await asyncio.wait_for(consumer._initialize_connection(candidate), timeout=0.2)

    assert consumer._current_ws is old_ws
    candidate.close.assert_awaited_once()
    old_ws.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_control_cancellation_compensates_without_commit() -> None:
    consumer, ws = _consumer()
    entered = asyncio.Event()
    blocked = asyncio.Event()

    async def _blocked_send(_payload: str) -> None:
        entered.set()
        await blocked.wait()

    ws.send.side_effect = _blocked_send
    task = asyncio.create_task(consumer.replace_candidate_set(["c"]))
    await asyncio.wait_for(entered.wait(), timeout=0.2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)

    assert consumer._candidate_set == {"a", "b"}
    ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_initializer_cancellation_compensates_then_propagates() -> None:
    consumer, _old_ws = _consumer()
    candidate = MagicMock()
    entered = asyncio.Event()
    blocked = asyncio.Event()

    async def _blocked_send(_payload: str) -> None:
        entered.set()
        await blocked.wait()

    candidate.send = AsyncMock(side_effect=_blocked_send)
    candidate.close = AsyncMock(return_value=None)
    task = asyncio.create_task(consumer._initialize_connection(candidate))
    await asyncio.wait_for(entered.wait(), timeout=0.2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)
    candidate.close.assert_awaited_once()
    assert len(consumer._watchdog._reconnect_timestamps) == 1


@pytest.mark.asyncio
async def test_initial_subscription_desired_change_never_commits_unsent_tokens() -> None:
    """A desired mutation during the send makes the candidate generation ambiguous."""
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        initial_assets=["candidate"],
    )
    consumer.set_l3_desired(["sent-l3"])
    consumer._l3_committed_set = {"stale-l3"}
    consumer._l3_business_evidence = {"stale-l3": (0, datetime(2026, 7, 23, tzinfo=UTC))}
    candidate = MagicMock()
    send_entered = asyncio.Event()
    release_send = asyncio.Event()
    sent_payloads: list[dict[str, object]] = []

    async def _blocked_send(raw: str) -> None:
        sent_payloads.append(json.loads(raw))
        send_entered.set()
        await release_send.wait()

    candidate.send = AsyncMock(side_effect=_blocked_send)
    candidate.close = AsyncMock(return_value=None)
    task = asyncio.create_task(consumer._initialize_connection(candidate))
    await asyncio.wait_for(send_entered.wait(), timeout=0.2)

    consumer.set_l3_desired(["sent-l3", "unsent-l3"])
    release_send.set()

    with pytest.raises(RuntimeError, match="initial WS subscription failed"):
        await asyncio.wait_for(task, timeout=0.2)

    assert sent_payloads == [
        {
            "type": "market",
            "assets_ids": ["candidate", "sent-l3"],
            "initial_dump": True,
        }
    ]
    candidate.close.assert_awaited_once()
    status = consumer.l3_membership_snapshot()
    assert status.desired == frozenset({"sent-l3", "unsent-l3"})
    assert status.committed == frozenset()
    assert status.evidenced == frozenset()
    assert consumer._current_ws is None


@pytest.mark.asyncio
async def test_empty_add_is_true_no_send_no_publish_no_mutation() -> None:
    publications = []
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        initial_assets=["candidate"],
        membership_observer=publications.append,
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    consumer._current_ws = ws
    before = consumer.l3_membership_snapshot()
    publication_count = len(publications)

    assert await consumer.add_subscriptions([]) is True

    ws.send.assert_not_awaited()
    assert consumer.l3_membership_snapshot() == before
    assert len(publications) == publication_count


@pytest.mark.asyncio
async def test_empty_remove_is_true_no_send_no_publish_no_mutation() -> None:
    publications = []
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        initial_assets=["candidate"],
        membership_observer=publications.append,
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._l3_committed_set = {"kept"}
    before = consumer.l3_membership_snapshot()
    publication_count = len(publications)

    assert await consumer.remove_subscriptions([]) is True

    ws.send.assert_not_awaited()
    assert consumer.l3_membership_snapshot() == before
    assert len(publications) == publication_count


@pytest.mark.asyncio
async def test_compensated_generation_history_is_bounded() -> None:
    consumer, _ws = _consumer()

    async def _exercise() -> None:
        for generation in range(140):
            ws = MagicMock()
            ws.close = AsyncMock(return_value=None)
            await consumer._compensate_generation(ws, generation)

    await asyncio.wait_for(_exercise(), timeout=0.3)
    assert len(consumer._compensated_generations) <= 128
    assert len(consumer._compensated_generation_order) <= 128


@pytest.mark.asyncio
async def test_identity_change_after_first_unsubscribe_closes_only_captured_old() -> None:
    consumer, old_ws = _consumer()
    replacement = MagicMock()
    replacement.close = AsyncMock(return_value=None)

    async def _replace_after_unsubscribe(_payload: str) -> None:
        consumer._current_ws = replacement
        consumer._connection_generation = 8

    old_ws.send.side_effect = _replace_after_unsubscribe

    assert await asyncio.wait_for(consumer.request_book_refresh(), timeout=0.2) is False
    assert old_ws.send.await_count == 1
    old_ws.close.assert_awaited_once()
    replacement.close.assert_not_awaited()
    assert consumer._book_evidence_waiters == {}


@pytest.mark.asyncio
async def test_prepare_target_collects_evidence_without_publishing_membership() -> None:
    publications = []
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        initial_assets=["candidate"],
        membership_observer=publications.append,
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._connection_generation = 7
    consumer.set_l3_desired(["old-a", "old-b"])
    consumer._l3_committed_set = {"old-a", "old-b"}
    observed_at = datetime(2026, 7, 26, tzinfo=UTC)
    consumer._l3_business_evidence = {
        "old-a": (7, observed_at),
        "old-b": (7, observed_at),
    }
    before = consumer.l3_membership_snapshot()
    publication_count = len(publications)

    task = asyncio.create_task(
        consumer.prepare_l3_target(frozenset({"new-a", "new-b"}))
    )
    await _wait_until(lambda: ws.send.await_count >= 2)
    for asset_id in ("new-a", "new-b"):
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=7,
            book_levels_succeeded=True,
            observed_at=observed_at,
        )
    prepared = await asyncio.wait_for(task, timeout=0.2)

    assert prepared is not None
    assert prepared.generation == 7
    assert prepared.asset_ids == frozenset({"new-a", "new-b"})
    assert dict(prepared.evidenced_at) == {
        "new-a": observed_at,
        "new-b": observed_at,
    }
    assert consumer.l3_membership_snapshot() == before
    assert len(publications) == publication_count


@pytest.mark.asyncio
async def test_prepare_unchanged_exact_target_reuses_current_evidence_without_controls() -> None:
    consumer, ws = _consumer()
    target = frozenset({"old-a", "old-b"})
    observed_at = datetime(2026, 7, 26, tzinfo=UTC)
    consumer.set_l3_desired(target)
    consumer._l3_committed_set = set(target)
    consumer._l3_business_evidence = {
        asset_id: (consumer._connection_generation, observed_at) for asset_id in target
    }

    prepared = await consumer.prepare_l3_target(target)

    assert prepared is not None
    assert prepared.generation == consumer._connection_generation
    assert prepared.asset_ids == target
    assert dict(prepared.evidenced_at) == {
        "old-a": observed_at,
        "old-b": observed_at,
    }
    ws.send.assert_not_awaited()
    ws.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_commit_l3_target_publishes_one_exact_make_before_break_snapshot() -> None:
    publications = []
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        membership_observer=publications.append,
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._connection_generation = 7
    consumer._l3_desired_set = {"old-a", "old-b"}
    consumer._l3_committed_set = {"old-a", "old-b"}
    observed_at = datetime(2026, 7, 26, tzinfo=UTC)

    prepare = asyncio.create_task(
        consumer.prepare_l3_target(frozenset({"old-b", "new-a"}))
    )
    await _wait_until(lambda: ws.send.await_count >= 2)
    for asset_id in ("old-b", "new-a"):
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=7,
            book_levels_succeeded=True,
            observed_at=observed_at,
        )
    prepared = await asyncio.wait_for(prepare, timeout=0.2)
    assert prepared is not None
    ws.send.reset_mock()
    publication_count = len(publications)

    assert await consumer.commit_l3_target(prepared) is True

    assert [json.loads(call.args[0]) for call in ws.send.await_args_list] == [
        {
            "operation": "subscribe",
            "assets_ids": ["new-a"],
            "initial_dump": True,
        },
        {"operation": "unsubscribe", "assets_ids": ["old-a"]},
    ]
    assert len(publications) == publication_count + 1
    committed = publications[-1]
    assert committed.desired == frozenset({"old-b", "new-a"})
    assert committed.committed == committed.desired
    assert committed.evidenced == committed.desired


@pytest.mark.asyncio
async def test_commit_l3_target_rejects_stale_generation_without_publication() -> None:
    publications = []
    consumer, ws = _consumer()
    consumer._membership_observer = publications.append
    observed_at = datetime(2026, 7, 26, tzinfo=UTC)
    prepare = asyncio.create_task(consumer.prepare_l3_target(frozenset({"new-a"})))
    await _wait_until(lambda: ws.send.await_count >= 2)
    consumer.record_book_evidence(
        asset_id="new-a",
        generation=7,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    prepared = await asyncio.wait_for(prepare, timeout=0.2)
    assert prepared is not None
    consumer._connection_generation = 8
    ws.send.reset_mock()

    assert await consumer.commit_l3_target(prepared) is False
    ws.send.assert_not_awaited()
    assert publications == []


@pytest.mark.asyncio
async def test_commit_l3_target_second_control_failure_compensates_without_publish() -> None:
    publications = []
    consumer, ws = _consumer()
    consumer._membership_observer = publications.append
    consumer._l3_desired_set = {"old-a"}
    consumer._l3_committed_set = {"old-a"}
    observed_at = datetime(2026, 7, 26, tzinfo=UTC)
    prepare = asyncio.create_task(consumer.prepare_l3_target(frozenset({"new-a"})))
    await _wait_until(lambda: ws.send.await_count >= 2)
    consumer.record_book_evidence(
        asset_id="new-a",
        generation=7,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    prepared = await asyncio.wait_for(prepare, timeout=0.2)
    assert prepared is not None
    ws.send.reset_mock()
    ws.send.side_effect = [None, RuntimeError("ambiguous")]

    assert await consumer.commit_l3_target(prepared) is False
    ws.close.assert_awaited_once()
    assert publications
    assert all(
        snapshot.desired != frozenset({"new-a"})
        or snapshot.committed != frozenset({"new-a"})
        for snapshot in publications
    )
