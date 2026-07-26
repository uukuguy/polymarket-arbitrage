"""Truthful desired/committed/evidenced WebSocket membership contracts."""

from __future__ import annotations

import inspect
import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

# Allow tests to use POLYARB_ALLOW_EXTERNAL_PATHS for fixtures that touch
# tmp_path (matches conftest.py convention).
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def _make_consumer(
    initial_assets: list[str] | None = None,
    *,
    membership_observer=None,
):
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    return WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30.0),
        on_event=lambda ev: None,
        initial_assets=initial_assets,
        membership_observer=membership_observer,
        event_recorder=lambda *args, **kwargs: None,
    )


def _live_ws() -> MagicMock:
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    return ws


def test_setting_desired_does_not_imply_committed() -> None:
    consumer = _make_consumer(initial_assets=["candidate"])

    consumer.set_l3_desired(["yes", "no"])

    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.desired == frozenset({"yes", "no"})
    assert snapshot.committed == frozenset()
    assert snapshot.evidenced == frozenset()
    assert consumer.subscribed_assets == ["candidate", "no", "yes"]


async def test_offline_add_and_remove_return_false_without_changing_committed() -> None:
    consumer = _make_consumer(initial_assets=["candidate"])
    consumer.set_l3_desired(["a", "b"])
    before = consumer.l3_membership_snapshot().committed

    assert await consumer.add_subscriptions(["a", "b"]) is False
    assert consumer.l3_membership_snapshot().committed == before
    assert await consumer.remove_subscriptions(["a"]) is False
    assert consumer.l3_membership_snapshot().committed == before
    assert consumer.l3_membership_snapshot().desired == frozenset({"a", "b"})


async def test_failed_control_resolution_still_publishes_truthful_snapshot() -> None:
    snapshots = []
    consumer = _make_consumer(membership_observer=snapshots.append)
    consumer.set_l3_desired(["token"])
    ws = _live_ws()
    consumer._current_ws = ws
    consumer._connection_generation = 3
    before = len(snapshots)
    consumer._send_control = AsyncMock(return_value=False)

    assert await consumer.add_subscriptions(["token"]) is False

    assert len(snapshots) >= before + 2  # failed resolution, then compensation
    assert all(snapshot.committed == frozenset() for snapshot in snapshots[before:])
    assert snapshots[-1].generation == 3


async def test_live_add_commits_only_after_current_generation_send() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["a", "b"])
    ws = _live_ws()
    consumer._current_ws = ws
    consumer._connection_generation = 4

    assert await consumer.add_subscriptions(["a", "b"]) is True

    assert json.loads(ws.send.await_args.args[0]) == {
        "operation": "subscribe",
        "assets_ids": ["a", "b"],
        "initial_dump": True,
    }
    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.generation == 4
    assert snapshot.desired == frozenset({"a", "b"})
    assert snapshot.committed == frozenset({"a", "b"})
    assert snapshot.evidenced == frozenset()


@pytest.mark.parametrize("failure", ["false", "exception"])
async def test_failed_add_never_commits(failure: str) -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["a"])
    ws = _live_ws()
    if failure == "false":
        consumer._send_control = AsyncMock(return_value=False)
    else:
        ws.send.side_effect = RuntimeError("connection closed")
    consumer._current_ws = ws
    consumer._connection_generation = 2

    assert await consumer.add_subscriptions(["a"]) is False
    assert consumer.l3_membership_snapshot().committed == frozenset()
    ws.close.assert_awaited_once()


async def test_connection_identity_change_during_send_cannot_commit() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["latest"])
    old_ws = _live_ws()
    replacement = _live_ws()

    async def _replace_connection(_raw: str) -> None:
        consumer._current_ws = replacement
        consumer._connection_generation = 8

    old_ws.send.side_effect = _replace_connection
    consumer._current_ws = old_ws
    consumer._connection_generation = 7

    assert await consumer.add_subscriptions(["latest"]) is False
    assert consumer.l3_membership_snapshot().committed == frozenset()
    old_ws.close.assert_awaited_once()
    replacement.close.assert_not_awaited()


async def test_reconnect_commits_latest_desired_only_after_initial_subscribe() -> None:
    snapshots = []
    consumer = _make_consumer(initial_assets=["candidate"], membership_observer=snapshots.append)
    consumer.set_l3_desired(["yes", "no"])
    ws = _live_ws()

    await consumer._initialize_connection(ws)

    assert json.loads(ws.send.await_args.args[0]) == {
        "type": "market",
        "assets_ids": ["candidate", "no", "yes"],
        "initial_dump": True,
    }
    assert snapshots[-2].generation == 1
    assert snapshots[-2].committed == frozenset()
    assert snapshots[-1].committed == frozenset({"yes", "no"})
    assert consumer._current_ws is ws


async def test_active_connection_truth_tracks_successful_initialize_and_release() -> None:
    consumer = _make_consumer(initial_assets=["candidate"])
    ws = _live_ws()

    assert consumer.has_active_connection is False

    await consumer._initialize_connection(ws)
    assert consumer.has_active_connection is True

    await consumer._release_connection(ws)
    assert consumer.has_active_connection is False


async def test_release_clears_quiet_refresh_retry_state() -> None:
    consumer = _make_consumer(initial_assets=["candidate"])
    ws = _live_ws()
    await consumer._initialize_connection(ws)
    consumer._connection_initialized_at_s = 123.0
    consumer._last_quiet_refresh_missing_assets = frozenset({"candidate"})
    consumer._last_quiet_refresh_missing_generation = consumer._connection_generation

    await consumer._release_connection(ws)

    assert consumer._connection_initialized_at_s is None
    assert consumer.last_quiet_refresh_missing_assets == frozenset()
    assert consumer._last_quiet_refresh_missing_generation is None


async def test_failed_reconnect_does_not_commit_desired() -> None:
    consumer = _make_consumer(initial_assets=["candidate"])
    consumer.set_l3_desired(["yes", "no"])
    ws = _live_ws()
    ws.send.side_effect = RuntimeError("initial subscribe failed")

    with pytest.raises(RuntimeError, match="initial WS subscription failed"):
        await consumer._initialize_connection(ws)

    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.generation == 1
    assert snapshot.desired == frozenset({"yes", "no"})
    assert snapshot.committed == frozenset()
    ws.close.assert_awaited_once()


async def test_disconnect_clears_committed_and_evidenced_but_retains_desired() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["token"])
    ws = _live_ws()
    await consumer._initialize_connection(ws)
    observed_at = datetime(2026, 7, 23, tzinfo=UTC)
    consumer.record_book_evidence(
        asset_id="token",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    assert consumer.l3_membership_snapshot().evidenced == frozenset({"token"})

    await consumer._release_connection(ws)

    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.desired == frozenset({"token"})
    assert snapshot.committed == frozenset()
    assert snapshot.evidenced == frozenset()
    assert snapshot.evidenced_at == {}


async def test_old_generation_and_failed_book_writes_never_become_evidence() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["token"])
    ws = _live_ws()
    await consumer._initialize_connection(ws)
    observed_at = datetime(2026, 7, 23, tzinfo=UTC)

    consumer.record_book_evidence(
        asset_id="token",
        generation=consumer._connection_generation - 1,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    consumer.record_book_evidence(
        asset_id="token",
        generation=consumer._connection_generation,
        book_levels_succeeded=False,
        observed_at=observed_at,
    )
    assert consumer.l3_membership_snapshot().evidenced == frozenset()

    consumer.record_book_evidence(
        asset_id="token",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=observed_at,
    )
    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.evidenced == frozenset({"token"})
    assert snapshot.evidenced_at == {"token": observed_at}


async def test_new_generation_invalidates_previous_business_evidence() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["token"])
    first_ws = _live_ws()
    await consumer._initialize_connection(first_ws)
    consumer.record_book_evidence(
        asset_id="token",
        generation=1,
        book_levels_succeeded=True,
        observed_at=datetime(2026, 7, 23, tzinfo=UTC),
    )

    second_ws = _live_ws()
    await consumer._initialize_connection(second_ws)

    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.generation == 2
    assert snapshot.committed == frozenset({"token"})
    assert snapshot.evidenced == frozenset()


async def test_live_remove_changes_committed_without_rewriting_desired() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["a", "b"])
    ws = _live_ws()
    await consumer._initialize_connection(ws)
    ws.send.reset_mock()
    consumer.set_l3_desired(["b"])

    assert await consumer.remove_subscriptions(["a"]) is True

    assert json.loads(ws.send.await_args.args[0]) == {
        "operation": "unsubscribe",
        "assets_ids": ["a"],
    }
    snapshot = consumer.l3_membership_snapshot()
    assert snapshot.desired == frozenset({"b"})
    assert snapshot.committed == frozenset({"b"})


def test_membership_snapshots_are_immutable_defensive_copies() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["a"])
    first = consumer.l3_membership_snapshot()

    with pytest.raises(AttributeError):
        first.desired.add("forged")  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        first.evidenced_at["forged"] = datetime.now(UTC)  # type: ignore[index]

    consumer.set_l3_desired(["b"])
    assert first.desired == frozenset({"a"})
    assert consumer.l3_membership_snapshot().desired == frozenset({"b"})


def test_compute_active_assets_uses_candidate_and_desired_not_committed() -> None:
    consumer = _make_consumer()
    consumer._candidate_set = {"a", "b"}
    consumer.set_l3_desired(["b", "c"])

    assert consumer._compute_active_assets() == ["a", "b", "c"]
    assert consumer.l3_membership_snapshot().committed == frozenset()


def test_add_subscriptions_keeps_control_serialization_out_of_call_site() -> None:
    """The helper owns serialization; callers do not create a second lock."""
    from polyarb.daemon.ws_consumer import WsConsumer

    assert "Lock" not in inspect.getsource(WsConsumer.add_subscriptions)


async def test_add_subscriptions_accepts_ten_yes_no_tokens() -> None:
    consumer = _make_consumer()
    ten_tokens = [
        "yes1",
        "no1",
        "yes2",
        "no2",
        "yes3",
        "no3",
        "yes4",
        "no4",
        "yes5",
        "no5",
    ]
    consumer.set_l3_desired(ten_tokens)
    ws = _live_ws()
    consumer._current_ws = ws
    consumer._connection_generation = 1

    assert await consumer.add_subscriptions(ten_tokens) is True
    assert consumer.l3_membership_snapshot().committed == frozenset(ten_tokens)
    assert set(json.loads(ws.send.await_args.args[0])["assets_ids"]) == set(ten_tokens)


async def test_stale_timestamp_cannot_overwrite_newer_evidence() -> None:
    consumer = _make_consumer()
    consumer.set_l3_desired(["token"])
    ws = _live_ws()
    await consumer._initialize_connection(ws)
    newer = datetime(2026, 7, 23, 2, tzinfo=UTC)
    older = newer - timedelta(minutes=1)

    consumer.record_book_evidence(
        asset_id="token",
        generation=1,
        book_levels_succeeded=True,
        observed_at=newer,
    )
    consumer.record_book_evidence(
        asset_id="token",
        generation=1,
        book_levels_succeeded=True,
        observed_at=older,
    )

    assert consumer.l3_membership_snapshot().evidenced_at == {"token": newer}
