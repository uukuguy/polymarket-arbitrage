"""Phase 05.1 durable L2 reconciliation pump contracts."""
from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


class FakeCursorStore:
    def __init__(self, *, cursor: int = 0, latest: int = 0) -> None:
        self.cursor = cursor
        self.latest = latest
        self.reads = 0
        self.commits: list[int] = []
        self.commit_error: Exception | None = None

    async def read_position(self) -> tuple[int, int]:
        self.reads += 1
        return self.cursor, self.latest

    async def commit(self, snapshot_id: int) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.cursor = snapshot_id
        self.commits.append(snapshot_id)


def _api():
    from polyarb.events.reconciliation import ReconciliationPump, ReconciliationState

    return ReconciliationPump, ReconciliationState


async def _eventually(predicate, *, timeout: float = 0.5) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_initial_wake_processes_only_latest_and_commits_after_success():
    ReconciliationPump, ReconciliationState = _api()
    store = FakeCursorStore(cursor=10, latest=15)
    calls: list[int] = []

    async def refresh(payload: dict) -> bool:
        calls.append(payload["snapshot_id"])
        assert store.commits == []
        return True

    state = ReconciliationState()
    pump = ReconciliationPump(store=store, refresh=refresh, state=state, poll_seconds=60)
    stop = asyncio.Event()
    task = asyncio.create_task(pump.run(stop))
    await _eventually(lambda: store.commits == [15])
    stop.set()
    await asyncio.wait_for(task, timeout=0.5)

    assert calls == [15]
    assert state.committed_cursor == 15
    assert state.cursor_lag == 0
    assert state.last_reconciliation_success_s is not None


@pytest.mark.asyncio
async def test_timer_catches_up_without_notify():
    ReconciliationPump, ReconciliationState = _api()
    store = FakeCursorStore(cursor=1, latest=1)
    refresh = AsyncMock(return_value=True)
    pump = ReconciliationPump(
        store=store,
        refresh=refresh,
        state=ReconciliationState(),
        poll_seconds=0.01,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(pump.run(stop))
    await _eventually(lambda: store.reads >= 1)
    store.latest = 2
    await _eventually(lambda: store.commits == [2])
    stop.set()
    await asyncio.wait_for(task, timeout=0.5)


@pytest.mark.asyncio
async def test_notifications_coalesce_and_refresh_never_overlaps():
    ReconciliationPump, ReconciliationState = _api()
    store = FakeCursorStore(cursor=0, latest=7)
    entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    max_active = 0
    calls = 0

    async def refresh(payload: dict) -> bool:  # noqa: ARG001
        nonlocal active, max_active, calls
        calls += 1
        active += 1
        max_active = max(max_active, active)
        entered.set()
        await release.wait()
        active -= 1
        return True

    pump = ReconciliationPump(
        store=store,
        refresh=refresh,
        state=ReconciliationState(),
        poll_seconds=60,
    )
    for _ in range(20):
        pump.notify({"snapshot_id": 7})
    stop = asyncio.Event()
    task = asyncio.create_task(pump.run(stop))
    await asyncio.wait_for(entered.wait(), timeout=0.5)
    store.latest = 8
    for _ in range(20):
        pump.notify({"snapshot_id": 8})
    release.set()
    await _eventually(lambda: store.cursor == 8)
    stop.set()
    await asyncio.wait_for(task, timeout=0.5)

    assert max_active == 1
    assert calls == 2
    assert store.commits == [7, 8]


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, RuntimeError("refresh failed")])
async def test_refresh_failure_retains_cursor(outcome):
    ReconciliationPump, ReconciliationState = _api()
    store = FakeCursorStore(cursor=3, latest=4)

    async def refresh(payload: dict) -> bool:  # noqa: ARG001
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    state = ReconciliationState()
    pump = ReconciliationPump(store=store, refresh=refresh, state=state, poll_seconds=60)
    await pump.reconcile_once()

    assert store.cursor == 3
    assert store.commits == []
    assert state.committed_cursor == 3
    assert state.cursor_lag == 1
    assert state.last_error


@pytest.mark.asyncio
async def test_cursor_commit_failure_retains_cursor():
    ReconciliationPump, ReconciliationState = _api()
    store = FakeCursorStore(cursor=5, latest=6)
    store.commit_error = RuntimeError("write failed")
    state = ReconciliationState()
    pump = ReconciliationPump(
        store=store,
        refresh=AsyncMock(return_value=True),
        state=state,
        poll_seconds=60,
    )
    await pump.reconcile_once()

    assert store.cursor == 5
    assert state.committed_cursor == 5
    assert state.cursor_lag == 1
    assert "write failed" in state.last_error


@pytest.mark.asyncio
async def test_asyncpg_cursor_commit_updates_freshness(monkeypatch):
    from polyarb.events import reconciliation

    conn = MagicMock()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    conn.close = AsyncMock()
    monkeypatch.setattr(
        reconciliation.asyncpg, "connect", AsyncMock(return_value=conn)
    )
    store = reconciliation.AsyncpgCursorStore(
        dsn="postgresql://test", consumer="l2-candidate-refresh"
    )

    await store.commit(42)

    sql, consumer, snapshot_id = conn.execute.await_args.args
    assert "updated_at=now()" in sql
    assert (consumer, snapshot_id) == ("l2-candidate-refresh", 42)
    conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_caught_up_poll_records_reconciliation_success_without_refresh():
    ReconciliationPump, ReconciliationState = _api()
    store = FakeCursorStore(cursor=9, latest=9)
    refresh = AsyncMock(return_value=True)
    state = ReconciliationState()
    pump = ReconciliationPump(store=store, refresh=refresh, state=state, poll_seconds=60)
    await pump.reconcile_once()

    refresh.assert_not_awaited()
    assert state.committed_cursor == 9
    assert state.latest_snapshot_id == 9
    assert state.cursor_lag == 0
    assert state.last_reconciliation_success_s is not None
    assert state.last_error is None


def test_notify_rejects_negative_id_but_still_wakes_reconciliation():
    ReconciliationPump, ReconciliationState = _api()
    state = ReconciliationState()
    pump = ReconciliationPump(
        store=FakeCursorStore(),
        refresh=AsyncMock(return_value=True),
        state=state,
        poll_seconds=60,
    )
    pump.notify({"snapshot_id": -1})

    assert pump.wake_event.is_set()
    assert state.last_notification_s is None


@pytest.mark.asyncio
async def test_shutdown_is_bounded_and_cancellation_propagates():
    ReconciliationPump, ReconciliationState = _api()
    pump = ReconciliationPump(
        store=FakeCursorStore(),
        refresh=AsyncMock(return_value=True),
        state=ReconciliationState(),
        poll_seconds=60,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(pump.run(stop))
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, timeout=0.5)

    task = asyncio.create_task(pump.run(asyncio.Event()))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
