"""End-to-end runtime-event durability and transition-chain tests."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from polyarb.daemon import l2_main, ws_watchdog
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.observation import l3_evidence, l3_sampler
from polyarb.observation.l3_evidence import (
    HealthStatus,
    L3EvidenceRuntime,
    PromoteStatus,
    RuntimeEventKind,
    RuntimeEventRecord,
    RuntimeIdentity,
    WsMembershipSnapshot,
)
from polyarb.storage import l3_evidence_store as store_module
from polyarb.storage.l3_evidence_store import SamplingMarketState

START = datetime(2026, 7, 23, 6, 0, tzinfo=UTC)


def _detail(kind: RuntimeEventKind, values: dict[str, object]):
    assert hasattr(l3_evidence, "build_runtime_event_detail"), (
        "runtime event detail whitelist is not implemented"
    )
    return l3_evidence.build_runtime_event_detail(kind, values)


def _runtime() -> L3EvidenceRuntime:
    return L3EvidenceRuntime(
        RuntimeIdentity(
            machine_id="machine",
            machine_version="version",
            image_ref="image",
            release_id="release",
            code_version="code",
            recipe_sha256="a" * 64,
            acceptance_config_hash="b" * 64,
        ),
        started_at=START,
    )


def _runtime_at(now: datetime) -> L3EvidenceRuntime:
    runtime = _runtime()
    return L3EvidenceRuntime(runtime.snapshot().identity, started_at=now - timedelta(minutes=5))


def _market_states(
    sampled_at: datetime,
    *,
    stale_markets: frozenset[int] = frozenset(),
) -> tuple[SamplingMarketState, ...]:
    return tuple(
        SamplingMarketState(
            market_id=f"market-{index}",
            yes_token_id=f"yes-{index}",
            no_token_id=f"no-{index}",
            yes_book_at=sampled_at - timedelta(
                seconds=121 if index in stale_markets else 5
            ),
            no_book_at=sampled_at - timedelta(
                seconds=122 if index in stale_markets else 6
            ),
            yes_ohlc_at=sampled_at - timedelta(
                seconds=123 if index in stale_markets else 7
            ),
        )
        for index in range(5)
    )


def _tokens(states: tuple[SamplingMarketState, ...]) -> frozenset[str]:
    return frozenset(
        token
        for state in states
        for token in (state.yes_token_id, state.no_token_id)
    )


def _publish_membership(
    runtime: L3EvidenceRuntime,
    states: tuple[SamplingMarketState, ...],
    *,
    at: datetime,
) -> None:
    tokens = _tokens(states)
    evidence_times = {
        token_id: book_at or at
        for state in states
        for token_id, book_at in (
            (state.yes_token_id, state.yes_book_at),
            (state.no_token_id, state.no_book_at),
        )
    }
    runtime.update_membership(
        WsMembershipSnapshot(
            generation=1,
            desired=tokens,
            committed=tokens,
            evidenced=tokens,
            evidenced_at=evidence_times,
        )
    )


def _sampler_settings() -> SimpleNamespace:
    return SimpleNamespace(
        l3_evidence_sample_interval_s=30,
        l3_market_book_fresh_s=120,
        l3_market_ohlc_fresh_s=120,
    )


def _reconciliation_state(now: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        is_connected=True,
        reconnect_count=0,
        cursor_lag=0,
        last_reconciliation_success_s=now.timestamp(),
    )


class _SampleStore:
    def __init__(
        self,
        states: tuple[SamplingMarketState, ...],
        *,
        append_results: list[bool] | None = None,
    ) -> None:
        self.states = states
        self.append_results = list(append_results or [True])
        self.batches = []

    async def fetch_sampling_market_state(self, _token_ids):
        return self.states

    async def append_sample(self, batch):
        self.batches.append(batch)
        return self.append_results.pop(0)


def _assert_strict_failure(runtime: L3EvidenceRuntime, failed_key: str, ws_consumer=None):
    from pydantic import SecretStr
    from starlette.testclient import TestClient

    from polyarb.config import Settings
    from polyarb.http.l2_app import create_l2_app

    app = create_l2_app(
        sqlite_store=SimpleNamespace(),
        settings=Settings(scan_shared_secret=SecretStr("test-secret")),
        ws_consumer=ws_consumer,
        evidence_runtime=runtime,
    )
    with TestClient(app) as client:
        strict = client.get("/health")
        fly_probe = client.get("/healthz")
    assert strict.status_code == 503
    assert fly_probe.status_code == 200
    assert strict.json()["checks"][failed_key][0]["status"] == "fail"
    return strict.json()


class _CommitThenClientErrorStore:
    def __init__(self) -> None:
        self.attempts = []
        self.durable = {}
        self._lost_first_ack = False

    async def append_event(self, event):
        self.attempts.append(event)
        existing = self.durable.get(event.event_id)
        if existing is not None:
            assert existing == event
            return True
        self.durable[event.event_id] = event
        if not self._lost_first_ack:
            self._lost_first_ack = True
            return False
        return True


async def test_event_writer_retries_original_event_then_persists_failure_and_recovery():
    runtime = _runtime()
    runtime.note_writer_result(True, START - timedelta(days=1), "ok")
    original_at = START + timedelta(seconds=1)
    runtime.record_event(
        RuntimeEventKind.WATCHDOG_STALE,
        occurred_at=original_at,
        reason_code="data_silence",
        detail=_detail(
            RuntimeEventKind.WATCHDOG_STALE,
            {"stale_seconds": 30},
        ),
    )
    runtime.record_event(
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        occurred_at=START + timedelta(seconds=2),
        reason_code="signal",
        detail=_detail(
            RuntimeEventKind.SHUTDOWN_SIGNAL,
            {"signal": "SIGTERM"},
        ),
    )
    store = _CommitThenClientErrorStore()
    stop_event = asyncio.Event()
    stop_event.set()

    await asyncio.wait_for(
        l3_sampler.run_event_writer(
            stop_event,
            runtime=runtime,
            store=store,
            flush_interval_s=0,
        ),
        timeout=5,
    )

    assert [event.event_seq for event in store.attempts] == [0, 0, 1, 2, 3]
    assert store.attempts[0] is store.attempts[1]
    assert store.attempts[1].occurred_at == original_at
    assert [event.kind for event in store.attempts[1:]] == [
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        RuntimeEventKind.EVIDENCE_WRITER_FAILED,
        RuntimeEventKind.EVIDENCE_WRITER_RECOVERED,
    ]
    assert [event.kind for event in store.durable.values()] == [
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        RuntimeEventKind.EVIDENCE_WRITER_FAILED,
        RuntimeEventKind.EVIDENCE_WRITER_RECOVERED,
    ]
    status = runtime.snapshot()
    assert status.pending_event_count == 0
    assert status.writer_ok is True
    assert status.last_promote_persisted_at is None
    assert status.last_sample_persisted_at is None


async def test_event_writer_late_event_failure_stays_visible_and_never_retimestamps():
    runtime = _runtime()
    occurred_at = datetime.now(UTC) - timedelta(hours=25)
    original = runtime.record_event(
        RuntimeEventKind.WATCHDOG_STALE,
        occurred_at=occurred_at,
        reason_code="late_event",
        detail=_detail(
            RuntimeEventKind.WATCHDOG_STALE,
            {"stale_seconds": 30},
        ),
    )
    attempted_twice = asyncio.Event()

    class _RejectLateStore:
        def __init__(self) -> None:
            self.attempts = []

        async def append_event(self, event):
            assert datetime.now(UTC) - event.occurred_at > timedelta(hours=24)
            self.attempts.append(event)
            if len(self.attempts) == 2:
                attempted_twice.set()
            return False

    store = _RejectLateStore()
    task = asyncio.create_task(
        l3_sampler.run_event_writer(
            asyncio.Event(),
            runtime=runtime,
            store=store,
            flush_interval_s=0.001,
        )
    )
    await asyncio.wait_for(attempted_twice.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.attempts[0] is original
    assert store.attempts[1] is original
    assert all(event.occurred_at == occurred_at for event in store.attempts)
    status = runtime.snapshot()
    assert status.writer_ok is False
    assert status.event_integrity_failed is False
    assert status.pending_event_count == 2
    assert status.last_promote_persisted_at is None
    assert status.last_sample_persisted_at is None


async def test_event_writer_quarantines_permanent_conflict_and_persists_tail():
    assert hasattr(store_module, "RuntimeEventIntegrityConflict"), (
        "store must distinguish permanent identity/payload conflicts from transient false"
    )
    runtime = _runtime()
    runtime.note_writer_result(True, START - timedelta(days=1), "ok")
    poison = runtime.record_event(
        RuntimeEventKind.WATCHDOG_STALE,
        occurred_at=START,
        reason_code="data_silence",
        detail={"stale_seconds": 30},
    )
    shutdown = runtime.record_event(
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        occurred_at=START + timedelta(seconds=1),
        reason_code="signal",
        detail={"signal": "SIGTERM"},
    )

    class _PermanentConflictThenPersistStore:
        def __init__(self) -> None:
            self.attempts = []
            self.durable = []

        async def append_event(self, event):
            self.attempts.append(event)
            if event.event_id == poison.event_id:
                raise store_module.RuntimeEventIntegrityConflict()
            self.durable.append(event)
            return True

    store = _PermanentConflictThenPersistStore()
    stop_event = asyncio.Event()
    stop_event.set()

    await asyncio.wait_for(
        l3_sampler.run_event_writer(
            stop_event,
            runtime=runtime,
            store=store,
            flush_interval_s=0,
        ),
        timeout=1,
    )

    assert [event.event_seq for event in store.attempts] == [0, 1, 2, 3]
    assert poison not in store.durable
    assert [event.kind for event in store.durable] == [
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        RuntimeEventKind.EVIDENCE_WRITER_FAILED,
        RuntimeEventKind.EVIDENCE_WRITER_RECOVERED,
    ]
    assert store.durable[0] is shutdown
    status = runtime.snapshot()
    assert status.pending_event_count == 0
    assert status.writer_ok is True
    assert status.event_integrity_failed is True
    assert status.event_integrity_reason_code == "event_replay_conflict"
    assert status.status is l3_evidence.HealthStatus.FAIL
    assert status.reason_code == "event_integrity_failed"


async def test_shutdown_waits_for_late_producer_cleanup_event_before_writer_exit():
    runtime = _runtime()
    runtime.record_event(
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        occurred_at=START,
        reason_code="signal",
        detail={"signal": "SIGTERM"},
    )
    durable = []

    class _Store:
        async def append_event(self, event):
            durable.append(event)
            return True

    stop_event = asyncio.Event()
    stop_event.set()
    producers_done = asyncio.Event()
    writer_task = asyncio.create_task(
        l3_sampler.run_event_writer(
            stop_event,
            runtime=runtime,
            store=_Store(),
            flush_interval_s=0,
            producers_done=producers_done,
        ),
        name="l3-event-writer",
    )

    async def _late_producer():
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            runtime.record_event(
                RuntimeEventKind.SUBSCRIPTION_COMPENSATED,
                occurred_at=START + timedelta(seconds=1),
                reason_code="shutdown_cleanup",
                detail={"operation": "connection_close", "close_succeeded": True},
            )

    async def _server():
        await asyncio.sleep(0)

    peer_task = asyncio.create_task(_late_producer(), name="ws-consumer")
    server_task = asyncio.create_task(_server(), name="l2-http-server")
    await asyncio.sleep(0)

    await l2_main._drain_daemon_tasks(
        server_task=server_task,
        event_writer_task=writer_task,
        peer_tasks=(peer_task,),
        normal_shutdown=True,
        producers_done=producers_done,
        timeout_s=0.2,
    )

    assert producers_done.is_set()
    assert writer_task.done()
    assert [event.kind for event in durable] == [
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        RuntimeEventKind.SUBSCRIPTION_COMPENSATED,
    ]
    assert runtime.snapshot().pending_event_count == 0


async def test_shutdown_never_reports_clean_when_late_event_writer_hits_deadline():
    runtime = _runtime()
    runtime.record_event(
        RuntimeEventKind.SHUTDOWN_SIGNAL,
        occurred_at=START,
        reason_code="signal",
        detail={"signal": "SIGTERM"},
    )

    class _Store:
        def __init__(self):
            self.calls = 0

        async def append_event(self, _event):
            self.calls += 1
            if self.calls == 1:
                return True
            await asyncio.Event().wait()

    stop_event = asyncio.Event()
    stop_event.set()
    producers_done = asyncio.Event()
    writer_task = asyncio.create_task(
        l3_sampler.run_event_writer(
            stop_event,
            runtime=runtime,
            store=_Store(),
            flush_interval_s=0,
            producers_done=producers_done,
        ),
        name="l3-event-writer",
    )

    async def _late_producer():
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.02)
            runtime.record_event(
                RuntimeEventKind.SUBSCRIPTION_COMPENSATED,
                occurred_at=START + timedelta(seconds=1),
                reason_code="shutdown_cleanup",
                detail={"operation": "connection_close", "close_succeeded": True},
            )

    async def _server():
        await asyncio.sleep(0)

    peer_task = asyncio.create_task(_late_producer(), name="ws-consumer")
    server_task = asyncio.create_task(_server(), name="l2-http-server")
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="event writer drain deadline"):
        await l2_main._drain_daemon_tasks(
            server_task=server_task,
            event_writer_task=writer_task,
            peer_tasks=(peer_task,),
            normal_shutdown=True,
            producers_done=producers_done,
            timeout_s=0.05,
        )

    assert writer_task.done()
    assert runtime.snapshot().pending_event_count == 1


async def test_cancellation_during_producer_wait_reaps_owned_tasks_and_propagates():
    stop_event = asyncio.Event()
    stop_event.set()
    producers_done = asyncio.Event()
    cleanup_started = asyncio.Event()

    async def _writer():
        await producers_done.wait()

    async def _producer():
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await asyncio.sleep(0)

    async def _server():
        await asyncio.Event().wait()

    writer_task = asyncio.create_task(_writer(), name="l3-event-writer")
    peer_task = asyncio.create_task(_producer(), name="ws-consumer")
    server_task = asyncio.create_task(_server(), name="l2-http-server")
    drain_task = asyncio.create_task(
        l2_main._drain_daemon_tasks(
            server_task=server_task,
            event_writer_task=writer_task,
            peer_tasks=(peer_task,),
            normal_shutdown=True,
            producers_done=producers_done,
            timeout_s=0.2,
        )
    )
    await asyncio.wait_for(cleanup_started.wait(), timeout=0.1)
    drain_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await drain_task

    assert server_task.done()
    assert writer_task.done()
    assert peer_task.done()


def test_event_queue_overflow_is_bounded_and_fail_closed():
    runtime = _runtime()
    detail = _detail(
        RuntimeEventKind.WATCHDOG_STALE,
        {"stale_seconds": 30},
    )
    for _ in range(128):
        runtime.record_event(
            RuntimeEventKind.WATCHDOG_STALE,
            occurred_at=START,
            reason_code="data_silence",
            detail=detail,
        )

    with pytest.raises(OverflowError, match="128"):
        runtime.record_event(
            RuntimeEventKind.WATCHDOG_STALE,
            occurred_at=START,
            reason_code="data_silence",
            detail=detail,
        )

    status = runtime.snapshot()
    assert status.pending_event_count == 128
    assert status.event_queue_overflowed is True
    assert status.status.value == "fail"


@pytest.mark.parametrize(
    "forbidden",
    ["raw_frame", "payload", "secret", "dsn", "asset_id", "token_ids"],
)
def test_runtime_event_detail_rejects_payload_secret_and_identity_keys(forbidden: str):
    with pytest.raises(ValueError, match="not allowed"):
        _detail(
            RuntimeEventKind.WATCHDOG_STALE,
            {"stale_seconds": 30, forbidden: "do-not-store"},
        )
    with pytest.raises(ValueError, match="not allowed"):
        _runtime().record_event(
            RuntimeEventKind.WATCHDOG_STALE,
            occurred_at=START,
            detail={"stale_seconds": 30, forbidden: "do-not-store"},
        )
    with pytest.raises(ValueError, match="not allowed"):
        RuntimeEventRecord(
            event_id=_runtime().snapshot().boot_id,
            boot_id=_runtime().snapshot().boot_id,
            event_seq=0,
            occurred_at=START,
            kind=RuntimeEventKind.WATCHDOG_STALE,
            detail={"stale_seconds": 30, forbidden: "do-not-store"},
        )


@pytest.mark.parametrize(
    ("kind", "detail"),
    [
        (RuntimeEventKind.RECONNECT_STARTED, {"source": "secret-source"}),
        (
            RuntimeEventKind.RECONNECT_FAILED,
            {"operation": "on_reconnect", "error_type": "CredentialSecret"},
        ),
        (RuntimeEventKind.SHUTDOWN_SIGNAL, {"signal": "TERM"}),
        (RuntimeEventKind.SOAK_MANIFEST_BOUND, {"manifest_sha256": "not-a-hash"}),
        (RuntimeEventKind.WATCHDOG_STALE, {"stale_seconds": True}),
    ],
)
def test_runtime_event_detail_rejects_unsafe_allowed_key_values(
    kind: RuntimeEventKind,
    detail: dict[str, object],
):
    with pytest.raises(ValueError, match="invalid"):
        RuntimeEventRecord(
            event_id=_runtime().snapshot().boot_id,
            boot_id=_runtime().snapshot().boot_id,
            event_seq=0,
            occurred_at=START,
            kind=kind,
            detail=detail,
        )


async def test_watchdog_records_only_real_stale_budget_transitions(
    monkeypatch: pytest.MonkeyPatch,
):
    runtime = _runtime()
    monkeypatch.setattr(ws_watchdog.time, "monotonic", lambda: 1000.0)
    monkeypatch.setattr(ws_watchdog.asyncio, "sleep", AsyncMock())
    watchdog = ws_watchdog.WsWatchdog(
        stale_s=30.0,
        liveness_check=lambda: False,
        event_recorder=runtime.record_event,
    )

    await watchdog._on_stale()

    events = runtime.drain_pending_events()
    assert [event.kind for event in events] == [
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.RECONNECT_DEFERRED,
    ]
    assert events[-1].reason_code == "reconnect_hook_missing"
    assert len(watchdog._reconnect_timestamps) == 0
    assert all(len(json.dumps(dict(event.detail)).encode()) <= 2048 for event in events)


async def test_watchdog_records_reconnect_hook_failure_at_the_real_transition(
    monkeypatch: pytest.MonkeyPatch,
):
    runtime = _runtime()
    monkeypatch.setattr(ws_watchdog.time, "monotonic", lambda: 1000.0)
    monkeypatch.setattr(ws_watchdog.asyncio, "sleep", AsyncMock())

    def _raise() -> None:
        raise ConnectionError("raw frame and credential must not be retained")

    watchdog = ws_watchdog.WsWatchdog(
        stale_s=30.0,
        on_reconnect=_raise,
        liveness_check=lambda: False,
        event_recorder=runtime.record_event,
    )

    await watchdog._on_stale()

    events = runtime.drain_pending_events()
    assert [event.kind for event in events] == [
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.RECONNECT_RESERVED,
        RuntimeEventKind.RECONNECT_FAILED,
    ]
    assert "credential" not in json.dumps([dict(event.detail) for event in events])


async def test_watchdog_and_consumer_emit_one_owned_reconnect_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
):
    runtime = _runtime()
    monkeypatch.setattr(ws_watchdog.time, "monotonic", lambda: 1000.0)
    monkeypatch.setattr(ws_watchdog.asyncio, "sleep", AsyncMock())
    watchdog = ws_watchdog.WsWatchdog(
        stale_s=30.0,
        liveness_check=lambda: False,
        event_recorder=runtime.record_event,
    )
    consumer = WsConsumer(
        settings=SimpleNamespace(),
        watchdog=watchdog,
        on_event=lambda _event: None,
        initial_assets=[],
        membership_observer=runtime.update_membership,
        event_recorder=runtime.record_event,
    )
    consumer._connection_generation = 1
    ws = SimpleNamespace(send=AsyncMock(return_value=None), close=AsyncMock(return_value=None))
    reconnect_tasks: list[asyncio.Task[None]] = []

    def _start_consumer_reconnect() -> None:
        reconnect_tasks.append(asyncio.create_task(consumer._initialize_connection(ws)))

    watchdog._on_reconnect = _start_consumer_reconnect

    await watchdog._on_stale()
    await reconnect_tasks[0]

    kinds = [event.kind for event in runtime.drain_pending_events()]
    assert kinds == [
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.RECONNECT_RESERVED,
        RuntimeEventKind.RECONNECT_STARTED,
        RuntimeEventKind.WS_GENERATION_CHANGED,
        RuntimeEventKind.RECONNECT_SUCCEEDED,
    ]
    assert kinds.count(RuntimeEventKind.RECONNECT_STARTED) == 1


def _consumer(runtime: L3EvidenceRuntime) -> WsConsumer:
    return WsConsumer(
        settings=SimpleNamespace(),
        watchdog=ws_watchdog.WsWatchdog(stale_s=30.0),
        on_event=lambda _event: None,
        initial_assets=[],
        membership_observer=runtime.update_membership,
        event_recorder=runtime.record_event,
    )


async def test_consumer_records_generation_success_control_failure_and_compensation():
    runtime = _runtime()
    consumer = _consumer(runtime)
    consumer._connection_generation = 1
    ws = SimpleNamespace(send=AsyncMock(return_value=None), close=AsyncMock(return_value=None))

    await consumer._initialize_connection(ws)
    ws.send.side_effect = RuntimeError("frame includes token-secret-123")
    assert await consumer.add_subscriptions(["token-secret-123"]) is False

    events = runtime.drain_pending_events()
    kinds = [event.kind for event in events]
    assert kinds[:3] == [
        RuntimeEventKind.RECONNECT_STARTED,
        RuntimeEventKind.WS_GENERATION_CHANGED,
        RuntimeEventKind.RECONNECT_SUCCEEDED,
    ]
    assert RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED in kinds
    assert RuntimeEventKind.SUBSCRIPTION_COMPENSATED in kinds
    encoded = json.dumps([dict(event.detail) for event in events])
    assert "token-secret-123" not in encoded
    assert "frame" not in encoded


async def test_consumer_records_failed_initial_reconnect_without_claiming_success():
    runtime = _runtime()
    consumer = _consumer(runtime)
    consumer._connection_generation = 1
    ws = SimpleNamespace(
        send=AsyncMock(side_effect=ConnectionError("secret raw frame")),
        close=AsyncMock(return_value=None),
    )

    with pytest.raises(Exception, match="initial WS subscription failed"):
        await consumer._initialize_connection(ws)

    kinds = [event.kind for event in runtime.drain_pending_events()]
    assert RuntimeEventKind.RECONNECT_STARTED in kinds
    assert RuntimeEventKind.RECONNECT_FAILED in kinds
    assert RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED in kinds
    assert RuntimeEventKind.SUBSCRIPTION_COMPENSATED in kinds
    assert RuntimeEventKind.RECONNECT_SUCCEEDED not in kinds


async def test_consumer_initial_connection_does_not_fake_reconnect_lifecycle():
    runtime = _runtime()
    consumer = _consumer(runtime)
    ws = SimpleNamespace(send=AsyncMock(return_value=None), close=AsyncMock(return_value=None))

    await consumer._initialize_connection(ws)

    assert [event.kind for event in runtime.drain_pending_events()] == [
        RuntimeEventKind.WS_GENERATION_CHANGED
    ]


async def test_consumer_no_connection_control_fails_without_fake_compensation():
    runtime = _runtime()
    consumer = _consumer(runtime)

    assert await consumer.add_subscriptions(["token-a"]) is False
    assert await consumer.remove_subscriptions(["token-a"]) is False

    events = runtime.drain_pending_events()
    assert [event.kind for event in events] == [
        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
    ]
    assert [event.reason_code for event in events] == [
        "no_active_connection",
        "no_active_connection",
    ]
    assert [event.detail["operation"] for event in events] == [
        "subscribe",
        "unsubscribe",
    ]


def test_shutdown_signal_is_enqueued_before_stop_for_both_signals():
    for sig in (l2_main.signal.SIGINT, l2_main.signal.SIGTERM):
        runtime = _runtime()
        stop_event = asyncio.Event()

        l2_main._request_shutdown(sig, stop_event=stop_event, runtime=runtime)

        assert stop_event.is_set()
        events = runtime.drain_pending_events()
        assert len(events) == 1
        assert events[0].kind is RuntimeEventKind.SHUTDOWN_SIGNAL
        assert events[0].detail == {"signal": sig.name}


async def test_sampler_writer_failure_ages_health_to_503(
    monkeypatch: pytest.MonkeyPatch,
):
    """append false -> anchor unchanged -> strict public sample-age failure."""
    now = datetime.now(UTC)
    old_sample_at = now - timedelta(seconds=76)
    old_states = _market_states(old_sample_at)
    runtime = _runtime_at(now)
    _publish_membership(runtime, old_states, at=old_sample_at)
    runtime.mark_promote_persisted(now - timedelta(seconds=10))
    old_store = _SampleStore(old_states)
    sample_times = iter((old_sample_at, old_sample_at, now, now))
    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: next(sample_times))
    common = dict(
        settings=_sampler_settings(),
        ws_consumer=SimpleNamespace(last_event_at_s=now.timestamp()),
        reconciliation_state=_reconciliation_state(now),
        runtime=runtime,
    )
    assert await l3_sampler.sample_once(
        scheduled_at=old_sample_at,
        sample_seq=0,
        store=old_store,
        **common,
    )

    fresh_states = _market_states(now)
    failed_store = _SampleStore(fresh_states, append_results=[False])
    assert not await l3_sampler.sample_once(
        scheduled_at=now,
        sample_seq=1,
        store=failed_store,
        **common,
    )
    assert runtime.snapshot().last_sample_persisted_at == old_sample_at

    _assert_strict_failure(runtime, "l3:evidence_sample_age_seconds")


async def test_promoter_ledger_failure_ages_health_to_503():
    """terminal append false -> promoter anchor unchanged -> public failure."""
    from polyarb.observation import l3_promote

    now = datetime.now(UTC)
    states = _market_states(now)
    runtime = _runtime_at(now)
    _publish_membership(runtime, states, at=now)
    runtime.mark_sample_persisted(
        now,
        tuple(
            l3_evidence.MarketSampleRecord(
                boot_id=runtime.snapshot().boot_id,
                sample_seq=0,
                sampled_at=now,
                market_id=state.market_id,
                yes_token_id=state.yes_token_id,
                no_token_id=state.no_token_id,
                yes_desired=True,
                no_desired=True,
                yes_committed=True,
                no_committed=True,
                yes_evidenced=True,
                no_evidenced=True,
                evidence_generation=1,
                yes_book_at=state.yes_book_at,
                no_book_at=state.no_book_at,
                yes_book_age_ms=5000,
                no_book_age_ms=6000,
                worst_book_age_ms=6000,
                yes_ohlc_at=state.yes_ohlc_at,
                yes_ohlc_age_ms=7000,
                status=HealthStatus.PASS,
                reason_code="ok",
            )
            for state in states
        ),
    )
    old_anchor = now - timedelta(seconds=361)
    runtime.mark_promote_persisted(old_anchor)

    class _RejectPromoteStore:
        calls = 0

        async def append_promote_run(self, _record):
            self.calls += 1
            return False

    store = _RejectPromoteStore()
    tokens = _tokens(states)
    mapping = tuple(
        {
            "market_id": state.market_id,
            "yes_token_id": state.yes_token_id,
            "no_token_id": state.no_token_id,
        }
        for state in states
    )
    result = await l3_promote._finalize_promote_run(
        draft=l3_promote._PromoteTerminalDraft(
            status=PromoteStatus.SUCCESS,
            reason_code="ok",
            selected_count=5,
            desired=tokens,
            committed=tokens,
            evidenced=tokens,
            mapping=mapping,
            ws_generation=1,
            mirror_succeeded=True,
        ),
        started_at=now,
        scheduled_at=now,
        run_seq=1,
        acceptance_config_hash=runtime.snapshot().acceptance_config_hash,
        evidence_store=store,
        evidence_runtime=runtime,
        apply_mutations=True,
        staged_state=l3_promote._PromoteStagedState(
            tob_rows=[],
            market_token_map={
                state.market_id: (state.yes_token_id, state.no_token_id)
                for state in states
            },
            active_set=tokens,
            mirrored_market_ids=frozenset(state.market_id for state in states),
        ),
        append_attempt=l3_promote._PromoteAppendAttempt(),
    )
    assert result.persisted is False
    assert store.calls == 1
    assert runtime.snapshot().last_promote_persisted_at == old_anchor

    _assert_strict_failure(runtime, "l3:promoter_ledger_age_seconds")


async def test_ws_control_false_surfaces_membership_503():
    """real control false publishes desired/committed mismatch to runtime."""
    now = datetime.now(UTC)
    states = _market_states(now)
    tokens = _tokens(states)
    runtime = _runtime_at(now)
    consumer = WsConsumer(
        settings=SimpleNamespace(),
        watchdog=ws_watchdog.WsWatchdog(stale_s=30.0),
        on_event=lambda _event: None,
        initial_assets=[],
        membership_observer=runtime.update_membership,
        event_recorder=runtime.record_event,
    )
    consumer.set_l3_desired(tokens)
    ws = SimpleNamespace(send=AsyncMock(return_value=None), close=AsyncMock(return_value=None))
    await consumer._initialize_connection(ws)
    for token in tokens:
        consumer.record_book_evidence(
            asset_id=token,
            generation=consumer.l3_membership_snapshot().generation,
            book_levels_succeeded=True,
            observed_at=now,
        )
    store = _SampleStore(states)
    assert await l3_sampler.sample_once(
        scheduled_at=now,
        sample_seq=0,
        settings=_sampler_settings(),
        ws_consumer=consumer,
        reconciliation_state=_reconciliation_state(now),
        runtime=runtime,
        store=store,
    )
    runtime.mark_promote_persisted(now)

    removed = sorted(tokens)[0]
    consumer.set_l3_desired(tokens - {removed})
    ws.send.side_effect = RuntimeError("control rejected")
    assert await consumer.remove_subscriptions([removed]) is False
    status = runtime.snapshot()
    assert status.desired != status.committed

    body = _assert_strict_failure(
        runtime,
        "l3:membership_convergence",
        ws_consumer=consumer,
    )
    assert body["checks"]["l3:membership_convergence"][0]["observedValue"] == "mismatch"


async def test_one_hot_four_silent_surfaces_worst_market_503():
    """real atomic sample keeps per-market clocks; global heat cannot mask four."""
    now = datetime.now(UTC)
    states = _market_states(now, stale_markets=frozenset({1, 2, 3, 4}))
    runtime = _runtime_at(now)
    _publish_membership(runtime, states, at=now)
    runtime.mark_promote_persisted(now)
    store = _SampleStore(states)

    assert await l3_sampler.sample_once(
        scheduled_at=now,
        sample_seq=0,
        settings=_sampler_settings(),
        ws_consumer=SimpleNamespace(last_event_at_s=now.timestamp()),
        reconciliation_state=_reconciliation_state(now),
        runtime=runtime,
        store=store,
    )
    persisted = runtime.snapshot().last_market_samples
    assert persisted[0].status is HealthStatus.PASS
    assert all(sample.status is HealthStatus.FAIL for sample in persisted[1:])

    body = _assert_strict_failure(runtime, "l3:worst_market_freshness")
    assert body["checks"]["l3:worst_market_freshness"][0]["observedValue"] >= 123


async def test_reconnect_requires_current_generation_sample_before_health_recovers():
    """Current membership cannot bless a previous-generation persisted sample."""
    from pydantic import SecretStr
    from starlette.testclient import TestClient

    from polyarb.config import Settings
    from polyarb.http.l2_app import create_l2_app

    base = datetime.now(UTC) - timedelta(seconds=5)
    states_v1 = _market_states(base)
    tokens = _tokens(states_v1)
    runtime = _runtime_at(base)
    consumer = WsConsumer(
        settings=SimpleNamespace(),
        watchdog=ws_watchdog.WsWatchdog(stale_s=30.0),
        on_event=lambda _event: None,
        initial_assets=[],
        membership_observer=runtime.update_membership,
        event_recorder=runtime.record_event,
    )
    consumer.set_l3_desired(tokens)
    ws_v1 = SimpleNamespace(send=AsyncMock(return_value=None), close=AsyncMock(return_value=None))
    await consumer._initialize_connection(ws_v1)
    generation_one = runtime.snapshot().ws_generation
    book_at_v1 = {
        token_id: book_at
        for state in states_v1
        for token_id, book_at in (
            (state.yes_token_id, state.yes_book_at),
            (state.no_token_id, state.no_book_at),
        )
    }
    for token in tokens:
        consumer.record_book_evidence(
            asset_id=token,
            generation=generation_one,
            book_levels_succeeded=True,
            observed_at=book_at_v1[token],
        )
    assert await l3_sampler.sample_once(
        scheduled_at=base,
        sample_seq=0,
        settings=_sampler_settings(),
        ws_consumer=consumer,
        reconciliation_state=_reconciliation_state(base),
        runtime=runtime,
        store=_SampleStore(states_v1),
    )
    runtime.mark_promote_persisted(base)
    assert {row.evidence_generation for row in runtime.snapshot().last_market_samples} == {
        generation_one
    }

    sampled_v2_at = base + timedelta(seconds=2)
    states_v2 = _market_states(sampled_v2_at)
    book_at_v2 = {
        token_id: book_at
        for state in states_v2
        for token_id, book_at in (
            (state.yes_token_id, state.yes_book_at),
            (state.no_token_id, state.no_book_at),
        )
    }
    ws_v2 = SimpleNamespace(send=AsyncMock(return_value=None), close=AsyncMock(return_value=None))
    await consumer._initialize_connection(ws_v2)
    generation_two = runtime.snapshot().ws_generation
    assert generation_two > generation_one
    for token in tokens:
        consumer.record_book_evidence(
            asset_id=token,
            generation=generation_two,
            book_levels_succeeded=True,
            observed_at=book_at_v2[token],
        )
    current = runtime.snapshot()
    assert current.desired == current.committed == current.evidenced

    app = create_l2_app(
        sqlite_store=SimpleNamespace(),
        settings=Settings(scan_shared_secret=SecretStr("test-secret")),
        # Isolate legacy WS status: this test's strict verdict is owned by the
        # evidence runtime, while the real consumer above drives its mutations.
        ws_consumer=SimpleNamespace(
            current_state="CONNECTED",
            last_event_at_s=datetime.now(UTC).timestamp(),
            subscribed_assets=list(tokens),
        ),
        evidence_runtime=runtime,
    )
    with TestClient(app) as client:
        before_strict = client.get("/health")
        before_probe = client.get("/healthz")
    assert before_strict.status_code == 503
    assert before_probe.status_code == 200
    assert before_strict.json()["checks"]["l3:membership_convergence"][0]["status"] == "fail"

    assert await l3_sampler.sample_once(
        scheduled_at=sampled_v2_at,
        sample_seq=1,
        settings=_sampler_settings(),
        ws_consumer=consumer,
        reconciliation_state=_reconciliation_state(sampled_v2_at),
        runtime=runtime,
        store=_SampleStore(states_v2),
    )

    with TestClient(app) as client:
        after_strict = client.get("/health")
        after_probe = client.get("/healthz")
    strict_names = (
        "l3:evidence_sample_age_seconds",
        "l3:promoter_ledger_age_seconds",
        "l3:membership_convergence",
        "l3:worst_market_freshness",
    )
    assert after_strict.status_code == 200, after_strict.json()
    assert after_probe.status_code == 200
    assert {
        after_strict.json()["checks"][name][0]["status"] for name in strict_names
    } == {"pass"}
