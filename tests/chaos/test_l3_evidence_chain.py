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
    L3EvidenceRuntime,
    RuntimeEventKind,
    RuntimeEventRecord,
    RuntimeIdentity,
)

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
    assert status.pending_event_count == 2
    assert status.last_promote_persisted_at is None
    assert status.last_sample_persisted_at is None


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
        RuntimeEventKind.RECONNECT_STARTED,
        RuntimeEventKind.RECONNECT_FAILED,
    ]
    assert "credential" not in json.dumps([dict(event.detail) for event in events])


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
        RuntimeEventKind.WS_GENERATION_CHANGED,
        RuntimeEventKind.RECONNECT_STARTED,
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
