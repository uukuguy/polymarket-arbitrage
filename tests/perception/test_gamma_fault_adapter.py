from __future__ import annotations

import asyncio
import json
import time
from types import MappingProxyType, SimpleNamespace
from uuid import UUID

import httpx
import pytest

from polyarb.clients.gamma_client import EventPage
from polyarb.perception.discovery import DiscoveryRunner, DiscoveryWorker
from polyarb.perception.fault_adapters import (
    FaultingGammaPageClient,
    PartialGammaPageError,
)
from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCall,
    FaultCallClass,
    FaultDecision,
    FaultIntentRequest,
    FaultKind,
    FaultRuntimeIdentity,
)
from polyarb.perception.fault_runtime import FaultRuntime
from polyarb.perception.reconciliation import (
    ReconciliationRunner,
    ReconciliationWorker,
)
from polyarb.perception.store import OpportunityPerceptionStore


def _page(
    *,
    cursor: str | None = None,
    next_cursor: str | None = "next-1",
    completed: bool = False,
    event_count: int = 3,
) -> EventPage:
    return EventPage(
        events=tuple({"id": f"event-{index}"} for index in range(event_count)),
        requested_cursor=cursor,
        next_cursor=next_cursor,
        completed=completed,
        started_at_ms=1_000,
        finished_at_ms=1_001,
    )


class _Gamma:
    def __init__(self, page: EventPage) -> None:
        self.page = page
        self.calls: list[tuple[str | None, int]] = []

    async def fetch_active_event_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> EventPage:
        self.calls.append((cursor, limit))
        return self.page


class _Runtime:
    degraded = False

    def __init__(self, decision: FaultDecision | BaseException) -> None:
        self.decision = decision
        self.calls: list[FaultCall] = []
        self.injections: list[str] = []
        self.cleanups: list[tuple[str, str]] = []

    def consume(self, call: FaultCall) -> FaultDecision:
        self.calls.append(call)
        if isinstance(self.decision, BaseException):
            raise self.decision
        return self.decision

    async def record_injection(self, fault_id: str):
        self.injections.append(fault_id)
        return SimpleNamespace(
            fault_id=fault_id,
            call_id="call-1",
            occurred_at_ms=1_000,
        )

    async def sync_before_batch(self) -> None:
        return None

    async def cleanup(self, fault_id: str, reason: str):
        self.cleanups.append((fault_id, reason))
        return SimpleNamespace(memory_cleared=True, receipt_persisted=True)


def _decision(kind: FaultKind, **parameters: int) -> FaultDecision:
    return FaultDecision(
        inject=True,
        fault_id="fault-1",
        kind=kind,
        parameters=MappingProxyType(parameters),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "parameters", "error_type", "message"),
    [
        (
            FaultKind.GAMMA_TIMEOUT,
            {"delay_ms": 1},
            httpx.ReadTimeout,
            "qualified-gamma-timeout",
        ),
        (
            FaultKind.GAMMA_MALFORMED,
            {},
            json.JSONDecodeError,
            "qualified-gamma-malformed",
        ),
    ],
)
async def test_discovery_exception_faults_are_typed_and_do_not_call_gamma(
    kind: FaultKind,
    parameters: dict[str, int],
    error_type: type[BaseException],
    message: str,
) -> None:
    inner = _Gamma(_page())
    runtime = _Runtime(_decision(kind, **parameters))
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
    )

    with pytest.raises(error_type, match=message):
        await client.fetch_active_event_page(None, 100)

    assert runtime.calls == [
        FaultCall(FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE, "discovery")
    ]
    assert runtime.injections == ["fault-1"]
    assert inner.calls == []


@pytest.mark.asyncio
async def test_partial_reads_real_page_then_raises_redacted_coverage_error() -> None:
    inner = _Gamma(_page(cursor="cursor-secret", event_count=3))
    runtime = _Runtime(_decision(FaultKind.GAMMA_PARTIAL, keep_events=1))
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
    )

    with pytest.raises(PartialGammaPageError) as caught:
        await client.fetch_active_event_page("cursor-secret", 100)

    error = caught.value
    assert error.original_count == 3
    assert error.kept_count == 1
    assert len(error.requested_cursor_digest) == 64
    assert len(error.next_cursor_digest) == 64
    assert error.coverage_id.startswith("coverage-")
    assert len(error.coverage_id) == 73
    assert "cursor-secret" not in str(error)
    assert "event-0" not in str(error)
    assert inner.calls == [("cursor-secret", 100)]


@pytest.mark.asyncio
async def test_partial_that_cannot_truncate_abandons_injection_and_returns_real_page() -> None:
    inner = _Gamma(_page(event_count=1, completed=True))
    runtime = _Runtime(_decision(FaultKind.GAMMA_PARTIAL, keep_events=2))
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
    )

    result = await client.fetch_active_event_page(None, 100)

    assert result is inner.page
    assert inner.calls == [(None, 100)]
    assert runtime.cleanups == [("fault-1", "partial-not-applicable")]


@pytest.mark.asyncio
async def test_reconciliation_cursor_fault_returns_mismatched_page() -> None:
    inner = _Gamma(_page(cursor="cursor-1"))
    runtime = _Runtime(_decision(FaultKind.GAMMA_CURSOR))
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
        target_key="reconciliation",
    )

    page = await client.fetch_active_event_page("cursor-1", 100)

    assert page.requested_cursor != "cursor-1"
    assert page.events == inner.page.events
    assert inner.calls == [("cursor-1", 100)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision",
    [
        FaultDecision(False),
        RuntimeError("controller-unavailable"),
    ],
)
async def test_pass_through_and_runtime_failure_call_real_gamma_once(
    decision: FaultDecision | BaseException,
) -> None:
    inner = _Gamma(_page(completed=True))
    runtime = _Runtime(decision)
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
    )

    result = await client.fetch_active_event_page(None, 100)

    assert result is inner.page
    assert inner.calls == [(None, 100)]
    assert runtime.injections == []


@pytest.mark.asyncio
async def test_failed_injection_receipt_fails_open_to_real_gamma_once() -> None:
    inner = _Gamma(_page(completed=True))
    runtime = _Runtime(_decision(FaultKind.GAMMA_MALFORMED))

    async def fail_receipt(fault_id: str):
        runtime.injections.append(fault_id)
        return None

    runtime.record_injection = fail_receipt
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
    )

    result = await client.fetch_active_event_page(None, 100)

    assert result is inner.page
    assert inner.calls == [(None, 100)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("call_class", "target_key", "wrong_kind"),
    [
        (
            FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            "discovery",
            FaultKind.GAMMA_CURSOR,
        ),
        (
            FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            "reconciliation",
            FaultKind.GAMMA_TIMEOUT,
        ),
    ],
)
async def test_cross_scope_fault_kinds_call_real_gamma_once(
    call_class: FaultCallClass,
    target_key: str,
    wrong_kind: FaultKind,
) -> None:
    inner = _Gamma(_page(completed=True))
    parameters = {"delay_ms": 1} if wrong_kind is FaultKind.GAMMA_TIMEOUT else {}
    runtime = _Runtime(_decision(wrong_kind, **parameters))
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=call_class,
        target_key=target_key,
    )

    result = await client.fetch_active_event_page(None, 100)

    assert result is inner.page
    assert runtime.injections == []
    assert inner.calls == [(None, 100)]


@pytest.mark.asyncio
async def test_cancellation_during_timeout_is_typed_and_propagates() -> None:
    inner = _Gamma(_page())
    runtime = _Runtime(_decision(FaultKind.GAMMA_TIMEOUT, delay_ms=30_000))
    client = FaultingGammaPageClient(
        inner=inner,
        runtime=runtime,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
    )
    task = asyncio.create_task(client.fetch_active_event_page(None, 100))
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime.injections == ["fault-1"]
    assert inner.calls == []


@pytest.mark.asyncio
async def test_discovery_partial_never_publishes_or_advances_cursor(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    inner = _Gamma(_page(cursor=None, next_cursor="next-secret", event_count=3))
    runtime = _Runtime(_decision(FaultKind.GAMMA_PARTIAL, keep_events=1))
    worker = DiscoveryWorker(
        gamma=inner,
        store=store,
        clock_ms=lambda: 1_000,
        fault_runtime=runtime,
    )

    with pytest.raises(PartialGammaPageError):
        await worker.run_batch()

    assert store.discovery_cursor() is None
    with store._connect() as con:
        assert (
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_discovery_batches"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.asyncio
async def test_real_partial_chain_persists_redacted_coverage_then_recovers(
    tmp_path,
) -> None:
    path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    identity = FaultRuntimeIdentity(
        component="discovery",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )
    authority = FaultAuthorityStore(path)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-partial",
            kind=FaultKind.GAMMA_PARTIAL,
            call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            target_key="discovery",
            parameters={"keep_events": 0},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="b" * 64,
            authorization_digest="c" * 64,
        ),
        accepted_at_ms=1_001,
    ).accepted
    wall = iter(range(1_100, 1_200))
    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=wall.__next__,
        monotonic=lambda: 10.0,
    )
    stop = asyncio.Event()

    class Gamma:
        calls = 0

        async def fetch_active_event_page(self, cursor, limit):
            self.calls += 1
            if self.calls == 1:
                return EventPage(
                    events=(
                        {
                            "id": "event-payload-must-not-persist",
                            "body": "response-body-must-not-persist",
                            "url": "https://secret.invalid/events",
                        },
                    ),
                    requested_cursor=None,
                    next_cursor="secret-next-cursor",
                    completed=False,
                    started_at_ms=1_110,
                    finished_at_ms=1_111,
                )
            stop.set()
            return EventPage(
                events=(),
                requested_cursor=None,
                next_cursor=None,
                completed=True,
                started_at_ms=1_120,
                finished_at_ms=1_121,
            )

        async def aclose(self):
            return None

    gamma = Gamma()
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        clock_ms=lambda: 1_100,
        fault_runtime=runtime,
    )
    await DiscoveryRunner(
        worker=worker,
        gamma=gamma,
        interval_s=0.001,
        store=store,
    ).run(stop)

    history = authority.validate_history("fault-partial")
    assert history.valid is True
    assert [event.state.value for event in history.events] == [
        "authorized",
        "armed",
        "injected",
        "detected",
        "contained",
        "cleaned",
        "recovered",
    ]
    detected = next(
        event for event in history.events if event.state.value == "detected"
    )
    assert set(detected.evidence) == {"coverage_id"}
    assert detected.evidence["coverage_id"].startswith("coverage-")
    with store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_incident_events"
        ).fetchone()[0] == 0
        batches = con.execute(
            "SELECT requested_cursor,next_cursor,page_event_count "
            "FROM neg_risk_discovery_batches"
        ).fetchall()
        persisted = "\n".join(
            str(row[0])
            for row in con.execute(
                "SELECT evidence_json FROM neg_risk_fault_events"
            )
        )
    assert [tuple(row) for row in batches] == [(None, None, 0)]
    assert "event-payload" not in persisted
    assert "response-body" not in persisted
    assert "secret.invalid" not in persisted
    assert "secret-next-cursor" not in persisted


@pytest.mark.asyncio
async def test_timeout_cancellation_cleans_before_any_recovery_poll(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "cancel.db")
    store.init_schema()
    injected = asyncio.Event()
    order: list[str] = []

    class Runtime(_Runtime):
        active_fault_id = "fault-1"
        pending_recovery_fault_id = None

        async def record_injection(self, fault_id):
            order.append("injected")
            injected.set()
            return await super().record_injection(fault_id)

        async def cleanup(self, fault_id, reason):
            order.append("cleaned")
            self.active_fault_id = None
            return SimpleNamespace(memory_cleared=True, receipt_persisted=True)

        async def record_recovery(self, recovery_id):
            order.append("recovered")
            return True

    runtime = Runtime(_decision(FaultKind.GAMMA_TIMEOUT, delay_ms=30_000))
    gamma = _Gamma(_page())
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        clock_ms=lambda: 1_000,
        fault_runtime=runtime,
    )
    runner = DiscoveryRunner(
        worker=worker,
        gamma=gamma,
        interval_s=1,
        store=store,
    )
    task = asyncio.create_task(runner.run(asyncio.Event()))
    await asyncio.wait_for(injected.wait(), timeout=1)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert order == ["injected", "cleaned"]


@pytest.mark.asyncio
async def test_real_reconciliation_cursor_chain_recovers_on_new_checkpoint(
    tmp_path,
) -> None:
    path = tmp_path / "reconciliation.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    base_ms = int(time.time() * 1_000)
    identity = FaultRuntimeIdentity(
        component="reconciliation",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    )
    authority = FaultAuthorityStore(path)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=base_ms,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-cursor",
            kind=FaultKind.GAMMA_CURSOR,
            call_class=FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            target_key="reconciliation",
            parameters={},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="d" * 64,
            authorization_digest="e" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted
    wall = iter(range(base_ms + 10, base_ms + 100))
    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=wall.__next__,
        monotonic=lambda: 10.0,
    )
    stop = asyncio.Event()

    class Gamma:
        calls = 0

        async def fetch_active_event_page(self, cursor, limit):
            self.calls += 1
            if self.calls == 2:
                await asyncio.sleep(0.01)
                stop.set()
            page_ms = int(time.time() * 1_000)
            return EventPage(
                events=(),
                requested_cursor=cursor,
                next_cursor=None,
                completed=True,
                started_at_ms=page_ms,
                finished_at_ms=page_ms,
            )

        async def aclose(self):
            return None

    gamma = Gamma()
    worker = ReconciliationWorker(
        gamma=gamma,
        store=store,
        clock_ms=lambda: base_ms + 10,
        fault_runtime=runtime,
    )
    await ReconciliationRunner(
        worker=worker,
        gamma=gamma,
        interval_s=0.001,
        store=store,
    ).run(stop)

    history = authority.validate_history("fault-cursor")
    assert history.valid is True
    assert [event.state.value for event in history.events] == [
        "authorized",
        "armed",
        "injected",
        "detected",
        "contained",
        "cleaned",
        "recovered",
    ]
    detected = next(
        event for event in history.events if event.state.value == "detected"
    )
    assert set(detected.evidence) == {"incident_id"}
    assert store.open_incidents() == ()
    checkpoint = store.current_reconciliation()
    assert checkpoint is not None
    assert checkpoint.checkpoint_at_ms > next(
        event.occurred_at_ms
        for event in history.events
        if event.state.value == "injected"
    )
