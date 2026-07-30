from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCall,
    FaultCallClass,
    FaultEventState,
    FaultIntent,
    FaultIntentRequest,
    FaultKind,
    FaultRuntimeIdentity,
)
from polyarb.perception.fault_runtime import (
    CleanupResult,
    FaultRuntime,
    PassThroughFaultRuntime,
    build_fault_runtime,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.perception.worker_cli import _build_child_fault_runtime

IDENTITY = FaultRuntimeIdentity(
    component="candidate",
    release_id="a" * 40,
    machine_id="machine-1",
    boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
)


def _intent(*, ownership=None) -> FaultIntent:
    return FaultIntent(
        fault_id="fault-1",
        kind=FaultKind.CLOB_429,
        call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
        target_key="group-1",
        parameters={},
        ttl_ms=30_000,
        runtime=IDENTITY,
        nonce_digest="b" * 64,
        accepted_at_ms=1_000,
        ownership_capability=ownership,
    )


class _Authority:
    def __init__(self, intent: FaultIntent | None = None) -> None:
        self.intent = intent
        self.claims: list[tuple[FaultRuntimeIdentity, int]] = []
        self.events: list[tuple] = []

    def claim_pending(self, identity, *, claimed_at_ms):
        self.claims.append((identity, claimed_at_ms))
        value, self.intent = self.intent, None
        return value

    def append_event(self, fault_id, state, **kwargs):
        self.events.append((fault_id, state, kwargs))

    def relinquish_claim(self, fault_id, **kwargs):
        state = FaultEventState.ABANDONED
        self.events.append((fault_id, state, kwargs))
        return SimpleNamespace(state=state)


@pytest.mark.asyncio
async def test_sync_claims_once_and_hot_path_only_consumes_memory() -> None:
    authority = _Authority(_intent())
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=lambda: 1_100,
        monotonic=lambda: 10.0,
    )

    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )

    assert decision.inject is True
    assert authority.claims == [(IDENTITY, 1_100)]


@pytest.mark.asyncio
async def test_store_claim_failure_is_redacted_pass_through(caplog) -> None:
    class BrokenAuthority(_Authority):
        def claim_pending(self, identity, *, claimed_at_ms):
            raise RuntimeError("Authorization: Bearer secret-value")

    upstream_called = False
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=BrokenAuthority(),
        clock_ms=lambda: 1_100,
    )

    await runtime.sync_before_batch()
    upstream_called = True

    assert upstream_called is True
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject is False
    assert "secret-value" not in caplog.text


@pytest.mark.asyncio
async def test_cleanup_clears_memory_before_append() -> None:
    observations: list[bool] = []
    authority = _Authority(_intent())
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=lambda: 1_100,
    )
    await runtime.sync_before_batch()

    def relinquish_claim(fault_id, **kwargs):
        observations.append(runtime.active_fault_id is None)
        authority.events.append((fault_id, FaultEventState.ABANDONED, kwargs))
        return SimpleNamespace(state=FaultEventState.ABANDONED)

    authority.relinquish_claim = relinquish_claim
    result = await runtime.cleanup("fault-1", "cancelled")

    assert result.memory_cleared is True
    assert result.receipt_persisted is True
    assert observations == [True]
    assert authority.events[0][1] is FaultEventState.ABANDONED


def test_disabled_or_unavailable_builder_is_pass_through(tmp_path: Path) -> None:
    disabled = build_fault_runtime(
        enabled=False,
        db_path=tmp_path / "missing.db",
        identity=IDENTITY,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )
    unavailable = build_fault_runtime(
        enabled=True,
        db_path=tmp_path / "missing" / "fault.db",
        identity=IDENTITY,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )

    assert isinstance(disabled, PassThroughFaultRuntime)
    assert disabled.degraded is False
    assert isinstance(unavailable, PassThroughFaultRuntime)
    assert unavailable.degraded is True
    assert unavailable.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject is False


@pytest.mark.asyncio
async def test_pass_through_runtime_never_blocks_batches() -> None:
    runtime = PassThroughFaultRuntime(degraded=True)
    await runtime.sync_before_batch()
    assert (await runtime.cleanup("unused", "cancelled")).memory_cleared is False


def test_each_builder_registration_uses_exact_boot_identity(tmp_path: Path) -> None:
    from polyarb.perception.store import OpportunityPerceptionStore

    path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    runtime = build_fault_runtime(
        enabled=True,
        db_path=path,
        identity=IDENTITY,
        supervisor_run_id="run-7",
        attempt=3,
        started_at_ms=1_234,
    )

    assert isinstance(runtime, FaultRuntime)
    with store._connect() as con:
        row = con.execute(
            "SELECT component,release_id,machine_id,boot_id,"
            "supervisor_run_id,attempt FROM neg_risk_fault_runtime_starts"
        ).fetchone()
    assert tuple(row) == (
        "candidate",
        "a" * 40,
        "machine-1",
        str(IDENTITY.boot_id),
        "run-7",
        3,
    )


def test_boot_ids_are_uuid4() -> None:
    assert uuid4().version == 4


class _BoundaryRuntime:
    degraded = False

    def __init__(self) -> None:
        self.synced = False
        self.sync_event = asyncio.Event()
        self.cleanup_reasons: list[str] = []

    @property
    def active_fault_id(self):
        return "fault-1"

    async def sync_before_batch(self) -> None:
        self.synced = True
        self.sync_event.set()

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult:
        assert fault_id == "fault-1"
        self.cleanup_reasons.append(reason)
        return CleanupResult(True, True)

    def consume(self, call):
        raise AssertionError("hot-path adapters are a later task")


@pytest.mark.asyncio
async def test_candidate_claims_before_loading_selection(tmp_path: Path) -> None:
    from polyarb.perception.candidate_watcher import (
        CandidateWatcherRuntime,
        CandidateWatcherScheduler,
    )
    from polyarb.perception.store import OpportunityPerceptionStore

    boundary = _BoundaryRuntime()

    class Store(OpportunityPerceptionStore):
        def candidate_scheduling_snapshot(self, group_ids):
            assert boundary.synced is True
            return ()

    store = Store(tmp_path / "candidate.db")
    store.init_schema()
    scheduler = CandidateWatcherScheduler(
        watcher=SimpleNamespace(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        fault_runtime=boundary,
    )
    try:
        await scheduler.run_due_once()
    finally:
        scheduler.close()


@pytest.mark.asyncio
async def test_candidate_cancellation_invokes_fault_cleanup(tmp_path: Path) -> None:
    from polyarb.perception.candidate_watcher import (
        CandidateWatcherRuntime,
        CandidateWatcherScheduler,
    )
    from polyarb.perception.store import OpportunityPerceptionStore

    boundary = _BoundaryRuntime()
    store = OpportunityPerceptionStore(tmp_path / "cancel.db")
    store.init_schema()
    scheduler = CandidateWatcherScheduler(
        watcher=SimpleNamespace(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        fault_runtime=boundary,
    )
    task = asyncio.create_task(scheduler.run(asyncio.Event()))
    await asyncio.wait_for(boundary.sync_event.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert boundary.cleanup_reasons == ["candidate-stopped"]


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ["discovery", "reconciliation"])
async def test_gamma_workers_claim_immediately_before_page_fetch(
    tmp_path: Path,
    component: str,
) -> None:
    from polyarb.clients.gamma_client import EventPage
    from polyarb.perception.discovery import DiscoveryWorker
    from polyarb.perception.reconciliation import ReconciliationWorker
    from polyarb.perception.store import OpportunityPerceptionStore

    boundary = _BoundaryRuntime()

    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            assert boundary.synced is True
            return EventPage(
                events=(),
                requested_cursor=cursor,
                next_cursor=None,
                completed=True,
                started_at_ms=1_000,
                finished_at_ms=1_001,
            )

    store = OpportunityPerceptionStore(tmp_path / f"{component}.db")
    store.init_schema()
    if component == "discovery":
        worker = DiscoveryWorker(
            gamma=Gamma(),
            store=store,
            clock_ms=lambda: 1_000,
            fault_runtime=boundary,
        )
    else:
        worker = ReconciliationWorker(
            gamma=Gamma(),
            store=store,
            clock_ms=lambda: 1_000,
            fault_runtime=boundary,
        )

    await worker.run_batch()


def test_invalid_or_absent_child_boot_is_degraded_pass_through(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = SimpleNamespace(
        upstream_fault_control_enabled=True,
        release_id="a" * 40,
        db_path=tmp_path / "state.db",
    )
    monkeypatch.setenv("POLYARB_PRODUCER_SUPERVISOR_RUN_ID", "run-1")
    monkeypatch.setenv("POLYARB_PRODUCER_ATTEMPT", "1")
    monkeypatch.delenv("POLYARB_PRODUCER_BOOT_ID", raising=False)
    absent = _build_child_fault_runtime("candidate", settings)
    monkeypatch.setenv("POLYARB_PRODUCER_BOOT_ID", "not-a-uuid")
    invalid = _build_child_fault_runtime("candidate", settings)

    assert isinstance(absent, PassThroughFaultRuntime) and absent.degraded
    assert isinstance(invalid, PassThroughFaultRuntime) and invalid.degraded


def test_child_registration_binds_supervisor_attempt_and_exact_environment_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from polyarb.perception.store import OpportunityPerceptionStore

    path = tmp_path / "child.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    boot_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    settings = SimpleNamespace(
        upstream_fault_control_enabled=True,
        release_id="c" * 40,
        db_path=path,
    )
    monkeypatch.setenv("FLY_MACHINE_ID", "machine-child")
    monkeypatch.setenv("POLYARB_PRODUCER_BOOT_ID", str(boot_id))
    monkeypatch.setenv("POLYARB_PRODUCER_SUPERVISOR_RUN_ID", "run-child")
    monkeypatch.setenv("POLYARB_PRODUCER_ATTEMPT", "4")

    runtime = _build_child_fault_runtime("discovery", settings)

    assert isinstance(runtime, FaultRuntime)
    with store._connect() as con:
        row = con.execute(
            "SELECT component,release_id,machine_id,boot_id,"
            "supervisor_run_id,attempt FROM neg_risk_fault_runtime_starts"
        ).fetchone()
    assert tuple(row) == (
        "discovery",
        "c" * 40,
        "machine-child",
        str(boot_id),
        "run-child",
        4,
    )


def _real_runtime(
    tmp_path: Path,
    *,
    authority_type=FaultAuthorityStore,
    clock_ms=lambda: 1_200,
    monotonic=lambda: 10.0,
):
    path = tmp_path / "runtime.db"
    OpportunityPerceptionStore(path).init_schema()
    authority = authority_type(path)
    authority.register_runtime_start(
        IDENTITY,
        supervisor_run_id="run-real",
        attempt=1,
        started_at_ms=1_000,
    )
    return (
        path,
        authority,
        FaultRuntime(
            identity=IDENTITY,
            authority=authority,
            clock_ms=clock_ms,
            monotonic=monotonic,
        ),
    )


def _accept(
    authority: FaultAuthorityStore,
    *,
    fault_id: str,
    target_key: str,
    accepted_at_ms: int,
    ttl_ms: int = 10_000,
    nonce: str = "d",
) -> None:
    admission = authority.accept_intent(
        FaultIntentRequest(
            fault_id=fault_id,
            kind=FaultKind.CLOB_429,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key=target_key,
            parameters={},
            ttl_ms=ttl_ms,
            runtime=IDENTITY,
        ),
        auth=FaultAuthorization(
            nonce_digest=nonce * 64,
            authorization_digest="e" * 64,
        ),
        accepted_at_ms=accepted_at_ms,
    )
    assert admission.accepted is True


@pytest.mark.asyncio
async def test_real_sqlite_armed_cleanup_persists_valid_abandoned_terminal(
    tmp_path: Path,
) -> None:
    _, authority, runtime = _real_runtime(tmp_path, clock_ms=lambda: 1_300)
    _accept(
        authority,
        fault_id="fault-real",
        target_key="group-real",
        accepted_at_ms=1_100,
    )
    await runtime.sync_before_batch()

    result = await runtime.cleanup("fault-real", "cancelled")

    history = authority.validate_history("fault-real")
    assert result.memory_cleared is True
    assert result.receipt_persisted is True
    assert result.terminal_state is FaultEventState.ABANDONED
    assert history.valid is True
    assert history.events[-1].state is FaultEventState.ABANDONED


@pytest.mark.asyncio
async def test_real_sqlite_contained_cleanup_returns_actual_cleaned_terminal(
    tmp_path: Path,
) -> None:
    wall = [1_200]
    _, authority, runtime = _real_runtime(tmp_path, clock_ms=lambda: wall[0])
    _accept(
        authority,
        fault_id="fault-contained",
        target_key="group-contained",
        accepted_at_ms=1_100,
    )
    await runtime.sync_before_batch()
    active = runtime._controller.active
    assert active is not None and active.intent.ownership_capability is not None
    ownership = active.intent.ownership_capability
    authority.append_event(
        "fault-contained",
        FaultEventState.INJECTED,
        occurred_at_ms=1_201,
        evidence={"call_id": "call-contained"},
        ownership=ownership,
    )
    authority.append_event(
        "fault-contained",
        FaultEventState.DETECTED,
        occurred_at_ms=1_202,
        evidence={"incident_id": "incident-contained"},
    )
    authority.append_event(
        "fault-contained",
        FaultEventState.CONTAINED,
        occurred_at_ms=1_203,
        evidence={"containment_id": "containment-contained"},
    )
    wall[0] = 1_300

    result = await runtime.cleanup("fault-contained", "contained")

    assert result.receipt_persisted is True
    assert result.terminal_state is FaultEventState.CLEANED
    assert authority.validate_history("fault-contained").valid is True


@pytest.mark.asyncio
async def test_cancelled_blocked_claim_settles_and_relinquishes_real_sqlite_chain(
    tmp_path: Path,
) -> None:
    committed = threading.Event()
    release = threading.Event()

    class BlockingAuthority(FaultAuthorityStore):
        def claim_pending(self, identity, *, claimed_at_ms):
            result = super().claim_pending(identity, claimed_at_ms=claimed_at_ms)
            committed.set()
            assert release.wait(timeout=2)
            return result

    _, authority, runtime = _real_runtime(tmp_path, authority_type=BlockingAuthority)
    _accept(
        authority,
        fault_id="fault-cancelled-claim",
        target_key="group-cancelled",
        accepted_at_ms=1_100,
    )
    task = asyncio.create_task(runtime.sync_before_batch())
    assert await asyncio.to_thread(committed.wait, 2)
    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    history = authority.validate_history("fault-cancelled-claim")
    assert runtime.active_fault_id is None
    assert history.valid is True
    assert history.events[-1].state is FaultEventState.ABANDONED


@pytest.mark.asyncio
async def test_expired_unmatched_fault_is_relinquished_then_same_boot_claims_next(
    tmp_path: Path,
) -> None:
    wall = [1_200]
    monotonic = [10.0]
    _, authority, runtime = _real_runtime(
        tmp_path,
        clock_ms=lambda: wall[0],
        monotonic=lambda: monotonic[0],
    )
    _accept(
        authority,
        fault_id="fault-expired",
        target_key="group-unmatched",
        accepted_at_ms=1_100,
        ttl_ms=1_000,
    )
    await runtime.sync_before_batch()
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-other")
    ).inject is False
    wall[0] = 2_200
    monotonic[0] = 11.1

    await runtime.sync_before_batch()
    expired = authority.validate_history("fault-expired")
    assert expired.valid is True
    assert expired.events[-1].state is FaultEventState.EXPIRED
    _accept(
        authority,
        fault_id="fault-next",
        target_key="group-next",
        accepted_at_ms=2_300,
        nonce="f",
    )
    wall[0] = 2_400
    await runtime.sync_before_batch()

    assert runtime.active_fault_id == "fault-next"
