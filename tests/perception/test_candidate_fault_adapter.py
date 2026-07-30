from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from uuid import UUID

import pytest
from py_clob_client.exceptions import PolyApiException

from polyarb.perception.candidate_watcher import (
    CandidateWatcher,
    CandidateWatcherRuntime,
    CandidateWatcherScheduler,
    IntervalController,
)
from polyarb.perception.fault_adapters import CandidateBooksFault
from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCallClass,
    FaultDecision,
    FaultIntent,
    FaultIntentRequest,
    FaultKind,
    FaultRecoveryReceipt,
    FaultRecoveryWriter,
    FaultRuntimeIdentity,
)
from polyarb.perception.fault_runtime import FaultRecoveryOutcome, FaultRuntime
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore


class _Runtime:
    degraded = False
    active_fault_id = "fault-1"
    pending_recovery_fault_id = None

    def __init__(self, decision: FaultDecision) -> None:
        self.decision = decision
        self.calls = []
        self.events: list[str] = []

    def consume(self, call):
        self.calls.append(call)
        return self.decision

    async def sync_before_batch(self):
        return None

    async def record_injection(self, fault_id):
        self.events.append("injected")
        return SimpleNamespace(
            fault_id=fault_id,
            call_id="call-1",
            occurred_at_ms=1_000,
        )

    async def cleanup(self, fault_id, reason):
        self.events.append(f"cleaned:{reason}")
        self.active_fault_id = None
        return SimpleNamespace(memory_cleared=True, receipt_persisted=True)

    async def link_detection(self, fault_id, *, kind, detection_id):
        self.events.append(f"linked:{kind.value}:{detection_id}")
        return True

    def make_recovery_receipt(self, writer, *, writer_id, writer_occurred_at_ms):
        self.events.append(f"recovery-receipt:{writer.value}:{writer_id}:{writer_occurred_at_ms}")
        return SimpleNamespace(writer=writer, writer_id=writer_id)

    async def record_recovery(self, receipt):
        self.events.append(f"recovered:{receipt.writer_id}")
        return True

    async def record_recovery_outcome(self, receipt):
        return (
            FaultRecoveryOutcome.RECORDED
            if await self.record_recovery(receipt)
            else FaultRecoveryOutcome.INVALID
        )

    async def record_writer_recovery_outcome(
        self,
        writer,
        *,
        target_key,
        writer_id,
        writer_occurred_at_ms,
    ):
        if self.pending_recovery_fault_id is None:
            return FaultRecoveryOutcome.NOT_APPLICABLE
        receipt = self.make_recovery_receipt(
            writer,
            writer_id=writer_id,
            writer_occurred_at_ms=writer_occurred_at_ms,
        )
        if receipt is None:
            await self.invalidate_evidence(
                self.pending_recovery_fault_id,
                "candidate-recovery-evidence-invalid",
            )
            return FaultRecoveryOutcome.INVALID
        outcome = await self.record_recovery_outcome(receipt)
        if outcome is FaultRecoveryOutcome.INVALID:
            await self.invalidate_evidence(
                self.pending_recovery_fault_id,
                "candidate-recovery-evidence-invalid",
            )
        return outcome

    async def invalidate_evidence(self, fault_id, reason):
        self.events.append(f"invalid:{reason}")
        self.degraded = True
        return await self.cleanup(fault_id, reason)

    async def evidence_unavailable(self, fault_id, reason):
        self.events.append(f"unavailable:{reason}")
        self.degraded = True
        return await self.cleanup(fault_id, reason)


def _decision(kind: FaultKind, **parameters: int) -> FaultDecision:
    return FaultDecision(
        True,
        fault_id="fault-1",
        kind=kind,
        parameters=MappingProxyType(parameters),
    )


@pytest.mark.asyncio
async def test_before_books_consumes_exact_typed_group_and_receipts_before_fault() -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_429))
    adapter = CandidateBooksFault(runtime=runtime)

    decision = await adapter.before_books("group-a")

    assert runtime.calls[0].call_class is FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH
    assert runtime.calls[0].target_key == "group-a"
    assert runtime.events == ["injected"]
    with pytest.raises(PolyApiException) as raised:
        await adapter.after_books(
            decision,
            token_ids=("yes-1", "yes-2"),
            books=({"asset_id": "yes-1"}, {"asset_id": "yes-2"}),
        )
    assert raised.value.status_code == 429
    assert getattr(raised.value, "_polyarb_fault_call_id") == "call-1"


@pytest.mark.asyncio
async def test_group_a_decision_cannot_affect_group_b() -> None:
    class ExactGroupRuntime(_Runtime):
        def consume(self, call):
            self.calls.append(call)
            return (
                _decision(FaultKind.CLOB_429)
                if call.target_key == "group-a"
                else FaultDecision(False)
            )

    runtime = ExactGroupRuntime(FaultDecision(False))
    adapter = CandidateBooksFault(runtime=runtime)

    group_b = await adapter.before_books("group-b")
    books = ({"asset_id": "yes-b"},)

    assert (
        await adapter.after_books(
            group_b,
            token_ids=("yes-b",),
            books=books,
        )
        is books
    )
    assert runtime.calls[0].target_key == "group-b"
    assert runtime.events == []

    group_a = await adapter.before_books("group-a")
    with pytest.raises(PolyApiException):
        await adapter.after_books(
            group_a,
            token_ids=("yes-a",),
            books=({"asset_id": "yes-a"},),
        )
    assert [call.target_key for call in runtime.calls] == ["group-b", "group-a"]
    assert runtime.events == ["injected"]


@pytest.mark.asyncio
async def test_missing_leg_removes_only_real_bounded_index() -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_MISSING_LEG, leg_index=1))
    adapter = CandidateBooksFault(runtime=runtime)
    books = (
        {"asset_id": "yes-1", "asks": []},
        {"asset_id": "yes-2", "asks": []},
        {"asset_id": "yes-3", "asks": []},
    )

    decision = await adapter.before_books("group-a")
    transformed = await adapter.after_books(
        decision,
        token_ids=("yes-1", "yes-2", "yes-3"),
        books=books,
    )

    assert transformed == (books[0], books[2])
    assert books == (
        {"asset_id": "yes-1", "asks": []},
        {"asset_id": "yes-2", "asks": []},
        {"asset_id": "yes-3", "asks": []},
    )


@pytest.mark.asyncio
async def test_missing_leg_outside_real_batch_abandons_without_partial_result() -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_MISSING_LEG, leg_index=2))
    adapter = CandidateBooksFault(runtime=runtime)
    decision = await adapter.before_books("group-a")
    books = ({"asset_id": "yes-1"}, {"asset_id": "yes-2"})

    assert (
        await adapter.after_books(
            decision,
            token_ids=("yes-1", "yes-2"),
            books=books,
        )
        is books
    )
    assert runtime.events == ["injected", "cleaned:missing-leg-not-applicable"]


@pytest.mark.asyncio
async def test_latency_uses_bounded_async_delay() -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_LATENCY, delay_ms=20))
    adapter = CandidateBooksFault(runtime=runtime)
    decision = await adapter.before_books("group-a")
    loop = asyncio.get_running_loop()
    started = loop.time()

    books = ({"asset_id": "yes-1"},)
    assert (
        await adapter.after_books(
            decision,
            token_ids=("yes-1",),
            books=books,
        )
        is books
    )

    assert loop.time() - started >= 0.018


@pytest.mark.asyncio
async def test_inner_failure_after_injection_is_settled_and_preserved() -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_MISSING_LEG, leg_index=0))
    adapter = CandidateBooksFault(runtime=runtime)
    decision = await adapter.before_books("group-a")
    organic = RuntimeError("organic-clob-failure")

    with pytest.raises(RuntimeError) as raised:
        try:
            raise organic
        except BaseException:
            await adapter.settle_inner_failure(decision)
            raise

    assert raised.value is organic
    assert runtime.events == ["injected", "cleaned:injected-books-call-failed"]


def _group(group_id: str = "group-a") -> GroupRevision:
    return GroupRevision.certified(
        group_id=group_id,
        event_id=f"event-{group_id}",
        revision=1,
        started_at_ms=900,
        observed_at_ms=1_000,
        source_cursor="cursor",
        legs=(
            GroupLeg("m-1", "c-1", "yes-1", "one"),
            GroupLeg("m-2", "c-2", "yes-2", "two"),
        ),
    )


class _Structure:
    def __init__(self, revision: GroupRevision, order: list[str]) -> None:
        self.revision = revision
        self.order = order

    async def read_group(self, group_id: str):
        assert group_id == self.revision.group_id
        self.order.append("structure")
        return self.revision


class _Books:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.calls = 0

    async def get_books(self, token_ids, *, projection="full"):
        self.calls += 1
        self.order.append("books")
        assert projection == "top"
        return (
            {"asset_id": token_ids[0], "asks": [{"price": "0.4", "size": "10"}]},
            {"asset_id": token_ids[1], "asks": [{"price": "0.5", "size": "10"}]},
        )


def _watcher(
    tmp_path: Path,
    *,
    runtime: _Runtime,
    order: list[str],
    group_id: str = "group-a",
) -> tuple[CandidateWatcher, OpportunityPerceptionStore, _Books]:
    revision = _group(group_id)
    store = OpportunityPerceptionStore(tmp_path / f"{group_id}.db")
    store.init_schema()
    store.publish_group_revision(revision)
    books = _Books(order)
    original_consume = runtime.consume
    original_injection = runtime.record_injection

    def consume(call):
        order.append("fault-check")
        return original_consume(call)

    async def record_injection(fault_id):
        order.append("injection-receipt")
        return await original_injection(fault_id)

    runtime.consume = consume
    runtime.record_injection = record_injection
    return (
        CandidateWatcher(
            structure_reader=_Structure(revision, order),
            books_reader=books,
            store=store,
            runtime=CandidateWatcherRuntime(),
            interval_controller=IntervalController(),
            clock_ms=iter((2_000, 2_100)).__next__,
            fault_runtime=runtime,
        ),
        store,
        books,
    )


@pytest.mark.asyncio
async def test_candidate_fault_seam_is_after_group_snapshot_and_around_selected_books(
    tmp_path,
) -> None:
    order: list[str] = []
    runtime = _Runtime(_decision(FaultKind.CLOB_429))
    watcher, store, reader = _watcher(tmp_path, runtime=runtime, order=order)

    observation = await watcher.run_once("group-a")
    await watcher.flush_incidents()

    assert observation.status == "unavailable"
    assert reader.calls == 1
    assert order[:5] == [
        "structure",
        "fault-check",
        "injection-receipt",
        "books",
    ]
    assert store.current_quote_batch("group-a", 2_100, 1_000) is None
    assert any(item.startswith("linked:clob-429:") for item in runtime.events)
    assert runtime.events[-1] == "cleaned:candidate-clob-fault-contained"


@pytest.mark.asyncio
async def test_candidate_missing_leg_flows_to_integrity_and_publishes_no_partial_batch(
    tmp_path,
) -> None:
    order: list[str] = []
    runtime = _Runtime(_decision(FaultKind.CLOB_MISSING_LEG, leg_index=0))
    watcher, store, reader = _watcher(tmp_path, runtime=runtime, order=order)

    observation = await watcher.run_once("group-a")
    await watcher.flush_incidents()

    assert observation.reason == "incomplete-quotes"
    assert reader.calls == 1
    assert store.current_quote_batch("group-a", 2_100, 1_000) is None
    assert any(item.startswith("linked:clob-missing-leg:") for item in runtime.events)


@pytest.mark.asyncio
async def test_candidate_latency_is_classified_by_scheduler_timeout_boundary(
    tmp_path,
) -> None:
    order: list[str] = []
    runtime = _Runtime(_decision(FaultKind.CLOB_LATENCY, delay_ms=100))
    watcher, store, _ = _watcher(tmp_path, runtime=runtime, order=order)
    scheduler = CandidateWatcherScheduler(
        watcher=watcher,
        store=store,
        candidate_group_ids=lambda: ("group-a",),
        runtime=CandidateWatcherRuntime(),
        cycle_max_groups=2,
        reserved_non_high_slots=1,
        group_timeout_s=0.02,
        lower_lane_max_wait_s=1,
        discovery_candidate_max_wait_s=0.5,
        terminal_write_budget_s=5,
        fault_runtime=runtime,
    )
    try:
        await scheduler.run_due_once()
    finally:
        scheduler.close()

    incident = store.open_incidents()[0]
    assert incident.scope == "candidate:group-a"
    assert incident.kind == "clob-latency"
    assert any(item.startswith("linked:clob-latency:") for item in runtime.events)
    assert runtime.events[-1] == "cleaned:candidate-clob-fault-contained"
    assert watcher._pending_latency_faults == {}
    assert watcher._timeout_contexts == {}


@pytest.mark.asyncio
async def test_external_latency_cancellation_abandons_injected_call(
    tmp_path,
) -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_LATENCY, delay_ms=30_000))
    watcher, _, _ = _watcher(tmp_path, runtime=runtime, order=[])
    task = asyncio.create_task(watcher.run_once("group-a"))
    for _ in range(100):
        if "injected" in runtime.events:
            break
        await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime.events[-1] == "cleaned:injected-books-call-failed"


@pytest.mark.asyncio
async def test_scheduler_stale_wait_snapshot_cannot_turn_success_into_timeout(
    tmp_path,
    monkeypatch,
) -> None:
    import polyarb.perception.candidate_watcher as candidate_module

    class Watcher:
        timeout_writes = 0

        async def run_once(self, group_id, **kwargs):
            return None

        async def record_timeout(self, group_id):
            self.timeout_writes += 1

    async def stale_pending_snapshot(tasks, *, timeout):
        await asyncio.gather(*tasks)
        return set(), set(tasks)

    monkeypatch.setattr(candidate_module.asyncio, "wait", stale_pending_snapshot)
    store = OpportunityPerceptionStore(tmp_path / "stale-wait.db")
    store.init_schema()
    watcher = Watcher()
    runtime = CandidateWatcherRuntime()
    scheduler = CandidateWatcherScheduler(
        watcher=watcher,
        store=store,
        candidate_group_ids=lambda: (),
        runtime=runtime,
        group_timeout_s=0.01,
    )
    try:
        await scheduler._run_selected_group(
            (0, 0, "group-a"),
            priority_by_rank={0: "high"},
            admission_contexts={},
        )
    finally:
        scheduler.close()

    snapshot = runtime.snapshot()
    assert watcher.timeout_writes == 0
    assert snapshot.group_failure_count == 0
    assert snapshot.group_recovery_count == 0


@pytest.mark.asyncio
async def test_scheduler_stale_wait_snapshot_preserves_organic_error(
    tmp_path,
    monkeypatch,
) -> None:
    import polyarb.perception.candidate_watcher as candidate_module

    class Watcher:
        timeout_writes = 0
        incident_errors: list[BaseException] = []

        async def run_once(self, group_id, **kwargs):
            raise RuntimeError("organic-candidate-error")

        async def record_timeout(self, group_id):
            self.timeout_writes += 1

        def queue_incident_failure(self, group_id, error):
            self.incident_errors.append(error)

    async def stale_pending_snapshot(tasks, *, timeout):
        await asyncio.gather(*tasks, return_exceptions=True)
        return set(), set(tasks)

    monkeypatch.setattr(candidate_module.asyncio, "wait", stale_pending_snapshot)
    store = OpportunityPerceptionStore(tmp_path / "stale-organic.db")
    store.init_schema()
    watcher = Watcher()
    runtime = CandidateWatcherRuntime()
    scheduler = CandidateWatcherScheduler(
        watcher=watcher,
        store=store,
        candidate_group_ids=lambda: (),
        runtime=runtime,
        group_timeout_s=0.01,
    )
    try:
        await scheduler._run_selected_group(
            (0, 0, "group-a"),
            priority_by_rank={0: "high"},
            admission_contexts={},
        )
    finally:
        scheduler.close()

    snapshot = runtime.snapshot()
    assert watcher.timeout_writes == 0
    assert len(watcher.incident_errors) == 1
    assert isinstance(watcher.incident_errors[0], RuntimeError)
    assert snapshot.group_failure_count == 1
    assert snapshot.last_group_error_kind == "RuntimeError"


@pytest.mark.asyncio
@pytest.mark.parametrize("_repeat", range(3))
async def test_cancelled_committed_timeout_detaches_exact_fault_decision(
    tmp_path,
    monkeypatch,
    _repeat,
) -> None:
    committed = threading.Event()
    release = threading.Event()
    path = tmp_path / "cancelled-timeout-terminal.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    revision = _group("group-a")
    store.publish_group_revision(revision)
    identity = FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
    )
    authority = FaultAuthorityStore(path)
    base_ms = int(time.time() * 1_000)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=base_ms,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-cancelled-timeout",
            kind=FaultKind.CLOB_LATENCY,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key="group-a",
            parameters={"delay_ms": 100},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="7" * 64,
            authorization_digest="8" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted
    now = base_ms + 10

    def clock_ms():
        nonlocal now
        now += 1
        return now

    fault_runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    await fault_runtime.sync_before_batch()
    watcher_runtime = CandidateWatcherRuntime()
    watcher = CandidateWatcher(
        structure_reader=_Structure(revision, []),
        books_reader=_Books([]),
        store=store,
        runtime=watcher_runtime,
        interval_controller=IntervalController(),
        clock_ms=clock_ms,
        fault_runtime=fault_runtime,
    )
    original_writer = store.record_candidate_watch_fact

    def blocking_writer(*args, **kwargs):
        fact = original_writer(*args, **kwargs)
        committed.set()
        assert release.wait(timeout=2)
        return fact

    monkeypatch.setattr(store, "record_candidate_watch_fact", blocking_writer)
    scheduler = CandidateWatcherScheduler(
        watcher=watcher,
        store=store,
        candidate_group_ids=lambda: (),
        runtime=watcher_runtime,
        group_timeout_s=0.01,
        terminal_write_budget_s=5,
        fault_runtime=fault_runtime,
    )
    selected = asyncio.create_task(
        scheduler._run_selected_group(
            (0, 0, "group-a"),
            priority_by_rank={0: "high"},
            admission_contexts={},
        )
    )
    assert await asyncio.to_thread(committed.wait, 2)
    selected.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await selected
    scheduler.close()

    assert watcher._pending_latency_faults == {}
    assert watcher._timeout_contexts == {}
    assert watcher_runtime.group_attempt_count("group-a") == 1
    assert len(watcher._clob_incident_operations) == 1
    exact_error = watcher._clob_incident_operations[0].error
    assert getattr(exact_error, "_polyarb_fault_call_id") is not None

    await watcher.flush_incidents()
    assert authority.validate_history("fault-cancelled-timeout").events[-1].state.value == "cleaned"

    monkeypatch.setattr(store, "record_candidate_watch_fact", original_writer)
    await watcher.record_timeout("group-a")
    organic_error = watcher._clob_incident_operations[-1].error
    assert getattr(organic_error, "_polyarb_fault_call_id", None) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper_recovery", [False, True])
async def test_real_candidate_chain_recovers_from_new_exact_group_receipt(
    tmp_path,
    tamper_recovery: bool,
) -> None:
    path = tmp_path / "real-chain.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    revision = _group("group-a")
    store.publish_group_revision(revision)
    identity = FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )
    authority = FaultAuthorityStore(path)
    base_ms = int(time.time() * 1_000)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=base_ms,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-candidate",
            kind=FaultKind.CLOB_MISSING_LEG,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key="group-a",
            parameters={"leg_index": 0},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="d" * 64,
            authorization_digest="e" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted

    def clock_ms():
        return int(time.time() * 1_000)

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    reader = _Books([])
    watcher = CandidateWatcher(
        structure_reader=_Structure(revision, []),
        books_reader=reader,
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=clock_ms,
        fault_runtime=runtime,
    )

    failed = await watcher.run_once("group-a")
    await watcher.flush_incidents()
    time.sleep(0.01)
    recovered = await watcher.run_once("group-a")
    if tamper_recovery:
        with store._connect() as con:
            con.execute(
                "UPDATE neg_risk_candidate_success_receipts "
                "SET receipt_hash='tampered'"
            )
        with pytest.raises(ValueError, match="pending-owner-mutation"):
            await watcher.flush_incidents()
    else:
        await watcher.flush_incidents()

    assert failed.reason == "incomplete-quotes"
    assert recovered.status == "watching"
    history = authority.validate_history("fault-candidate")
    assert history.valid is True
    if tamper_recovery:
        assert history.events[-1].state.value == "evidence-invalid"
        assert runtime.degraded is True
        return
    assert [event.state.value for event in history.events] == [
        "authorized",
        "armed",
        "injected",
        "detected",
        "contained",
        "cleaned",
        "recovered",
    ]
    assert store.open_incidents() == ()
    with store._connect() as con:
        receipt = con.execute(
            "SELECT group_id,membership_hash,quote_batch_id,observed_at_ms "
            "FROM neg_risk_candidate_success_receipts ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert tuple(receipt[:2]) == ("group-a", revision.membership_hash)


@pytest.mark.asyncio
async def test_other_group_success_does_not_consume_pending_candidate_recovery(
    tmp_path,
) -> None:
    path = tmp_path / "exact-recovery-target.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    revisions = {group_id: _group(group_id) for group_id in ("group-a", "group-b")}
    for revision in revisions.values():
        store.publish_group_revision(revision)

    identity = FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
    )
    authority = FaultAuthorityStore(path)
    base_ms = int(time.time() * 1_000)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=base_ms,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-exact-target",
            kind=FaultKind.CLOB_MISSING_LEG,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key="group-a",
            parameters={"leg_index": 0},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="3" * 64,
            authorization_digest="4" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted

    class Structures:
        async def read_group(self, group_id):
            return revisions[group_id]

    def clock_ms():
        return int(time.time() * 1_000)

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    watcher = CandidateWatcher(
        structure_reader=Structures(),
        books_reader=_Books([]),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=clock_ms,
        fault_runtime=runtime,
    )

    assert (await watcher.run_once("group-a")).status == "unavailable"
    await watcher.flush_incidents()
    cleaned_history = authority.validate_history("fault-exact-target")
    assert cleaned_history.events[-1].state.value == "cleaned"

    assert (await watcher.run_once("group-b")).status == "watching"
    await watcher.flush_incidents()
    after_other_group = authority.validate_history("fault-exact-target")
    assert after_other_group.events[-1].state.value == "cleaned"
    assert len(after_other_group.events) == len(cleaned_history.events)
    assert runtime.pending_recovery_fault_id == "fault-exact-target"
    assert runtime.degraded is False

    assert (await watcher.run_once("group-a")).status == "watching"
    assert (await watcher.run_once("group-a")).status == "watching"
    await watcher.flush_incidents()
    recovered = authority.validate_history("fault-exact-target").events[-1]
    assert recovered.state.value == "recovered"
    assert recovered.evidence == {"recovery_id": "candidate-success-3"}


@pytest.mark.asyncio
async def test_cancelled_committed_candidate_recovery_is_idempotently_recorded(
    tmp_path,
) -> None:
    committed = threading.Event()
    release = threading.Event()
    path = tmp_path / "cancelled-recovery.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    revision = _group("group-a")
    store.publish_group_revision(revision)

    class BlockingAuthority(FaultAuthorityStore):
        def append_recovery_event(self, receipt, **kwargs):
            result = super().append_recovery_event(receipt, **kwargs)
            committed.set()
            assert release.wait(timeout=2)
            return result

    identity = FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
    )
    authority = BlockingAuthority(path)
    base_ms = int(time.time() * 1_000)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=base_ms,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-cancelled-recovery",
            kind=FaultKind.CLOB_MISSING_LEG,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key="group-a",
            parameters={"leg_index": 0},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="5" * 64,
            authorization_digest="6" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted
    now = base_ms + 10

    def clock_ms():
        nonlocal now
        now += 1
        return now

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    watcher = CandidateWatcher(
        structure_reader=_Structure(revision, []),
        books_reader=_Books([]),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=clock_ms,
        fault_runtime=runtime,
    )
    assert (await watcher.run_once("group-a")).status == "unavailable"
    await watcher.flush_incidents()
    assert (await watcher.run_once("group-a")).status == "watching"

    flush = asyncio.create_task(watcher.flush_incidents())
    assert await asyncio.to_thread(committed.wait, 2)
    flush.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await flush

    history = authority.validate_history("fault-cancelled-recovery")
    assert history.valid is True
    assert history.events[-1].state.value == "recovered"
    assert runtime.pending_recovery_fault_id is None
    event_count = len(history.events)

    await watcher.flush_incidents()
    retried = authority.validate_history("fault-cancelled-recovery")
    assert retried.valid is True
    assert retried.events[-1].state.value == "recovered"
    assert len(retried.events) == event_count
    assert runtime.degraded is False


@pytest.mark.asyncio
async def test_ambiguous_candidate_incident_freezes_fault_evidence(
    tmp_path,
) -> None:
    path = tmp_path / "ambiguous-real.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    revision = _group("group-a")
    store.publish_group_revision(revision)
    from polyarb.perception.clob_incidents import CandidateGroupIncidents
    from polyarb.routing.focused_quote_collector import QuoteCollectionIntegrityError

    base_ms = int(time.time() * 1_000)
    CandidateGroupIncidents(store, clock_ms=lambda: base_ms + 5).record_failure(
        "group-a",
        QuoteCollectionIntegrityError(),
    )
    identity = FaultRuntimeIdentity(
        component="candidate",
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
            fault_id="fault-ambiguous",
            kind=FaultKind.CLOB_MISSING_LEG,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key="group-a",
            parameters={"leg_index": 0},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="1" * 64,
            authorization_digest="2" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted
    now = base_ms + 10

    def clock_ms():
        nonlocal now
        now += 1
        return now

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    watcher = CandidateWatcher(
        structure_reader=_Structure(revision, []),
        books_reader=_Books([]),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=clock_ms,
        fault_runtime=runtime,
    )

    await watcher.run_once("group-a")
    await watcher.flush_incidents()

    assert runtime.degraded is True
    assert runtime.active_fault_id is None
    history = authority.validate_history("fault-ambiguous")
    assert history.valid is True
    assert history.events[-1].state.value == "evidence-invalid"


@pytest.mark.asyncio
async def test_candidate_recovery_evidence_failure_degrades_runtime(
    tmp_path,
) -> None:
    class Runtime(_Runtime):
        pending_recovery_fault_id = "fault-1"

        def make_recovery_receipt(self, *args, **kwargs):
            return None

    runtime = Runtime(FaultDecision(False))
    watcher, _, _ = _watcher(tmp_path, runtime=runtime, order=[])

    result = await watcher.run_once("group-a")
    await watcher.flush_incidents()

    assert result.status == "watching"
    assert runtime.degraded is True
    assert "invalid:candidate-recovery-evidence-invalid" in runtime.events


@pytest.mark.asyncio
async def test_candidate_incident_authority_unavailable_freezes_without_invalid_claim(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = _Runtime(_decision(FaultKind.CLOB_429))
    watcher, _, _ = _watcher(tmp_path, runtime=runtime, order=[])

    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        watcher._clob_incidents,
        "record_qualified_failure",
        unavailable,
    )
    await watcher.run_once("group-a")
    await watcher.flush_incidents()

    assert runtime.degraded is True
    assert "unavailable:candidate-incident-evidence-unavailable" in runtime.events
    assert not any(event.startswith("invalid:") for event in runtime.events)


@pytest.mark.parametrize(
    "corruption",
    [
        "receipt-hash",
        "membership-changed",
        "wrong-target",
        "cross-family",
        "stale-writer",
    ],
)
def test_candidate_recovery_db_validator_fails_closed(
    tmp_path,
    corruption: str,
) -> None:
    path = tmp_path / f"validator-{corruption}.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    revision = _group("group-a")
    store.publish_group_revision(revision)
    batch = GroupQuoteBatch.complete(
        group_id="group-a",
        membership_hash=revision.membership_hash,
        quote_batch_id="candidate-qb-1",
        started_at_ms=1_100,
        quoted_at_ms=1_200,
        legs=(
            GroupQuoteLeg("yes-1", revision.membership_hash, 0.4, 10, "executable"),
            GroupQuoteLeg("yes-2", revision.membership_hash, 0.5, 10, "executable"),
        ),
    )
    store.publish_candidate_success(
        batch,
        observed_at_ms=1_200,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="test",
        next_due_at_ms=16_200,
    )
    identity = FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
    )
    intent = FaultIntent(
        fault_id="fault-validator",
        kind=FaultKind.CLOB_MISSING_LEG,
        call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
        target_key="group-a",
        parameters={"leg_index": 0},
        ttl_ms=30_000,
        runtime=identity,
        nonce_digest="1" * 64,
        accepted_at_ms=1_000,
    )
    receipt = FaultRecoveryReceipt(
        fault_id=intent.fault_id,
        kind=intent.kind,
        call_class=intent.call_class,
        component="candidate",
        runtime=identity,
        writer=FaultRecoveryWriter.CANDIDATE_SUCCESS,
        writer_id=batch.quote_batch_id,
        writer_occurred_at_ms=1_200,
    )
    with store._connect() as con:
        assert (
            FaultAuthorityStore._validated_recovery_writer_id(
                con,
                receipt,
                intent,
            )
            == "candidate-success-1"
        )
    if corruption == "receipt-hash":
        with store._connect() as con:
            con.execute("UPDATE neg_risk_candidate_success_receipts SET receipt_hash='tampered'")
    elif corruption == "membership-changed":
        changed = GroupRevision.certified(
            group_id="group-a",
            event_id="event-group-a",
            revision=2,
            started_at_ms=1_300,
            observed_at_ms=1_400,
            source_cursor="next",
            legs=(
                GroupLeg("m-1", "c-1", "yes-1", "one"),
                GroupLeg("m-3", "c-3", "yes-3", "three"),
            ),
        )
        store.publish_group_revision(changed)
    elif corruption == "wrong-target":
        intent = replace(intent, target_key="group-b")
    elif corruption == "stale-writer":
        newer_batch = replace(
            batch,
            quote_batch_id="candidate-qb-2",
            started_at_ms=1_201,
            quoted_at_ms=1_300,
        )
        store.publish_candidate_success(
            newer_batch,
            observed_at_ms=1_300,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="test",
            next_due_at_ms=16_300,
        )
        newer_receipt = replace(
            receipt,
            writer_id=newer_batch.quote_batch_id,
            writer_occurred_at_ms=1_300,
        )
        with store._connect() as con:
            assert (
                FaultAuthorityStore._validated_recovery_writer_id(
                    con,
                    newer_receipt,
                    intent,
                )
                == "candidate-success-2"
            )
    else:
        receipt = replace(
            receipt,
            writer=FaultRecoveryWriter.RECONCILIATION_CHECKPOINT,
            writer_id="candidate-qb-1",
        )

    with store._connect() as con:
        assert (
            FaultAuthorityStore._validated_recovery_writer_id(
                con,
                receipt,
                intent,
            )
            is None
        )
