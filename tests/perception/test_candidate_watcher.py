from __future__ import annotations

import asyncio
import inspect
import sqlite3
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from py_clob_client.exceptions import PolyApiException

from polyarb.perception.candidate_watcher import (
    CandidateWatcher,
    CandidateWatcherRuntime,
    CandidateWatcherScheduler,
    IntervalController,
    next_interval_s,
)
from polyarb.perception.group_structure import GroupStructureReader
from polyarb.perception.models import CandidateWatchFact, GroupLeg, GroupRevision
from polyarb.perception.store import (
    CandidateAdmissionContext,
    OpportunityPerceptionStore,
)


def certified_group(
    group_id: str = "g-1",
    *,
    revision: int = 1,
    tokens: tuple[str, ...] = ("yes-1", "yes-2"),
) -> GroupRevision:
    return GroupRevision.certified(
        group_id=group_id,
        event_id="e-1",
        revision=revision,
        started_at_ms=900,
        observed_at_ms=1_000,
        source_cursor="cursor",
        legs=tuple(
            GroupLeg(f"m-{index}", f"c-{index}", token, f"Leg {index}")
            for index, token in enumerate(tokens, start=1)
        ),
    )


@dataclass
class SequenceStructureReader:
    revisions: list[GroupRevision]

    async def read_group(self, group_id: str) -> GroupRevision:
        revision = self.revisions.pop(0)
        assert revision.group_id == group_id
        return revision


class FakeBooksReader:
    def __init__(self, books: Sequence[dict[str, Any]]) -> None:
        self.books = books
        self.requests: list[tuple[str, ...]] = []
        self.projections: list[str] = []

    async def get_books(
        self,
        token_ids: list[str],
        *,
        projection: str = "full",
    ) -> Sequence[dict[str, Any]]:
        self.requests.append(tuple(token_ids))
        self.projections.append(projection)
        return self.books


def books(*values: tuple[str, str, str]) -> list[dict[str, Any]]:
    return [
        {"asset_id": token, "asks": [{"price": price, "size": size}]}
        for token, price, size in values
    ]


def watcher(
    tmp_path: Path,
    *,
    structure: tuple[GroupRevision, GroupRevision],
    reader: FakeBooksReader,
    clock_values: tuple[int, ...] = (2_000, 2_100),
) -> tuple[CandidateWatcher, OpportunityPerceptionStore]:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(structure[0])
    times = iter(clock_values)
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader(list(structure)),
        books_reader=reader,
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=lambda: next(times),
    )
    return candidate, store


@pytest.mark.asyncio
async def test_candidate_watcher_publishes_only_one_complete_group_batch(
    tmp_path: Path,
) -> None:
    structure = certified_group(tokens=("yes-1", "yes-2"))
    reader = FakeBooksReader(books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8")))
    candidate, store = watcher(
        tmp_path,
        structure=(structure, structure),
        reader=reader,
    )

    observation = await candidate.run_once("g-1")

    assert observation.status == "watching"
    assert observation.bundle_cost == 0.90
    assert observation.gross_edge_bps == 1_000
    assert observation.max_bundle_size == 8
    assert reader.requests == [("yes-1", "yes-2")]
    assert reader.projections == ["top"]
    batch = store.current_quote_batch("g-1", now_ms=2_100, max_age_ms=1_000)
    assert batch is not None
    assert batch.quote_batch_id == observation.quote_batch_id
    assert tuple(leg.yes_token_id for leg in batch.legs) == ("yes-1", "yes-2")


@pytest.mark.asyncio
async def test_attempt_start_is_first_run_once_operation_and_late_skips_io(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    operations: list[str] = []

    class Store(OpportunityPerceptionStore):
        def record_candidate_attempt_start(self, **_kwargs):
            operations.append("attempt-start")
            return CandidateWatchFact(
                id=1,
                group_id="g-1",
                membership_hash=revision.membership_hash,
                quote_batch_id=None,
                observed_at_ms=70_001,
                last_result="unavailable",
                reason="candidate-start-deadline-breached",
                bundle_cost=None,
                gross_edge_bps=None,
                max_bundle_size=None,
                priority_class="normal",
                consecutive_failures=1,
                effective_interval_s=60,
                schedule_reason="candidate-start-deadline-breached",
                next_due_at_ms=130_001,
            )

    class Structure:
        async def read_group(self, group_id: str) -> GroupRevision:
            operations.append("structure")
            return revision

    store = Store(tmp_path / "state.db")
    store.init_schema()
    candidate = CandidateWatcher(
        structure_reader=Structure(),
        books_reader=FakeBooksReader([]),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
    )
    admission = CandidateAdmissionContext(
        group_id="g-1",
        event_id="e-1",
        membership_hash=revision.membership_hash,
        promoted_at_ms=10_000,
        candidate_start_deadline_at_ms=70_000,
    )

    result = await candidate.run_once("g-1", admission_context=admission)

    assert result.reason == "candidate-start-deadline-breached"
    assert operations == ["attempt-start"]


@pytest.mark.asyncio
async def test_cancellation_during_attempt_start_waits_for_transaction(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    entered = threading.Event()
    release = threading.Event()
    structure_calls: list[str] = []

    class Store(OpportunityPerceptionStore):
        def record_candidate_attempt_start(self, **_kwargs):
            entered.set()
            assert release.wait(timeout=2)
            return None

    class Structure:
        async def read_group(self, group_id: str) -> GroupRevision:
            structure_calls.append(group_id)
            return revision

    store = Store(tmp_path / "state.db")
    store.init_schema()
    candidate = CandidateWatcher(
        structure_reader=Structure(),
        books_reader=FakeBooksReader([]),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
    )
    admission = CandidateAdmissionContext(
        group_id="g-1",
        event_id="e-1",
        membership_hash=revision.membership_hash,
        promoted_at_ms=10_000,
        candidate_start_deadline_at_ms=70_000,
    )
    task = asyncio.create_task(
        candidate.run_once("g-1", admission_context=admission)
    )
    assert await asyncio.to_thread(entered.wait, 2)

    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert structure_calls == []


@pytest.mark.asyncio
async def test_membership_change_during_quote_fails_closed(tmp_path: Path) -> None:
    before = certified_group(revision=1)
    after = certified_group(revision=2, tokens=("yes-1", "yes-3"))
    reader = FakeBooksReader(books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8")))
    candidate, store = watcher(
        tmp_path,
        structure=(before, after),
        reader=reader,
    )

    result = await candidate.run_once("g-1")

    assert result.status == "unavailable"
    assert result.reason == "structure-membership-changed"
    assert store.current_quote_batch("g-1", now_ms=2_100, max_age_ms=1_000) is None
    facts = store.candidate_watch_facts("g-1")
    assert len(facts) == 1
    assert facts[0].last_result == "unavailable"
    assert facts[0].priority_class == "high"
    assert facts[0].effective_interval_s == 30


@pytest.mark.asyncio
async def test_membership_supersession_at_success_commit_cannot_publish_positive_fact(
    tmp_path: Path,
) -> None:
    before = certified_group(revision=1)
    changed = certified_group(revision=2, tokens=("yes-1", "yes-3"))

    class RacingStore(OpportunityPerceptionStore):
        def publish_candidate_success(self, batch, **kwargs):
            self.publish_group_revision(changed)
            return super().publish_candidate_success(batch, **kwargs)

    store = RacingStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(before)
    runtime = CandidateWatcherRuntime()
    times = iter((2_000, 2_100))
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader([before, before]),
        books_reader=FakeBooksReader(
            books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8"))
        ),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(),
        clock_ms=lambda: next(times),
    )

    observation = await candidate.run_once("g-1")

    assert observation.status == "unavailable"
    assert store.current_quote_batch("g-1", now_ms=2_100, max_age_ms=1_000) is None
    facts = store.candidate_watch_facts("g-1")
    assert [fact.last_result for fact in facts] == ["unavailable"]


@pytest.mark.asyncio
async def test_positive_fact_failure_rolls_back_quote_batch(tmp_path: Path) -> None:
    revision = certified_group()

    class FactFailingStore(OpportunityPerceptionStore):
        def _insert_candidate_watch_fact(self, con, **kwargs):
            if kwargs["quote_batch_id"] is not None:
                raise RuntimeError("injected-positive-fact-failure")
            return super()._insert_candidate_watch_fact(
                con,
                **kwargs,
            )

    store = FactFailingStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(revision)
    times = iter((2_000, 2_100))
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision, revision]),
        books_reader=FakeBooksReader(
            books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8"))
        ),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=lambda: next(times),
    )

    observation = await candidate.run_once("g-1")

    assert observation.status == "unavailable"
    assert store.current_quote_batch("g-1", now_ms=2_100, max_age_ms=1_000) is None
    assert [fact.last_result for fact in store.candidate_watch_facts("g-1")] == [
        "unavailable"
    ]


@pytest.mark.asyncio
async def test_cancellation_after_atomic_commit_converges_runtime_before_reraise(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    committed = threading.Event()
    release = threading.Event()

    class BlockingReturnStore(OpportunityPerceptionStore):
        def publish_candidate_success(self, batch, **kwargs):
            fact = super().publish_candidate_success(batch, **kwargs)
            committed.set()
            assert release.wait(timeout=2)
            return fact

    store = BlockingReturnStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(revision)
    runtime = CandidateWatcherRuntime()
    times = iter((2_000, 2_100))
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision, revision]),
        books_reader=FakeBooksReader(
            books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8"))
        ),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(),
        clock_ms=lambda: next(times),
    )

    task = asyncio.create_task(candidate.run_once("g-1"))
    assert await asyncio.to_thread(committed.wait, 2)
    # First cancellation models the scheduler's group timeout; the second
    # models daemon shutdown while the non-cancellable DB thread is returning.
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.latest_candidate_watch_fact("g-1").last_result == "watching"
    snapshot = runtime.snapshot()
    assert snapshot.attempt_count == 1
    assert snapshot.last_result == "watching"


@pytest.mark.asyncio
async def test_repeated_cancellation_does_not_swallow_writer_error_or_cancellation(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    entered = threading.Event()
    release = threading.Event()

    class FailingStore(OpportunityPerceptionStore):
        def publish_candidate_success(self, batch, **kwargs):
            entered.set()
            assert release.wait(timeout=2)
            raise RuntimeError("writer-failed-after-cancellation")

    store = FailingStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(revision)
    runtime = CandidateWatcherRuntime()
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision, revision]),
        books_reader=FakeBooksReader(
            books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8"))
        ),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(),
        clock_ms=iter((2_000, 2_100)).__next__,
    )

    task = asyncio.create_task(candidate.run_once("g-1"))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert runtime.snapshot().attempt_count == 0
    assert store.candidate_watch_facts("g-1") == ()


@pytest.mark.asyncio
async def test_cancellation_after_unavailable_fact_commit_also_converges_runtime(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    committed = threading.Event()
    release = threading.Event()

    class BlockingReturnStore(OpportunityPerceptionStore):
        def record_candidate_watch_fact(self, **kwargs):
            fact = super().record_candidate_watch_fact(**kwargs)
            committed.set()
            assert release.wait(timeout=2)
            return fact

    store = BlockingReturnStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(revision)
    runtime = CandidateWatcherRuntime()
    times = iter((2_000, 2_100))
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision, revision]),
        books_reader=FakeBooksReader(books(("yes-1", "0.40", "10"))),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(),
        clock_ms=lambda: next(times),
    )

    task = asyncio.create_task(candidate.run_once("g-1"))
    assert await asyncio.to_thread(committed.wait, 2)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.latest_candidate_watch_fact("g-1").last_result == "unavailable"
    assert runtime.snapshot().last_result == "unavailable"


@pytest.mark.asyncio
async def test_incomplete_book_batch_records_one_terminal_fact(tmp_path: Path) -> None:
    structure = certified_group()
    reader = FakeBooksReader(books(("yes-1", "0.40", "10")))
    candidate, store = watcher(
        tmp_path,
        structure=(structure, structure),
        reader=reader,
    )

    result = await candidate.run_once("g-1")

    assert result.status == "unavailable"
    assert result.reason == "incomplete-quotes"
    assert store.current_quote_batch("g-1", now_ms=2_100, max_age_ms=1_000) is None
    assert len(store.candidate_watch_facts("g-1")) == 1


@pytest.mark.asyncio
async def test_missing_leg_incident_closes_only_after_same_group_success(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(revision)
    failed_at_ms = int(time.time() * 1_000)
    failed = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision, revision]),
        books_reader=FakeBooksReader(books(("yes-1", "0.40", "10"))),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=iter((failed_at_ms, failed_at_ms)).__next__,
    )

    observation = await failed.run_once("g-1")
    await failed.flush_incidents()

    assert observation.status == "unavailable"
    incident = store.open_incidents()[0]
    assert incident.scope == "candidate:g-1"
    assert incident.kind == "clob-missing-leg"
    assert incident.state == "recovering"

    time.sleep(0.01)
    recovered_at_ms = int(time.time() * 1_000)
    recovered = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision, revision]),
        books_reader=FakeBooksReader(
            books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8"))
        ),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=iter((recovered_at_ms, recovered_at_ms)).__next__,
    )
    success = await recovered.run_once("g-1")
    await recovered.flush_incidents()

    assert success.status == "watching"
    assert store.open_incidents() == ()


@pytest.mark.asyncio
async def test_sdk_429_records_group_scoped_clob_incident(tmp_path: Path) -> None:
    revision = certified_group()

    class RateLimitedBooks:
        async def get_books(self, _token_ids, *, projection="full"):
            request = httpx.Request("GET", "https://clob.example.test/books")
            raise PolyApiException(
                resp=httpx.Response(
                    429,
                    request=request,
                    json={"error": "rate"},
                )
            )

    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(revision)
    now_ms = int(time.time() * 1_000)
    candidate = CandidateWatcher(
        structure_reader=SequenceStructureReader([revision]),
        books_reader=RateLimitedBooks(),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        clock_ms=iter((now_ms, now_ms)).__next__,
    )

    observation = await candidate.run_once("g-1")
    await candidate.flush_incidents()

    assert observation.status == "unavailable"
    incident = store.open_incidents()[0]
    assert incident.scope == "candidate:g-1"
    assert incident.kind == "clob-429"


@pytest.mark.asyncio
async def test_record_timeout_opens_group_scoped_latency_incident(
    tmp_path: Path,
) -> None:
    revision = certified_group()
    candidate, store = watcher(
        tmp_path,
        structure=(revision, revision),
        reader=FakeBooksReader([]),
        clock_values=(int(time.time() * 1_000),),
    )

    await candidate.record_timeout("g-1")
    await candidate.flush_incidents()

    incident = store.open_incidents()[0]
    assert incident.scope == "candidate:g-1"
    assert incident.kind == "clob-latency"


@pytest.mark.asyncio
async def test_sqlite_busy_opens_group_incident_after_terminal_writer_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision = certified_group()
    candidate, store = watcher(
        tmp_path,
        structure=(revision, revision),
        reader=FakeBooksReader(
            books(("yes-1", "0.40", "10"), ("yes-2", "0.50", "8"))
        ),
        clock_values=(
            int(time.time() * 1_000),
            int(time.time() * 1_000),
        ),
    )

    async def locked_terminal_writer(**_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(candidate, "_record", locked_terminal_writer)
    runtime = candidate._runtime
    scheduler = CandidateWatcherScheduler(
        watcher=candidate,
        store=store,
        candidate_group_ids=lambda: ("g-1",),
        runtime=runtime,
        clock_ms=lambda: 2_000,
        cycle_max_groups=2,
        reserved_non_high_slots=1,
    )

    await scheduler.run_due_once()

    incident = store.open_incidents()[0]
    assert incident.scope == "candidate:g-1"
    assert incident.kind == "sqlite-busy"
    assert incident.state == "recovering"


def test_priority_policy_preserves_quote_freshness_and_anti_starvation() -> None:
    assert next_interval_s(priority="high", consecutive_failures=0) == 15
    assert next_interval_s(priority="normal", consecutive_failures=0) == 60
    assert next_interval_s(priority="explore", consecutive_failures=0) == 300
    assert next_interval_s(priority="high", consecutive_failures=3) <= 90


def test_candidate_hot_path_never_calls_all_known_token_subprocess() -> None:
    source = inspect.getsource(CandidateWatcher.run_once)
    assert "collect_quotes_in_subprocess" not in source


def test_interval_controller_inputs_are_configurable_and_backoff_is_bounded() -> None:
    controller = IntervalController(
        high_interval_s=10,
        normal_interval_s=40,
        explore_interval_s=180,
        quote_hard_stale_s=75,
    )

    transition = controller.transition(
        priority="high",
        consecutive_failures=4,
        observed_at_ms=1_000,
        last_result="unavailable",
    )

    assert transition.effective_interval_s == 75
    assert transition.next_due_at_ms == 76_000
    assert transition.reason == "failure-backoff-capped-by-hard-stale"


def test_huge_failure_count_clamps_and_persists_without_overflow(
    tmp_path: Path,
) -> None:
    transition = IntervalController().transition(
        priority="high",
        consecutive_failures=100_000,
        observed_at_ms=1_000,
        last_result="unavailable",
    )

    assert transition.effective_interval_s == 90
    assert transition.next_due_at_ms == 91_000
    assert transition.reason == "failure-backoff-capped-by-hard-stale"
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    fact = store.record_candidate_watch_fact(
        group_id="g-1",
        membership_hash=None,
        quote_batch_id=None,
        observed_at_ms=1_000,
        last_result="unavailable",
        reason="fixture",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority_class="high",
        consecutive_failures=100_000,
        effective_interval_s=transition.effective_interval_s,
        schedule_reason=transition.reason,
        next_due_at_ms=transition.next_due_at_ms,
    )
    assert fact.consecutive_failures == 100_000
    assert store.latest_candidate_watch_fact("g-1") == fact


@pytest.mark.asyncio
async def test_scheduler_uses_durable_due_time_and_prioritizes_high(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    runtime = CandidateWatcherRuntime()
    calls: list[str] = []

    class StubWatcher:
        async def run_once(self, group_id: str, **_kwargs):
            calls.append(group_id)

    store.record_candidate_watch_fact(
        group_id="normal",
        membership_hash=None,
        quote_batch_id=None,
        observed_at_ms=1_000,
        last_result="unavailable",
        reason="fixture",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority_class="normal",
        consecutive_failures=1,
        effective_interval_s=60,
        schedule_reason="normal-cadence",
        next_due_at_ms=2_000,
    )
    store.record_candidate_watch_fact(
        group_id="high",
        membership_hash=None,
        quote_batch_id=None,
        observed_at_ms=1_000,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=8,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="high-cadence",
        next_due_at_ms=2_000,
    )
    scheduler = CandidateWatcherScheduler(
        watcher=StubWatcher(),
        store=store,
        candidate_group_ids=lambda: ("normal", "high"),
        runtime=runtime,
        clock_ms=lambda: 2_000,
    )

    await scheduler.run_due_once()

    assert calls == ["high", "normal"]


@pytest.mark.asyncio
async def test_scheduler_supervises_cycle_failure_and_recovers_without_dying(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    runtime = CandidateWatcherRuntime()
    stop_event = asyncio.Event()
    calls = 0

    def candidate_group_ids() -> tuple[str, ...]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("candidate-source-temporary")
        stop_event.set()
        return ()

    scheduler = CandidateWatcherScheduler(
        watcher=object(),
        store=store,
        candidate_group_ids=candidate_group_ids,
        runtime=runtime,
        supervisor_retry_s=0.001,
    )

    await asyncio.wait_for(scheduler.run(stop_event), timeout=1)

    snapshot = runtime.snapshot()
    assert calls == 2
    assert snapshot.supervisor_failure_count == 1
    assert snapshot.supervisor_recovery_count == 1
    assert snapshot.supervisor_state == "running"
    assert snapshot.last_supervisor_error_kind is None


@pytest.mark.asyncio
async def test_candidate_resource_disabled_never_reads_decision(tmp_path: Path) -> None:
    class Store(OpportunityPerceptionStore):
        def latest_resource_decision(self, **_kwargs):
            raise AssertionError("disabled candidate consumed resource decision")

    store = Store(tmp_path / "state.db")
    store.init_schema()
    watcher = CandidateWatcher(
        structure_reader=object(),
        books_reader=object(),
        store=store,
        runtime=CandidateWatcherRuntime(),
        interval_controller=IntervalController(),
        require_resource_decision=False,
    )

    observation = await watcher._record(
        group_id="g-disabled",
        membership_hash=None,
        quote_batch_id=None,
        observed_at_ms=2_000,
        status="unavailable",
        reason="fixture",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority="normal",
        consecutive_failures=1,
    )

    assert observation.group_id == "g-disabled"


@pytest.mark.asyncio
async def test_unrelated_group_success_does_not_recover_failed_group_boundary(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    runtime = CandidateWatcherRuntime()
    failed_once = True

    class StubWatcher:
        async def run_once(self, group_id: str, **_kwargs):
            nonlocal failed_once
            if group_id == "failed" and failed_once:
                failed_once = False
                raise RuntimeError("failed-group")

        async def record_timeout(self, group_id: str) -> None:
            raise AssertionError(group_id)

    scheduler = CandidateWatcherScheduler(
        watcher=StubWatcher(),
        store=store,
        candidate_group_ids=lambda: ("failed", "other"),
        runtime=runtime,
        cycle_max_groups=2,
        reserved_non_high_slots=1,
    )

    await scheduler.run_due_once()
    after_unrelated_success = runtime.snapshot()
    assert after_unrelated_success.degraded_group_ids == ("failed",)
    assert after_unrelated_success.group_failure_count == 1
    assert after_unrelated_success.group_recovery_count == 0

    await scheduler.run_due_once()
    recovered = runtime.snapshot()
    assert recovered.degraded_group_ids == ()
    assert recovered.group_recovery_count == 1


@pytest.mark.asyncio
async def test_reserved_slots_prevent_stuck_high_candidates_from_starving_lower_lanes(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    runtime = CandidateWatcherRuntime()
    calls: list[str] = []

    class StubWatcher:
        async def run_once(self, group_id: str, **_kwargs):
            calls.append(group_id)
            if group_id.startswith("high"):
                await asyncio.Event().wait()

        async def record_timeout(self, group_id: str) -> None:
            calls.append(f"timeout:{group_id}")

    for group_id, priority in (
        ("high-1", "high"),
        ("high-2", "high"),
        ("high-3", "high"),
        ("normal-1", "normal"),
        ("explore-1", "explore"),
    ):
        store.record_candidate_watch_fact(
            group_id=group_id,
            membership_hash=None,
            quote_batch_id=None,
            observed_at_ms=1_000,
            last_result="unavailable",
            reason="fixture",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            priority_class=priority,
            consecutive_failures=1,
            effective_interval_s=15,
            schedule_reason="fixture-due",
            next_due_at_ms=2_000,
        )
    scheduler = CandidateWatcherScheduler(
        watcher=StubWatcher(),
        store=store,
        candidate_group_ids=lambda: (
            "high-1",
            "high-2",
            "high-3",
            "normal-1",
            "explore-1",
        ),
        runtime=runtime,
        clock_ms=lambda: 2_000,
        cycle_max_groups=3,
        reserved_non_high_slots=2,
        group_timeout_s=0.01,
    )

    await asyncio.wait_for(scheduler.run_due_once(), timeout=0.2)

    assert "normal-1" in calls
    assert "explore-1" in calls
    assert sum(group.startswith("high") for group in calls) == 1


def test_single_reserved_slot_rotates_between_normal_and_explore(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    scheduler = CandidateWatcherScheduler(
        watcher=object(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        cycle_max_groups=2,
        reserved_non_high_slots=1,
    )
    due = [
        (0, 1_000, "high"),
        (1, 1_000, "normal"),
        (2, 1_000, "explore"),
    ]

    first = scheduler._select_cycle(due)
    second = scheduler._select_cycle(due)

    assert [item[2] for item in first] == ["high", "normal"]
    assert [item[2] for item in second] == ["high", "explore"]


def test_cycle_interleaves_reserved_lanes_after_configured_high_burst(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    scheduler = CandidateWatcherScheduler(
        watcher=object(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        cycle_max_groups=6,
        reserved_non_high_slots=2,
        high_burst_groups=1,
        group_timeout_s=30,
        lower_lane_max_wait_s=120,
    )
    due = [
        (0, 1_000, "high-1"),
        (0, 1_000, "high-2"),
        (0, 1_000, "high-3"),
        (0, 1_000, "high-4"),
        (1, 1_000, "normal"),
        (2, 1_000, "explore"),
    ]

    selected = scheduler._select_cycle(due)

    assert [item[2] for item in selected] == [
        "high-1",
        "normal",
        "explore",
        "high-2",
        "high-3",
        "high-4",
    ]


@pytest.mark.asyncio
async def test_more_stalled_highs_than_workers_cannot_delay_reserved_lanes_to_bound(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    revisions = {
        group_id: certified_group(
            group_id,
            tokens=(f"{group_id}-yes-1", f"{group_id}-yes-2"),
        )
        for group_id in (
            "high-1",
            "high-2",
            "high-3",
            "high-4",
            "normal-1",
            "explore-1",
        )
    }
    for revision in revisions.values():
        store.publish_group_revision(revision)
    for group_id, priority in (
        ("high-1", "high"),
        ("high-2", "high"),
        ("high-3", "high"),
        ("high-4", "high"),
        ("normal-1", "normal"),
        ("explore-1", "explore"),
    ):
        store.record_candidate_watch_fact(
            group_id=group_id,
            membership_hash=revisions[group_id].membership_hash,
            quote_batch_id=None,
            observed_at_ms=1_000,
            last_result="unavailable",
            reason="fixture",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            priority_class=priority,
            consecutive_failures=1,
            effective_interval_s=1,
            schedule_reason="fixture-due",
            next_due_at_ms=2_000,
        )

    release_high = threading.Event()
    all_high_started = threading.Event()
    high_started_count = 0
    lower_started_at: float | None = None
    high_count_when_lower_started: int | None = None
    high_started_lock = threading.Lock()
    high_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="test-high-clob")
    lower_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-lower-clob")

    class ExecutorReader:
        def __init__(self, executor, *, blocking: bool) -> None:
            self.executor = executor
            self.blocking = blocking

        async def get_books(self, token_ids, *, projection="full"):
            assert projection == "top"

            def fetch():
                nonlocal high_count_when_lower_started
                nonlocal high_started_count
                nonlocal lower_started_at
                if self.blocking:
                    with high_started_lock:
                        high_started_count += 1
                        if high_started_count == 2:
                            all_high_started.set()
                    assert release_high.wait(timeout=2)
                else:
                    lower_started_at = time.monotonic()
                    with high_started_lock:
                        high_count_when_lower_started = high_started_count
                return books(
                    (token_ids[0], "0.40", "10"),
                    (token_ids[1], "0.50", "8"),
                )

            return await asyncio.get_running_loop().run_in_executor(
                self.executor,
                fetch,
            )

    runtime = CandidateWatcherRuntime()
    candidate = CandidateWatcher(
        structure_reader=GroupStructureReader(store),
        books_reader=ExecutorReader(high_pool, blocking=True),
        lower_priority_books_reader=ExecutorReader(lower_pool, blocking=False),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(
            high_interval_s=0.01,
            normal_interval_s=0.01,
            explore_interval_s=0.01,
            quote_hard_stale_s=0.05,
        ),
        clock_ms=lambda: 3_000,
    )
    closed: list[str] = []
    scheduler = CandidateWatcherScheduler(
        watcher=candidate,
        store=store,
        candidate_group_ids=lambda: tuple(revisions),
        runtime=runtime,
        clock_ms=lambda: 2_000,
        cycle_max_groups=6,
        reserved_non_high_slots=2,
        group_timeout_s=0.02,
        high_burst_groups=1,
        lower_lane_max_wait_s=0.05,
        close_callbacks=(
            lambda: closed.append("closed"),
        ),
    )

    started_at = time.monotonic()
    await asyncio.wait_for(scheduler.run_due_once(), timeout=0.5)
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.3
    assert all_high_started.is_set()
    assert lower_started_at is not None
    assert lower_started_at - started_at < 0.05
    assert high_count_when_lower_started == 1
    assert store.latest_candidate_watch_fact("high-1").reason == "candidate-group-timeout"
    assert store.latest_candidate_watch_fact("high-2").reason == "candidate-group-timeout"
    assert store.latest_candidate_watch_fact("high-3").reason == "candidate-group-timeout"
    assert store.latest_candidate_watch_fact("high-4").reason == "candidate-group-timeout"
    assert store.latest_candidate_watch_fact("normal-1").last_result == "watching"
    assert store.latest_candidate_watch_fact("explore-1").last_result == "watching"
    assert runtime.snapshot().degraded_group_ids == (
        "high-1",
        "high-2",
        "high-3",
        "high-4",
    )

    stop_event = asyncio.Event()
    stop_event.set()
    await scheduler.run(stop_event)
    assert closed == ["closed"]
    release_high.set()
    await asyncio.to_thread(high_pool.shutdown, True, cancel_futures=True)
    await asyncio.to_thread(lower_pool.shutdown, True, cancel_futures=True)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"high_interval_s": float("inf")},
        {"normal_interval_s": float("nan")},
        {"quote_hard_stale_s": float("inf")},
        {"high_interval_s": 91, "quote_hard_stale_s": 90},
    ],
)
def test_interval_controller_rejects_non_finite_or_impossible_freshness(
    kwargs: dict[str, float],
) -> None:
    with pytest.raises(ValueError):
        IntervalController(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"group_timeout_s": float("inf")},
        {"poll_interval_s": float("nan")},
        {"cycle_max_groups": 2, "reserved_non_high_slots": 2},
        {
            "group_timeout_s": 40,
            "high_burst_groups": 3,
            "lower_lane_max_wait_s": 120,
        },
        {
            "cycle_max_groups": 12,
            "reserved_non_high_slots": 2,
        },
    ],
)
def test_scheduler_rejects_non_finite_or_silently_reduced_inputs(
    tmp_path: Path,
    kwargs: dict[str, float | int],
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with pytest.raises(ValueError):
        CandidateWatcherScheduler(
            watcher=object(),
            store=store,
            candidate_group_ids=lambda: (),
            runtime=CandidateWatcherRuntime(),
            **kwargs,
        )


@pytest.mark.asyncio
async def test_scheduler_bounds_slow_candidate_source_on_isolated_executor(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()

    def slow_source() -> tuple[str, ...]:
        time.sleep(0.1)
        return ("g-1",)

    scheduler = CandidateWatcherScheduler(
        watcher=object(),
        store=store,
        candidate_group_ids=slow_source,
        runtime=CandidateWatcherRuntime(),
        selection_budget_s=0.01,
    )
    started = time.monotonic()

    with pytest.raises(TimeoutError):
        await scheduler.run_due_once()

    assert time.monotonic() - started < 0.08
    scheduler.close()


@pytest.mark.asyncio
async def test_scheduler_reads_many_candidates_in_one_bounded_store_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    calls: list[str] = []

    class Watcher:
        async def run_once(
            self, group_id: str, *, priority_hint: str, admission_context=None
        ) -> None:
            calls.append(group_id)

    def forbidden(*args, **kwargs):
        raise AssertionError("per-group read is forbidden")

    monkeypatch.setattr(store, "latest_candidate_watch_fact", forbidden)
    scheduler = CandidateWatcherScheduler(
        watcher=Watcher(),
        store=store,
        candidate_group_ids=lambda: tuple(f"g-{index:03}" for index in range(200)),
        runtime=CandidateWatcherRuntime(),
        source_max_groups=250,
    )

    await scheduler.run_due_once()

    assert len(calls) == 12
