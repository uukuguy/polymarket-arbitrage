from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from polyarb.perception.candidate_watcher import (
    CandidateWatcher,
    CandidateWatcherRuntime,
    CandidateWatcherScheduler,
    IntervalController,
    next_interval_s,
)
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore


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
        @staticmethod
        def _insert_candidate_watch_fact(con, **kwargs):
            if kwargs["quote_batch_id"] is not None:
                raise RuntimeError("injected-positive-fact-failure")
            return OpportunityPerceptionStore._insert_candidate_watch_fact(
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
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.latest_candidate_watch_fact("g-1").last_result == "watching"
    snapshot = runtime.snapshot()
    assert snapshot.attempt_count == 1
    assert snapshot.last_result == "watching"


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
        async def run_once(self, group_id: str):
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
async def test_reserved_slots_prevent_stuck_high_candidates_from_starving_lower_lanes(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    runtime = CandidateWatcherRuntime()
    calls: list[str] = []

    class StubWatcher:
        async def run_once(self, group_id: str):
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

    assert {item[2] for item in first} == {"high", "normal"}
    assert {item[2] for item in second} == {"high", "explore"}
