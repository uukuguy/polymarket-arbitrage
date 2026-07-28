from __future__ import annotations

import inspect
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
