"""Group-certified, observer-only Candidate Watcher hot path."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from loguru import logger

from polyarb.perception.group_structure import GroupStructureUnavailableError
from polyarb.perception.models import (
    CandidatePriority,
    CandidateResult,
    CandidateWatchFact,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.focused_quote_collector import (
    BooksReader,
    QuoteCollectionIntegrityError,
    build_complete_group_quote_batch,
)


class StructureReader(Protocol):
    async def read_group(self, group_id: str) -> GroupRevision: ...


@dataclass(frozen=True)
class CandidateObservation:
    group_id: str
    membership_hash: str | None
    quote_batch_id: str | None
    status: CandidateResult
    reason: str | None
    bundle_cost: float | None
    gross_edge_bps: float | None
    max_bundle_size: float | None
    observed_at_ms: int
    priority_class: CandidatePriority
    consecutive_failures: int
    effective_interval_s: float
    next_due_at_ms: int
    schedule_reason: str


@dataclass(frozen=True)
class SchedulingTransition:
    priority_class: CandidatePriority
    effective_interval_s: float
    next_due_at_ms: int
    reason: str


@dataclass(frozen=True)
class CandidateWatcherSnapshot:
    attempt_count: int
    success_count: int
    failure_count: int
    last_group_id: str | None
    last_result: CandidateResult | None
    last_observed_at_ms: int | None
    next_due_at_ms: int | None
    priority_class: CandidatePriority | None
    effective_interval_s: float | None
    schedule_reason: str | None


class CandidateWatcherRuntime:
    """Process-local projection mutated from the same persisted terminal fact."""

    def __init__(self) -> None:
        self._snapshot = CandidateWatcherSnapshot(
            attempt_count=0,
            success_count=0,
            failure_count=0,
            last_group_id=None,
            last_result=None,
            last_observed_at_ms=None,
            next_due_at_ms=None,
            priority_class=None,
            effective_interval_s=None,
            schedule_reason=None,
        )

    def record(self, fact: CandidateWatchFact) -> None:
        previous = self._snapshot
        success = fact.last_result in {"watching", "no-edge"}
        self._snapshot = CandidateWatcherSnapshot(
            attempt_count=previous.attempt_count + 1,
            success_count=previous.success_count + int(success),
            failure_count=previous.failure_count + int(not success),
            last_group_id=fact.group_id,
            last_result=fact.last_result,
            last_observed_at_ms=fact.observed_at_ms,
            next_due_at_ms=fact.next_due_at_ms,
            priority_class=fact.priority_class,
            effective_interval_s=fact.effective_interval_s,
            schedule_reason=fact.schedule_reason,
        )

    def snapshot(self) -> CandidateWatcherSnapshot:
        return self._snapshot


@dataclass(frozen=True)
class IntervalController:
    high_interval_s: float = 15.0
    normal_interval_s: float = 60.0
    explore_interval_s: float = 300.0
    quote_hard_stale_s: float = 90.0

    def transition(
        self,
        *,
        priority: CandidatePriority,
        consecutive_failures: int,
        observed_at_ms: int,
        last_result: CandidateResult,
    ) -> SchedulingTransition:
        base = {
            "high": self.high_interval_s,
            "normal": self.normal_interval_s,
            "explore": self.explore_interval_s,
        }[priority]
        interval = base * (2**consecutive_failures)
        if consecutive_failures == 0:
            reason = f"{priority}-cadence"
        else:
            failure_cap = (
                max(self.explore_interval_s, self.quote_hard_stale_s)
                if priority == "explore"
                else self.quote_hard_stale_s
            )
            if interval >= failure_cap:
                interval = failure_cap
                reason = "failure-backoff-capped-by-hard-stale"
            else:
                reason = "bounded-failure-backoff"
        # Persisted scheduling decisions use milliseconds so restart behavior
        # is independent of process-local timers.
        next_due_at_ms = observed_at_ms + int(interval * 1_000)
        return SchedulingTransition(
            priority_class=priority,
            effective_interval_s=float(interval),
            next_due_at_ms=next_due_at_ms,
            reason=reason,
        )


def next_interval_s(
    *,
    priority: CandidatePriority,
    consecutive_failures: int,
    high_interval_s: float = 15.0,
    normal_interval_s: float = 60.0,
    explore_interval_s: float = 300.0,
    quote_hard_stale_s: float = 90.0,
) -> float:
    """Compatibility helper that makes every initial cadence an input."""
    return IntervalController(
        high_interval_s=high_interval_s,
        normal_interval_s=normal_interval_s,
        explore_interval_s=explore_interval_s,
        quote_hard_stale_s=quote_hard_stale_s,
    ).transition(
        priority=priority,
        consecutive_failures=consecutive_failures,
        observed_at_ms=0,
        last_result="unavailable" if consecutive_failures else "watching",
    ).effective_interval_s


class CandidateWatcher:
    """Collect one complete ordered group and publish only certified evidence."""

    def __init__(
        self,
        *,
        structure_reader: StructureReader,
        books_reader: BooksReader,
        store: OpportunityPerceptionStore,
        runtime: CandidateWatcherRuntime,
        interval_controller: IntervalController,
        clock_ms: Callable[[], int] | None = None,
        min_edge_bps: float = 100.0,
    ) -> None:
        self._structure_reader = structure_reader
        self._books_reader = books_reader
        self._store = store
        self._runtime = runtime
        self._interval_controller = interval_controller
        self._clock_ms = clock_ms or _wall_clock_ms
        self._min_edge_bps = Decimal(str(min_edge_bps))

    async def run_once(self, group_id: str) -> CandidateObservation:
        started_at_ms = self._clock_ms()
        observed_at_ms: int | None = None
        before: GroupRevision | None = None
        try:
            before = await self._structure_reader.read_group(group_id)
            books = await self._books_reader.get_books(
                [leg.yes_token_id for leg in before.legs],
                projection="top",
            )
            after = await self._structure_reader.read_group(group_id)
            observed_at_ms = self._clock_ms()
            if after.membership_hash != before.membership_hash:
                return await self._record_unavailable(
                    group_id=group_id,
                    before=before,
                    observed_at_ms=observed_at_ms,
                    reason="structure-membership-changed",
                )
            batch = build_complete_group_quote_batch(
                before,
                books,
                started_at_ms=started_at_ms,
                quoted_at_ms=observed_at_ms,
            )
            await asyncio.to_thread(self._store.publish_quote_batch, batch)
            bundle_cost = sum(
                (Decimal(str(leg.best_ask_price)) for leg in batch.legs),
                Decimal(0),
            )
            edge = (Decimal(1) - bundle_cost) * Decimal(10_000)
            status: CandidateResult = (
                "watching" if edge >= self._min_edge_bps else "no-edge"
            )
            priority: CandidatePriority = "high" if status == "watching" else "normal"
            return await self._record(
                group_id=group_id,
                membership_hash=before.membership_hash,
                quote_batch_id=batch.quote_batch_id,
                observed_at_ms=observed_at_ms,
                status=status,
                reason=None,
                bundle_cost=float(bundle_cost),
                gross_edge_bps=float(edge),
                max_bundle_size=min(leg.best_ask_size for leg in batch.legs),
                priority=priority,
                consecutive_failures=0,
            )
        except asyncio.CancelledError:
            raise
        except QuoteCollectionIntegrityError:
            return await self._record_unavailable(
                group_id=group_id,
                before=before,
                observed_at_ms=(
                    observed_at_ms
                    if observed_at_ms is not None
                    else self._clock_ms()
                ),
                reason="incomplete-quotes",
            )
        except GroupStructureUnavailableError:
            return await self._record_unavailable(
                group_id=group_id,
                before=before,
                observed_at_ms=(
                    observed_at_ms
                    if observed_at_ms is not None
                    else self._clock_ms()
                ),
                reason="group-not-certified",
            )
        except Exception as error:
            logger.warning(
                "candidate group collection failed "
                f"group_id={group_id} kind={type(error).__name__}"
            )
            return await self._record_unavailable(
                group_id=group_id,
                before=before,
                observed_at_ms=(
                    observed_at_ms
                    if observed_at_ms is not None
                    else self._clock_ms()
                ),
                reason="candidate-collection-failed",
            )

    async def _record_unavailable(
        self,
        *,
        group_id: str,
        before: GroupRevision | None,
        observed_at_ms: int,
        reason: str,
    ) -> CandidateObservation:
        previous = await asyncio.to_thread(
            self._store.latest_candidate_watch_fact,
            group_id,
        )
        consecutive_failures = (
            previous.consecutive_failures + 1
            if previous is not None and previous.last_result == "unavailable"
            else 1
        )
        # A transient failure never demotes a known candidate into the slower
        # exploration lane. Preserve its prior class; a newly promoted
        # candidate starts high so the first retry remains freshness-bounded.
        priority: CandidatePriority = (
            previous.priority_class if previous is not None else "high"
        )
        return await self._record(
            group_id=group_id,
            membership_hash=None if before is None else before.membership_hash,
            quote_batch_id=None,
            observed_at_ms=observed_at_ms,
            status="unavailable",
            reason=reason,
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            priority=priority,
            consecutive_failures=consecutive_failures,
        )

    async def _record(
        self,
        *,
        group_id: str,
        membership_hash: str | None,
        quote_batch_id: str | None,
        observed_at_ms: int,
        status: CandidateResult,
        reason: str | None,
        bundle_cost: float | None,
        gross_edge_bps: float | None,
        max_bundle_size: float | None,
        priority: CandidatePriority,
        consecutive_failures: int,
    ) -> CandidateObservation:
        transition = self._interval_controller.transition(
            priority=priority,
            consecutive_failures=consecutive_failures,
            observed_at_ms=observed_at_ms,
            last_result=status,
        )
        fact = await asyncio.to_thread(
            self._store.record_candidate_watch_fact,
            group_id=group_id,
            membership_hash=membership_hash,
            quote_batch_id=quote_batch_id,
            observed_at_ms=observed_at_ms,
            last_result=status,
            reason=reason,
            bundle_cost=bundle_cost,
            gross_edge_bps=gross_edge_bps,
            max_bundle_size=max_bundle_size,
            priority_class=transition.priority_class,
            consecutive_failures=consecutive_failures,
            effective_interval_s=transition.effective_interval_s,
            schedule_reason=transition.reason,
            next_due_at_ms=transition.next_due_at_ms,
        )
        self._runtime.record(fact)
        return CandidateObservation(
            group_id=group_id,
            membership_hash=membership_hash,
            quote_batch_id=quote_batch_id,
            status=status,
            reason=reason,
            bundle_cost=bundle_cost,
            gross_edge_bps=gross_edge_bps,
            max_bundle_size=max_bundle_size,
            observed_at_ms=observed_at_ms,
            priority_class=transition.priority_class,
            consecutive_failures=consecutive_failures,
            effective_interval_s=transition.effective_interval_s,
            next_due_at_ms=transition.next_due_at_ms,
            schedule_reason=transition.reason,
        )


class CandidateGroupIds(Protocol):
    def __call__(self) -> Sequence[str]: ...


class CandidateWatcherScheduler:
    """Restart-safe due scheduler; Candidate freshness wins equal due times."""

    def __init__(
        self,
        *,
        watcher: CandidateWatcher,
        store: OpportunityPerceptionStore,
        candidate_group_ids: CandidateGroupIds,
        runtime: CandidateWatcherRuntime,
        clock_ms: Callable[[], int] | None = None,
        poll_interval_s: float = 1.0,
    ) -> None:
        self._watcher = watcher
        self._store = store
        self._candidate_group_ids = candidate_group_ids
        self._runtime = runtime
        self._clock_ms = clock_ms or _wall_clock_ms
        self._poll_interval_s = poll_interval_s

    @property
    def runtime(self) -> CandidateWatcherRuntime:
        return self._runtime

    async def run_due_once(self) -> None:
        group_ids = tuple(await asyncio.to_thread(self._candidate_group_ids))
        now_ms = self._clock_ms()
        due: list[tuple[int, int, str]] = []
        rank = {"high": 0, "normal": 1, "explore": 2}
        for group_id in group_ids:
            fact = await asyncio.to_thread(
                self._store.latest_candidate_watch_fact,
                group_id,
            )
            if fact is None:
                due.append((0, 0, group_id))
            elif fact.next_due_at_ms <= now_ms:
                due.append((rank[fact.priority_class], fact.next_due_at_ms, group_id))
        for _, _, group_id in sorted(due):
            await self._watcher.run_once(group_id)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.run_due_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval_s)
            except TimeoutError:
                continue


def _wall_clock_ms() -> int:
    return int(time.time() * 1_000)


def build_production_candidate_watcher(
    settings: Any,
    *,
    candidate_group_ids: CandidateGroupIds,
) -> CandidateWatcherScheduler:
    """Build the opt-in sibling worker from existing read-only CLOB seams."""
    from polyarb.clients.clob_client import ClobReaderClient
    from polyarb.perception.group_structure import GroupStructureReader

    store = OpportunityPerceptionStore(settings.db_path)
    store.init_schema()
    runtime = CandidateWatcherRuntime()
    watcher = CandidateWatcher(
        structure_reader=GroupStructureReader(store),
        books_reader=ClobReaderClient(settings),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(
            high_interval_s=settings.candidate_high_interval_s,
            normal_interval_s=settings.candidate_normal_interval_s,
            explore_interval_s=settings.candidate_explore_interval_s,
            quote_hard_stale_s=settings.candidate_quote_hard_stale_s,
        ),
        min_edge_bps=settings.neg_risk_observe_min_edge_bps,
    )
    return CandidateWatcherScheduler(
        watcher=watcher,
        store=store,
        candidate_group_ids=candidate_group_ids,
        runtime=runtime,
    )
