"""Group-certified, observer-only Candidate Watcher hot path."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Literal, Protocol

from loguru import logger

from polyarb.perception.group_structure import GroupStructureUnavailableError
from polyarb.perception.models import (
    CandidatePriority,
    CandidateResult,
    CandidateWatchFact,
    GroupQuoteBatch,
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
    supervisor_failure_count: int
    supervisor_recovery_count: int
    supervisor_state: Literal["running", "degraded"]
    last_supervisor_error_kind: str | None
    group_failure_count: int
    group_recovery_count: int
    degraded_group_ids: tuple[str, ...]
    last_group_error_kind: str | None


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
            supervisor_failure_count=0,
            supervisor_recovery_count=0,
            supervisor_state="running",
            last_supervisor_error_kind=None,
            group_failure_count=0,
            group_recovery_count=0,
            degraded_group_ids=(),
            last_group_error_kind=None,
        )
        self._group_errors: dict[str, str] = {}

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
            supervisor_failure_count=previous.supervisor_failure_count,
            supervisor_recovery_count=previous.supervisor_recovery_count,
            supervisor_state=previous.supervisor_state,
            last_supervisor_error_kind=previous.last_supervisor_error_kind,
            group_failure_count=previous.group_failure_count,
            group_recovery_count=previous.group_recovery_count,
            degraded_group_ids=previous.degraded_group_ids,
            last_group_error_kind=previous.last_group_error_kind,
        )

    def snapshot(self) -> CandidateWatcherSnapshot:
        return self._snapshot

    def record_supervisor_failure(self, error: BaseException) -> None:
        previous = self._snapshot
        self._snapshot = replace(
            previous,
            supervisor_failure_count=previous.supervisor_failure_count + 1,
            supervisor_state="degraded",
            last_supervisor_error_kind=type(error).__name__,
        )

    def record_supervisor_recovery(self) -> None:
        previous = self._snapshot
        if previous.supervisor_state != "degraded":
            return
        self._snapshot = replace(
            previous,
            supervisor_recovery_count=previous.supervisor_recovery_count + 1,
            supervisor_state="running",
            last_supervisor_error_kind=None,
        )

    def record_group_failure(self, group_id: str, error: BaseException) -> None:
        self._group_errors[group_id] = type(error).__name__
        previous = self._snapshot
        self._snapshot = replace(
            previous,
            group_failure_count=previous.group_failure_count + 1,
            degraded_group_ids=tuple(sorted(self._group_errors)),
            last_group_error_kind=type(error).__name__,
        )

    def record_group_success(self, group_id: str) -> None:
        if group_id not in self._group_errors:
            return
        del self._group_errors[group_id]
        previous = self._snapshot
        self._snapshot = replace(
            previous,
            group_recovery_count=previous.group_recovery_count + 1,
            degraded_group_ids=tuple(sorted(self._group_errors)),
            last_group_error_kind=(
                self._group_errors[next(reversed(self._group_errors))]
                if self._group_errors
                else None
            ),
        )


@dataclass(frozen=True)
class IntervalController:
    high_interval_s: float = 15.0
    normal_interval_s: float = 60.0
    explore_interval_s: float = 300.0
    quote_hard_stale_s: float = 90.0

    def __post_init__(self) -> None:
        values = (
            self.high_interval_s,
            self.normal_interval_s,
            self.explore_interval_s,
            self.quote_hard_stale_s,
        )
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("candidate-intervals-must-be-positive-finite")
        if self.high_interval_s > self.quote_hard_stale_s:
            raise ValueError("candidate-high-interval-exceeds-hard-stale")

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
        failure_cap = (
            max(self.explore_interval_s, self.quote_hard_stale_s)
            if priority == "explore"
            else self.quote_hard_stale_s
        )
        if consecutive_failures <= 0:
            interval = base
        elif base >= failure_cap:
            interval = failure_cap
        else:
            capped_exponent = min(
                consecutive_failures,
                max(0, math.ceil(math.log2(failure_cap / base))),
            )
            interval = min(base * (2**capped_exponent), failure_cap)
        if consecutive_failures == 0:
            reason = f"{priority}-cadence"
        else:
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
        lower_priority_books_reader: BooksReader | None = None,
        store: OpportunityPerceptionStore,
        runtime: CandidateWatcherRuntime,
        interval_controller: IntervalController,
        clock_ms: Callable[[], int] | None = None,
        min_edge_bps: float = 100.0,
    ) -> None:
        self._structure_reader = structure_reader
        self._books_reader = books_reader
        self._lower_priority_books_reader = (
            lower_priority_books_reader or books_reader
        )
        self._store = store
        self._runtime = runtime
        self._interval_controller = interval_controller
        self._clock_ms = clock_ms or _wall_clock_ms
        self._min_edge_bps = Decimal(str(min_edge_bps))

    async def run_once(
        self,
        group_id: str,
        *,
        priority_hint: CandidatePriority = "high",
    ) -> CandidateObservation:
        started_at_ms = self._clock_ms()
        observed_at_ms: int | None = None
        before: GroupRevision | None = None
        try:
            before = await self._structure_reader.read_group(group_id)
            books_reader = (
                self._books_reader
                if priority_hint == "high"
                else self._lower_priority_books_reader
            )
            books = await books_reader.get_books(
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
                batch=batch,
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
        batch: GroupQuoteBatch | None = None,
    ) -> CandidateObservation:
        transition = self._interval_controller.transition(
            priority=priority,
            consecutive_failures=consecutive_failures,
            observed_at_ms=observed_at_ms,
            last_result=status,
        )
        terminal_fields = dict(
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
        if batch is not None:
            await self._commit_terminal_fact(
                self._store.publish_candidate_success,
                batch,
                **terminal_fields,
            )
        else:
            await self._commit_terminal_fact(
                self._store.record_candidate_watch_fact,
                group_id=group_id,
                membership_hash=membership_hash,
                quote_batch_id=quote_batch_id,
                **terminal_fields,
            )
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

    async def _commit_terminal_fact(
        self,
        writer: Callable[..., CandidateWatchFact],
        *args: Any,
        **kwargs: Any,
    ) -> CandidateWatchFact:
        """Finish a started SQLite commit before propagating cancellation."""
        task = asyncio.create_task(
            asyncio.to_thread(writer, *args, **kwargs)
        )
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                fact = await asyncio.shield(task)
                break
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
                continue
            except BaseException as error:
                if cancellation is not None:
                    raise cancellation from error
                raise
        self._runtime.record(fact)
        if cancellation is not None:
            raise cancellation
        return fact

    async def record_timeout(self, group_id: str) -> None:
        """Persist an explicit unavailable transition for a bounded group timeout."""
        await self._record_unavailable(
            group_id=group_id,
            before=None,
            observed_at_ms=self._clock_ms(),
            reason="candidate-group-timeout",
        )


class CandidateGroupIds(Protocol):
    def __call__(self) -> Sequence[str]: ...


class CandidateClobExecutors:
    """Bounded, lane-isolated pools for the sync CLOB SDK."""

    def __init__(self, *, high_workers: int, lower_workers: int) -> None:
        if high_workers <= 0 or lower_workers <= 0:
            raise ValueError("candidate-clob-workers-must-be-positive")
        self.high = ThreadPoolExecutor(
            max_workers=high_workers,
            thread_name_prefix="candidate-high-clob",
        )
        self.lower = ThreadPoolExecutor(
            max_workers=lower_workers,
            thread_name_prefix="candidate-lower-clob",
        )
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Running sync SDK calls cannot be killed by Future.cancel(). Do not
        # block daemon shutdown on them; cap their number by pool size and
        # cancel every call that has not started.
        self.high.shutdown(wait=False, cancel_futures=True)
        self.lower.shutdown(wait=False, cancel_futures=True)


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
        supervisor_retry_s: float = 1.0,
        cycle_max_groups: int = 12,
        reserved_non_high_slots: int = 3,
        group_timeout_s: float = 30.0,
        high_burst_groups: int = 1,
        lower_lane_max_wait_s: float = 120.0,
        discovery_candidate_max_wait_s: float = 600.0,
        close_callbacks: Sequence[Callable[[], None]] = (),
    ) -> None:
        self._watcher = watcher
        self._store = store
        self._candidate_group_ids = candidate_group_ids
        self._runtime = runtime
        self._clock_ms = clock_ms or _wall_clock_ms
        self._poll_interval_s = poll_interval_s
        self._supervisor_retry_s = supervisor_retry_s
        self._cycle_max_groups = cycle_max_groups
        self._reserved_non_high_slots = reserved_non_high_slots
        self._group_timeout_s = group_timeout_s
        self._high_burst_groups = high_burst_groups
        self._lower_lane_max_wait_s = lower_lane_max_wait_s
        self._discovery_candidate_max_wait_s = discovery_candidate_max_wait_s
        self._reserved_lane_cursor = 0
        self._close_callbacks = tuple(close_callbacks)
        self._closed = False
        if (
            not math.isfinite(poll_interval_s)
            or poll_interval_s <= 0
            or not math.isfinite(supervisor_retry_s)
            or supervisor_retry_s <= 0
            or cycle_max_groups < 2
            or reserved_non_high_slots <= 0
            or reserved_non_high_slots >= cycle_max_groups
            or reserved_non_high_slots * 5 < cycle_max_groups
            or not math.isfinite(group_timeout_s)
            or group_timeout_s <= 0
            or high_burst_groups <= 0
            or high_burst_groups > cycle_max_groups - reserved_non_high_slots
            or not math.isfinite(lower_lane_max_wait_s)
            or lower_lane_max_wait_s <= 0
            or lower_lane_max_wait_s > 120
            or high_burst_groups * group_timeout_s >= lower_lane_max_wait_s
            or not math.isfinite(discovery_candidate_max_wait_s)
            or discovery_candidate_max_wait_s <= 0
        ):
            raise ValueError("invalid-candidate-scheduler-controller-input")

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
                schedule = await asyncio.to_thread(
                    self._store.group_schedule,
                    group_id,
                )
                if schedule is None:
                    due.append((0, 0, group_id))
                else:
                    # Discovery persists Decimal score evidence.  Until the
                    # first Candidate terminal fact exists, preserve that
                    # ordering instead of falling back to lexical group ID.
                    overdue_at_ms = schedule.first_discovered_at_ms + int(
                        self._discovery_candidate_max_wait_s * 1_000
                    )
                    overdue = now_ms >= overdue_at_ms
                    score_order = (
                        -(10**18) + overdue_at_ms
                        if overdue
                        else -int(schedule.priority_score * 1_000)
                    )
                    due.append(
                        (
                            0 if overdue else rank[schedule.priority_class],
                            score_order,
                            group_id,
                        )
                    )
            elif fact.next_due_at_ms <= now_ms:
                due.append((rank[fact.priority_class], fact.next_due_at_ms, group_id))
        priority_by_rank: dict[int, CandidatePriority] = {
            0: "high",
            1: "normal",
            2: "explore",
        }
        for rank_value, _, group_id in self._select_cycle(due):
            before_count = self._runtime.snapshot().attempt_count
            try:
                await asyncio.wait_for(
                    self._watcher.run_once(
                        group_id,
                        priority_hint=priority_by_rank[rank_value],
                    ),
                    timeout=self._group_timeout_s,
                )
            except asyncio.CancelledError:
                raise
            except TimeoutError as error:
                self._runtime.record_group_failure(group_id, error)
                if self._runtime.snapshot().attempt_count == before_count:
                    await self._watcher.record_timeout(group_id)
                logger.warning(f"candidate group timed out group_id={group_id}")
            except Exception as error:
                self._runtime.record_group_failure(group_id, error)
                logger.warning(
                    "candidate group task failed "
                    f"group_id={group_id} kind={type(error).__name__}"
                )
            else:
                self._runtime.record_group_success(group_id)

    def _select_cycle(
        self,
        due: list[tuple[int, int, str]],
    ) -> tuple[tuple[int, int, str], ...]:
        ordered = sorted(due, key=lambda item: (item[1], item[0], item[2]))
        high = [item for item in ordered if item[0] == 0]
        normal = [item for item in ordered if item[0] == 1]
        explore = [item for item in ordered if item[0] == 2]
        reserved: list[tuple[int, int, str]] = []
        lanes = [normal, explore]
        lane_index = self._reserved_lane_cursor
        while len(reserved) < self._reserved_non_high_slots and any(lanes):
            for offset in range(len(lanes)):
                lane = lanes[(lane_index + offset) % len(lanes)]
                if lane and len(reserved) < self._reserved_non_high_slots:
                    reserved.append(lane.pop(0))
            lane_index = (lane_index + 1) % len(lanes)
        self._reserved_lane_cursor = (
            self._reserved_lane_cursor + 1
        ) % len(lanes)
        remaining = self._cycle_max_groups - len(reserved)
        selected_high = high[:remaining]
        remaining -= len(selected_high)
        selected_lower = (normal + explore)[:remaining] if remaining else []
        # At least one hot candidate gets first service. Reserved lower-lane
        # work then runs before any queue-only high can consume another timeout
        # budget. Remaining selected work retains priority order afterwards.
        high_burst = selected_high[: self._high_burst_groups]
        tail = selected_high[self._high_burst_groups :] + selected_lower
        return tuple(
            high_burst
            + reserved
            + sorted(tail, key=lambda item: (item[0], item[1], item[2]))
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                delay_s = self._poll_interval_s
                try:
                    await self.run_due_once()
                    # Only the source/enumeration boundary is recovered here.
                    # Per-group recovery is recorded by the same group succeeding.
                    self._runtime.record_supervisor_recovery()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    delay_s = self._supervisor_retry_s
                    self._runtime.record_supervisor_failure(error)
                    logger.warning(
                        "candidate scheduler cycle failed "
                        f"kind={type(error).__name__}"
                    )
                if stop_event.is_set():
                    break
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
                except TimeoutError:
                    pass
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for callback in self._close_callbacks:
            try:
                callback()
            except Exception as error:
                logger.warning(
                    "candidate scheduler close callback failed "
                    f"kind={type(error).__name__}"
                )


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
    executors = CandidateClobExecutors(
        high_workers=settings.candidate_high_clob_workers,
        lower_workers=settings.candidate_lower_clob_workers,
    )
    try:
        watcher = CandidateWatcher(
            structure_reader=GroupStructureReader(store),
            books_reader=ClobReaderClient(settings, executor=executors.high),
            lower_priority_books_reader=ClobReaderClient(
                settings,
                executor=executors.lower,
            ),
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
            poll_interval_s=settings.candidate_scheduler_poll_s,
            supervisor_retry_s=settings.candidate_supervisor_retry_s,
            cycle_max_groups=settings.candidate_cycle_max_groups,
            reserved_non_high_slots=settings.candidate_reserved_non_high_slots,
            group_timeout_s=settings.candidate_group_timeout_s,
            high_burst_groups=settings.candidate_high_burst_groups,
            lower_lane_max_wait_s=settings.candidate_lower_lane_max_wait_s,
            discovery_candidate_max_wait_s=(
                settings.discovery_candidate_max_wait_s
            ),
            close_callbacks=(executors.close,),
        )
    except BaseException:
        executors.close()
        raise
