"""One-page rolling Discovery with atomic certification and promotion."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Protocol

from loguru import logger

from polyarb.clients.gamma_client import EventPage
from polyarb.perception.fault_adapters import (
    FaultingGammaPageClient,
    PartialGammaPageError,
    gamma_fault_id,
    gamma_injected_at_ms,
)
from polyarb.perception.fault_control import FaultCallClass, FaultKind
from polyarb.perception.fault_runtime import (
    FaultRuntimeProtocol,
    PassThroughFaultRuntime,
    cleanup_active_fault,
)
from polyarb.perception.gamma_incidents import GammaBatchIncidents
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import (
    DiscoveryAdmissionProof,
    DiscoveryScheduleCandidate,
    OpportunityPerceptionStore,
)
from polyarb.snapshot.normalizer import normalize_events, normalize_market


class DiscoveryGamma(Protocol):
    async def fetch_active_event_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> EventPage: ...


@dataclass(frozen=True)
class CandidateFreshness:
    candidate_count: int
    quote_p95_age_ms: int | None
    missing_quote_count: int = 0


@dataclass(frozen=True)
class DiscoveryLoadController:
    candidate_hard_stale_ms: int

    def __post_init__(self) -> None:
        if self.candidate_hard_stale_ms <= 0:
            raise ValueError("discovery-stale-threshold-must-be-positive")

    def yield_reason(self, freshness: CandidateFreshness) -> str | None:
        if freshness.candidate_count > 0 and freshness.missing_quote_count > 0:
            return "candidate-quote-missing"
        if (
            freshness.candidate_count > 0
            and freshness.quote_p95_age_ms is not None
            and freshness.quote_p95_age_ms >= self.candidate_hard_stale_ms
        ):
            return "candidate-quote-stale"
        return None


@dataclass(frozen=True)
class DiscoveryBatchResult:
    requested_cursor: str | None
    next_cursor: str | None
    completed: bool
    page_event_count: int
    groups_seen: int
    promoted_group_ids: tuple[str, ...]
    started_at_ms: int
    finished_at_ms: int
    yielded: bool = False
    yield_reason: str | None = None
    batch_id: int | None = None


class CandidateGroupIds(Protocol):
    def __call__(self) -> Sequence[str]: ...


def effective_promotion_admission_capacity(
    *,
    candidate_max_wait_s: float,
    selection_budget_s: float,
    poll_interval_s: float,
    group_timeout_s: float,
    terminal_write_budget_s: float,
    attempt_start_write_budget_s: float = 5.0,
    high_burst_groups: int,
    reserved_non_high_slots: int,
) -> int:
    candidate_max_wait_ms = int(candidate_max_wait_s * 1_000)
    poll_interval_ms = math.ceil(poll_interval_s * 1_000)
    selection_budget_ms = math.ceil(selection_budget_s * 1_000)
    group_timeout_ms = math.ceil(group_timeout_s * 1_000)
    terminal_write_budget_ms = math.ceil(terminal_write_budget_s * 1_000)
    attempt_start_write_budget_ms = math.ceil(
        attempt_start_write_budget_s * 1_000
    )
    capacity = 0
    for candidate_capacity in range(1, reserved_non_high_slots + 1):
        bound = (
            poll_interval_ms
            + selection_budget_ms
            + high_burst_groups
            * (group_timeout_ms + terminal_write_budget_ms)
            + candidate_capacity * attempt_start_write_budget_ms
            + (candidate_capacity - 1)
            * (group_timeout_ms + terminal_write_budget_ms)
        )
        if bound > candidate_max_wait_ms:
            break
        capacity = candidate_capacity
    return capacity


def compose_candidate_group_ids(
    legacy_source: CandidateGroupIds,
    store: OpportunityPerceptionStore,
) -> CandidateGroupIds:
    """Return only current durable Candidate authorities, preserving seed order."""

    def source() -> tuple[str, ...]:
        actual = store.actual_candidate_group_ids()
        actual_set = set(actual)
        return tuple(
            dict.fromkeys(
                (
                    *(group_id for group_id in legacy_source() if group_id in actual_set),
                    *actual,
                )
            )
        )

    return source


class DiscoveryWorker:
    def __init__(
        self,
        *,
        gamma: DiscoveryGamma,
        store: OpportunityPerceptionStore,
        page_limit: int = 100,
        load_controller: DiscoveryLoadController | None = None,
        candidate_freshness: Callable[[], CandidateFreshness] | None = None,
        degraded_probe_every_cycles: int = 10,
        promotion_admission_capacity: int | None = None,
        candidate_max_wait_s: float = 60.0,
        candidate_selection_budget_s: float = 6.0,
        candidate_poll_interval_s: float = 1.0,
        candidate_group_timeout_s: float = 30.0,
        candidate_terminal_write_budget_s: float = 5.0,
        candidate_attempt_start_write_budget_s: float = 5.0,
        candidate_high_burst_groups: int = 1,
        candidate_reserved_non_high_slots: int = 3,
        clock_ms: Callable[[], int] | None = None,
        require_resource_decision: bool = False,
        fault_runtime: FaultRuntimeProtocol | None = None,
    ) -> None:
        if not 1 <= page_limit <= 100:
            raise ValueError("discovery-page-limit-must-be-within-1..100")
        if (load_controller is None) != (candidate_freshness is None):
            raise ValueError("discovery-load-controller-inputs-must-be-paired")
        if degraded_probe_every_cycles < 2:
            raise ValueError("discovery-probe-period-must-be-at-least-two")
        computed_capacity = effective_promotion_admission_capacity(
            candidate_max_wait_s=candidate_max_wait_s,
            selection_budget_s=candidate_selection_budget_s,
            poll_interval_s=candidate_poll_interval_s,
            group_timeout_s=candidate_group_timeout_s,
            terminal_write_budget_s=candidate_terminal_write_budget_s,
            attempt_start_write_budget_s=candidate_attempt_start_write_budget_s,
            high_burst_groups=candidate_high_burst_groups,
            reserved_non_high_slots=candidate_reserved_non_high_slots,
        )
        effective_capacity = (
            computed_capacity
            if promotion_admission_capacity is None
            else promotion_admission_capacity
        )
        self._admission_proof = DiscoveryAdmissionProof(
            effective_capacity=effective_capacity,
            candidate_max_wait_ms=int(candidate_max_wait_s * 1_000),
            selection_budget_ms=math.ceil(
                candidate_selection_budget_s * 1_000
            ),
            poll_interval_ms=math.ceil(candidate_poll_interval_s * 1_000),
            group_timeout_ms=math.ceil(candidate_group_timeout_s * 1_000),
            terminal_write_budget_ms=math.ceil(
                candidate_terminal_write_budget_s * 1_000
            ),
            high_burst_groups=candidate_high_burst_groups,
            reserved_non_high_slots=candidate_reserved_non_high_slots,
            attempt_start_write_budget_ms=math.ceil(
                candidate_attempt_start_write_budget_s * 1_000
            ),
        )
        self._admission_proof.validate()
        if effective_capacity > computed_capacity:
            raise ValueError("discovery-admission-capacity-exceeds-proof")
        self._fault_runtime = fault_runtime or PassThroughFaultRuntime()
        self._gamma = FaultingGammaPageClient(
            inner=gamma,
            runtime=self._fault_runtime,
            call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            target_key="discovery",
        )
        self._store = store
        self._page_limit = page_limit
        self._load_controller = load_controller
        self._candidate_freshness = candidate_freshness
        self._degraded_probe_every_cycles = degraded_probe_every_cycles
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))
        self._require_resource_decision = require_resource_decision
        self._store.configure_discovery_admission(
            self._admission_proof,
            now_ms=self._clock_ms(),
        )

    async def run_batch(self) -> DiscoveryBatchResult:
        if self._load_controller is not None:
            assert self._candidate_freshness is not None
            freshness = await asyncio.to_thread(self._candidate_freshness)
            reason = self._load_controller.yield_reason(freshness)
            now_ms = self._clock_ms()
            load_state = await asyncio.to_thread(
                self._store.record_discovery_load_decision,
                degraded_reason=reason,
                probe_every_cycles=self._degraded_probe_every_cycles,
                now_ms=now_ms,
            )
            if reason is not None and load_state.last_decision == "yield":
                cursor = await asyncio.to_thread(self._store.discovery_cursor)
                return DiscoveryBatchResult(
                    requested_cursor=cursor,
                    next_cursor=cursor,
                    completed=False,
                    page_event_count=0,
                    groups_seen=0,
                    promoted_group_ids=(),
                    started_at_ms=now_ms,
                    finished_at_ms=now_ms,
                    yielded=True,
                    yield_reason=reason,
                )

        requested_cursor = await asyncio.to_thread(self._store.discovery_cursor)
        resource_decision = (
            await asyncio.to_thread(
                self._store.latest_resource_decision,
                now_ms=self._clock_ms(),
                required=True,
            )
            if self._require_resource_decision
            else None
        )
        page_limit = (
            self._page_limit
            if resource_decision is None
            else min(100, int(resource_decision["discovery_batch_limit"]))
        )
        await self._fault_runtime.sync_before_batch()
        page = await self._gamma.fetch_active_event_page(
            requested_cursor,
            page_limit,
        )
        if page.requested_cursor != requested_cursor:
            raise ValueError("discovery-page-cursor-mismatch")
        candidates = await asyncio.to_thread(self._normalize_page, page)
        batch_id, promoted = await self._commit_batch(page, candidates)
        return DiscoveryBatchResult(
            requested_cursor=requested_cursor,
            next_cursor=page.next_cursor,
            completed=page.completed,
            page_event_count=len(page.events),
            groups_seen=len(candidates),
            promoted_group_ids=promoted,
            started_at_ms=page.started_at_ms,
            finished_at_ms=page.finished_at_ms,
            batch_id=batch_id,
        )

    async def _commit_batch(
        self,
        page: EventPage,
        candidates: tuple[DiscoveryScheduleCandidate, ...],
    ) -> tuple[int, tuple[str, ...]]:
        task = asyncio.create_task(
            asyncio.to_thread(
                self._store.publish_discovery_batch,
                requested_cursor=page.requested_cursor,
                next_cursor=page.next_cursor,
                completed=page.completed,
                started_at_ms=page.started_at_ms,
                finished_at_ms=page.finished_at_ms,
                page_event_count=len(page.events),
                candidates=candidates,
                admission_proof=self._admission_proof,
            )
        )
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
                continue
            except BaseException as error:
                if cancellation is not None:
                    raise cancellation from error
                raise
        if cancellation is not None:
            raise cancellation
        return result

    @staticmethod
    def _normalize_page(
        page: EventPage,
    ) -> tuple[DiscoveryScheduleCandidate, ...]:
        raw_events = list(page.events)
        event_rows, _, _, _, truths = normalize_events(raw_events)
        rows_by_event = {str(row["id"]): row for row in event_rows}
        raw_by_event = {str(event["id"]): event for event in raw_events}
        activity_ranks = _rank_by_event(event_rows, "volume_usd")
        liquidity_ranks = _rank_by_event(event_rows, "liquidity_usd")
        candidates: list[DiscoveryScheduleCandidate] = []
        seen_group_ids: set[str] = set()
        for truth in truths:
            if truth.group_id in seen_group_ids:
                raise ValueError("duplicate-discovery-group")
            seen_group_ids.add(truth.group_id)
            row = rows_by_event[truth.event_id]
            raw = raw_by_event[truth.event_id]
            quality = truth.quality
            reason = truth.reason
            legs: tuple[GroupLeg, ...] | None = None
            if quality == "complete-supported":
                legs = _certified_legs(raw, truth.group_id)
                if legs is None:
                    quality = "incomplete-source"
                    reason = "group-leg-identity-incomplete"
            membership_hash = (
                GroupRevision.membership_digest(legs)
                if legs is not None
                else truth.membership_hash
            )
            liquidity_weight = _decimal_or_zero(row.get("liquidity_usd"))
            candidates.append(
                DiscoveryScheduleCandidate(
                    event_id=truth.event_id,
                    group_id=truth.group_id,
                    membership_hash=membership_hash,
                    quality=quality,
                    reason=reason,
                    activity_rank=activity_ranks[truth.event_id],
                    liquidity_rank=liquidity_ranks[truth.event_id],
                    liquidity_weight=liquidity_weight,
                    legs=legs,
                )
            )
        candidates.sort(key=lambda item: item.group_id)
        return tuple(candidates)


def _certified_legs(raw_event: dict, group_id: str) -> tuple[GroupLeg, ...] | None:
    markets = raw_event.get("markets")
    event_id = raw_event.get("id")
    if not isinstance(markets, list) or not isinstance(event_id, str):
        return None
    legs: list[GroupLeg] = []
    for raw_market in markets:
        if not isinstance(raw_market, dict):
            return None
        if (
            raw_market.get("active") is not True
            or raw_market.get("closed") is True
            or raw_market.get("negRiskOther") is True
        ):
            continue
        enriched = {
            **raw_market,
            "negRisk": True,
            "negRiskMarketID": group_id,
        }
        normalized = normalize_market(
            enriched,
            {str(raw_market.get("id")): event_id},
        )
        if normalized is None:
            return None
        market_id = normalized.get("market_id")
        condition_id = normalized.get("condition_id")
        yes_token_id = normalized.get("yes_token_id")
        title = raw_market.get("groupItemTitle") or normalized.get("question")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (market_id, condition_id, yes_token_id, title)
        ):
            return None
        legs.append(
            GroupLeg(
                market_id=market_id,
                condition_id=condition_id,
                yes_token_id=yes_token_id,
                title=title,
            )
        )
    if len(legs) < 2:
        return None
    return tuple(legs)


def _decimal_or_zero(value: object) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return result if result.is_finite() and result >= 0 else Decimal("0")


def _rank_by_event(rows: list[dict], field: str) -> dict[str, Decimal]:
    values = {
        str(row["id"]): _decimal_or_zero(row.get(field))
        for row in rows
    }
    denominator = Decimal(max(1, len(values)))
    return {
        event_id: (
            Decimal(sum(other <= value for other in values.values()))
            / denominator
            * Decimal("100")
        )
        for event_id, value in values.items()
    }


class DiscoveryRunner:
    """Contain one bounded batch failure and preserve restartable cursor state."""

    def __init__(
        self,
        *,
        worker: DiscoveryWorker,
        gamma: object,
        interval_s: float,
        store: OpportunityPerceptionStore | None = None,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("discovery-interval-must-be-positive")
        self._worker = worker
        self._fault_runtime = getattr(
            worker,
            "_fault_runtime",
            PassThroughFaultRuntime(),
        )
        self._store = store or worker._store
        self._gamma = gamma
        self._interval_s = interval_s
        self._gamma_incidents = GammaBatchIncidents(
            self._store,
            scope="discovery",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                requested_nonce = await asyncio.to_thread(
                    self._store.pending_operator_wakeup,
                    "discovery",
                    now_ms=int(time.time() * 1_000),
                    require_resource_decision=self._worker._require_resource_decision,
                )
                delay_s = self._interval_s
                successful_checkpoint = False
                try:
                    result = await self._worker.run_batch()
                    await asyncio.to_thread(
                        self._store.record_producer_heartbeat,
                        "discovery",
                        observed_at_ms=result.finished_at_ms,
                        state="yielded" if result.yielded else "progress",
                    )
                    if not result.yielded and result.batch_id is not None:
                        await self._fault_runtime.record_recovery(
                            f"discovery-batch-{result.batch_id}"
                        )
                        await asyncio.to_thread(
                            self._gamma_incidents.verify_discovery,
                            result.batch_id,
                        )
                    decision = (
                        await asyncio.to_thread(
                            self._store.latest_resource_decision,
                            now_ms=int(time.time() * 1_000),
                            required=True,
                        )
                        if self._worker._require_resource_decision
                        else None
                    )
                    if decision is not None:
                        duty = float(decision["discovery_duty_multiplier"])
                        if not math.isfinite(duty) or not 0.1 <= duty <= 4.0:
                            raise ValueError("invalid-discovery-duty-multiplier")
                        delay_s = self._interval_s / duty
                    successful_checkpoint = not result.yielded
                except asyncio.CancelledError:
                    raise
                except PartialGammaPageError as error:
                    fault_id = gamma_fault_id(error)
                    if fault_id is not None:
                        await self._fault_runtime.link_detection(
                            fault_id,
                            kind=FaultKind.GAMMA_PARTIAL,
                            detection_id=error.coverage_id,
                        )
                        await self._fault_runtime.cleanup(
                            fault_id,
                            "partial-or-rejected-page",
                        )
                    logger.warning(
                        "discovery batch rejected coverage "
                        f"original_count={error.original_count} "
                        f"kept_count={error.kept_count}"
                    )
                except Exception as error:
                    incident = await asyncio.to_thread(
                        self._gamma_incidents.record_failure,
                        error,
                    )
                    fault_id = gamma_fault_id(error)
                    if incident is not None and fault_id is not None:
                        kind = FaultKind(incident.kind)
                        injected_at_ms = gamma_injected_at_ms(error)
                        matches = await asyncio.to_thread(
                            self._gamma_incidents.unique_match,
                            incident.id,
                            kind=incident.kind,
                            injected_at_ms=injected_at_ms,
                        )
                        if matches:
                            await self._fault_runtime.link_detection(
                                fault_id,
                                kind=kind,
                                detection_id=incident.id,
                            )
                        await self._fault_runtime.cleanup(
                            fault_id,
                            "gamma-fault-contained",
                        )
                    logger.warning(
                        "discovery batch failed "
                        f"kind={type(error).__name__}"
                    )
                if requested_nonce is not None and successful_checkpoint:
                    await asyncio.to_thread(
                        self._store.consume_operator_wakeup,
                        "discovery",
                        occurred_at_ms=int(time.time() * 1_000),
                        expected_nonce=requested_nonce,
                        require_resource_decision=self._worker._require_resource_decision,
                    )
                deadline = time.monotonic() + delay_s
                while not stop_event.is_set() and time.monotonic() < deadline:
                    if await asyncio.to_thread(
                        self._store.pending_operator_wakeup,
                        "discovery",
                        now_ms=int(time.time() * 1_000),
                        require_resource_decision=self._worker._require_resource_decision,
                    ) is not None:
                        break
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=min(1.0, max(0.0, deadline - time.monotonic())),
                        )
                    except TimeoutError:
                        pass
        finally:
            await cleanup_active_fault(
                self._fault_runtime,
                reason="discovery-stopped",
            )
            close = getattr(self._gamma, "aclose", None)
            if close is not None:
                await close()


def build_production_discovery(
    settings: object,
    *,
    candidate_freshness: Callable[[], CandidateFreshness],
    fault_runtime: FaultRuntimeProtocol | None = None,
) -> DiscoveryRunner:
    """Build the opt-in bounded producer without changing legacy paths."""
    from polyarb.clients.gamma_client import GammaClient

    gamma = GammaClient(settings)
    store = OpportunityPerceptionStore(settings.db_path)
    store.init_schema()
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        page_limit=settings.discovery_page_limit,
        load_controller=DiscoveryLoadController(
            candidate_hard_stale_ms=int(
                settings.candidate_quote_hard_stale_s * 1_000
            )
        ),
        candidate_freshness=candidate_freshness,
        degraded_probe_every_cycles=settings.discovery_degraded_probe_every_cycles,
        promotion_admission_capacity=(
            settings.discovery_effective_admission_capacity
        ),
        candidate_max_wait_s=settings.discovery_candidate_max_wait_s,
        candidate_selection_budget_s=settings.candidate_selection_budget_s,
        candidate_poll_interval_s=settings.candidate_scheduler_poll_s,
        candidate_group_timeout_s=settings.candidate_group_timeout_s,
        candidate_terminal_write_budget_s=(
            settings.candidate_terminal_write_budget_s
        ),
        candidate_attempt_start_write_budget_s=(
            settings.candidate_attempt_start_write_budget_s
        ),
        candidate_high_burst_groups=settings.candidate_high_burst_groups,
        candidate_reserved_non_high_slots=(
            settings.candidate_reserved_non_high_slots
        ),
        require_resource_decision=(
            settings.opportunity_resource_controller_enabled
        ),
        fault_runtime=fault_runtime,
    )
    return DiscoveryRunner(
        worker=worker,
        gamma=gamma,
        interval_s=settings.discovery_interval_s,
        store=store,
    )
