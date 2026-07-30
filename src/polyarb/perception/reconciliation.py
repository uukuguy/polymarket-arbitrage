"""Checkpointed, low-priority full-universe calibration."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from loguru import logger

from polyarb.clients.gamma_client import EventPage
from polyarb.perception.discovery import DiscoveryWorker
from polyarb.perception.fault_adapters import (
    FaultingGammaPageClient,
    gamma_cursor_error,
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
from polyarb.perception.store import (
    OpportunityPerceptionStore,
    ReconciliationDiff,
    ReconciliationIncompleteError,
    ReconciliationUnprovableError,
)


class ReconciliationGamma(Protocol):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage: ...


@dataclass(frozen=True)
class ReconciliationBatchResult:
    window_id: str
    requested_cursor: str | None
    next_cursor: str | None
    completed: bool
    page_event_count: int
    groups_staged: int
    rejected_count: int
    started_at_ms: int
    finished_at_ms: int
    diff: ReconciliationDiff | None
    failed: bool = False
    failure_reason: str | None = None


class ReconciliationWorker:
    """Consume exactly one Gamma page and durably advance one window."""

    def __init__(
        self,
        *,
        gamma: ReconciliationGamma,
        store: OpportunityPerceptionStore,
        page_limit: int = 100,
        clock_ms: Callable[[], int] | None = None,
        fault_runtime: FaultRuntimeProtocol | None = None,
    ) -> None:
        if not 1 <= page_limit <= 100:
            raise ValueError("reconciliation-page-limit-must-be-within-1..100")
        self._fault_runtime = fault_runtime or PassThroughFaultRuntime()
        self._gamma = FaultingGammaPageClient(
            inner=gamma,
            runtime=self._fault_runtime,
            call_class=FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            target_key="reconciliation",
        )
        self._store = store
        self._page_limit = page_limit
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))

    async def run_batch(self) -> ReconciliationBatchResult:
        try:
            window = await asyncio.to_thread(self._store.current_reconciliation)
        except ReconciliationUnprovableError:
            window = None
        if window is not None and window.status in {"applied", "failed"}:
            window = None
        if window is None:
            window = await asyncio.to_thread(
                self._store.begin_reconciliation,
                started_at_ms=self._clock_ms(),
            )
        if window is not None and window.status == "complete":
            diff = await asyncio.to_thread(self._store.apply_reconciliation_diff, window.id)
            return ReconciliationBatchResult(
                window_id=window.id,
                requested_cursor=window.next_cursor,
                next_cursor=None,
                completed=True,
                page_event_count=0,
                groups_staged=0,
                rejected_count=0,
                started_at_ms=window.checkpoint_at_ms,
                finished_at_ms=window.finished_at_ms or window.checkpoint_at_ms,
                diff=diff,
            )
        requested_cursor = window.next_cursor
        await self._fault_runtime.sync_before_batch()
        page = await self._gamma.fetch_active_event_page(requested_cursor, self._page_limit)
        if page.requested_cursor != requested_cursor:
            raise gamma_cursor_error(
                page,
                "reconciliation-page-cursor-mismatch",
            )
        candidates = await asyncio.to_thread(DiscoveryWorker._normalize_page, page)
        committed = await self._commit_page(window.id, page, candidates)
        diff = None
        if committed.status == "failed":
            return ReconciliationBatchResult(
                window_id=committed.id,
                requested_cursor=requested_cursor,
                next_cursor=committed.next_cursor,
                completed=False,
                page_event_count=len(page.events),
                groups_staged=0,
                rejected_count=0,
                started_at_ms=page.started_at_ms,
                finished_at_ms=page.finished_at_ms,
                diff=None,
                failed=True,
                failure_reason=committed.failure_reason,
            )
        if committed.status == "complete":
            diff = await asyncio.to_thread(self._store.apply_reconciliation_diff, committed.id)
        return ReconciliationBatchResult(
            window_id=committed.id,
            requested_cursor=requested_cursor,
            next_cursor=page.next_cursor,
            completed=committed.status in {"complete", "applied"},
            page_event_count=len(page.events),
            groups_staged=len(candidates),
            rejected_count=sum(
                candidate.quality != "complete-supported" for candidate in candidates
            ),
            started_at_ms=page.started_at_ms,
            finished_at_ms=page.finished_at_ms,
            diff=diff,
        )

    async def _commit_page(self, window_id, page, candidates):
        task = asyncio.create_task(
            asyncio.to_thread(
                self._store.publish_reconciliation_batch,
                window_id=window_id,
                requested_cursor=page.requested_cursor,
                next_cursor=page.next_cursor,
                completed=page.completed,
                started_at_ms=page.started_at_ms,
                finished_at_ms=page.finished_at_ms,
                page_event_count=len(page.events),
                candidates=candidates,
            )
        )
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
                continue
            except BaseException as error:
                if cancellation is not None:
                    raise cancellation from error
                raise
        if cancellation is not None:
            raise cancellation
        return result


class ReconciliationRunner:
    """Contain bounded batch failures while preserving the durable checkpoint."""

    def __init__(
        self,
        *,
        worker: ReconciliationWorker,
        gamma: object,
        interval_s: float,
        idle_heartbeat_interval_s: float = 15.0,
        store: OpportunityPerceptionStore | None = None,
        require_resource_decision: bool = False,
    ) -> None:
        if interval_s <= 0 or idle_heartbeat_interval_s <= 0:
            raise ValueError("reconciliation-interval-must-be-positive")
        self._worker = worker
        self._fault_runtime = getattr(
            worker,
            "_fault_runtime",
            PassThroughFaultRuntime(),
        )
        self._store = store or worker._store
        self._gamma = gamma
        self._interval_s = interval_s
        self._idle_heartbeat_interval_s = idle_heartbeat_interval_s
        self._require_resource_decision = require_resource_decision
        self._gamma_incidents = GammaBatchIncidents(
            self._store,
            scope="reconciliation",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                requested_nonce = await asyncio.to_thread(
                    self._store.pending_operator_wakeup,
                    "reconciliation",
                    now_ms=int(time.time() * 1_000),
                    require_resource_decision=self._require_resource_decision,
                )
                successful_checkpoint = False
                try:
                    decision = (
                        await asyncio.to_thread(
                            self._store.latest_resource_decision,
                            now_ms=int(time.time() * 1_000),
                            required=True,
                        )
                        if self._require_resource_decision
                        else None
                    )
                    if decision is not None and not decision["reconciliation_enabled"]:
                        await asyncio.to_thread(
                            self._store.record_producer_heartbeat,
                            "reconciliation",
                            observed_at_ms=int(time.time() * 1_000),
                            state="paused",
                        )
                    else:
                        result = await self._worker.run_batch()
                        await asyncio.to_thread(
                            self._store.record_producer_heartbeat,
                            "reconciliation",
                            observed_at_ms=result.finished_at_ms,
                        )
                        successful_checkpoint = not result.failed
                        if successful_checkpoint:
                            await self._fault_runtime.record_recovery(
                                f"reconciliation-window-{result.window_id}"
                            )
                            await asyncio.to_thread(
                                self._gamma_incidents.verify_reconciliation,
                                result.window_id,
                            )
                except asyncio.CancelledError:
                    raise
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
                    logger.warning(f"reconciliation batch failed kind={type(error).__name__}")
                if requested_nonce is not None and successful_checkpoint:
                    await asyncio.to_thread(
                        self._store.consume_operator_wakeup,
                        "reconciliation",
                        occurred_at_ms=int(time.time() * 1_000),
                        expected_nonce=requested_nonce,
                        require_resource_decision=self._require_resource_decision,
                    )
                deadline = time.monotonic() + self._interval_s
                idle_heartbeat_deadline = (
                    time.monotonic() + self._idle_heartbeat_interval_s
                )
                while not stop_event.is_set() and time.monotonic() < deadline:
                    if await asyncio.to_thread(
                        self._store.pending_operator_wakeup,
                        "reconciliation",
                        now_ms=int(time.time() * 1_000),
                        require_resource_decision=self._require_resource_decision,
                    ) is not None:
                        break
                    now = time.monotonic()
                    if now >= idle_heartbeat_deadline:
                        try:
                            await asyncio.to_thread(
                                self._store.record_producer_heartbeat,
                                "reconciliation",
                                observed_at_ms=int(time.time() * 1_000),
                                state="yielded",
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as error:
                            logger.warning(
                                "reconciliation idle heartbeat failed "
                                f"kind={type(error).__name__}"
                            )
                        idle_heartbeat_deadline = (
                            time.monotonic() + self._idle_heartbeat_interval_s
                        )
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=min(
                                1.0,
                                max(0.0, deadline - time.monotonic()),
                                max(
                                    0.0,
                                    idle_heartbeat_deadline - time.monotonic(),
                                ),
                            ),
                        )
                    except TimeoutError:
                        pass
        finally:
            await cleanup_active_fault(
                self._fault_runtime,
                reason="reconciliation-stopped",
            )
            close = getattr(self._gamma, "aclose", None)
            if close is not None:
                await close()


def build_production_reconciliation(
    settings: object,
    *,
    fault_runtime: FaultRuntimeProtocol | None = None,
) -> ReconciliationRunner:
    """Build the opt-in bounded calibration producer."""
    from polyarb.clients.gamma_client import GammaClient

    gamma = GammaClient(settings)
    store = OpportunityPerceptionStore(settings.db_path)
    store.init_schema()
    return ReconciliationRunner(
        worker=ReconciliationWorker(
            gamma=gamma,
            store=store,
            page_limit=settings.reconciliation_page_limit,
            fault_runtime=fault_runtime,
        ),
        gamma=gamma,
        interval_s=settings.reconciliation_interval_s,
        idle_heartbeat_interval_s=min(
            settings.reconciliation_interval_s,
            getattr(settings, "producer_stall_detection_s", 30.0) / 2,
        ),
        store=store,
        require_resource_decision=(
            settings.opportunity_resource_controller_enabled
        ),
    )


__all__ = [
    "ReconciliationBatchResult",
    "ReconciliationIncompleteError",
    "ReconciliationRunner",
    "ReconciliationWorker",
    "build_production_reconciliation",
]
