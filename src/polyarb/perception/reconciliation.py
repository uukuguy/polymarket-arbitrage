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
    ) -> None:
        if not 1 <= page_limit <= 100:
            raise ValueError("reconciliation-page-limit-must-be-within-1..100")
        self._gamma = gamma
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
        page = await self._gamma.fetch_active_event_page(requested_cursor, self._page_limit)
        if page.requested_cursor != requested_cursor:
            raise ValueError("reconciliation-page-cursor-mismatch")
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
    ) -> None:
        if interval_s <= 0:
            raise ValueError("reconciliation-interval-must-be-positive")
        self._worker = worker
        self._gamma = gamma
        self._interval_s = interval_s

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                try:
                    await self._worker.run_batch()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    logger.warning(f"reconciliation batch failed kind={type(error).__name__}")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._interval_s)
                except TimeoutError:
                    pass
        finally:
            close = getattr(self._gamma, "aclose", None)
            if close is not None:
                await close()


def build_production_reconciliation(settings: object) -> ReconciliationRunner:
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
        ),
        gamma=gamma,
        interval_s=settings.reconciliation_interval_s,
    )


__all__ = [
    "ReconciliationBatchResult",
    "ReconciliationIncompleteError",
    "ReconciliationRunner",
    "ReconciliationWorker",
    "build_production_reconciliation",
]
