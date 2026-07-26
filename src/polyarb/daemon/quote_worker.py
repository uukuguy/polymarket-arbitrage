"""Fail-soft periodic producer for atomic neg-risk quote runs."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loguru import logger

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_collector import (
    QuoteCollectionResult,
    collect_neg_risk_quotes,
)
from polyarb.routing.neg_risk_quote_store import (
    CompleteQuoteProjection,
    NegRiskQuoteStore,
    QuoteProjectionIntegrityError,
)

CollectOnce = Callable[[], Awaitable[QuoteCollectionResult]]
CertifyProjection = Callable[
    [QuoteCollectionResult],
    Awaitable[CompleteQuoteProjection],
]
WaitForStop = Callable[[asyncio.Event, float], Awaitable[bool]]


@dataclass(frozen=True)
class QuoteWorkerSnapshot:
    state: str
    attempt_count: int
    success_count: int
    failure_count: int
    consecutive_failures: int
    last_attempt_started_at_s: float | None
    last_attempt_finished_at_s: float | None
    last_run_id: int | None
    last_requested_token_count: int | None
    last_successful_response_count: int | None
    last_elapsed_ms: int | None
    last_error_kind: str | None


class QuoteWorkerRuntime:
    """Bounded process-local attempt state; durable success truth stays in SQLite."""

    def __init__(self) -> None:
        self.state = "cold-start"
        self.attempt_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.consecutive_failures = 0
        self.last_attempt_started_at_s: float | None = None
        self.last_attempt_finished_at_s: float | None = None
        self.last_run_id: int | None = None
        self.last_requested_token_count: int | None = None
        self.last_successful_response_count: int | None = None
        self.last_elapsed_ms: int | None = None
        self.last_error_kind: str | None = None
        self._certified_projection: CompleteQuoteProjection | None = None

    def mark_started(self) -> None:
        self.state = "collecting"
        self.attempt_count += 1
        self.last_attempt_started_at_s = time.time()

    def mark_success(self, result: QuoteCollectionResult) -> None:
        self.state = "pass"
        self.success_count += 1
        self.consecutive_failures = 0
        self.last_attempt_finished_at_s = time.time()
        self.last_run_id = result.run_id
        self.last_requested_token_count = result.requested_token_count
        self.last_successful_response_count = result.successful_response_count
        self.last_elapsed_ms = result.elapsed_ms
        self.last_error_kind = None

    def mark_failure(self, error: Exception) -> None:
        self.state = "error"
        self.failure_count += 1
        self.consecutive_failures += 1
        self.last_attempt_finished_at_s = time.time()
        self.last_error_kind = type(error).__name__

    def mark_stopped(self) -> None:
        self.state = "stopped"

    def publish_certified_projection(
        self,
        projection: CompleteQuoteProjection,
    ) -> None:
        """Atomically replace the bounded immutable HTTP read projection."""
        self._certified_projection = projection

    def certified_projection(self) -> CompleteQuoteProjection | None:
        """Return one immutable projection pointer without SQLite work."""
        return self._certified_projection

    def snapshot(self) -> QuoteWorkerSnapshot:
        return QuoteWorkerSnapshot(
            state=self.state,
            attempt_count=self.attempt_count,
            success_count=self.success_count,
            failure_count=self.failure_count,
            consecutive_failures=self.consecutive_failures,
            last_attempt_started_at_s=self.last_attempt_started_at_s,
            last_attempt_finished_at_s=self.last_attempt_finished_at_s,
            last_run_id=self.last_run_id,
            last_requested_token_count=self.last_requested_token_count,
            last_successful_response_count=self.last_successful_response_count,
            last_elapsed_ms=self.last_elapsed_ms,
            last_error_kind=self.last_error_kind,
        )


async def _wait_for_stop(stop_event: asyncio.Event, delay_s: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
    except TimeoutError:
        return False
    return True


class QuoteWorker:
    """Run one collection at a time and retry ordinary failures next interval."""

    def __init__(
        self,
        *,
        collect_once: CollectOnce,
        certify_projection: CertifyProjection | None = None,
        interval_s: float,
        runtime: QuoteWorkerRuntime | None = None,
        wait_for_stop: WaitForStop = _wait_for_stop,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(interval_s, bool) or interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self._collect_once = collect_once
        self._certify_projection = certify_projection
        self._interval_s = interval_s
        self._wait_for_stop = wait_for_stop
        self._monotonic = monotonic
        self.runtime = runtime or QuoteWorkerRuntime()

    @property
    def interval_s(self) -> float:
        return self._interval_s

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                attempt_started = self._monotonic()
                self.runtime.mark_started()
                try:
                    result = await self._collect_once()
                    certified_projection = None
                    if self._certify_projection is not None:
                        certified_projection = await self._certify_projection(result)
                        if certified_projection.run_id != result.run_id:
                            raise QuoteProjectionIntegrityError()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # fail-soft producer boundary
                    self.runtime.mark_failure(error)
                    logger.exception(
                        "neg-risk quote collection failed "
                        f"kind={type(error).__name__} "
                        f"consecutive={self.runtime.consecutive_failures}"
                    )
                else:
                    if certified_projection is not None:
                        self.runtime.publish_certified_projection(
                            certified_projection
                        )
                    self.runtime.mark_success(result)
                    logger.info(
                        "neg-risk quote collection complete "
                        f"run_id={result.run_id} "
                        f"responses={result.successful_response_count}/"
                        f"{result.requested_token_count} "
                        f"elapsed_ms={result.elapsed_ms}"
                    )
                elapsed_s = max(0.0, self._monotonic() - attempt_started)
                delay_s = max(0.0, self._interval_s - elapsed_s)
                if await self._wait_for_stop(stop_event, delay_s):
                    break
        finally:
            self.runtime.mark_stopped()


def build_production_quote_worker(settings: Settings) -> QuoteWorker | None:
    """Build the public-read-only production worker when explicitly enabled."""
    if not settings.neg_risk_quote_worker_enabled:
        return None
    quote_store = NegRiskQuoteStore(settings.db_path)
    reader = ClobReaderClient(settings)

    async def collect_once() -> QuoteCollectionResult:
        return await collect_neg_risk_quotes(
            quote_store=quote_store,
            reader=reader,
        )

    async def certify_projection(
        result: QuoteCollectionResult,
    ) -> CompleteQuoteProjection:
        started = time.perf_counter()
        projection = await asyncio.to_thread(
            quote_store.latest_complete_projection
        )
        if projection is None or projection.run_id != result.run_id:
            raise QuoteProjectionIntegrityError()
        logger.info(
            "neg-risk quote projection certified "
            f"run_id={projection.run_id} "
            f"elapsed_ms={int((time.perf_counter() - started) * 1000)}"
        )
        return projection

    return QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        interval_s=settings.neg_risk_quote_interval_s,
    )
