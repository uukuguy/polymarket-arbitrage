"""Fail-soft periodic producer for atomic neg-risk quote runs."""

from __future__ import annotations

import asyncio
import ctypes
import gc
import json
import sqlite3
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loguru import logger

from polyarb.config import Settings
from polyarb.daemon.opportunity_watcher import OpportunityWatcher
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult
from polyarb.routing.neg_risk_quote_store import (
    CompleteQuoteProjection,
    NegRiskQuoteStore,
    QuoteProjectionIntegrityError,
)
from polyarb.routing.opportunity_scanner import (
    OpportunityScanResult,
    scan_certified_neg_risk_quote_projection,
)
from polyarb.storage.sqlite_store import SQLiteStore

CollectOnce = Callable[[], Awaitable[QuoteCollectionResult]]
CertifyProjection = Callable[
    [QuoteCollectionResult],
    Awaitable[CompleteQuoteProjection],
]
PrepareOpportunities = Callable[
    [CompleteQuoteProjection],
    Awaitable[OpportunityScanResult],
]
ReconcileGlobalProjection = Callable[[CompleteQuoteProjection], Awaitable[None]]
CleanupOldRuns = Callable[[], Awaitable[int]]
WaitForStop = Callable[[asyncio.Event, float], Awaitable[bool]]
ReleaseProjectionMemory = Callable[[], None]


def _release_projection_memory() -> None:
    """Best-effort return of one released full projection to the cgroup."""
    try:
        gc.collect()
        if not sys.platform.startswith("linux"):
            return
        malloc_trim = ctypes.CDLL(None).malloc_trim
        malloc_trim.argtypes = [ctypes.c_size_t]
        malloc_trim.restype = ctypes.c_int
        malloc_trim(0)
    except Exception as error:  # noqa: BLE001 - memory trim must stay fail-soft
        logger.debug(
            "certified projection memory release skipped "
            f"kind={type(error).__name__}"
        )


@dataclass(frozen=True)
class QuoteWorkerSnapshot:
    state: str
    pipeline_active: bool
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
    cleanup_success_count: int
    cleanup_failure_count: int
    cleanup_consecutive_failures: int
    last_cleanup_error_kind: str | None


@dataclass(frozen=True)
class CertifiedQuoteMetadata:
    """Compact identity/freshness proof retained after full certification."""

    run_id: int
    universe_snapshot_id: int
    universe_taken_at_ms: int
    quoted_at_ms: int
    requested_token_count: int
    successful_response_count: int
    universe_hash: str
    source_truth_hash: str

    @classmethod
    def from_projection(
        cls,
        projection: CompleteQuoteProjection,
    ) -> CertifiedQuoteMetadata:
        return cls(
            run_id=projection.run_id,
            universe_snapshot_id=projection.universe_snapshot_id,
            universe_taken_at_ms=projection.universe_taken_at_ms,
            quoted_at_ms=projection.quoted_at_ms,
            requested_token_count=projection.requested_token_count,
            successful_response_count=projection.successful_response_count,
            universe_hash=projection.universe_hash,
            source_truth_hash=projection.source_truth_hash,
        )


@dataclass(frozen=True)
class CertifiedQuoteFeed:
    projection: CertifiedQuoteMetadata
    opportunity_scan: OpportunityScanResult | None


RestoreFeed = Callable[[], Awaitable[CertifiedQuoteFeed | None]]
CleanupCollectingRuns = Callable[[], Awaitable[int]]


class QuoteCollectionSubprocessError(RuntimeError):
    """The isolated quote collector did not return one valid complete result."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"quote-collection-subprocess-{reason}")


class QuoteCollectionSourceSupersededError(QuoteCollectionSubprocessError):
    """A newer certified Structure revision replaced the quote input mid-run."""

    def __init__(self) -> None:
        super().__init__("source-superseded")


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
        self.cleanup_success_count = 0
        self.cleanup_failure_count = 0
        self.cleanup_consecutive_failures = 0
        self.last_cleanup_error_kind: str | None = None
        self._certified_feed: CertifiedQuoteFeed | None = None
        self._pipeline_active = False

    def mark_pipeline_started(self) -> None:
        self._pipeline_active = True

    def mark_pipeline_finished(self) -> None:
        self._pipeline_active = False

    def pipeline_active(self) -> bool:
        return self._pipeline_active

    def pipeline_due(self, interval_s: float, *, now_s: float | None = None) -> bool:
        """Return whether start-to-start cadence gives Quote admission priority."""
        if self._pipeline_active:
            return True
        if self.state == "stopped":
            return False
        if self.last_attempt_started_at_s is None:
            return True
        observed_at_s = time.time() if now_s is None else now_s
        return observed_at_s >= self.last_attempt_started_at_s + interval_s

    def mark_started(self) -> None:
        self.state = "collecting"
        self.attempt_count += 1
        self.last_attempt_started_at_s = time.time()
        # Health describes the current attempt.  Keep the durable/cumulative
        # failure evidence, but do not attach a previous error to a fresh
        # in-flight re-quote.
        self.last_error_kind = None

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
        self._pipeline_active = False
        self.state = "stopped"

    def mark_cleanup_success(self) -> None:
        self.cleanup_success_count += 1
        self.cleanup_consecutive_failures = 0
        self.last_cleanup_error_kind = None

    def mark_cleanup_failure(self, error: Exception) -> None:
        self.cleanup_failure_count += 1
        self.cleanup_consecutive_failures += 1
        self.last_cleanup_error_kind = type(error).__name__

    def publish_certified_projection(
        self,
        projection: CompleteQuoteProjection,
    ) -> None:
        """Compatibility helper for tests that do not exercise opportunities."""
        self._certified_feed = CertifiedQuoteFeed(
            CertifiedQuoteMetadata.from_projection(projection),
            None,
        )

    def publish_certified_feed(
        self,
        projection: CompleteQuoteProjection,
        opportunity_scan: OpportunityScanResult,
    ) -> None:
        """Atomically retain compact proof and its precomputed opportunity scan."""
        self._certified_feed = CertifiedQuoteFeed(
            CertifiedQuoteMetadata.from_projection(projection),
            opportunity_scan,
        )

    def restore_certified_feed(self, feed: CertifiedQuoteFeed) -> None:
        """Restore an already-validated durable feed after process restart."""
        self._certified_feed = feed
        self.state = "pass"
        self.last_run_id = feed.projection.run_id
        self.last_requested_token_count = feed.projection.requested_token_count
        self.last_successful_response_count = feed.projection.successful_response_count
        self.last_elapsed_ms = None
        self.last_error_kind = None

    def certified_feed(self) -> CertifiedQuoteFeed | None:
        """Return one immutable projection/result pair without SQLite work."""
        return self._certified_feed

    def certified_projection(self) -> CertifiedQuoteMetadata | None:
        """Return compact immutable certified metadata without SQLite work."""
        feed = self._certified_feed
        return feed.projection if feed is not None else None

    def snapshot(self) -> QuoteWorkerSnapshot:
        return QuoteWorkerSnapshot(
            state=self.state,
            pipeline_active=self._pipeline_active,
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
            cleanup_success_count=self.cleanup_success_count,
            cleanup_failure_count=self.cleanup_failure_count,
            cleanup_consecutive_failures=self.cleanup_consecutive_failures,
            last_cleanup_error_kind=self.last_cleanup_error_kind,
        )


async def _wait_for_stop(stop_event: asyncio.Event, delay_s: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
    except TimeoutError:
        return False
    return True


async def certify_latest_quote_projection(
    quote_store: NegRiskQuoteStore,
    result: QuoteCollectionResult,
) -> CompleteQuoteProjection:
    """Build one full proof off-loop and bind it to the just-finished run."""
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


def _required_json_int(payload: object, key: str) -> int:
    if not isinstance(payload, dict):
        raise QuoteCollectionSubprocessError("invalid-json")
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise QuoteCollectionSubprocessError("invalid-json")
    return value


async def collect_quotes_in_subprocess(
    settings: Settings,
    *,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = (
        asyncio.create_subprocess_exec
    ),
    terminate_timeout_s: float = 3.0,
) -> QuoteCollectionResult:
    """Run all SDK fetch/decode/SQLite collection work outside the HTTP process."""
    attempt_store = NegRiskQuoteStore(settings.db_path)
    try:
        attempt_id = await asyncio.to_thread(attempt_store.start_collection_attempt)
    except sqlite3.OperationalError as error:
        if "no such table: neg_risk_quote_attempts" not in str(error):
            raise
        await asyncio.to_thread(SQLiteStore(settings.db_path).init_schema)
        attempt_id = await asyncio.to_thread(attempt_store.start_collection_attempt)
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.cli_arbitrage",
        "collect-neg-risk-quotes",
        "--db-path",
        str(settings.db_path),
        "--attempt-id",
        str(attempt_id),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.perf_counter()
    logger.info(
        "isolated quote collection started "
        f"pid={getattr(process, 'pid', None)}"
    )
    # Keep ownership of one pipe-reader task across cancellation.  Awaiting
    # communicate() directly lets task cancellation tear down its stream
    # readers; a second communicate() then cannot reliably reap the still-live
    # child.  The shutdown path must prove the child exited before the durable
    # collecting lease can be released.
    communicate_task = asyncio.create_task(process.communicate())
    try:
        stdout, stderr = await asyncio.shield(communicate_task)
    except asyncio.CancelledError:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=terminate_timeout_s,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(
                    asyncio.shield(communicate_task),
                    timeout=terminate_timeout_s,
                )
            except TimeoutError:
                # ``kill`` is authoritative for a real child, but never let a
                # wedged pipe reader prevent daemon cancellation forever.
                communicate_task.cancel()
                await asyncio.gather(communicate_task, return_exceptions=True)
        await asyncio.to_thread(
            attempt_store.checkpoint_collection_attempt,
            attempt_id,
            phase="failed",
            failure_kind="parent-cancelled",
        )
        raise

    if process.returncode != 0:
        await asyncio.to_thread(
            attempt_store.checkpoint_collection_attempt,
            attempt_id,
            phase="failed",
            failure_kind="child-failed",
        )
        diagnostic = stderr.decode("utf-8", errors="replace")
        if "verified universe snapshot is no longer the latest published truth" in diagnostic:
            logger.info(
                "isolated quote collection superseded by a newer Structure revision "
                f"pid={getattr(process, 'pid', None)}"
            )
            raise QuoteCollectionSourceSupersededError()
        logger.warning(
            "isolated quote collection failed "
            f"returncode={process.returncode} "
            f"stderr_bytes={len(stderr)}"
        )
        raise QuoteCollectionSubprocessError("failed")
    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QuoteCollectionSubprocessError("invalid-json") from error
    if not isinstance(payload, dict) or payload.get("status") != "complete":
        raise QuoteCollectionSubprocessError("invalid-json")
    universe_hash = payload.get("universe_hash")
    if not isinstance(universe_hash, str) or len(universe_hash) != 64:
        raise QuoteCollectionSubprocessError("invalid-json")
    result = QuoteCollectionResult(
        run_id=_required_json_int(payload, "run_id"),
        status="complete",
        universe_snapshot_id=_required_json_int(
            payload,
            "universe_snapshot_id",
        ),
        requested_token_count=_required_json_int(
            payload,
            "requested_token_count",
        ),
        successful_response_count=_required_json_int(
            payload,
            "successful_response_count",
        ),
        quote_taken_at_ms=_required_json_int(
            payload,
            "quote_taken_at_ms",
        ),
        elapsed_ms=_required_json_int(payload, "elapsed_ms"),
        universe_hash=universe_hash,
        attempt_id=_required_json_int(payload, "attempt_id"),
        universe_ms=_required_json_int(payload, "universe_ms"),
        admission_ms=_required_json_int(payload, "admission_ms"),
        fetch_ms=_required_json_int(payload, "fetch_ms"),
        transform_ms=_required_json_int(payload, "transform_ms"),
        persist_ms=_required_json_int(payload, "persist_ms"),
        structure_receipt_digest=str(payload.get("structure_receipt_digest", "")),
    )
    if (
        result.run_id <= 0
        or result.universe_snapshot_id <= 0
        or result.requested_token_count < 0
        or result.successful_response_count < 0
        or result.successful_response_count > result.requested_token_count
        or result.quote_taken_at_ms < 0
        or result.elapsed_ms < 0
        or result.attempt_id != attempt_id
        or any(
            value < 0
            for value in (
                result.universe_ms,
                result.admission_ms,
                result.fetch_ms,
                result.transform_ms,
                result.persist_ms,
            )
        )
        or len(result.structure_receipt_digest) != 64
    ):
        raise QuoteCollectionSubprocessError("invalid-json")
    logger.info(
        "isolated quote collection complete "
        f"pid={getattr(process, 'pid', None)} "
        f"process_elapsed_ms={int((time.perf_counter() - started) * 1000)} "
        f"run_id={result.run_id} "
        f"collection_elapsed_ms={result.elapsed_ms} "
        f"responses={result.successful_response_count}/"
        f"{result.requested_token_count} "
        f"attempt_id={result.attempt_id} universe_ms={result.universe_ms} "
        f"admission_ms={result.admission_ms} fetch_ms={result.fetch_ms} "
        f"transform_ms={result.transform_ms} persist_ms={result.persist_ms}"
    )
    return result


class QuoteWorker:
    """Run one collection at a time and retry ordinary failures next interval."""

    def __init__(
        self,
        *,
        collect_once: CollectOnce,
        certify_projection: CertifyProjection | None = None,
        prepare_opportunities: PrepareOpportunities | None = None,
        reconcile_global_projection: ReconcileGlobalProjection | None = None,
        restore_feed: RestoreFeed | None = None,
        cleanup_collecting_runs: CleanupCollectingRuns | None = None,
        cleanup_old_runs: CleanupOldRuns | None = None,
        producer_lock: asyncio.Lock | None = None,
        interval_s: float,
        runtime: QuoteWorkerRuntime | None = None,
        wait_for_stop: WaitForStop = _wait_for_stop,
        monotonic: Callable[[], float] = time.monotonic,
        release_projection_memory: ReleaseProjectionMemory = (
            _release_projection_memory
        ),
    ) -> None:
        if isinstance(interval_s, bool) or interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self._collect_once = collect_once
        self._certify_projection = certify_projection
        self._prepare_opportunities = prepare_opportunities
        self._reconcile_global_projection = reconcile_global_projection
        self._restore_feed = restore_feed
        self._cleanup_collecting_runs = cleanup_collecting_runs
        self._cleanup_old_runs = cleanup_old_runs
        self._producer_lock = producer_lock
        self._interval_s = interval_s
        self._wait_for_stop = wait_for_stop
        self._monotonic = monotonic
        self._release_projection_memory = release_projection_memory
        self.runtime = runtime or QuoteWorkerRuntime()
        self._request_now_event = asyncio.Event()

    @property
    def interval_s(self) -> float:
        return self._interval_s

    def request_now(self) -> bool:
        """Queue one normal collection in the worker's existing single loop."""
        if self._request_now_event.is_set():
            return False
        self._request_now_event.set()
        return True

    async def _wait_for_next_attempt(self, stop_event: asyncio.Event, delay_s: float) -> bool:
        """Preserve the testable stop seam while allowing one coalesced wake-up."""
        if self._request_now_event.is_set():
            self._request_now_event.clear()
            return False
        stop_task = asyncio.create_task(self._wait_for_stop(stop_event, delay_s))
        request_task = asyncio.create_task(self._request_now_event.wait())
        try:
            done, pending = await asyncio.wait(
                (stop_task, request_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if stop_task in done and stop_task.result():
                return True
            if request_task in done and request_task.result():
                self._request_now_event.clear()
            return False
        except asyncio.CancelledError:
            stop_task.cancel()
            request_task.cancel()
            await asyncio.gather(stop_task, request_task, return_exceptions=True)
            raise

    async def run(self, stop_event: asyncio.Event) -> None:
        async def cleanup_after_cancellation() -> None:
            if self._cleanup_collecting_runs is None:
                return
            try:
                released = await self._cleanup_collecting_runs()
                logger.info(
                    "released collecting quote runs after cancellation "
                    f"count={released}"
                )
            except Exception as error:  # preserve cancellation semantics
                logger.warning(
                    "collecting quote run cleanup failed "
                    f"kind={type(error).__name__}"
                )

        try:
            if self._restore_feed is not None:
                try:
                    restored_feed = await self._restore_feed()
                    if restored_feed is not None:
                        self.runtime.restore_certified_feed(restored_feed)
                        logger.info(
                            "restored certified neg-risk quote feed "
                            f"run_id={restored_feed.projection.run_id}"
                        )
                except asyncio.CancelledError:
                    await cleanup_after_cancellation()
                    raise
                except Exception as error:  # fail-soft; fresh collection follows
                    logger.warning(
                        "certified quote feed restore failed "
                        f"kind={type(error).__name__}"
                    )
            while not stop_event.is_set():
                attempt_started = self._monotonic()
                retry_immediately = False
                self.runtime.mark_pipeline_started()
                try:
                    self.runtime.mark_started()
                    if self._producer_lock is None:
                        result = await self._collect_once()
                    else:
                        async with self._producer_lock:
                            result = await self._collect_once()
                    certified_projection = None
                    certified_opportunities = None
                    if self._certify_projection is not None:
                        certified_projection = await self._certify_projection(result)
                        if certified_projection.run_id != result.run_id:
                            raise QuoteProjectionIntegrityError()
                        if self._prepare_opportunities is not None:
                            certified_opportunities = await self._prepare_opportunities(
                                certified_projection
                            )
                            if (
                                certified_opportunities.quote_run_id
                                != certified_projection.run_id
                                or certified_opportunities.source_snapshot_id
                                != certified_projection.universe_snapshot_id
                                or certified_opportunities.universe_hash
                                != certified_projection.universe_hash
                            ):
                                raise QuoteProjectionIntegrityError()
                        # Publish the independently certified M2 feed before
                        # durable opportunity reconciliation.  A large
                        # Structure universe can make observer-ledger and
                        # Telegram work slow; that side effect must not hold
                        # the fresh public feed in COLLECTING.
                        if certified_opportunities is None:
                            self.runtime.publish_certified_projection(
                                certified_projection
                            )
                        else:
                            self.runtime.publish_certified_feed(
                                certified_projection,
                                certified_opportunities,
                            )
                        # The certified feed is the user-facing success
                        # boundary.  Publish that truth to /health before
                        # bounded housekeeping so a slow SQLite delete cannot
                        # falsely leave a usable feed in COLLECTING.
                        self.runtime.mark_success(result)
                        logger.info(
                            "neg-risk quote collection complete "
                            f"run_id={result.run_id} "
                            f"responses={result.successful_response_count}/"
                            f"{result.requested_token_count} "
                            f"elapsed_ms={result.elapsed_ms}"
                        )
                        if self._cleanup_old_runs is not None:
                            try:
                                deleted_runs = await self._cleanup_old_runs()
                                logger.info(
                                    "old neg-risk quote runs purged "
                                    f"count={deleted_runs}"
                                )
                                self.runtime.mark_cleanup_success()
                            except Exception as error:
                                self.runtime.mark_cleanup_failure(error)
                                logger.warning(
                                    "old neg-risk quote run cleanup failed "
                                    f"kind={type(error).__name__}"
                                )
                        if self._reconcile_global_projection is not None:
                            try:
                                await self._reconcile_global_projection(
                                    certified_projection
                                )
                            except Exception as error:
                                # A durable observer/Telegram failure must not
                                # invalidate the independently certified quote
                                # feed or turn a successful collection false.
                                logger.exception(
                                    "neg-risk opportunity watcher failed "
                                    f"kind={type(error).__name__}"
                                )
                except asyncio.CancelledError:
                    await cleanup_after_cancellation()
                    raise
                except QuoteCollectionSourceSupersededError:
                    # Structure publication invalidated the quote input while
                    # the child was collecting.  The old run was safely
                    # rejected; immediately bind a new run rather than
                    # waiting a full periodic interval and raising a false
                    # operational incident.
                    retry_immediately = True
                    logger.info(
                        "neg-risk quote collection superseded by Structure; "
                        "retrying immediately"
                    )
                except Exception as error:  # fail-soft producer boundary
                    self.runtime.mark_failure(error)
                    logger.exception(
                        "neg-risk quote collection failed "
                        f"kind={type(error).__name__} "
                        f"consecutive={self.runtime.consecutive_failures}"
                    )
                else:
                    # Collection-only test workers have no certification
                    # boundary, so their successful result is published here.
                    if self._certify_projection is None:
                        self.runtime.mark_success(result)
                        logger.info(
                            "neg-risk quote collection complete "
                            f"run_id={result.run_id} "
                            f"responses={result.successful_response_count}/"
                            f"{result.requested_token_count} "
                            f"elapsed_ms={result.elapsed_ms}"
                        )
                finally:
                    # Certification needs the full run legs, quotes, and source
                    # universe, but steady-state health/opportunity reads do not.
                    # Drop the local owner before the interval wait so the next
                    # snapshot has the memory headroom certification consumed.
                    try:
                        certified_projection = None
                        self._release_projection_memory()
                    finally:
                        self.runtime.mark_pipeline_finished()
                elapsed_s = max(0.0, self._monotonic() - attempt_started)
                delay_s = 0.0 if retry_immediately else max(0.0, self._interval_s - elapsed_s)
                if await self._wait_for_next_attempt(stop_event, delay_s):
                    break
        finally:
            self.runtime.mark_stopped()


def build_production_quote_worker(
    settings: Settings,
    *,
    opportunity_watcher: OpportunityWatcher | None = None,
    producer_lock: asyncio.Lock | None = None,
) -> QuoteWorker | None:
    """Build the public-read-only production worker when explicitly enabled."""
    if not settings.neg_risk_quote_worker_enabled:
        return None
    quote_store = NegRiskQuoteStore(
        settings.db_path,
        structure_generation_read_mode=settings.structure_generation_read_mode,
    )
    opportunity_watcher = opportunity_watcher or OpportunityWatcher(settings)

    async def collect_once() -> QuoteCollectionResult:
        return await collect_quotes_in_subprocess(settings)

    async def certify_projection(
        result: QuoteCollectionResult,
    ) -> CompleteQuoteProjection:
        started = time.perf_counter()
        projection = await certify_latest_quote_projection(
            quote_store,
            result,
        )
        certify_ms = int((time.perf_counter() - started) * 1000)
        if result.attempt_id:
            await asyncio.to_thread(
                quote_store.checkpoint_collection_attempt,
                result.attempt_id,
                phase="projection",
                phase_timings={
                    "universe_ms": result.universe_ms,
                    "admission_ms": result.admission_ms,
                    "fetch_ms": result.fetch_ms,
                    "transform_ms": result.transform_ms,
                    "persist_ms": result.persist_ms,
                    "certify_ms": certify_ms,
                },
            )
        return projection

    async def prepare_opportunities(
        projection: CompleteQuoteProjection,
    ) -> OpportunityScanResult:
        started = time.perf_counter()
        result = await asyncio.to_thread(
            scan_certified_neg_risk_quote_projection,
            projection,
            min_edge_bps=0,
            limit=projection.requested_token_count,
        )
        logger.info(
            "neg-risk opportunity projection prepared "
            f"run_id={projection.run_id} "
            f"count={len(result.opportunities)} "
            f"elapsed_ms={int((time.perf_counter() - started) * 1000)}"
        )
        attempt = await asyncio.to_thread(quote_store.latest_collection_attempt)
        if (
            attempt is not None
            and attempt["outcome"] == "collecting"
            and attempt["quote_run_id"] == projection.run_id
        ):
            timings = dict(attempt["phase_timings"])
            timings["projection_ms"] = int((time.perf_counter() - started) * 1000)
            await asyncio.to_thread(
                quote_store.checkpoint_collection_attempt,
                int(attempt["id"]),
                phase="complete",
                phase_timings=timings,
            )
        return result

    async def restore_feed() -> CertifiedQuoteFeed | None:
        """Rebuild the compact M2 feed from a durable, already-certified run."""
        projection = await asyncio.to_thread(quote_store.latest_complete_projection)
        if projection is None:
            return None
        opportunities = await prepare_opportunities(projection)
        if (
            opportunities.quote_run_id != projection.run_id
            or opportunities.source_snapshot_id != projection.universe_snapshot_id
            or opportunities.universe_hash != projection.universe_hash
        ):
            raise QuoteProjectionIntegrityError()
        return CertifiedQuoteFeed(
            CertifiedQuoteMetadata.from_projection(projection),
            opportunities,
        )

    async def cleanup_collecting_runs() -> int:
        return await asyncio.to_thread(
            quote_store.fail_collecting_runs,
            failure_reason="collector-cancelled",
        )

    async def cleanup_old_runs() -> int:
        return await asyncio.to_thread(
            quote_store.purge_old_runs,
            keep_last_per_status=10,
            # One produced run per cycle means one deletion is enough for
            # steady-state boundedness.  Large batches caused 12-129 second
            # SQLite writer stalls on the production history database.
            max_runs=1,
        )

    async def reconcile_global_projection(
        projection: CompleteQuoteProjection,
    ) -> None:
        await opportunity_watcher.reconcile_global_projection(projection)

    return QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        reconcile_global_projection=reconcile_global_projection,
        restore_feed=restore_feed,
        cleanup_collecting_runs=cleanup_collecting_runs,
        cleanup_old_runs=cleanup_old_runs,
        producer_lock=producer_lock,
        interval_s=settings.neg_risk_quote_interval_s,
    )
