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
from polyarb.daemon.producer_arbitration import ProducerArbitrator, ProducerLease
from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.neg_risk_quote_collector import (
    QUOTE_FETCH_TIMEOUT_EXIT_CODE,
    QUOTE_PERSIST_TIMEOUT_EXIT_CODE,
    QuoteCollectionResult,
)
from polyarb.routing.neg_risk_quote_store import (
    CompleteQuoteProjection,
    NegRiskQuoteStore,
    QuoteProjectionIntegrityError,
)
from polyarb.routing.opportunity_scanner import (
    NegRiskOpportunity,
    OpportunityLeg,
    OpportunityScanResult,
    StaleQuoteRunError,
    StaleUniverseError,
    scan_certified_neg_risk_quote_projection,
)
from polyarb.routing.quote_timing import bounded_quote_supervisor_timeout_s
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
ReclaimFailedPayloads = Callable[[], Awaitable[int]]
WaitForStop = Callable[[asyncio.Event, float], Awaitable[bool]]
ReleaseProjectionMemory = Callable[[], None]
CompleteAttempt = Callable[[QuoteCollectionResult], Awaitable[None]]
FailAttempt = Callable[[QuoteCollectionResult, str], Awaitable[None]]
RecordTimeoutIncident = Callable[
    ["QuoteCollectionSubprocessError", "QuoteWorkerRuntime"], Awaitable[None]
]
RecordFailureIncident = Callable[
    ["QuoteCollectionSubprocessError", "QuoteWorkerRuntime"], Awaitable[None]
]
RecordPipelineFailureIncident = Callable[
    [BaseException, "QuoteWorkerRuntime", QuoteCollectionResult | None], Awaitable[None]
]
RecordCertifiedSuccessIncident = Callable[[QuoteCollectionResult], Awaitable[None]]
OnCycleStarted = Callable[[], Awaitable[None]]
_BACKGROUND_REAP_TASKS: set[asyncio.Task[object]] = set()


def _retain_background_reap(task: asyncio.Task[object]) -> None:
    """Retain deadline-exhausted reap work and consume its final exception."""
    _BACKGROUND_REAP_TASKS.add(task)

    def consume(completed: asyncio.Task[object]) -> None:
        _BACKGROUND_REAP_TASKS.discard(completed)
        try:
            completed.result()
        except BaseException:
            pass

    task.add_done_callback(consume)


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
        logger.debug(f"certified projection memory release skipped kind={type(error).__name__}")


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
    last_success_at_s: float | None
    last_run_id: int | None
    last_requested_token_count: int | None
    last_successful_response_count: int | None
    last_elapsed_ms: int | None
    last_error_kind: str | None
    cleanup_success_count: int
    cleanup_failure_count: int
    cleanup_consecutive_failures: int
    last_cleanup_error_kind: str | None
    hydration_consecutive_failures: int
    hydration_last_error_kind: str | None
    hydration_last_attempt_at_s: float | None


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

    def __init__(
        self,
        reason: str,
        *,
        diagnostic: str | None = None,
        attempt_id: int | None = None,
        run_id: int | None = None,
        requested_token_count: int | None = None,
    ) -> None:
        self.reason = reason
        self.diagnostic = diagnostic
        self.attempt_id = attempt_id
        self.run_id = run_id
        self.requested_token_count = requested_token_count
        super().__init__(f"quote-collection-subprocess-{reason}")


class QuoteCollectionSourceSupersededError(QuoteCollectionSubprocessError):
    """A newer certified Structure revision replaced the quote input mid-run."""

    def __init__(self) -> None:
        super().__init__("source-superseded")


def _child_stderr_tail(stderr: bytes, *, limit: int = 1_024) -> str | None:
    """Return bounded operator evidence without treating child stdout as logs."""
    text = stderr.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    # Tracebacks put the actionable exception at the tail.  Bound it before
    # logging so a pathological upstream response cannot create log pressure.
    return text[-limit:]


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
        self.last_success_at_s: float | None = None
        self.last_run_id: int | None = None
        self.last_requested_token_count: int | None = None
        self.last_successful_response_count: int | None = None
        self.last_elapsed_ms: int | None = None
        self.last_error_kind: str | None = None
        self.cleanup_success_count = 0
        self.cleanup_failure_count = 0
        self.cleanup_consecutive_failures = 0
        self.last_cleanup_error_kind: str | None = None
        self.hydration_consecutive_failures = 0
        self.hydration_last_error_kind: str | None = None
        self.hydration_last_attempt_at_s: float | None = None
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
        self.last_success_at_s = self.last_attempt_finished_at_s
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

    def mark_hydration_failure(self, error: Exception) -> None:
        """Retain parent-side durable-feed retry evidence for health/dashboard."""
        self.hydration_consecutive_failures += 1
        self.hydration_last_error_kind = type(error).__name__
        self.hydration_last_attempt_at_s = time.time()

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
        self.hydration_consecutive_failures = 0
        self.hydration_last_error_kind = None
        self.hydration_last_attempt_at_s = time.time()

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
            last_success_at_s=self.last_success_at_s,
            last_run_id=self.last_run_id,
            last_requested_token_count=self.last_requested_token_count,
            last_successful_response_count=self.last_successful_response_count,
            last_elapsed_ms=self.last_elapsed_ms,
            last_error_kind=self.last_error_kind,
            cleanup_success_count=self.cleanup_success_count,
            cleanup_failure_count=self.cleanup_failure_count,
            cleanup_consecutive_failures=self.cleanup_consecutive_failures,
            last_cleanup_error_kind=self.last_cleanup_error_kind,
            hydration_consecutive_failures=self.hydration_consecutive_failures,
            hydration_last_error_kind=self.hydration_last_error_kind,
            hydration_last_attempt_at_s=self.hydration_last_attempt_at_s,
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
    projection = await asyncio.to_thread(quote_store.latest_complete_projection)
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


async def _terminalize_quote_attempt(
    store: NegRiskQuoteStore,
    attempt_id: int,
    failure_kind: str,
) -> None:
    """Best-effort terminal evidence; never mask the producer's root error."""
    try:
        await asyncio.to_thread(
            store.checkpoint_collection_attempt,
            attempt_id,
            phase="failed",
            failure_kind=failure_kind,
        )
    except Exception as error:  # noqa: BLE001 - preserve root failure
        logger.warning(
            "quote attempt terminal checkpoint failed "
            f"attempt_id={attempt_id} kind={type(error).__name__}"
        )


async def _terminate_quote_child(
    process: asyncio.subprocess.Process,
    communicate_task: asyncio.Task[tuple[bytes, bytes]],
    *,
    terminate_timeout_s: float,
) -> None:
    shutdown_deadline = time.monotonic() + (2 * terminate_timeout_s)

    def remaining_s() -> float:
        return max(0.0, shutdown_deadline - time.monotonic())

    try:
        process.terminate()
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(
            asyncio.shield(communicate_task),
            timeout=min(terminate_timeout_s, remaining_s()),
        )
        return
    except TimeoutError:
        pass
    try:
        process.kill()
    except ProcessLookupError:
        pass
    wait_task: asyncio.Task[object] = asyncio.create_task(process.wait())
    done, _pending = await asyncio.wait(
        {communicate_task, wait_task},
        timeout=remaining_s(),
        return_when=asyncio.FIRST_COMPLETED,
    )
    if wait_task in done:
        wait_task.result()
        if not communicate_task.done():
            communicate_task.cancel()
            _retain_background_reap(communicate_task)
        return
    if communicate_task in done:
        communicate_task.result()
        if not wait_task.done():
            _retain_background_reap(wait_task)
        return
    communicate_task.cancel()
    _retain_background_reap(communicate_task)
    _retain_background_reap(wait_task)
    raise TimeoutError("quote child reap exceeded shutdown deadline")


async def collect_quotes_in_subprocess(
    settings: Settings,
    *,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = (asyncio.create_subprocess_exec),
    terminate_timeout_s: float | None = None,
) -> QuoteCollectionResult:
    """Run all SDK fetch/decode/SQLite collection work outside the HTTP process."""
    attempt_started = time.monotonic()
    if terminate_timeout_s is None:
        terminate_timeout_s = settings.neg_risk_quote_shutdown_reserve_s / 2
    attempt_store = NegRiskQuoteStore(
        settings.db_path,
        writer_timeout_s=settings.neg_risk_quote_writer_timeout_s,
    )
    try:
        attempt_id = await asyncio.to_thread(attempt_store.start_collection_attempt)
    except sqlite3.OperationalError as error:
        if "no such table: neg_risk_quote_attempts" not in str(error):
            raise
        await asyncio.to_thread(SQLiteStore(settings.db_path).init_schema)
        attempt_id = await asyncio.to_thread(attempt_store.start_collection_attempt)
    try:
        process = await spawn(
            sys.executable,
            "-m",
            "polyarb.cli_arbitrage",
            "collect-neg-risk-quotes",
            "--db-path",
            str(settings.db_path),
            "--attempt-id",
            str(attempt_id),
            "--schema-ready",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except BaseException:
        await _terminalize_quote_attempt(attempt_store, attempt_id, "spawn-failed")
        raise
    started = time.perf_counter()
    logger.info(f"isolated quote collection started pid={getattr(process, 'pid', None)}")
    # Keep ownership of one pipe-reader task across cancellation.  Awaiting
    # communicate() directly lets task cancellation tear down its stream
    # readers; a second communicate() then cannot reliably reap the still-live
    # child.  The shutdown path must prove the child exited before the durable
    # collecting lease can be released.
    communicate_task = asyncio.create_task(process.communicate())
    shutdown_reserve_s = 2 * terminate_timeout_s
    communicate_budget_s = max(
        0.0,
        settings.neg_risk_quote_child_hard_limit_s
        - (time.monotonic() - attempt_started)
        - shutdown_reserve_s,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.shield(communicate_task),
            timeout=communicate_budget_s,
        )
    except TimeoutError as error:
        termination_error: Exception | None = None
        try:
            await _terminate_quote_child(
                process,
                communicate_task,
                terminate_timeout_s=terminate_timeout_s,
            )
        except Exception as child_error:  # preserve timeout evidence and cleanup
            termination_error = child_error
            logger.error(
                f"quote child reap failed attempt_id={attempt_id} kind={type(child_error).__name__}"
            )
        try:
            await asyncio.to_thread(
                attempt_store.fail_collecting_runs,
                failure_reason="collector-hard-timeout",
            )
        except Exception as cleanup_error:  # attempt terminalization must still run
            logger.error(
                "quote timeout run cleanup failed "
                f"attempt_id={attempt_id} kind={type(cleanup_error).__name__}"
            )
        await _terminalize_quote_attempt(
            attempt_store,
            attempt_id,
            ("child-reap-timeout" if termination_error is not None else "child-hard-timeout"),
        )
        raise QuoteCollectionSubprocessError("timeout", attempt_id=attempt_id) from error
    except asyncio.CancelledError:
        try:
            await _terminate_quote_child(
                process,
                communicate_task,
                terminate_timeout_s=terminate_timeout_s,
            )
        except BaseException as cleanup_error:
            logger.error(
                "quote cancellation child cleanup failed "
                f"attempt_id={attempt_id} pid={getattr(process, 'pid', None)} "
                f"kind={type(cleanup_error).__name__}"
            )
        try:
            await asyncio.to_thread(
                attempt_store.fail_collecting_runs,
                failure_reason="collector-cancelled",
            )
        except BaseException as cleanup_error:
            logger.error(
                "quote cancellation run cleanup failed "
                f"attempt_id={attempt_id} kind={type(cleanup_error).__name__}"
            )
        await _terminalize_quote_attempt(attempt_store, attempt_id, "parent-cancelled")
        raise

    if process.returncode != 0:
        if process.returncode in {
            QUOTE_FETCH_TIMEOUT_EXIT_CODE,
            QUOTE_PERSIST_TIMEOUT_EXIT_CODE,
        }:
            try:
                failure_payload = json.loads(stdout)
            except (UnicodeDecodeError, json.JSONDecodeError):
                failure_payload = None
            if (
                isinstance(failure_payload, dict)
                and set(failure_payload)
                == {"attempt_id", "elapsed_ms", "outcome", "reason", "stage"}
                and failure_payload.get("attempt_id") == attempt_id
                and not isinstance(failure_payload.get("elapsed_ms"), bool)
                and isinstance(failure_payload.get("elapsed_ms"), int)
                and failure_payload["elapsed_ms"] >= 0
                and failure_payload.get("outcome") == "failed"
                and failure_payload.get("reason")
                == (
                    "fetch-timeout"
                    if process.returncode == QUOTE_FETCH_TIMEOUT_EXIT_CODE
                    else "persist-timeout"
                )
                and failure_payload.get("stage")
                == (
                    "fetch"
                    if process.returncode == QUOTE_FETCH_TIMEOUT_EXIT_CODE
                    else "persist"
                )
            ):
                failure_kind = (
                    "child-fetch-timeout"
                    if process.returncode == QUOTE_FETCH_TIMEOUT_EXIT_CODE
                    else "child-persist-timeout"
                )
                failure_reason = (
                    "collector-fetch-timeout"
                    if process.returncode == QUOTE_FETCH_TIMEOUT_EXIT_CODE
                    else "collector-persist-timeout"
                )
                try:
                    await asyncio.to_thread(
                        attempt_store.fail_collecting_runs,
                        failure_reason=failure_reason,
                    )
                except Exception as cleanup_error:
                    logger.error(
                        "quote child timeout run cleanup failed "
                        f"attempt_id={attempt_id} kind={type(cleanup_error).__name__}"
                    )
                await _terminalize_quote_attempt(
                    attempt_store,
                    attempt_id,
                    failure_kind,
                )
                raise QuoteCollectionSubprocessError("timeout", attempt_id=attempt_id)
        await _terminalize_quote_attempt(attempt_store, attempt_id, "child-failed")
        diagnostic = _child_stderr_tail(stderr)
        if "verified universe snapshot is no longer the latest published truth" in (
            diagnostic or ""
        ):
            logger.info(
                "isolated quote collection superseded by a newer Structure revision "
                f"pid={getattr(process, 'pid', None)}"
            )
            raise QuoteCollectionSourceSupersededError()
        logger.warning(
            "isolated quote collection failed "
            f"returncode={process.returncode} "
            f"stderr_bytes={len(stderr)} "
            f"stderr_tail={diagnostic!r}"
        )
        raise QuoteCollectionSubprocessError("failed", diagnostic=diagnostic)
    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        await _terminalize_quote_attempt(attempt_store, attempt_id, "invalid-json")
        raise QuoteCollectionSubprocessError("invalid-json") from error
    if not isinstance(payload, dict) or payload.get("status") != "complete":
        await _terminalize_quote_attempt(attempt_store, attempt_id, "invalid-json")
        raise QuoteCollectionSubprocessError("invalid-json")
    universe_hash = payload.get("universe_hash")
    if not isinstance(universe_hash, str) or len(universe_hash) != 64:
        await _terminalize_quote_attempt(attempt_store, attempt_id, "invalid-json")
        raise QuoteCollectionSubprocessError("invalid-json")
    required_int_fields = (
        "run_id",
        "universe_snapshot_id",
        "requested_token_count",
        "successful_response_count",
        "quote_taken_at_ms",
        "elapsed_ms",
        "attempt_id",
        "universe_ms",
        "admission_ms",
        "fetch_ms",
        "transform_ms",
        "persist_ms",
    )
    if any(
        isinstance(payload.get(key), bool) or not isinstance(payload.get(key), int)
        for key in required_int_fields
    ):
        await _terminalize_quote_attempt(attempt_store, attempt_id, "invalid-json")
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
        await _terminalize_quote_attempt(attempt_store, attempt_id, "invalid-json")
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
        recover_orphaned_collecting_runs: CleanupCollectingRuns | None = None,
        cleanup_collecting_runs: CleanupCollectingRuns | None = None,
        cleanup_old_runs: CleanupOldRuns | None = None,
        reclaim_failed_payloads: ReclaimFailedPayloads | None = None,
        complete_attempt: CompleteAttempt | None = None,
        fail_attempt: FailAttempt | None = None,
        record_timeout_incident: RecordTimeoutIncident | None = None,
        record_failure_incident: RecordFailureIncident | None = None,
        record_pipeline_failure_incident: RecordPipelineFailureIncident | None = None,
        record_certified_success_incident: RecordCertifiedSuccessIncident | None = None,
        on_cycle_started: OnCycleStarted | None = None,
        producer_arbitrator: ProducerArbitrator | None = None,
        producer_lease_s: float = 180.0,
        producer_lock: asyncio.Lock | None = None,
        interval_s: float,
        stop_after_consecutive_timeouts: int | None = None,
        runtime: QuoteWorkerRuntime | None = None,
        wait_for_stop: WaitForStop = _wait_for_stop,
        monotonic: Callable[[], float] = time.monotonic,
        release_projection_memory: ReleaseProjectionMemory = (_release_projection_memory),
    ) -> None:
        if (
            isinstance(interval_s, bool)
            or interval_s <= 0
            or (
                stop_after_consecutive_timeouts is not None
                and (
                    isinstance(stop_after_consecutive_timeouts, bool)
                    or stop_after_consecutive_timeouts < 1
                )
            )
        ):
            raise ValueError("interval_s must be positive")
        self._collect_once = collect_once
        self._certify_projection = certify_projection
        self._prepare_opportunities = prepare_opportunities
        self._reconcile_global_projection = reconcile_global_projection
        self._restore_feed = restore_feed
        self._recover_orphaned_collecting_runs = recover_orphaned_collecting_runs
        self._cleanup_collecting_runs = cleanup_collecting_runs
        self._cleanup_old_runs = cleanup_old_runs
        self._reclaim_failed_payloads = reclaim_failed_payloads
        self._complete_attempt = complete_attempt
        self._fail_attempt = fail_attempt
        self._record_timeout_incident = record_timeout_incident
        self._record_failure_incident = record_failure_incident
        self._record_pipeline_failure_incident = record_pipeline_failure_incident
        self._record_certified_success_incident = record_certified_success_incident
        self._on_cycle_started = on_cycle_started
        self._producer_arbitrator = producer_arbitrator
        self._producer_lease_s = producer_lease_s
        self._producer_lock = producer_lock
        self._interval_s = interval_s
        self._stop_after_consecutive_timeouts = stop_after_consecutive_timeouts
        self._wait_for_stop = wait_for_stop
        self._monotonic = monotonic
        self._release_projection_memory = release_projection_memory
        self.runtime = runtime or QuoteWorkerRuntime()
        self._request_now_event = asyncio.Event()

    @property
    def interval_s(self) -> float:
        return self._interval_s

    @property
    def supervisor_recovery_requested(self) -> bool:
        """Whether a bounded timeout exit deliberately yielded to supervision."""
        return getattr(self, "_supervisor_recovery_requested", False)

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
                logger.info(f"released collecting quote runs after cancellation count={released}")
            except Exception as error:  # preserve cancellation semantics
                logger.warning(f"collecting quote run cleanup failed kind={type(error).__name__}")

        try:
            if self._recover_orphaned_collecting_runs is not None:
                try:
                    released = await self._recover_orphaned_collecting_runs()
                    logger.info(
                        "released orphaned collecting quote runs before admission "
                        f"count={released}"
                    )
                except asyncio.CancelledError:
                    await cleanup_after_cancellation()
                    raise
                except Exception as error:
                    logger.warning(
                        "orphaned collecting quote run recovery failed "
                        f"kind={type(error).__name__}"
                    )
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
                        f"certified quote feed restore failed kind={type(error).__name__}"
                    )
            while not stop_event.is_set():
                attempt_started = self._monotonic()
                retry_immediately = False
                next_delay_override_s: float | None = None
                exit_for_supervisor = False
                result = None
                producer_slot_acquired = False
                producer_lease: ProducerLease | None = None
                pipeline_started = False
                if self._producer_arbitrator is not None:
                    producer_lease = await asyncio.to_thread(
                        self._producer_arbitrator.acquire,
                        owner="quote",
                        lease_s=self._producer_lease_s,
                    )
                    if producer_lease is None:
                        # Structure owns at most a 45-second bounded slice.
                        # A short, interruptible retry avoids busy-spinning and
                        # lets Quote take the slot immediately after release.
                        if await self._wait_for_next_attempt(stop_event, 2.0):
                            break
                        continue
                self.runtime.mark_pipeline_started()
                pipeline_started = True
                try:
                    self.runtime.mark_started()
                    if self._on_cycle_started is not None:
                        try:
                            await self._on_cycle_started()
                        except Exception as error:
                            logger.warning(
                                "quote supervised progress heartbeat failed "
                                f"kind={type(error).__name__}"
                            )
                    # Quote is the M2 source-of-truth producer.  Its durable
                    # transaction is larger than the child collection alone:
                    # certification and feed publication also read/write the
                    # same SQLite database.  Releasing this slot after the
                    # child returns allowed Structure writers to interleave
                    # with that critical tail and stretch Quote persistence
                    # from seconds to tens of seconds in production.
                    if self._producer_lock is not None:
                        await self._producer_lock.acquire()
                        producer_slot_acquired = True
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
                                certified_opportunities.quote_run_id != certified_projection.run_id
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
                            self.runtime.publish_certified_projection(certified_projection)
                        else:
                            self.runtime.publish_certified_feed(
                                certified_projection,
                                certified_opportunities,
                            )
                        if self._complete_attempt is not None:
                            await self._complete_attempt(result)
                        # The certified feed is the user-facing success
                        # boundary.  Publish that truth to /health before
                        # bounded housekeeping so a slow SQLite delete cannot
                        # falsely leave a usable feed in COLLECTING.
                        self.runtime.mark_success(result)
                        if self._record_certified_success_incident is not None:
                            try:
                                await self._record_certified_success_incident(result)
                            except Exception as incident_error:
                                logger.exception(
                                    "quote recovery incident recording failed "
                                    f"kind={type(incident_error).__name__}"
                                )
                        logger.info(
                            "neg-risk quote collection complete "
                            f"run_id={result.run_id} "
                            f"responses={result.successful_response_count}/"
                            f"{result.requested_token_count} "
                            f"elapsed_ms={result.elapsed_ms}"
                        )
                        # The shared write-critical boundary ends at the
                        # certified public feed. Retention, global observer
                        # reconciliation and notification delivery are
                        # deliberately outside it: they are fail-soft side
                        # effects and must never monopolize Structure's slot.
                        if producer_slot_acquired:
                            self._producer_lock.release()
                            producer_slot_acquired = False
                        if producer_lease is not None:
                            await asyncio.to_thread(
                                self._producer_arbitrator.release, producer_lease
                            )
                            producer_lease = None
                        if self._cleanup_old_runs is not None:
                            try:
                                deleted_runs = await self._cleanup_old_runs()
                                logger.info(f"old neg-risk quote runs purged count={deleted_runs}")
                                self.runtime.mark_cleanup_success()
                            except Exception as error:
                                self.runtime.mark_cleanup_failure(error)
                                logger.warning(
                                    "old neg-risk quote run cleanup failed "
                                    f"kind={type(error).__name__}"
                                )
                        if self._reconcile_global_projection is not None:
                            try:
                                await self._reconcile_global_projection(certified_projection)
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
                        "neg-risk quote collection superseded by Structure; retrying immediately"
                    )
                except QuoteCollectionSubprocessError as error:
                    # Child timeout/protocol/fetch failures already released
                    # their process, lease and durable attempt. Do not spend
                    # the remaining cadence sleeping while the old feed ages.
                    retry_immediately = error.reason == "timeout"
                    if result is not None and self._fail_attempt is not None:
                        await self._fail_attempt(result, type(error).__name__)
                    self.runtime.mark_failure(error)
                    if self._reclaim_failed_payloads is not None:
                        try:
                            reclaimed_runs = await self._reclaim_failed_payloads()
                            logger.info(
                                f"failed neg-risk quote payloads reclaimed count={reclaimed_runs}"
                            )
                        except Exception as reclaim_error:
                            logger.warning(
                                "failed quote payload reclaim failed "
                                f"kind={type(reclaim_error).__name__}"
                            )
                    if self._record_timeout_incident is not None and error.reason == "timeout":
                        try:
                            await self._record_timeout_incident(error, self.runtime)
                        except Exception as incident_error:
                            logger.exception(
                                "quote timeout incident recording failed "
                                f"kind={type(incident_error).__name__}"
                            )
                    if self._record_failure_incident is not None and error.reason != "timeout":
                        try:
                            await self._record_failure_incident(error, self.runtime)
                        except Exception as incident_error:
                            logger.exception(
                                "quote failure incident recording failed "
                                f"kind={type(incident_error).__name__}"
                            )
                    logger.exception(
                        "neg-risk quote child failed "
                        f"retry_immediately={retry_immediately} "
                        f"kind={type(error).__name__} "
                        f"consecutive={self.runtime.consecutive_failures}"
                    )
                    if (
                        error.reason == "timeout"
                        and self._stop_after_consecutive_timeouts is not None
                        and self.runtime.consecutive_failures
                        >= self._stop_after_consecutive_timeouts
                    ):
                        exit_for_supervisor = True
                        self._supervisor_recovery_requested = True
                        logger.error(
                            "quote timeout threshold reached; "
                            "exiting for outer supervisor recovery "
                            f"threshold={self._stop_after_consecutive_timeouts}"
                        )
                except Exception as error:  # fail-soft producer boundary
                    if result is not None and self._fail_attempt is not None:
                        await self._fail_attempt(result, type(error).__name__)
                    self.runtime.mark_failure(error)
                    if self._record_pipeline_failure_incident is not None:
                        try:
                            await self._record_pipeline_failure_incident(
                                error, self.runtime, result
                            )
                        except Exception as incident_error:
                            logger.exception(
                                "quote pipeline failure incident recording failed "
                                f"kind={type(incident_error).__name__}"
                            )
                    logger.exception(
                        "neg-risk quote collection failed "
                        f"kind={type(error).__name__} "
                        f"consecutive={self.runtime.consecutive_failures}"
                    )
                    if isinstance(error, StaleUniverseError):
                        # The completed child proved the currently published
                        # Structure truth is too old. Repeating a 40k-token
                        # fetch only starves the in-flight Structure
                        # publication that can make the universe valid again.
                        # A successful pointer switch calls request_now(), so
                        # this bounded backoff never delays actual recovery.
                        next_delay_override_s = 300.0
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
                    if producer_slot_acquired:
                        self._producer_lock.release()
                    if producer_lease is not None:
                        await asyncio.to_thread(
                            self._producer_arbitrator.release, producer_lease
                        )
                    # Certification needs the full run legs, quotes, and source
                    # universe, but steady-state health/opportunity reads do not.
                    # Drop the local owner before the interval wait so the next
                    # snapshot has the memory headroom certification consumed.
                    try:
                        certified_projection = None
                        self._release_projection_memory()
                    finally:
                        if pipeline_started:
                            self.runtime.mark_pipeline_finished()
                elapsed_s = max(0.0, self._monotonic() - attempt_started)
                if exit_for_supervisor:
                    break
                delay_s = (
                    0.0
                    if retry_immediately
                    else max(
                        0.0,
                        (next_delay_override_s or self._interval_s) - elapsed_s,
                    )
                )
                if await self._wait_for_next_attempt(stop_event, delay_s):
                    break
        finally:
            self.runtime.mark_stopped()


def load_certified_quote_feed(
    settings: Settings,
    *,
    now_s: Callable[[], float] = time.time,
    max_quote_age_s: float = 300,
) -> CertifiedQuoteFeed | None:
    """Rebuild a compact public feed from one durably certified Quote run.

    This is intentionally read-only so the HTTP parent can hydrate its cache
    when collection is owned by an isolated producer process.
    """
    quote_store = NegRiskQuoteStore(
        settings.db_path,
        structure_generation_read_mode=settings.structure_generation_read_mode,
        writer_timeout_s=settings.neg_risk_quote_writer_timeout_s,
    )
    compact_reader = getattr(quote_store, "latest_compact_feed", None)
    compact = None if compact_reader is None else compact_reader()
    if compact is None:
        metadata_reader = getattr(quote_store, "latest_complete_projection_metadata", None)
        metadata = None if metadata_reader is None else metadata_reader()
        if metadata is not None and (
            max(0.0, now_s() - metadata.quoted_at_ms / 1_000) > max_quote_age_s
        ):
            raise StaleQuoteRunError("quote age exceeds compact-feed limit")
        return None
    metadata, payload = compact
    if metadata is not None:
        quote_age_s = max(0.0, now_s() - metadata.quoted_at_ms / 1_000)
        if quote_age_s > max_quote_age_s:
            raise StaleQuoteRunError(f"quote age {quote_age_s:.1f}s exceeds {max_quote_age_s:.1f}s")
    opportunities = _compact_feed_scan(metadata, payload)
    return CertifiedQuoteFeed(
        CertifiedQuoteMetadata(
            metadata.run_id,
            metadata.universe_snapshot_id,
            metadata.universe_taken_at_ms,
            metadata.quoted_at_ms,
            metadata.requested_token_count,
            metadata.successful_response_count,
            metadata.universe_hash,
            metadata.source_truth_hash,
        ),
        opportunities,
    )


def _compact_feed_scan(metadata, payload):
    rows = payload.get("opportunities", [])
    if not isinstance(rows, list):
        raise QuoteProjectionIntegrityError()
    values = []
    for row in rows:
        if not isinstance(row, dict):
            raise QuoteProjectionIntegrityError()
        legs = tuple(OpportunityLeg(**leg) for leg in row.pop("legs"))
        values.append(NegRiskOpportunity(**row, legs=legs))
    return OpportunityScanResult(
        tuple(values),
        payload.get("rejections", {}),
        metadata.universe_snapshot_id,
        metadata.universe_hash,
        metadata.run_id,
    )


def build_production_quote_worker(
    settings: Settings,
    *,
    opportunity_watcher: OpportunityWatcher | None = None,
    producer_lock: asyncio.Lock | None = None,
    perception_store: OpportunityPerceptionStore | None = None,
    stop_after_consecutive_timeouts: int | None = None,
    on_cycle_started: OnCycleStarted | None = None,
    producer_arbitrator: ProducerArbitrator | None = None,
) -> QuoteWorker | None:
    """Build the public-read-only production worker when explicitly enabled."""
    if not settings.neg_risk_quote_worker_enabled:
        return None
    quote_store = NegRiskQuoteStore(
        settings.db_path,
        structure_generation_read_mode=settings.structure_generation_read_mode,
        writer_timeout_s=settings.neg_risk_quote_writer_timeout_s,
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
        await asyncio.to_thread(
            quote_store.persist_compact_feed,
            projection.run_id,
            {
                "opportunities": [item.to_dict() for item in result.opportunities],
                "rejections": dict(result.rejections),
            },
        )
        logger.info(
            "neg-risk opportunity projection prepared "
            f"run_id={projection.run_id} "
            f"count={len(result.opportunities)} "
            f"elapsed_ms={int((time.perf_counter() - started) * 1000)}"
        )
        return result

    async def complete_attempt(result: QuoteCollectionResult) -> None:
        try:
            attempt = await asyncio.to_thread(quote_store.latest_collection_attempt)
            if (
                attempt is None
                or attempt["outcome"] != "collecting"
                or attempt["id"] != result.attempt_id
            ):
                return
            timings = dict(attempt["phase_timings"])
            await asyncio.to_thread(
                quote_store.checkpoint_collection_attempt,
                result.attempt_id,
                phase="complete",
                phase_timings=timings,
            )
        except Exception as error:  # feed is already certified and published
            logger.warning(
                "quote attempt completion checkpoint failed "
                f"attempt_id={result.attempt_id} kind={type(error).__name__}"
            )

    async def fail_attempt(result: QuoteCollectionResult, failure_kind: str) -> None:
        await _terminalize_quote_attempt(
            quote_store,
            result.attempt_id,
            f"parent-{failure_kind}",
        )

    async def restore_feed() -> CertifiedQuoteFeed | None:
        """Rebuild the compact M2 feed from a durable, already-certified run."""
        return await asyncio.to_thread(
            load_certified_quote_feed,
            settings,
        )

    async def cleanup_collecting_runs() -> int:
        return await asyncio.to_thread(
            quote_store.fail_collecting_runs,
            failure_reason="collector-cancelled",
        )

    async def recover_orphaned_collecting_runs() -> int:
        """A newly started sole worker owns no predecessor child process."""
        return await asyncio.to_thread(
            quote_store.recover_orphaned_collection_state,
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

    async def reclaim_failed_payloads() -> int:
        return await asyncio.to_thread(
            quote_store.reclaim_terminal_failed_payloads,
            max_runs=1,
        )

    async def reconcile_global_projection(
        projection: CompleteQuoteProjection,
    ) -> None:
        await opportunity_watcher.reconcile_global_projection(projection)

    incident_lifecycle = (
        None
        if perception_store is None
        else QuoteIncidentLifecycle(IncidentManager(perception_store))
    )

    async def record_timeout_incident(
        error: QuoteCollectionSubprocessError,
        runtime: QuoteWorkerRuntime,
    ) -> None:
        if incident_lifecycle is None:
            return
        snapshot = runtime.snapshot()
        has_attempt_identity = error.attempt_id is not None
        attempt = await asyncio.to_thread(quote_store.latest_collection_attempt)
        attempt_evidence = (
            attempt
            if attempt is not None and attempt.get("id") == error.attempt_id
            else {}
        )
        await asyncio.to_thread(
            incident_lifecycle.record_timeout,
            attempt_id=error.attempt_id,
            run_id=error.run_id if has_attempt_identity else snapshot.last_run_id,
            requested_token_count=(
                error.requested_token_count
                if has_attempt_identity
                else snapshot.last_requested_token_count
            ),
            deadline_s=settings.neg_risk_quote_child_hard_limit_s,
            consecutive_failures=snapshot.consecutive_failures,
            last_success_age_s=(
                None
                if snapshot.last_success_at_s is None
                else max(0.0, time.time() - snapshot.last_success_at_s)
            ),
            failure_kind=attempt_evidence.get("failure_kind"),
            attempt_phase=attempt_evidence.get("phase"),
            phase_timings=attempt_evidence.get("phase_timings"),
        )

    async def record_failure_incident(
        error: QuoteCollectionSubprocessError,
        runtime: QuoteWorkerRuntime,
    ) -> None:
        if incident_lifecycle is not None:
            await asyncio.to_thread(
                incident_lifecycle.record_failure,
                error=error,
                runtime=runtime,
            )

    async def record_pipeline_failure_incident(
        error: BaseException,
        runtime: QuoteWorkerRuntime,
        result: QuoteCollectionResult | None,
    ) -> None:
        if incident_lifecycle is not None:
            await asyncio.to_thread(
                incident_lifecycle.record_pipeline_failure,
                error=error,
                runtime=runtime,
                attempt_id=None if result is None else result.attempt_id,
                run_id=None if result is None else result.run_id,
            )

    async def record_certified_success_incident(result: QuoteCollectionResult) -> None:
        if incident_lifecycle is not None:
            await asyncio.to_thread(incident_lifecycle.record_certified_success, result)

    return QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        reconcile_global_projection=reconcile_global_projection,
        restore_feed=restore_feed,
        recover_orphaned_collecting_runs=recover_orphaned_collecting_runs,
        cleanup_collecting_runs=cleanup_collecting_runs,
        cleanup_old_runs=cleanup_old_runs,
        reclaim_failed_payloads=reclaim_failed_payloads,
        complete_attempt=complete_attempt,
        fail_attempt=fail_attempt,
        record_timeout_incident=record_timeout_incident,
        record_failure_incident=record_failure_incident,
        record_pipeline_failure_incident=record_pipeline_failure_incident,
        record_certified_success_incident=record_certified_success_incident,
        on_cycle_started=on_cycle_started,
        producer_arbitrator=producer_arbitrator,
        # The process child has a 180s collection cap, but the lease must
        # protect the entire supervised pipeline through certification and
        # compact-feed publication. Otherwise its lease expires while that
        # tail is still writing and a replacement worker overlaps it.
        producer_lease_s=bounded_quote_supervisor_timeout_s(
            settings.neg_risk_quote_supervisor_timeout_s,
            settings.neg_risk_quote_interval_s,
        ),
        producer_lock=producer_lock,
        interval_s=settings.neg_risk_quote_interval_s,
        stop_after_consecutive_timeouts=stop_after_consecutive_timeouts,
    )
