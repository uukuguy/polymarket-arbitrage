"""Fail-soft periodic producer for atomic neg-risk quote runs."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loguru import logger

from polyarb.config import Settings
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

CollectOnce = Callable[[], Awaitable[QuoteCollectionResult]]
CertifyProjection = Callable[
    [QuoteCollectionResult],
    Awaitable[CompleteQuoteProjection],
]
PrepareOpportunities = Callable[
    [CompleteQuoteProjection],
    Awaitable[OpportunityScanResult],
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


class QuoteCollectionSubprocessError(RuntimeError):
    """The isolated quote collector did not return one valid complete result."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"quote-collection-subprocess-{reason}")


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
        self._certified_feed: CertifiedQuoteFeed | None = None

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
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.cli_arbitrage",
        "collect-neg-risk-quotes",
        "--db-path",
        str(settings.db_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.perf_counter()
    logger.info(
        "isolated quote collection started "
        f"pid={getattr(process, 'pid', None)}"
    )
    try:
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(
                process.communicate(),
                timeout=terminate_timeout_s,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.communicate()
        raise

    if process.returncode != 0:
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
    )
    if (
        result.run_id <= 0
        or result.universe_snapshot_id <= 0
        or result.requested_token_count < 0
        or result.successful_response_count < 0
        or result.successful_response_count > result.requested_token_count
        or result.quote_taken_at_ms < 0
        or result.elapsed_ms < 0
    ):
        raise QuoteCollectionSubprocessError("invalid-json")
    logger.info(
        "isolated quote collection complete "
        f"pid={getattr(process, 'pid', None)} "
        f"process_elapsed_ms={int((time.perf_counter() - started) * 1000)} "
        f"run_id={result.run_id} "
        f"collection_elapsed_ms={result.elapsed_ms} "
        f"responses={result.successful_response_count}/"
        f"{result.requested_token_count}"
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
        interval_s: float,
        runtime: QuoteWorkerRuntime | None = None,
        wait_for_stop: WaitForStop = _wait_for_stop,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(interval_s, bool) or interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self._collect_once = collect_once
        self._certify_projection = certify_projection
        self._prepare_opportunities = prepare_opportunities
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
                        if certified_opportunities is None:
                            self.runtime.publish_certified_projection(
                                certified_projection
                            )
                        else:
                            self.runtime.publish_certified_feed(
                                certified_projection,
                                certified_opportunities,
                            )
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
                    certified_projection = None
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

    async def collect_once() -> QuoteCollectionResult:
        return await collect_quotes_in_subprocess(settings)

    async def certify_projection(
        result: QuoteCollectionResult,
    ) -> CompleteQuoteProjection:
        return await certify_latest_quote_projection(
            quote_store,
            result,
        )

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
        return result

    return QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        interval_s=settings.neg_risk_quote_interval_s,
    )
