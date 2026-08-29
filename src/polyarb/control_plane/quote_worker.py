"""One bounded, fenced worker turn for transactional Quote batches."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import asdict, dataclass
from datetime import datetime
from threading import Event, Thread
from time import monotonic
from typing import Any, Protocol, cast

from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_collector import BooksReader, _build_terminal_quotes
from polyarb.routing.neg_risk_quote_store import UniverseLeg

from .alert_delivery import incident_alert_channels
from .blocking_bridge import run_blocking_call
from .failure_identity import retry_failure_fingerprint
from .faults import IntentionalStagingRetryFault
from .models import JobLease, JobState, QuoteBatchSpec
from .postgres import (
    IncompleteQuoteGenerationError,
    PostgresControlPlane,
    StaleLeaseError,
)
from .quote_artifact import (
    QuoteArtifactError,
    QuoteBatchArtifact,
    canonical_quote_batch_bytes,
    parse_quote_batch_input_bytes,
    upload_quote_batch_artifact,
)
from .runtime_contract import AsyncAttemptRuntime, AttemptRuntime, ServiceStopRequested
from .runtime_deadlines import runtime_deadline_profile, runtime_policy
from .service_lifecycle import claim_worker_job


class QuoteBatchWorkerError(RuntimeError):
    """The immutable input cannot safely produce a transactional Quote result."""


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


async def _drain_task(task: asyncio.Task[Any]) -> Any:
    """Drain once; a repeated cancellation is terminal-grace expiry."""
    current = asyncio.current_task()
    was_cancelled = current is not None and current.cancelling() > 0
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if was_cancelled:
                task.cancel()
                task.add_done_callback(_consume_task_result)
                raise
            was_cancelled = True
            if current is not None and current.cancelling():
                current.uncancel()
    return task.result()


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    task.exception()


async def _to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run nonterminal blocking work under the shared service boundary."""
    return await run_blocking_call(
        call,
        *args,
        thread_name="quote-batch:blocking-call",
        **kwargs,
    )


async def _terminal_to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Allow a terminal transaction to finish only within stop grace."""
    return await run_blocking_call(
        call,
        *args,
        point_of_no_return=True,
        thread_name="quote-batch:terminal-call",
        **kwargs,
    )


async def _await_reader(
    reader: BooksReader,
    token_ids: list[str],
    *,
    timeout_seconds: float,
) -> Any:
    """Await one bounded CLOB request while draining scheduler cancellation."""
    task = asyncio.create_task(reader.get_books(token_ids, projection="full"))
    try:
        async with asyncio.timeout(timeout_seconds):
            return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        try:
            await _drain_task(task)
        except BaseException as error:
            raise cancellation from error
        raise
    except TimeoutError:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise


def _run_bounded_sync_call(
    call: Callable[[], Any],
    *,
    runtime: AttemptRuntime,
    terminal: bool,
) -> Any:
    """Run sync R2/DB work while polling the fenced runtime heartbeat."""
    future: Future[Any] = Future()

    def invoke() -> None:
        try:
            future.set_result(call())
        except BaseException as error:
            future.set_exception(error)

    Thread(target=invoke, name="quote-sync", daemon=True).start()
    primary_error: BaseException | None = None
    deadline = monotonic() + runtime.remaining_attempt_seconds()
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            primary_error = TimeoutError("quote sync call exceeded attempt deadline")
            break
        try:
            return future.result(timeout=min(float(runtime.profile.heartbeat_seconds), remaining))
        except FutureTimeoutError:
            try:
                runtime.heartbeat_if_due()
            except BaseException as error:
                if terminal:
                    # A terminal transaction owns the point of no return;
                    # drain it to a committed result before deciding which
                    # error is authoritative.
                    primary_error = error
                    continue
                primary_error = error
                break
        except BaseException as error:
            primary_error = error
            break
    underlying_error: BaseException | None = None
    underlying_result: Any = None
    try:
        underlying_result = future.result()
    except BaseException as error:
        underlying_error = error
    if underlying_error is not None:
        raise primary_error from underlying_error
    if terminal and primary_error is not None:
        return underlying_result
    assert primary_error is not None
    raise primary_error


def _runtime_sync_call(
    runtime: AttemptRuntime,
    call: Callable[[], Any],
    *,
    terminal: bool = False,
) -> Any:
    return _run_bounded_sync_call(call, runtime=runtime, terminal=terminal)


@dataclass(frozen=True, slots=True)
class QuoteBatchWorkerResult:
    job_key: str | None
    outcome: str


class _QuoteBatchLane(Protocol):
    _lease_seconds: int

    async def run_once(self) -> QuoteBatchWorkerResult: ...


class TransactionalQuoteBatchPool:
    """Run independently fenced Quote leases up to the existing CLOB bound."""

    def __init__(self, *, lanes: Sequence[_QuoteBatchLane]) -> None:
        if not lanes:
            raise ValueError("lanes must be non-empty")
        lease_seconds = {lane._lease_seconds for lane in lanes}
        if len(lease_seconds) != 1 or next(iter(lease_seconds)) <= 0:
            raise ValueError("quote batch lanes must share one positive lease policy")
        self._lanes = tuple(lanes)
        self._lease_seconds = next(iter(lease_seconds))

    async def run_once(self) -> QuoteBatchWorkerResult:
        """Wait for every sibling lane before exposing one aggregate turn."""
        results = await asyncio.gather(
            *(lane.run_once() for lane in self._lanes), return_exceptions=True
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise errors[0]
        completed = [
            result
            for result in results
            if isinstance(result, QuoteBatchWorkerResult) and result.job_key is not None
        ]
        if not completed:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")
        keys = sorted(str(result.job_key) for result in completed)
        succeeded = sum(
            result.outcome.startswith(("succeeded", "recovered")) for result in completed
        )
        outcome = (
            f"succeeded:{succeeded}/{len(self._lanes)}"
            if succeeded == len(completed)
            else f"mixed:{succeeded}/{len(completed)}"
        )
        return QuoteBatchWorkerResult(job_key=",".join(keys), outcome=outcome)

    async def aclose(self) -> None:
        closers: list[Awaitable[Any]] = []
        for lane in self._lanes:
            closer = getattr(lane, "aclose", None)
            if closer is not None:
                result = closer()
                if isinstance(result, Awaitable):
                    closers.append(result)
        if closers:
            await asyncio.gather(*closers)


class TransactionalQuoteBatchWorker:
    """Claim at most one immutable batch and commit its artifact under a lease."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        reader: BooksReader,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        lease_seconds: int = 120,
        crash_after_r2_upload: Callable[[JobLease], None] | None = None,
        retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
        acceptance_run_id: str | None = None,
        runtime_sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._control_plane = control_plane
        self._reader = reader
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._crash_after_r2_upload = crash_after_r2_upload
        self._retry_fault_before_receipt = retry_fault_before_receipt
        self._acceptance_run_id = acceptance_run_id
        self._runtime_sleep = runtime_sleep

    async def run_once(self) -> QuoteBatchWorkerResult:
        """Complete one recovery-safe batch; never rebuild input from SQLite."""
        lease = await claim_worker_job(
            self._control_plane,
            worker_id=self._worker_id,
            job_types=("quote-batch",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")
        runtime = AsyncAttemptRuntime(
            store=self._control_plane,
            lease=lease,
            profile=runtime_deadline_profile("quote-batch", self._lease_seconds),
            clock=self._now,
            sleep=self._runtime_sleep,
        )
        try:
            async with runtime:
                prior = await _to_thread(
                    self._control_plane.quote_batch_receipt,
                    runtime.current_lease.job_key,
                )
                await _to_thread(
                    runtime.progress,
                    stage="read-input",
                    current=1,
                    total=1,
                )
                if prior is not None:
                    await _to_thread(runtime.progress, stage="commit-receipt", current=1, total=1)
                    await runtime.stop()
                    recover = getattr(self._control_plane, "recover_quote_batch_success", None)
                    if callable(recover):
                        await _terminal_to_thread(
                            recover,
                            runtime.current_lease,
                            now=self._now(),
                        )
                    else:
                        await _terminal_to_thread(
                            self._control_plane.finish,
                            runtime.current_lease,
                            state=JobState.SUCCEEDED,
                            now=self._now(),
                        )
                    try:
                        await _terminal_to_thread(
                            self._control_plane.record_job_recovery,
                            runtime.current_lease,
                            component="quote-batch",
                            channels=incident_alert_channels(Settings()),
                            now=self._now(),
                            acceptance_run_id=self._acceptance_run_id,
                        )
                    except Exception:
                        return QuoteBatchWorkerResult(
                            job_key=runtime.current_lease.job_key,
                            outcome="recovered:recovery-pending",
                        )
                    return QuoteBatchWorkerResult(
                        job_key=runtime.current_lease.job_key,
                        outcome="recovered",
                    )

                batch = await _to_thread(self._batch_input, runtime.current_lease.job_key)
                if not batch.legs:
                    raise QuoteBatchWorkerError("quote-batch-leg-input-unavailable")
                books = await _await_reader(
                    self._reader,
                    list(batch.token_ids),
                    timeout_seconds=float(
                        runtime_policy(
                            "quote-batch", runtime.profile.lease_seconds
                        ).io_timeout_seconds
                    ),
                )
                await _to_thread(
                    runtime.progress,
                    stage="fetch-books",
                    current=len(batch.token_ids),
                    total=len(batch.token_ids),
                )
                artifact, successful_count = await _to_thread(
                    self._build_artifact,
                    batch,
                    books,
                )
                await _to_thread(runtime.progress, stage="build-artifact", current=1, total=1)
                await _to_thread(
                    upload_quote_batch_artifact,
                    self._object_client,
                    bucket=self._bucket,
                    artifact=artifact,
                )
                await _to_thread(runtime.progress, stage="upload-artifact", current=1, total=1)
                await _to_thread(runtime.progress, stage="commit-receipt", current=1, total=1)
                if self._crash_after_r2_upload is not None:
                    self._crash_after_r2_upload(runtime.current_lease)
                if self._retry_fault_before_receipt is not None:
                    self._retry_fault_before_receipt(runtime.current_lease)
                await runtime.stop()
                record = self._control_plane.record_quote_batch
                terminal = isinstance(self._control_plane, PostgresControlPlane)
                await _terminal_to_thread(
                    record,
                    runtime.current_lease,
                    token_range_digest=batch.token_range_digest,
                    quote_digest=artifact.sha256,
                    artifact_key=artifact.key,
                    artifact_digest=artifact.sha256,
                    successful_response_count=successful_count,
                    quoted_at=self._now(),
                    now=self._now(),
                    terminal=terminal,
                )
                if not terminal:
                    await _terminal_to_thread(
                        self._control_plane.finish,
                        runtime.current_lease,
                        state=JobState.SUCCEEDED,
                        now=self._now(),
                    )
                try:
                    await _terminal_to_thread(
                        self._control_plane.record_job_recovery,
                        runtime.current_lease,
                        component="quote-batch",
                        channels=incident_alert_channels(Settings()),
                        now=self._now(),
                        acceptance_run_id=self._acceptance_run_id,
                    )
                except Exception:
                    return QuoteBatchWorkerResult(
                        job_key=runtime.current_lease.job_key,
                        outcome="succeeded:recovery-pending",
                    )
                return QuoteBatchWorkerResult(
                    job_key=runtime.current_lease.job_key,
                    outcome="succeeded",
                )
        except asyncio.CancelledError:
            await _terminal_to_thread(
                self._control_plane.finish_interrupted,
                runtime.current_lease,
                component="quote-batch",
                now=self._now(),
            )
            raise
        except StaleLeaseError:
            raise

        except IntentionalStagingRetryFault as error:
            await _to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="quote-batch",
                summary="quote-batch retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="retryable")
        except Exception as error:
            await _to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="quote-batch",
                summary="quote-batch retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            raise
        raise AssertionError("quote batch runtime exited without a result")

    def _batch_input(self, job_key: str) -> QuoteBatchSpec:
        """Prefer the immutable R2 input; retain legacy rows during the staged migration."""
        reference_reader = getattr(self._control_plane, "quote_batch_input_reference", None)
        reference = (
            cast(tuple[str, str, int] | None, reference_reader(job_key))
            if callable(reference_reader)
            else None
        )
        if reference is None:
            return self._control_plane.quote_batch_spec(job_key)
        key, digest, leg_count = reference
        response = self._object_client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise QuoteBatchWorkerError("quote-batch-input-artifact-body-unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise QuoteBatchWorkerError("quote-batch-input-artifact-body-invalid")
        try:
            batch = parse_quote_batch_input_bytes(payload, expected_sha256=digest)
        except QuoteArtifactError as error:
            raise QuoteBatchWorkerError("quote-batch-input-artifact-invalid") from error
        if batch.job_key != job_key or len(batch.legs) != leg_count:
            raise QuoteBatchWorkerError("quote-batch-input-artifact-identity-mismatch")
        return batch

    def _build_artifact(
        self,
        batch: QuoteBatchSpec,
        books: Any,
    ) -> tuple[QuoteBatchArtifact, int]:
        legs = tuple(
            UniverseLeg(
                neg_risk_market_id=leg.neg_risk_market_id,
                market_id=leg.market_id,
                condition_id=leg.condition_id,
                slug=leg.slug,
                yes_token_id=leg.yes_token_id,
                event_id=leg.event_id,
                membership_hash=leg.membership_hash,
            )
            for leg in batch.legs
        )
        successful_count, quotes = _build_terminal_quotes(books, list(batch.token_ids), legs)
        payload = canonical_quote_batch_bytes(
            structure_receipt_digest=batch.structure_receipt_digest,
            universe_hash=batch.universe_hash,
            token_range_digest=batch.token_range_digest,
            quotes=tuple({"token_id": quote.yes_token_id, **asdict(quote)} for quote in quotes),
        )
        artifact = QuoteBatchArtifact.from_bytes(payload)
        return artifact, successful_count


class TransactionalQuoteCertifier:
    """Claim at most one terminal certification; partial generations only retry."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        worker_id: str,
        now: Callable[[], datetime],
        lease_seconds: int = 30,
    ) -> None:
        if not worker_id or lease_seconds <= 0:
            raise ValueError("worker_id and lease_seconds must be positive")
        self._control_plane = control_plane
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._stop_requested = Event()

    def request_stop(self) -> None:
        """Stop before the next fenced certification boundary."""
        self._stop_requested.set()

    def run_once(self) -> QuoteBatchWorkerResult:
        now = self._now()
        repair_ready = getattr(self._control_plane, "repair_ready_certifiers", None)
        if callable(repair_ready):
            repair_ready(job_type="quote-certify", now=now)
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("quote-certify",),
            lease_seconds=self._lease_seconds,
            now=now,
        )
        if lease is None:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")
        generation_key = lease.job_key.removesuffix(":certify")
        runtime_available = all(
            callable(getattr(self._control_plane, method, None))
            for method in ("record_runtime_progress", "heartbeat_runtime_attempt")
        )
        runtime = (
            AttemptRuntime(
                store=self._control_plane,
                lease=lease,
                profile=runtime_deadline_profile("quote-certify", self._lease_seconds),
                clock=self._now,
            )
            if runtime_available
            else None
        )
        try:
            if self._stop_requested.is_set():
                raise ServiceStopRequested("quote-certify service stop requested")
            if runtime is not None:
                runtime.progress(stage="verify-batches", current=1, total=1)
                runtime.heartbeat_if_due()
                runtime.progress(stage="publish-pointer", current=1, total=1)
                artifact_digest = _runtime_sync_call(
                    runtime,
                    lambda: self._control_plane.certify_quote_generation(
                        runtime.current_lease,
                        generation_key=generation_key,
                        now=self._now(),
                    ),
                    terminal=True,
                )
            else:
                artifact_digest = self._control_plane.certify_quote_generation(
                    lease, generation_key=generation_key, now=self._now()
                )
            if not isinstance(artifact_digest, str):
                raise TypeError("Quote certification must return its artifact digest")
            try:
                recovery = (
                    _runtime_sync_call(
                        runtime,
                        lambda: self._control_plane.record_job_recovery(
                            lease if runtime is None else runtime.current_lease,
                            component="quote-certify",
                            channels=incident_alert_channels(Settings()),
                            now=self._now(),
                        ),
                        terminal=True,
                    )
                    if runtime is not None
                    else self._control_plane.record_job_recovery(
                        lease,
                        component="quote-certify",
                        channels=incident_alert_channels(Settings()),
                        now=self._now(),
                    )
                )
            except Exception:
                return QuoteBatchWorkerResult(
                    job_key=lease.job_key,
                    outcome="certified:recovery-pending",
                )
            del recovery
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="certified")
        except ServiceStopRequested:
            current_lease = lease if runtime is None else runtime.current_lease
            if runtime is None:
                self._control_plane.finish_interrupted(
                    current_lease,
                    component="quote-certify",
                    now=self._now(),
                )
            else:
                _runtime_sync_call(
                    runtime,
                    lambda: self._control_plane.finish_interrupted(
                        current_lease,
                        component="quote-certify",
                        now=self._now(),
                    ),
                    terminal=True,
                )
            raise
        except IncompleteQuoteGenerationError as error:
            failure_class = type(error).__name__
            failure_fingerprint = retry_failure_fingerprint(error, component="quote-certify")
            current_lease = lease if runtime is None else runtime.current_lease
            if runtime is None:
                self._control_plane.finish_retryable_with_incident(
                    current_lease,
                    error_class="IncompleteQuoteGenerationError",
                    incident_key=f"incident:job-retry:{lease.job_key}",
                    dedupe_key=f"job-retry:{lease.job_key}",
                    component="quote-certify",
                    summary="quote-certify durable barrier invariant failed",
                    detail={
                        "job_key": lease.job_key,
                        "lease_epoch": lease.lease_epoch,
                        "error_class": failure_class,
                        "failure_fingerprint": failure_fingerprint,
                    },
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                )
            else:
                _runtime_sync_call(
                    runtime,
                    lambda: self._control_plane.finish_retryable_with_incident(
                        current_lease,
                        error_class="IncompleteQuoteGenerationError",
                        incident_key=f"incident:job-retry:{lease.job_key}",
                        dedupe_key=f"job-retry:{lease.job_key}",
                        component="quote-certify",
                        summary="quote-certify durable barrier invariant failed",
                        detail={
                            "job_key": lease.job_key,
                            "lease_epoch": lease.lease_epoch,
                            "error_class": failure_class,
                            "failure_fingerprint": failure_fingerprint,
                        },
                        channels=incident_alert_channels(Settings()),
                        now=self._now(),
                    ),
                    terminal=True,
                )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="retryable")
        except StaleLeaseError:
            raise
        except Exception as error:
            failure_class = type(error).__name__
            failure_fingerprint = retry_failure_fingerprint(error, component="quote-certify")
            current_lease = lease if runtime is None else runtime.current_lease
            if runtime is None:
                self._control_plane.finish_retryable_with_incident(
                    current_lease,
                    error_class=type(error).__name__,
                    incident_key=f"incident:job-retry:{lease.job_key}",
                    dedupe_key=f"job-retry:{lease.job_key}",
                    component="quote-certify",
                    summary="quote-certify retryable failure",
                    detail={
                        "job_key": lease.job_key,
                        "lease_epoch": lease.lease_epoch,
                        "error_class": type(error).__name__,
                        "failure_fingerprint": failure_fingerprint,
                    },
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                )
            else:
                _runtime_sync_call(
                    runtime,
                    lambda: self._control_plane.finish_retryable_with_incident(
                        current_lease,
                        error_class=failure_class,
                        incident_key=f"incident:job-retry:{lease.job_key}",
                        dedupe_key=f"job-retry:{lease.job_key}",
                        component="quote-certify",
                        summary="quote-certify retryable failure",
                        detail={
                            "job_key": lease.job_key,
                            "lease_epoch": lease.lease_epoch,
                            "error_class": failure_class,
                            "failure_fingerprint": failure_fingerprint,
                        },
                        channels=incident_alert_channels(Settings()),
                        now=self._now(),
                    ),
                    terminal=True,
                )
            raise
