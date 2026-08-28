"""R2-authenticated certifier for the formal opportunity projection."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Event
from typing import Any, Protocol, cast

from polyarb.config import Settings

from .alert_delivery import incident_alert_channels
from .models import JobLease, JobState
from .opportunity_projection import build_opportunity_rows, parse_quote_batch_bytes
from .postgres import (
    IncompleteQuoteGenerationError,
    OpportunityProjectionCurrentError,
    PostgresControlPlane,
    StaleLeaseError,
)
from .quote_artifact import QuoteArtifactError, parse_quote_batch_input_bytes
from .quote_worker import _runtime_sync_call
from .runtime_contract import AttemptRuntime, ServiceStopRequested
from .runtime_deadlines import runtime_deadline_profile


class _Body(Protocol):
    def read(self) -> bytes: ...


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class OpportunityCertifierResult:
    job_key: str | None
    outcome: str


class TransactionalOpportunityCertifier:
    """Build and atomically publish opportunities from the current Quote artifacts."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str = "opportunity-certifier",
        now: Callable[[], datetime],
        lease_seconds: int = 120,
    ) -> None:
        if not bucket or not worker_id or lease_seconds <= 0:
            raise ValueError("bucket, worker_id, and lease_seconds must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._now = now
        self._stop_requested = Event()

    def request_stop(self) -> None:
        """Stop projection assembly at its next immutable batch boundary."""
        self._stop_requested.set()

    def run_once(self) -> OpportunityCertifierResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("opportunity-certify",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return OpportunityCertifierResult(job_key=None, outcome="idle")
        runtime_available = all(
            callable(getattr(self._control_plane, method, None))
            for method in ("record_runtime_progress", "heartbeat_runtime_attempt")
        )
        runtime = (
            AttemptRuntime(
                store=self._control_plane,
                lease=lease,
                profile=runtime_deadline_profile("opportunity-certify", self._lease_seconds),
                clock=self._now,
            )
            if runtime_available
            else None
        )
        try:
            if self._stop_requested.is_set():
                raise ServiceStopRequested("opportunity-certify service stop requested")
            if runtime is not None:
                runtime.progress(stage="read-current-quote", current=1, total=1)
                quote_generation, structure_generation, batches = _runtime_sync_call(
                    runtime,
                    self._control_plane.current_quote_projection_inputs,
                )
            else:
                quote_generation, structure_generation, batches = (
                    self._control_plane.current_quote_projection_inputs()
                )
        except OpportunityProjectionCurrentError:
            if runtime is not None:
                runtime.progress(stage="publish-opportunity", current=1, total=1)
                recover = getattr(
                    self._control_plane,
                    "recover_opportunity_projection_success",
                    None,
                )
                if callable(recover):
                    _runtime_sync_call(
                        runtime,
                        lambda: recover(
                            runtime.current_lease,
                            quote_generation_key=runtime.current_lease.input_identity,
                            structure_generation_key=None,
                            now=self._now(),
                        ),
                        terminal=True,
                    )
                else:
                    _runtime_sync_call(
                        runtime,
                        lambda: self._control_plane.finish(
                            runtime.current_lease,
                            state=JobState.SUCCEEDED,
                            now=self._now(),
                        ),
                        terminal=True,
                    )
            else:
                self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
            return OpportunityCertifierResult(job_key=lease.job_key, outcome="current")
        except IncompleteQuoteGenerationError:
            if runtime is None:
                self._control_plane.finish(
                    lease,
                    state=JobState.RETRYABLE,
                    next_attempt_at=self._now() + timedelta(seconds=5),
                    now=self._now(),
                )
            else:
                _runtime_sync_call(
                    runtime,
                    lambda: self._control_plane.finish(
                        runtime.current_lease,
                        state=JobState.RETRYABLE,
                        next_attempt_at=self._now() + timedelta(seconds=5),
                        now=self._now(),
                    ),
                    terminal=True,
                )
            return OpportunityCertifierResult(job_key=lease.job_key, outcome="waiting")
        except StaleLeaseError:
            raise
        except Exception as error:
            failure = error
            if runtime is None:
                self._finish_retryable(lease, failure)
            else:
                _runtime_sync_call(
                    runtime,
                    lambda: self._finish_retryable(runtime.current_lease, failure),
                    terminal=True,
                )
            raise

        try:
            all_legs = []
            all_quotes = []
            quoted_at_ms = 0
            total_batches = len(batches)
            for batch_index, (legs, receipt, quoted_at) in enumerate(batches, start=1):
                if self._stop_requested.is_set():
                    raise ServiceStopRequested("opportunity-certify service stop requested")
                reference_reader = getattr(self._control_plane, "quote_batch_input_reference", None)
                reference_fn = (
                    cast(Callable[[str], tuple[str, str, int] | None], reference_reader)
                    if callable(reference_reader)
                    else None
                )
                reference = (
                    cast(
                        tuple[str, str, int] | None,
                        _runtime_sync_call(
                            runtime,
                            lambda: cast(
                                Callable[[str], tuple[str, str, int] | None], reference_fn
                            )(receipt.job_key),
                        ),
                    )
                    if runtime is not None and reference_fn is not None
                    else reference_fn(receipt.job_key)
                    if reference_fn is not None
                    else None
                )
                if reference is not None:
                    input_key, input_digest, leg_count = reference
                    input_payload = (
                        _runtime_sync_call(
                            runtime,
                            lambda: self._read_object(input_key, label="quote-input-artifact"),
                        )
                        if runtime is not None
                        else self._read_object(input_key, label="quote-input-artifact")
                    )
                    try:
                        parsed_input = parse_quote_batch_input_bytes(
                            input_payload, expected_sha256=input_digest
                        )
                    except QuoteArtifactError as error:
                        raise ValueError("quote-input-artifact-invalid") from error
                    if (
                        parsed_input.job_key != receipt.job_key
                        or len(parsed_input.legs) != leg_count
                    ):
                        raise ValueError("quote-input-artifact-identity-mismatch")
                    legs = parsed_input.legs
                payload = (
                    _runtime_sync_call(
                        runtime,
                        lambda: self._read_object(receipt.artifact_key),
                    )
                    if runtime is not None
                    else self._read_object(receipt.artifact_key)
                )
                all_legs.extend(legs)
                all_quotes.extend(
                    parse_quote_batch_bytes(payload, expected_digest=receipt.artifact_digest)
                )
                quoted_at_ms = max(quoted_at_ms, int(quoted_at.timestamp() * 1_000))
                if runtime is not None:
                    runtime.progress(
                        stage="compute-opportunities",
                        current=batch_index,
                        total=max(total_batches, 1),
                    )
            if runtime is not None:
                if total_batches == 0:
                    runtime.progress(stage="compute-opportunities", current=1, total=1)
                runtime.progress(stage="upload-projection", current=1, total=1)
                runtime.progress(stage="publish-opportunity", current=1, total=1)
                digest = _runtime_sync_call(
                    runtime,
                    lambda: self._publish(
                        quote_generation,
                        structure_generation,
                        all_legs,
                        all_quotes,
                        quoted_at_ms,
                        lease=runtime.current_lease,
                    ),
                    terminal=True,
                )
            else:
                digest = self._publish(
                    quote_generation,
                    structure_generation,
                    all_legs,
                    all_quotes,
                    quoted_at_ms,
                    lease=None,
                )
            if runtime is not None:
                recovery = getattr(self._control_plane, "record_job_recovery", None)
                if callable(recovery):
                    try:
                        _runtime_sync_call(
                            runtime,
                            lambda: recovery(
                                runtime.current_lease,
                                component="opportunity-certify",
                                channels=incident_alert_channels(Settings()),
                                now=self._now(),
                            ),
                            terminal=True,
                        )
                    except Exception:
                        return OpportunityCertifierResult(
                            job_key=lease.job_key,
                            outcome=f"certified:{digest}:recovery-pending",
                        )
            return OpportunityCertifierResult(job_key=lease.job_key, outcome=f"certified:{digest}")
        except StaleLeaseError:
            raise
        except Exception as error:
            failure = error
            if runtime is None:
                self._finish_retryable(lease, failure)
            else:
                _runtime_sync_call(
                    runtime,
                    lambda: self._finish_retryable(runtime.current_lease, failure),
                    terminal=True,
                )
            raise

    def _read_object(self, key: str, *, label: str = "quote-artifact") -> bytes:
        response = self._object_client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise ValueError(f"{label}-body-unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise ValueError(f"{label}-body-is-not-bytes")
        return payload

    def _publish(
        self,
        quote_generation: str,
        structure_generation: str,
        legs: list[Any],
        quotes: list[Any],
        quoted_at_ms: int,
        *,
        lease: JobLease | None,
    ) -> str:
        return self._control_plane.publish_opportunity_projection(
            quote_generation_key=quote_generation,
            structure_generation_key=structure_generation,
            rows=build_opportunity_rows(
                legs=legs,
                quotes=quotes,
                structure_observed_at_ms=quoted_at_ms,
                quote_started_at_ms=quoted_at_ms,
                quote_quoted_at_ms=quoted_at_ms,
            ),
            now=self._now(),
            **({} if lease is None else {"lease": lease}),
        )

    def _finish_retryable(self, lease: JobLease, error: Exception) -> None:
        self._control_plane.finish_retryable_with_incident(
            lease,
            error_class=type(error).__name__,
            incident_key=f"incident:job-retry:{lease.job_key}",
            dedupe_key=f"job-retry:{lease.job_key}",
            component="opportunity-certify",
            summary="opportunity-certify retryable failure",
            detail={
                "job_key": lease.job_key,
                "lease_epoch": lease.lease_epoch,
                "error_class": type(error).__name__,
            },
            channels=incident_alert_channels(Settings()),
            now=self._now(),
        )
