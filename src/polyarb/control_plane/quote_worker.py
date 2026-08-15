"""One bounded, fenced worker turn for transactional Quote batches."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_collector import BooksReader, _build_terminal_quotes
from polyarb.routing.neg_risk_quote_store import UniverseLeg

from .alert_delivery import incident_alert_channels
from .faults import IntentionalStagingRetryFault
from .models import JobLease, JobState, QuoteBatchSpec
from .postgres import (
    IncompleteQuoteGenerationError,
    PostgresControlPlane,
    StaleLeaseError,
)
from .quote_artifact import (
    QuoteBatchArtifact,
    canonical_quote_batch_bytes,
    upload_quote_batch_artifact,
)


class QuoteBatchWorkerError(RuntimeError):
    """The immutable input cannot safely produce a transactional Quote result."""


class _ObjectClient(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class QuoteBatchWorkerResult:
    job_key: str | None
    outcome: str


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
        retry_delay: timedelta = timedelta(seconds=15),
        crash_after_r2_upload: Callable[[JobLease], None] | None = None,
        retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
        acceptance_run_id: str | None = None,
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("lease_seconds and retry_delay must be positive")
        self._control_plane = control_plane
        self._reader = reader
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay
        self._crash_after_r2_upload = crash_after_r2_upload
        self._retry_fault_before_receipt = retry_fault_before_receipt
        self._acceptance_run_id = acceptance_run_id

    async def run_once(self) -> QuoteBatchWorkerResult:
        """Complete one recovery-safe batch; never rebuild input from SQLite."""
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("quote-batch",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")

        prior = self._control_plane.quote_batch_receipt(lease.job_key)
        if prior is not None:
            self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="recovered")

        try:
            batch = self._control_plane.quote_batch_spec(lease.job_key)
            if not batch.legs:
                raise QuoteBatchWorkerError("quote-batch-leg-input-unavailable")
            artifact, successful_count = await self._fetch_artifact(batch)
            if self._crash_after_r2_upload is not None:
                self._crash_after_r2_upload(lease)
            if self._retry_fault_before_receipt is not None:
                self._retry_fault_before_receipt(lease)
            self._control_plane.record_quote_batch(
                lease,
                token_range_digest=batch.token_range_digest,
                quote_digest=artifact.sha256,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                successful_response_count=successful_count,
                quoted_at=self._now(),
                now=self._now(),
            )
            self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
            self._control_plane.record_job_recovery(
                lease,
                component="quote-batch",
                channels=incident_alert_channels(Settings()),
                now=self._now(),
                acceptance_run_id=self._acceptance_run_id,
            )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except StaleLeaseError:
            raise
        except IntentionalStagingRetryFault as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
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
            self._control_plane.finish_retryable_with_incident(
                lease,
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

    async def _fetch_artifact(self, batch: QuoteBatchSpec) -> tuple[QuoteBatchArtifact, int]:
        books = await self._reader.get_books(list(batch.token_ids), projection="full")
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
        upload_quote_batch_artifact(self._object_client, bucket=self._bucket, artifact=artifact)
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
        retry_delay: timedelta = timedelta(seconds=5),
    ) -> None:
        if not worker_id or lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("worker_id, lease_seconds, and retry_delay must be positive")
        self._control_plane = control_plane
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    def run_once(self) -> QuoteBatchWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("quote-certify",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")
        generation_key = lease.job_key.removesuffix(":certify")
        try:
            self._control_plane.certify_quote_generation(
                lease, generation_key=generation_key, now=self._now()
            )
            self._control_plane.record_job_recovery(
                lease,
                component="quote-certify",
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="certified")
        except IncompleteQuoteGenerationError:
            self._control_plane.finish(
                lease,
                state=JobState.RETRYABLE,
                next_attempt_at=self._now() + self._retry_delay,
                error_class="IncompleteQuoteGenerationError",
                now=self._now(),
            )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="waiting")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="quote-certify",
                summary="quote-certify retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            raise
