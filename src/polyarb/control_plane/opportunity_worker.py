"""R2-authenticated certifier for the formal opportunity projection."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from .models import JobState
from .opportunity_projection import build_opportunity_rows, parse_quote_batch_bytes
from .postgres import (
    IncompleteQuoteGenerationError,
    OpportunityProjectionCurrentError,
    PostgresControlPlane,
)


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

    def run_once(self) -> OpportunityCertifierResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("opportunity-certify",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return OpportunityCertifierResult(job_key=None, outcome="idle")
        try:
            quote_generation, structure_generation, batches = (
                self._control_plane.current_quote_projection_inputs()
            )
        except OpportunityProjectionCurrentError:
            self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
            return OpportunityCertifierResult(job_key=lease.job_key, outcome="current")
        except IncompleteQuoteGenerationError:
            self._control_plane.finish(
                lease,
                state=JobState.RETRYABLE,
                next_attempt_at=self._now() + timedelta(seconds=5),
                now=self._now(),
            )
            return OpportunityCertifierResult(job_key=lease.job_key, outcome="waiting")
        all_legs = []
        all_quotes = []
        quoted_at_ms = 0
        for legs, receipt, quoted_at in batches:
            response = self._object_client.get_object(Bucket=self._bucket, Key=receipt.artifact_key)
            body = response.get("Body")
            if body is None or not hasattr(body, "read"):
                raise ValueError("quote-artifact-body-unavailable")
            payload = body.read()
            if not isinstance(payload, bytes):
                raise ValueError("quote-artifact-body-is-not-bytes")
            all_legs.extend(legs)
            all_quotes.extend(
                parse_quote_batch_bytes(payload, expected_digest=receipt.artifact_digest)
            )
            quoted_at_ms = max(quoted_at_ms, int(quoted_at.timestamp() * 1_000))
        digest = self._control_plane.publish_opportunity_projection(
            quote_generation_key=quote_generation,
            structure_generation_key=structure_generation,
            rows=build_opportunity_rows(
                legs=all_legs,
                quotes=all_quotes,
                structure_observed_at_ms=quoted_at_ms,
                quote_started_at_ms=quoted_at_ms,
                quote_quoted_at_ms=quoted_at_ms,
            ),
            now=self._now(),
        )
        self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
        return OpportunityCertifierResult(job_key=lease.job_key, outcome=f"certified:{digest}")
