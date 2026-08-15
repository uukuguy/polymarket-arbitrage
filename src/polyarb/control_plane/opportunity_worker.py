"""R2-authenticated certifier for the formal opportunity projection."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

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
        now: Callable[[], datetime],
    ) -> None:
        if not bucket:
            raise ValueError("bucket must be non-empty")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._now = now

    def run_once(self) -> OpportunityCertifierResult:
        try:
            quote_generation, structure_generation, batches = (
                self._control_plane.current_quote_projection_inputs()
            )
        except OpportunityProjectionCurrentError:
            return OpportunityCertifierResult(job_key=None, outcome="current")
        except IncompleteQuoteGenerationError:
            return OpportunityCertifierResult(job_key=None, outcome="idle")
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
        return OpportunityCertifierResult(job_key=quote_generation, outcome=f"certified:{digest}")
