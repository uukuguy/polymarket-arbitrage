"""Bridge certified Structure bundles into frozen transactional Quote inputs."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Protocol

from .models import JobState, QuoteBatchLeg
from .postgres import PostgresControlPlane, StaleLeaseError
from .quote_worker import QuoteBatchWorkerResult
from .structure_artifact import parse_structure_bundle_bytes


class QuoteAdmissionError(RuntimeError):
    """A certified Structure bundle cannot safely freeze Quote work."""


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


def quote_legs_from_structure_components(
    components: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[QuoteBatchLeg, ...]:
    """Select the exact CLOB YES legs eligible in one immutable Structure truth."""
    legs: list[QuoteBatchLeg] = []
    for market in components.get("markets", ()):
        if (
            market.get("active") is not True
            or market.get("closed") is not False
            or market.get("neg_risk") is not True
        ):
            continue
        values = {
            field: market.get(field)
            for field in (
                "neg_risk_market_id",
                "market_id",
                "condition_id",
                "yes_token_id",
                "event_id",
            )
        }
        if any(not isinstance(value, str) or not value.strip() for value in values.values()):
            continue
        slug = market.get("slug")
        if slug is not None and (not isinstance(slug, str) or not slug.strip()):
            slug = None
        legs.append(
            QuoteBatchLeg(
                neg_risk_market_id=str(values["neg_risk_market_id"]),
                market_id=str(values["market_id"]),
                condition_id=str(values["condition_id"]),
                slug=slug,
                yes_token_id=str(values["yes_token_id"]),
                event_id=str(values["event_id"]),
                membership_hash=_membership_hash(market),
            )
        )
    by_token = {leg.yes_token_id: leg for leg in legs}
    if len(by_token) != len(legs):
        raise QuoteAdmissionError("Structure bundle has duplicate YES token")
    if not by_token:
        raise QuoteAdmissionError("Structure bundle has no eligible Quote legs")
    return tuple(by_token[token] for token in sorted(by_token))


def canonical_quote_universe_hash(legs: Sequence[QuoteBatchLeg]) -> str:
    """Bind every CLOB leg mapping, not only the observable token list."""
    if not legs:
        raise QuoteAdmissionError("Quote universe is empty")
    payload = [
        {
            "neg_risk_market_id": leg.neg_risk_market_id,
            "market_id": leg.market_id,
            "condition_id": leg.condition_id,
            "slug": leg.slug,
            "yes_token_id": leg.yes_token_id,
            "event_id": leg.event_id,
            "membership_hash": leg.membership_hash,
        }
        for leg in sorted(legs, key=lambda leg: leg.yes_token_id)
    ]
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _membership_hash(market: Mapping[str, object]) -> str:
    return sha256(
        json.dumps(
            {
                "event_id": market["event_id"],
                "neg_risk_market_id": market["neg_risk_market_id"],
                "market_id": market["market_id"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class TransactionalQuoteAdmitter:
    """Claim one certified Structure intent and atomically freeze Quote batches."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        batch_size: int,
        lease_seconds: int = 120,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not bucket or not worker_id or batch_size <= 0 or lease_seconds <= 0:
            raise ValueError("Quote admission bounds and identities must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    async def run_once(self) -> QuoteBatchWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("quote-admit",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")
        try:
            _generation, bundle_key, bundle_digest = self._control_plane.quote_admission_input(
                lease.job_key
            )
            payload = self._read_bundle(bundle_key)
            _identity, components = parse_structure_bundle_bytes(
                payload, expected_sha256=bundle_digest
            )
            legs = quote_legs_from_structure_components(components)
            self._control_plane.admit_quote_generation(
                lease,
                structure_receipt_digest=bundle_digest,
                universe_hash=canonical_quote_universe_hash(legs),
                legs=legs,
                batch_size=self._batch_size,
                now=self._now(),
            )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="admitted")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish(
                lease,
                state=JobState.RETRYABLE,
                next_attempt_at=self._now() + self._retry_delay,
                error_class=type(error).__name__,
                now=self._now(),
            )
            if isinstance(error, QuoteAdmissionError):
                raise
            raise QuoteAdmissionError("Quote admission bundle digest or contract failed") from error

    def _read_bundle(self, key: str) -> bytes:
        response = self._object_client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise QuoteAdmissionError("Quote admission bundle body unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise QuoteAdmissionError("Quote admission bundle body invalid")
        return payload
