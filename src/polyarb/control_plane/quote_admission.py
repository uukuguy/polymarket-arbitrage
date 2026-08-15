"""Bridge certified Structure bundles into frozen transactional Quote inputs."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Protocol

from polyarb.config import Settings

from .alert_delivery import incident_alert_channels
from .models import QuoteBatchLeg
from .postgres import PostgresControlPlane, StaleLeaseError
from .quote_worker import QuoteBatchWorkerResult
from .structure_artifact import (
    StructureBundleError,
    parse_structure_bundle_bytes,
    parse_structure_shard_bytes,
    parse_structure_shard_manifest_bytes,
)


class QuoteAdmissionError(RuntimeError):
    """A certified Structure bundle cannot safely freeze Quote work."""


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


def quote_legs_from_structure_components(
    components: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[QuoteBatchLeg, ...]:
    """Select the exact CLOB YES legs eligible in one immutable Structure truth."""
    return quote_legs_from_market_rows(components.get("markets", ()))


def quote_legs_from_market_rows(
    markets: Sequence[Mapping[str, object]], *, require_nonempty: bool = True
) -> tuple[QuoteBatchLeg, ...]:
    """Select Quote legs from one bounded market shard or legacy component."""
    legs: list[QuoteBatchLeg] = []
    for market in markets:
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
    if require_nonempty and not by_token:
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
            try:
                identity, components = parse_structure_bundle_bytes(
                    payload, expected_sha256=bundle_digest
                )
            except StructureBundleError:
                identity, shards = parse_structure_shard_manifest_bytes(
                    payload, expected_sha256=bundle_digest
                )
                if identity.source_kind != "gamma-source-window-events-v3-sharded":
                    raise
                legs = self._read_v3_quote_legs(shards)
            else:
                legs = quote_legs_from_structure_components(components)
            self._control_plane.admit_quote_generation(
                lease,
                structure_receipt_digest=bundle_digest,
                universe_hash=canonical_quote_universe_hash(legs),
                legs=legs,
                batch_size=self._batch_size,
                now=self._now(),
            )
            self._control_plane.record_job_recovery(
                lease,
                component="quote-admit",
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return QuoteBatchWorkerResult(job_key=lease.job_key, outcome="admitted")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="quote-admit",
                summary="quote-admit retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
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

    def _read_v3_quote_legs(self, shards: Sequence[object]) -> tuple[QuoteBatchLeg, ...]:
        by_token: dict[str, QuoteBatchLeg] = {}
        for shard in sorted(
            (shard for shard in shards if getattr(shard, "component") == "markets"),
            key=lambda shard: getattr(shard, "ordinal"),
        ):
            payload = self._read_bundle(getattr(shard, "artifact_key"))
            header, rows = parse_structure_shard_bytes(
                payload, expected_sha256=getattr(shard, "artifact_digest")
            )
            if header.component != "markets" or header.ordinal != getattr(shard, "ordinal"):
                raise QuoteAdmissionError("Quote admission v3 shard identity invalid")
            for leg in quote_legs_from_market_rows(rows, require_nonempty=False):
                existing = by_token.setdefault(leg.yes_token_id, leg)
                if existing != leg:
                    raise QuoteAdmissionError("Structure bundle has duplicate YES token")
        if not by_token:
            raise QuoteAdmissionError("Structure bundle has no eligible Quote legs")
        return tuple(by_token[token] for token in sorted(by_token))
