"""Immutable R2 artifacts for fenced Quote batch payloads."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol

from .models import QuoteBatchLeg, QuoteBatchSpec


class QuoteArtifactError(RuntimeError):
    """A Quote batch artifact cannot be safely published or verified."""


class _ObjectClient(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class QuoteBatchArtifact:
    """One canonical, content-addressed Quote batch payload."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_bytes(cls, payload: bytes) -> QuoteBatchArtifact:
        if not payload:
            raise ValueError("quote batch artifact payload must be non-empty")
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=quote_batch_artifact_key(digest))


@dataclass(frozen=True, slots=True)
class QuoteBatchInputArtifact:
    """One canonical, content-addressed immutable Quote work input."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_spec(cls, spec: QuoteBatchSpec) -> QuoteBatchInputArtifact:
        payload = canonical_quote_batch_input_bytes(spec)
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=quote_batch_input_artifact_key(digest))


def canonical_quote_batch_bytes(
    *,
    structure_receipt_digest: str,
    universe_hash: str,
    token_range_digest: str,
    quotes: tuple[dict[str, object], ...],
) -> bytes:
    """Serialize one batch deterministically, with identity on every payload."""
    for name, digest in (
        ("structure_receipt_digest", structure_receipt_digest),
        ("universe_hash", universe_hash),
        ("token_range_digest", token_range_digest),
    ):
        if len(digest) != 64:
            raise ValueError(f"{name} must be a sha256 digest")
    if not quotes:
        raise ValueError("quote batch artifact requires at least one quote")
    token_ids = [item.get("token_id") for item in quotes]
    if any(not isinstance(token_id, str) or not token_id for token_id in token_ids):
        raise ValueError("every quote must have a non-empty token_id")
    if len(set(token_ids)) != len(token_ids):
        raise ValueError("quote batch token ids must be unique")
    header = {
        "structure_receipt_digest": structure_receipt_digest,
        "token_range_digest": token_range_digest,
        "universe_hash": universe_hash,
    }
    records = (header, *sorted(quotes, key=lambda quote: str(quote["token_id"])))
    return b"".join(
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        + b"\n"
        for record in records
    )


def quote_batch_artifact_key(sha256: str) -> str:
    """Return the only permitted content-addressed Quote batch key."""
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"quote-batches/{sha256}/batch.ndjson"


def canonical_quote_batch_input_bytes(spec: QuoteBatchSpec) -> bytes:
    """Serialize one fenced Quote work input without relying on Postgres JSONB."""
    if not spec.legs:
        raise ValueError("quote batch input artifact requires legs")
    header = {
        "ordinal": spec.ordinal,
        "structure_receipt_digest": spec.structure_receipt_digest,
        "token_range_digest": spec.token_range_digest,
        "universe_hash": spec.universe_hash,
    }
    legs = (
        {
            "condition_id": leg.condition_id,
            "event_id": leg.event_id,
            "market_id": leg.market_id,
            "membership_hash": leg.membership_hash,
            "neg_risk_market_id": leg.neg_risk_market_id,
            "slug": leg.slug,
            "yes_token_id": leg.yes_token_id,
        }
        for leg in spec.legs
    )
    return b"".join(
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        for record in (header, *legs)
    )


def quote_batch_input_artifact_key(sha256: str) -> str:
    """Return the only permitted content-addressed Quote-input key."""
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"quote-inputs/{sha256}/batch.ndjson"


def parse_quote_batch_input_bytes(
    payload: bytes, *, expected_sha256: str
) -> QuoteBatchSpec:
    """Authenticate and parse one canonical immutable Quote work input."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise QuoteArtifactError("quote-batch-input-artifact-digest-mismatch")
    try:
        records = [json.loads(line) for line in payload.splitlines() if line]
        header, *leg_rows = records
        if not isinstance(header, dict) or not leg_rows:
            raise ValueError
        legs = tuple(
            QuoteBatchLeg(
                neg_risk_market_id=str(row["neg_risk_market_id"]),
                market_id=str(row["market_id"]),
                condition_id=str(row["condition_id"]),
                slug=row.get("slug"),
                yes_token_id=str(row["yes_token_id"]),
                event_id=str(row.get("event_id", "")),
                membership_hash=str(row.get("membership_hash", "")),
            )
            for row in leg_rows
            if isinstance(row, dict)
        )
        spec = QuoteBatchSpec.from_legs(
            structure_receipt_digest=str(header["structure_receipt_digest"]),
            universe_hash=str(header["universe_hash"]),
            ordinal=int(header["ordinal"]),
            legs=legs,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise QuoteArtifactError("quote-batch-input-artifact-invalid") from error
    if spec.token_range_digest != header.get("token_range_digest"):
        raise QuoteArtifactError("quote-batch-input-artifact-identity-mismatch")
    return spec


def upload_quote_batch_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: QuoteBatchArtifact,
) -> QuoteBatchArtifact:
    """Put and HEAD-verify a payload before its control-plane receipt may exist."""
    if not bucket:
        raise ValueError("bucket must be non-empty")
    client.put_object(
        Bucket=bucket,
        Key=artifact.key,
        Body=artifact.payload,
        ContentType="application/x-ndjson",
        Metadata={"sha256": artifact.sha256},
    )
    head = client.head_object(Bucket=bucket, Key=artifact.key)
    remote_sha256 = str(head.get("Metadata", {}).get("sha256", ""))
    if (
        int(head.get("ContentLength", -1)) != len(artifact.payload)
        or remote_sha256 != artifact.sha256
    ):
        raise QuoteArtifactError("quote-batch-artifact-head-verification-failed")
    return artifact
