"""Immutable R2 artifacts for fenced Quote batch payloads."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol


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
