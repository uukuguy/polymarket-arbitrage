"""Immutable Quote-batch artifact contracts."""

from __future__ import annotations

import hashlib

from polyarb.control_plane.models import QuoteBatchLeg, QuoteBatchSpec
from polyarb.control_plane.quote_artifact import (
    QuoteBatchArtifact,
    QuoteBatchInputArtifact,
    canonical_quote_batch_bytes,
    parse_quote_batch_input_bytes,
    quote_batch_artifact_key,
    quote_batch_input_artifact_key,
    upload_quote_batch_artifact,
)


def test_quote_batch_artifact_is_canonical_content_addressed_and_head_verified() -> None:
    payload = canonical_quote_batch_bytes(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_range_digest="c" * 64,
        quotes=(
            {"token_id": "token-b", "best_ask": 0.42},
            {"token_id": "token-a", "best_ask": 0.41},
        ),
    )
    artifact = QuoteBatchArtifact.from_bytes(payload)
    assert artifact.sha256 == hashlib.sha256(payload).hexdigest()
    assert artifact.key == quote_batch_artifact_key(artifact.sha256)

    class Client:
        def __init__(self) -> None:
            self.put: dict[str, object] | None = None

        def put_object(self, **kwargs) -> None:
            self.put = kwargs

        def head_object(self, **kwargs):
            assert self.put is not None
            return {
                "ContentLength": len(self.put["Body"]),
                "Metadata": {"sha256": artifact.sha256},
            }

    client = Client()
    assert upload_quote_batch_artifact(client, bucket="quotes", artifact=artifact) == artifact
    assert client.put == {
        "Bucket": "quotes",
        "Key": artifact.key,
        "Body": payload,
        "ContentType": "application/x-ndjson",
        "Metadata": {"sha256": artifact.sha256},
    }


def test_quote_batch_input_artifact_round_trips_the_fenced_batch_spec() -> None:
    spec = QuoteBatchSpec.from_legs(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        ordinal=7,
        legs=(
            QuoteBatchLeg("neg-b", "market-b", "condition-b", None, "token-b"),
            QuoteBatchLeg("neg-a", "market-a", "condition-a", "a", "token-a"),
        ),
    )

    artifact = QuoteBatchInputArtifact.from_spec(spec)

    assert artifact.sha256 == hashlib.sha256(artifact.payload).hexdigest()
    assert artifact.key == quote_batch_input_artifact_key(artifact.sha256)
    assert parse_quote_batch_input_bytes(artifact.payload, expected_sha256=artifact.sha256) == spec
