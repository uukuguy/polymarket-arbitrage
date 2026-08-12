"""Canonical artifact contracts for transactional Structure shadow input."""

from __future__ import annotations

from hashlib import sha256

import pytest

from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleError,
    StructureBundleIdentity,
    StructureRangeArtifact,
    canonical_structure_bundle_bytes,
    canonical_structure_range_bytes,
    parse_structure_bundle_bytes,
    parse_structure_range_bytes,
    structure_bundle_artifact_key,
    upload_structure_bundle_artifact,
)


def _identity() -> StructureBundleIdentity:
    return StructureBundleIdentity(
        publication_id="publication-1",
        window_id="window-1",
        snapshot_id=42,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="structure-v7",
        component_counts={
            "events": 2,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 1,
            "issues": 0,
        },
    )


def test_structure_bundle_is_canonical_and_content_addressed() -> None:
    payload = canonical_structure_bundle_bytes(
        identity=_identity(),
        components={
            "events": ({"id": "event-b", "title": "B"}, {"id": "event-a", "title": "A"}),
            "event_tags": (),
            "memberships": (),
            "group_truth": (),
            "markets": ({"market_id": "market-a", "slug": "a"},),
            "issues": (),
        },
    )
    artifact = StructureBundleArtifact.from_bytes(payload)

    assert artifact.sha256 == sha256(payload).hexdigest()
    assert artifact.key == structure_bundle_artifact_key(artifact.sha256)
    assert payload.splitlines()[1] == b'{"component":"events","row":{"id":"event-a","title":"A"}}'
    assert payload.splitlines()[2] == b'{"component":"events","row":{"id":"event-b","title":"B"}}'


def test_structure_bundle_refuses_component_count_or_duplicate_record_mismatch() -> None:
    with pytest.raises(ValueError, match="component count"):
        canonical_structure_bundle_bytes(
            identity=_identity(),
            components={component: () for component in _identity().component_counts},
        )
    components = {
        "events": ({"id": "event-a"}, {"id": "event-a"}),
        "event_tags": (),
        "memberships": (),
        "group_truth": (),
        "markets": ({"market_id": "market-a"},),
        "issues": (),
    }
    with pytest.raises(ValueError, match="duplicate"):
        canonical_structure_bundle_bytes(identity=_identity(), components=components)


def test_structure_bundle_parse_authenticates_digest_and_recovers_components() -> None:
    payload = canonical_structure_bundle_bytes(
        identity=_identity(),
        components={
            "events": ({"id": "event-a"}, {"id": "event-b"}),
            "event_tags": (),
            "memberships": (),
            "group_truth": (),
            "markets": ({"market_id": "market-a"},),
            "issues": (),
        },
    )
    identity, components = parse_structure_bundle_bytes(
        payload, expected_sha256=sha256(payload).hexdigest()
    )

    assert identity == _identity()
    assert components["events"] == ({"id": "event-a"}, {"id": "event-b"})
    with pytest.raises(StructureBundleError, match="digest-mismatch"):
        parse_structure_bundle_bytes(payload, expected_sha256="b" * 64)


def test_structure_range_output_is_bound_to_one_bundle_and_range() -> None:
    payload = canonical_structure_range_bytes(
        bundle_digest="a" * 64,
        component="markets",
        range_digest="b" * 64,
        rows=({"market_id": "b"}, {"market_id": "a"}),
    )
    artifact = StructureRangeArtifact.from_bytes(payload)

    assert artifact.key == f"structure-ranges/{artifact.sha256}/rows.ndjson"
    assert payload.splitlines()[1] == b'{"row":{"market_id":"a"}}'
    identity, rows = parse_structure_range_bytes(
        payload,
        expected_sha256=artifact.sha256,
    )
    assert identity == ("a" * 64, "markets", "b" * 64)
    assert rows == ({"market_id": "a"}, {"market_id": "b"})
    with pytest.raises(StructureBundleError, match="range-digest-mismatch"):
        parse_structure_range_bytes(payload, expected_sha256="c" * 64)


def test_structure_bundle_upload_requires_head_identity() -> None:
    class Client:
        def __init__(self) -> None:
            self.put: dict[str, object] = {}

        def put_object(self, **kwargs: object) -> None:
            self.put = kwargs

        def head_object(self, **kwargs: object) -> dict[str, object]:
            return {"ContentLength": len(self.put["Body"]), "Metadata": self.put["Metadata"]}

    artifact = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    client = Client()

    assert (
        upload_structure_bundle_artifact(client, bucket="structure", artifact=artifact)
        == artifact
    )
    assert client.put["Key"] == artifact.key
    assert client.put["Metadata"] == {"sha256": artifact.sha256}


def test_structure_bundle_upload_refuses_wrong_remote_digest() -> None:
    class Client:
        def put_object(self, **kwargs: object) -> None:
            pass

        def head_object(self, **kwargs: object) -> dict[str, object]:
            return {"ContentLength": 1, "Metadata": {"sha256": "wrong"}}

    artifact = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    with pytest.raises(StructureBundleError, match="head-verification"):
        upload_structure_bundle_artifact(Client(), bucket="structure", artifact=artifact)
