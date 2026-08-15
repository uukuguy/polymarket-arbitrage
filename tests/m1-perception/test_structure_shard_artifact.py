from __future__ import annotations

import hashlib

import pytest

from polyarb.control_plane.structure_artifact import (
    StructureBundleError,
    StructureBundleIdentity,
    StructureShardArtifact,
    StructureShardBatchArtifact,
    StructureShardReceipt,
    canonical_structure_shard_bytes,
    parse_structure_shard_bytes,
)
from polyarb.control_plane.structure_shadow import plan_shard_structure_ranges


def test_structure_shard_canonicalizes_one_component_and_rejects_tampering() -> None:
    payload = canonical_structure_shard_bytes(
        window_key="source-window:1",
        source_digest="a" * 64,
        component="markets",
        ordinal=3,
        rows=({"market_id": "b"}, {"market_id": "a"}),
    )

    header, rows = parse_structure_shard_bytes(
        payload, expected_sha256=hashlib.sha256(payload).hexdigest()
    )

    assert header.component == "markets"
    assert header.ordinal == 3
    assert rows == ({"market_id": "a"}, {"market_id": "b"})
    with pytest.raises(StructureBundleError, match="digest-mismatch"):
        parse_structure_shard_bytes(payload, expected_sha256="b" * 64)
    assert StructureShardArtifact.from_bytes(payload).key.endswith("/rows.ndjson")


def test_shard_manifest_rejects_duplicate_component_ordinals() -> None:
    from polyarb.control_plane.structure_artifact import (
        canonical_structure_shard_manifest_bytes,
        parse_structure_shard_manifest_bytes,
    )

    identity = StructureBundleIdentity(
        publication_id="source-window:source-window:1",
        window_id="source-window:1",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 1,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    duplicate = StructureShardReceipt(
        component="markets",
        ordinal=0,
        artifact_key="structure-shards/a/rows.ndjson",
        artifact_digest="a" * 64,
        row_count=1,
    )

    with pytest.raises(ValueError, match="duplicate shard ordinal"):
        canonical_structure_shard_manifest_bytes(identity=identity, shards=(duplicate, duplicate))

    payload = canonical_structure_shard_manifest_bytes(identity=identity, shards=(duplicate,))
    parsed_identity, parsed_shards = parse_structure_shard_manifest_bytes(
        payload, expected_sha256=hashlib.sha256(payload).hexdigest()
    )
    assert parsed_identity == identity
    assert parsed_shards == (duplicate,)


def test_shard_batch_binds_one_source_page_interval_to_all_component_shards() -> None:
    from polyarb.control_plane.structure_artifact import (
        canonical_structure_shard_batch_bytes,
        parse_structure_shard_batch_bytes,
    )

    shard = StructureShardReceipt(
        component="markets",
        ordinal=4,
        artifact_key="structure-shards/a/rows.ndjson",
        artifact_digest="a" * 64,
        row_count=1,
    )
    payload = canonical_structure_shard_batch_bytes(
        window_key="source-window:1",
        source_digest="b" * 64,
        start_ordinal=4,
        end_ordinal=5,
        shards=(shard,),
    )

    artifact = StructureShardBatchArtifact.from_bytes(payload)
    assert artifact.key.endswith("/batch.ndjson")
    header, shards = parse_structure_shard_batch_bytes(
        payload, expected_sha256=artifact.sha256
    )
    assert header == ("source-window:1", "b" * 64, 4, 5)
    assert shards == (shard,)


def test_shard_ranges_name_exactly_one_component_ordinal() -> None:
    shards = (
        StructureShardReceipt(
            component="markets",
            ordinal=1,
            artifact_key="structure-shards/a/rows.ndjson",
            artifact_digest="a" * 64,
            row_count=1,
        ),
        StructureShardReceipt(
            component="events",
            ordinal=0,
            artifact_key="structure-shards/b/rows.ndjson",
            artifact_digest="b" * 64,
            row_count=1,
        ),
    )

    assert plan_shard_structure_ranges(shards) == (
        ("events", "shard:00000000", "shard:00000001"),
        ("markets", "shard:00000001", "shard:00000002"),
    )
