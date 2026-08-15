from __future__ import annotations

import hashlib

import pytest

from polyarb.control_plane.structure_artifact import (
    StructureBundleError,
    StructureBundleIdentity,
    StructureShardReceipt,
    canonical_structure_shard_bytes,
    parse_structure_shard_bytes,
)


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


def test_shard_manifest_rejects_duplicate_component_ordinals() -> None:
    from polyarb.control_plane.structure_artifact import canonical_structure_shard_manifest_bytes

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
