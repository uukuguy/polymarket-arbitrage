"""Canonical immutable source bundles for transactional Structure generations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

_COMPONENTS = (
    "events",
    "event_tags",
    "memberships",
    "group_truth",
    "markets",
    "issues",
)


class StructureBundleError(RuntimeError):
    """A Structure source bundle could not be authenticated."""


class _ObjectClient(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class StructureBundleIdentity:
    """The complete identity frozen before Structure shadow execution starts."""

    publication_id: str
    window_id: str
    snapshot_id: int
    comparison_receipt_digest: str
    normalization_contract_version: str
    component_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        if not self.publication_id or not self.window_id or not self.normalization_contract_version:
            raise ValueError("Structure bundle identity strings must be non-empty")
        if self.snapshot_id <= 0:
            raise ValueError("snapshot_id must be positive")
        if len(self.comparison_receipt_digest) != 64:
            raise ValueError("comparison_receipt_digest must be a sha256 digest")
        if set(self.component_counts) != set(_COMPONENTS):
            raise ValueError("component_counts must name every Structure component")
        if any(
            isinstance(count, bool) or not isinstance(count, int) or count < 0
            for count in self.component_counts.values()
        ):
            raise ValueError("component_counts must be non-negative integers")

    def header(self) -> dict[str, object]:
        return {
            "comparison_receipt_digest": self.comparison_receipt_digest,
            "component_counts": dict(sorted(self.component_counts.items())),
            "kind": "structure-bundle",
            "normalization_contract_version": self.normalization_contract_version,
            "publication_id": self.publication_id,
            "snapshot_id": self.snapshot_id,
            "window_id": self.window_id,
        }


@dataclass(frozen=True, slots=True)
class StructureBundleArtifact:
    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_bytes(cls, payload: bytes) -> StructureBundleArtifact:
        if not payload:
            raise ValueError("Structure bundle payload must be non-empty")
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=structure_bundle_artifact_key(digest))


def canonical_structure_bundle_bytes(
    *,
    identity: StructureBundleIdentity,
    components: Mapping[str, Sequence[Mapping[str, object]]],
) -> bytes:
    """Serialize every frozen component in one deterministic authenticated bundle."""
    if set(components) != set(_COMPONENTS):
        raise ValueError("components must name every Structure component")
    records: list[dict[str, object]] = [identity.header()]
    for component in _COMPONENTS:
        rows = components[component]
        if len(rows) != identity.component_counts[component]:
            raise ValueError(f"component count mismatch for {component}")
        canonical_rows: list[tuple[bytes, Mapping[str, object]]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError(f"component row must be an object for {component}")
            encoded = _canonical_json(row)
            canonical_rows.append((encoded, row))
        canonical_rows.sort(key=lambda item: item[0])
        if any(
            canonical_rows[index][0] == canonical_rows[index - 1][0]
            for index in range(1, len(canonical_rows))
        ):
            raise ValueError(f"duplicate canonical record in {component}")
        records.extend(
            {"component": component, "row": dict(row)}
            for _encoded, row in canonical_rows
        )
    return b"".join(_canonical_json(record) + b"\n" for record in records)


def structure_bundle_artifact_key(sha256: str) -> str:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"structure-bundles/{sha256}/generation.ndjson"


def upload_structure_bundle_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: StructureBundleArtifact,
) -> StructureBundleArtifact:
    """PUT and authenticate a bundle before any control-plane receipt uses it."""
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
    remote_digest = str(head.get("Metadata", {}).get("sha256", ""))
    if (
        int(head.get("ContentLength", -1)) != len(artifact.payload)
        or remote_digest != artifact.sha256
    ):
        raise StructureBundleError("structure-bundle-head-verification-failed")
    return artifact


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
