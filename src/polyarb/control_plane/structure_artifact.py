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


@dataclass(frozen=True, slots=True)
class StructureRangeArtifact:
    """Canonical normalized output for one frozen Structure input range."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_bytes(cls, payload: bytes) -> StructureRangeArtifact:
        if not payload:
            raise ValueError("Structure range payload must be non-empty")
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=structure_range_artifact_key(digest))


@dataclass(frozen=True, slots=True)
class StructureManifestArtifact:
    """Canonical receipt manifest for one fully normalized Structure generation."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_bytes(cls, payload: bytes) -> StructureManifestArtifact:
        if not payload:
            raise ValueError("Structure manifest payload must be non-empty")
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=structure_manifest_artifact_key(digest))


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


def canonical_structure_range_bytes(
    *,
    bundle_digest: str,
    component: str,
    range_digest: str,
    rows: Sequence[Mapping[str, object]],
) -> bytes:
    """Serialize normalized rows bound to exactly one admitted source range."""
    if len(bundle_digest) != 64 or len(range_digest) != 64 or component not in _COMPONENTS:
        raise ValueError("invalid Structure range artifact identity")
    records = [
        {
            "bundle_digest": bundle_digest,
            "component": component,
            "kind": "structure-range",
            "range_digest": range_digest,
        },
        *({"row": dict(row)} for row in sorted(rows, key=_canonical_json)),
    ]
    return b"".join(_canonical_json(record) + b"\n" for record in records)


def canonical_structure_manifest_bytes(
    *,
    generation_key: str,
    bundle_digest: str,
    receipts: Sequence[Mapping[str, object]],
) -> bytes:
    """Serialize the complete ordered receipt set that certifies one generation."""
    if not generation_key or len(bundle_digest) != 64:
        raise ValueError("invalid Structure manifest identity")
    records: list[dict[str, object]] = [
        {
            "bundle_digest": bundle_digest,
            "generation_key": generation_key,
            "kind": "structure-manifest",
        }
    ]
    for receipt in receipts:
        required = {
            "job_key",
            "component",
            "ordinal",
            "range_digest",
            "artifact_key",
            "artifact_digest",
            "record_count",
        }
        if set(receipt) != required:
            raise ValueError("Structure manifest receipt shape is invalid")
        identity_fields = required - {"ordinal", "record_count"}
        if any(
            not isinstance(receipt[key], str) or not receipt[key] for key in identity_fields
        ):
            raise ValueError("Structure manifest receipt identity is invalid")
        if isinstance(receipt["ordinal"], bool) or not isinstance(receipt["ordinal"], int):
            raise ValueError("Structure manifest ordinal is invalid")
        if isinstance(receipt["record_count"], bool) or not isinstance(
            receipt["record_count"], int
        ):
            raise ValueError("Structure manifest record count is invalid")
        records.append(dict(receipt))
    return b"".join(_canonical_json(record) + b"\n" for record in records)


def structure_range_artifact_key(sha256: str) -> str:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"structure-ranges/{sha256}/rows.ndjson"


def structure_manifest_artifact_key(sha256: str) -> str:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"structure-manifests/{sha256}/manifest.ndjson"


def parse_structure_bundle_bytes(
    payload: bytes,
    *,
    expected_sha256: str,
) -> tuple[StructureBundleIdentity, dict[str, tuple[dict[str, object], ...]]]:
    """Authenticate and decode a canonical bundle before a worker uses any row."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise StructureBundleError("structure-bundle-digest-mismatch")
    try:
        lines = [json.loads(line) for line in payload.splitlines()]
        header = lines[0]
        if not isinstance(header, dict) or header.pop("kind", None) != "structure-bundle":
            raise ValueError("header")
        identity = StructureBundleIdentity(
            publication_id=str(header["publication_id"]),
            window_id=str(header["window_id"]),
            snapshot_id=int(header["snapshot_id"]),
            comparison_receipt_digest=str(header["comparison_receipt_digest"]),
            normalization_contract_version=str(header["normalization_contract_version"]),
            component_counts=header["component_counts"],
        )
        components: dict[str, list[dict[str, object]]] = {
            component: [] for component in _COMPONENTS
        }
        for record in lines[1:]:
            if not isinstance(record, dict):
                raise ValueError("record")
            component, row = record.get("component"), record.get("row")
            if component not in components or not isinstance(row, dict):
                raise ValueError("record")
            components[component].append(row)
        frozen = {component: tuple(rows) for component, rows in components.items()}
        if canonical_structure_bundle_bytes(identity=identity, components=frozen) != payload:
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureBundleError("structure-bundle-malformed") from error
    return identity, frozen


def parse_structure_range_bytes(
    payload: bytes,
    *,
    expected_sha256: str,
) -> tuple[tuple[str, str, str], tuple[dict[str, object], ...]]:
    """Authenticate and decode one canonical normalized range artifact."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise StructureBundleError("structure-range-digest-mismatch")
    try:
        lines = [json.loads(line) for line in payload.splitlines()]
        header = lines[0]
        if not isinstance(header, dict) or header.get("kind") != "structure-range":
            raise ValueError("header")
        bundle_digest = str(header["bundle_digest"])
        component = str(header["component"])
        range_digest = str(header["range_digest"])
        if len(bundle_digest) != 64 or len(range_digest) != 64 or component not in _COMPONENTS:
            raise ValueError("header")
        rows: list[dict[str, object]] = []
        for record in lines[1:]:
            if not isinstance(record, dict) or set(record) != {"row"}:
                raise ValueError("record")
            row = record["row"]
            if not isinstance(row, dict):
                raise ValueError("record")
            rows.append(row)
        frozen = tuple(rows)
        if canonical_structure_range_bytes(
            bundle_digest=bundle_digest,
            component=component,
            range_digest=range_digest,
            rows=frozen,
        ) != payload:
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureBundleError("structure-range-malformed") from error
    return (bundle_digest, component, range_digest), frozen


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


def upload_structure_range_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: StructureRangeArtifact,
) -> StructureRangeArtifact:
    """PUT and authenticate a normalized range before its durable receipt."""
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
        raise StructureBundleError("structure-range-head-verification-failed")
    return artifact


def upload_structure_manifest_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: StructureManifestArtifact,
) -> StructureManifestArtifact:
    """PUT and authenticate a terminal manifest before its fenced certification."""
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
        raise StructureBundleError("structure-manifest-head-verification-failed")
    return artifact


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
