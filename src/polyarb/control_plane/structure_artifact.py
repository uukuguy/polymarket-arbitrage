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
    source_kind: str = "legacy-publication-v1"

    def __post_init__(self) -> None:
        if not self.publication_id or not self.window_id or not self.normalization_contract_version:
            raise ValueError("Structure bundle identity strings must be non-empty")
        if self.source_kind not in {
            "legacy-publication-v1",
            "gamma-source-window-v1",
            "gamma-source-window-events-v2",
            "gamma-source-window-events-v3-sharded",
        }:
            raise ValueError("Structure bundle source_kind is invalid")
        if self.snapshot_id < 0 or (
            self.source_kind == "legacy-publication-v1" and self.snapshot_id <= 0
        ):
            raise ValueError("snapshot_id is invalid for Structure bundle source")
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
            "source_kind": self.source_kind,
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
class StructureShardReceipt:
    """One authenticated bounded component shard named by a manifest."""

    component: str
    ordinal: int
    artifact_key: str
    artifact_digest: str
    row_count: int

    def __post_init__(self) -> None:
        if self.component not in _COMPONENTS or self.ordinal < 0 or self.row_count < 0:
            raise ValueError("invalid shard receipt")
        if not self.artifact_key or len(self.artifact_digest) != 64:
            raise ValueError("invalid shard receipt")

    def record(self) -> dict[str, object]:
        return {
            "artifact_digest": self.artifact_digest,
            "artifact_key": self.artifact_key,
            "component": self.component,
            "ordinal": self.ordinal,
            "row_count": self.row_count,
        }


@dataclass(frozen=True, slots=True)
class StructureShardHeader:
    window_key: str
    source_digest: str
    component: str
    ordinal: int


@dataclass(frozen=True, slots=True)
class StructureShardArtifact:
    """Content-addressed bounded source component evidence."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_bytes(cls, payload: bytes) -> StructureShardArtifact:
        if not payload:
            raise ValueError("Structure shard payload must be non-empty")
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=structure_shard_artifact_key(digest))


@dataclass(frozen=True, slots=True)
class StructureShardBatchArtifact:
    """A content-addressed receipt for all shards made from one page interval."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_bytes(cls, payload: bytes) -> StructureShardBatchArtifact:
        if not payload:
            raise ValueError("Structure shard batch payload must be non-empty")
        digest = hashlib.sha256(payload).hexdigest()
        return cls(payload=payload, sha256=digest, key=structure_shard_batch_artifact_key(digest))


def canonical_structure_shard_bytes(
    *,
    window_key: str,
    source_digest: str,
    component: str,
    ordinal: int,
    rows: Sequence[Mapping[str, object]],
) -> bytes:
    """Serialize one bounded canonical component slice for a source window."""
    if not window_key or len(source_digest) != 64 or component not in _COMPONENTS or ordinal < 0:
        raise ValueError("invalid Structure shard identity")
    encoded_rows = sorted(_canonical_json(row) for row in rows)
    if len(set(encoded_rows)) != len(encoded_rows):
        raise ValueError("duplicate canonical record in shard")
    header = {
        "component": component,
        "kind": "structure-shard",
        "ordinal": ordinal,
        "source_digest": source_digest,
        "window_key": window_key,
    }
    return b"".join(
        [_canonical_json(header) + b"\n", *[b'{"row":' + row + b"}\n" for row in encoded_rows]]
    )


def parse_structure_shard_bytes(
    payload: bytes, *, expected_sha256: str
) -> tuple[StructureShardHeader, tuple[dict[str, object], ...]]:
    """Authenticate one component shard without consulting mutable state."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise StructureBundleError("structure-shard-digest-mismatch")
    try:
        lines = [json.loads(line) for line in payload.splitlines()]
        raw_header = lines[0]
        if not isinstance(raw_header, dict) or raw_header.get("kind") != "structure-shard":
            raise ValueError("header")
        header = StructureShardHeader(
            window_key=str(raw_header["window_key"]),
            source_digest=str(raw_header["source_digest"]),
            component=str(raw_header["component"]),
            ordinal=int(raw_header["ordinal"]),
        )
        if (
            not header.window_key
            or len(header.source_digest) != 64
            or header.component not in _COMPONENTS
            or header.ordinal < 0
        ):
            raise ValueError("header")
        rows = tuple(record["row"] for record in lines[1:])
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("record")
        if canonical_structure_shard_bytes(
            window_key=header.window_key,
            source_digest=header.source_digest,
            component=header.component,
            ordinal=header.ordinal,
            rows=rows,
        ) != payload:
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureBundleError("structure-shard-malformed") from error
    return header, rows


def canonical_structure_shard_manifest_bytes(
    *, identity: StructureBundleIdentity, shards: Sequence[StructureShardReceipt]
) -> bytes:
    """Commit the exact ordered shard receipt set for a sharded generation."""
    if identity.source_kind != "gamma-source-window-events-v3-sharded":
        raise ValueError("shard manifest requires v3 sharded identity")
    ordered = tuple(sorted(shards, key=lambda shard: (shard.component, shard.ordinal)))
    if len({(shard.component, shard.ordinal) for shard in ordered}) != len(ordered):
        raise ValueError("duplicate shard ordinal")
    header = {**identity.header(), "kind": "structure-shard-manifest"}
    return b"".join(
        _canonical_json(record) + b"\n"
        for record in (header, *({"shard": shard.record()} for shard in ordered))
    )


def parse_structure_shard_manifest_bytes(
    payload: bytes, *, expected_sha256: str
) -> tuple[StructureBundleIdentity, tuple[StructureShardReceipt, ...]]:
    """Authenticate a v3 manifest without loading any referenced shard body."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise StructureBundleError("structure-shard-manifest-digest-mismatch")
    try:
        lines = [json.loads(line) for line in payload.splitlines()]
        header = lines[0]
        if not isinstance(header, dict) or header.get("kind") != "structure-shard-manifest":
            raise ValueError("header")
        identity_header = dict(header)
        identity_header["kind"] = "structure-bundle"
        identity = StructureBundleIdentity(
            publication_id=str(identity_header["publication_id"]),
            window_id=str(identity_header["window_id"]),
            snapshot_id=int(identity_header["snapshot_id"]),
            comparison_receipt_digest=str(identity_header["comparison_receipt_digest"]),
            normalization_contract_version=str(identity_header["normalization_contract_version"]),
            component_counts=identity_header["component_counts"],
            source_kind=str(identity_header["source_kind"]),
        )
        shards = tuple(
            StructureShardReceipt(
                component=str(record["shard"]["component"]),
                ordinal=int(record["shard"]["ordinal"]),
                artifact_key=str(record["shard"]["artifact_key"]),
                artifact_digest=str(record["shard"]["artifact_digest"]),
                row_count=int(record["shard"]["row_count"]),
            )
            for record in lines[1:]
        )
        if not shards or (
            canonical_structure_shard_manifest_bytes(identity=identity, shards=shards) != payload
        ):
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureBundleError("structure-shard-manifest-malformed") from error
    return identity, shards


def canonical_structure_shard_batch_bytes(
    *,
    window_key: str,
    source_digest: str,
    start_ordinal: int,
    end_ordinal: int,
    shards: Sequence[StructureShardReceipt],
) -> bytes:
    """Commit all component shards derived from one bounded source-page batch."""
    if (
        not window_key
        or len(source_digest) != 64
        or start_ordinal < 0
        or end_ordinal <= start_ordinal
    ):
        raise ValueError("invalid shard batch identity")
    ordered = tuple(sorted(shards, key=lambda shard: (shard.component, shard.ordinal)))
    if not ordered:
        raise ValueError("shard batch must not be empty")
    if len({(shard.component, shard.ordinal) for shard in ordered}) != len(ordered):
        raise ValueError("duplicate shard ordinal")
    header = {
        "end_ordinal": end_ordinal,
        "kind": "structure-shard-batch",
        "source_digest": source_digest,
        "start_ordinal": start_ordinal,
        "window_key": window_key,
    }
    return b"".join(
        _canonical_json(record) + b"\n"
        for record in (header, *({"shard": shard.record()} for shard in ordered))
    )


def parse_structure_shard_batch_bytes(
    payload: bytes, *, expected_sha256: str
) -> tuple[tuple[str, str, int, int], tuple[StructureShardReceipt, ...]]:
    """Authenticate and decode one bounded source-page shard receipt."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise StructureBundleError("structure-shard-batch-digest-mismatch")
    try:
        lines = [json.loads(line) for line in payload.splitlines()]
        header = lines[0]
        if not isinstance(header, dict) or header.get("kind") != "structure-shard-batch":
            raise ValueError("header")
        window_key = str(header["window_key"])
        source_digest = str(header["source_digest"])
        start = int(header["start_ordinal"])
        end = int(header["end_ordinal"])
        if not window_key or len(source_digest) != 64 or start < 0 or end <= start:
            raise ValueError("header")
        shards = tuple(
            StructureShardReceipt(
                component=str(record["shard"]["component"]),
                ordinal=int(record["shard"]["ordinal"]),
                artifact_key=str(record["shard"]["artifact_key"]),
                artifact_digest=str(record["shard"]["artifact_digest"]),
                row_count=int(record["shard"]["row_count"]),
            )
            for record in lines[1:]
        )
        if not shards or canonical_structure_shard_batch_bytes(
            window_key=window_key,
            source_digest=source_digest,
            start_ordinal=start,
            end_ordinal=end,
            shards=shards,
        ) != payload:
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureBundleError("structure-shard-batch-malformed") from error
    return (window_key, source_digest, start, end), shards


def structure_shard_artifact_key(sha256: str) -> str:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"structure-shards/{sha256}/rows.ndjson"


def structure_shard_batch_artifact_key(sha256: str) -> str:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a sha256 digest")
    return f"structure-shard-batches/{sha256}/batch.ndjson"


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
            source_kind=str(header.get("source_kind", "legacy-publication-v1")),
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


def upload_structure_shard_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: StructureShardArtifact,
) -> StructureShardArtifact:
    """PUT and authenticate one bounded shard before its checkpoint receipt."""
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
        raise StructureBundleError("structure-shard-head-verification-failed")
    return artifact


def upload_structure_shard_batch_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: StructureShardBatchArtifact,
) -> StructureShardBatchArtifact:
    """PUT and authenticate a shard-batch receipt before its fenced checkpoint."""
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
        raise StructureBundleError("structure-shard-batch-head-verification-failed")
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
