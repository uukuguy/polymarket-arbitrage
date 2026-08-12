"""One bounded, fenced worker turn for transactional Structure ranges."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from .models import JobState, StructureRangeSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .structure_artifact import (
    StructureBundleError,
    StructureManifestArtifact,
    StructureRangeArtifact,
    canonical_structure_bundle_bytes,
    canonical_structure_range_bytes,
    parse_structure_bundle_bytes,
    parse_structure_range_bytes,
    upload_structure_manifest_artifact,
    upload_structure_range_artifact,
)


class StructureWorkerError(RuntimeError):
    """An admitted Structure bundle violates its frozen range contract."""


class _Body(Protocol):
    def read(self) -> bytes: ...


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class StructureWorkerResult:
    job_key: str | None
    outcome: str


class TransactionalStructureWorker:
    """Claim one R2-backed range; SQLite is never reachable from this worker."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        lease_seconds: int = 120,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("lease_seconds and retry_delay must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    async def run_once(self) -> StructureWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("structure-normalize",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        prior = self._control_plane.structure_range_receipt(lease.job_key)
        if prior is not None:
            self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
            return StructureWorkerResult(job_key=lease.job_key, outcome="recovered")
        try:
            spec = self._control_plane.structure_range_spec(lease.job_key)
            artifact, record_count = self._process_range(spec)
            self._control_plane.record_structure_range(
                lease,
                range_digest=spec.range_digest,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                record_count=record_count,
                now=self._now(),
            )
            self._control_plane.finish(lease, state=JobState.SUCCEEDED, now=self._now())
            return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except (StructureBundleError, StructureWorkerError):
            self._control_plane.finish(
                lease,
                state=JobState.QUARANTINED,
                error_class="StructureBundleError",
                now=self._now(),
            )
            raise
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
                next_attempt_at=self._now() + self._retry_delay,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="structure-normalize",
                summary="structure-normalize retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=("dashboard",),
                now=self._now(),
            )
            raise

    def _process_range(self, spec: StructureRangeSpec) -> tuple[StructureRangeArtifact, int]:
        response = self._object_client.get_object(Bucket=self._bucket, Key=spec.bundle_key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise StructureWorkerError("structure-bundle-body-unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise StructureWorkerError("structure-bundle-body-is-not-bytes")
        _identity, components = parse_structure_bundle_bytes(
            payload, expected_sha256=spec.bundle_digest
        )
        rows = tuple(
            row
            for row in components[spec.component]
            if _in_range(_row_cursor(spec.component, row), spec)
        )
        artifact = StructureRangeArtifact.from_bytes(
            canonical_structure_range_bytes(
                bundle_digest=spec.bundle_digest,
                component=spec.component,
                range_digest=spec.range_digest,
                rows=rows,
            )
        )
        upload_structure_range_artifact(self._object_client, bucket=self._bucket, artifact=artifact)
        return artifact, len(rows)


class TransactionalStructureCertifier:
    """Upload one canonical manifest, then certify it under a lease fence."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        lease_seconds: int = 30,
        retry_delay: timedelta = timedelta(seconds=5),
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("lease_seconds and retry_delay must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    def run_once(self) -> StructureWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("structure-certify",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        generation_key = lease.job_key.removesuffix(":certify")
        try:
            self._verify_content_parity(generation_key)
            payload = self._control_plane.structure_manifest_payload(generation_key)
            artifact = StructureManifestArtifact.from_bytes(payload)
            upload_structure_manifest_artifact(
                self._object_client, bucket=self._bucket, artifact=artifact
            )
            self._control_plane.certify_structure_generation(
                lease,
                generation_key=generation_key,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="certified")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
                next_attempt_at=self._now() + self._retry_delay,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="structure-certify",
                summary="structure-certify retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=("dashboard",),
                now=self._now(),
            )
            raise

    def _verify_content_parity(self, generation_key: str) -> None:
        ranges = self._control_plane.structure_generation_receipts(generation_key)
        source_spec = ranges[0][0]
        source_payload = _read_object_bytes(
            self._object_client, bucket=self._bucket, key=source_spec.bundle_key
        )
        identity, source_components = parse_structure_bundle_bytes(
            source_payload, expected_sha256=source_spec.bundle_digest
        )
        rebuilt: dict[str, list[dict[str, object]]] = {
            component: [] for component in source_components
        }
        for spec, receipt in ranges:
            if (
                spec.bundle_key != source_spec.bundle_key
                or spec.bundle_digest != source_spec.bundle_digest
            ):
                raise StructureWorkerError("structure-content-parity-mixed-source")
            payload = _read_object_bytes(
                self._object_client, bucket=self._bucket, key=receipt.artifact_key
            )
            range_identity, rows = parse_structure_range_bytes(
                payload, expected_sha256=receipt.artifact_digest
            )
            if range_identity != (spec.bundle_digest, spec.component, spec.range_digest):
                raise StructureWorkerError("structure-content-parity-range-identity")
            if len(rows) != receipt.record_count or any(
                not _in_range(_row_cursor(spec.component, row), spec) for row in rows
            ):
                raise StructureWorkerError("structure-content-parity-range-content")
            rebuilt[spec.component].extend(rows)
        try:
            reassembled = canonical_structure_bundle_bytes(
                identity=identity,
                components={component: tuple(rows) for component, rows in rebuilt.items()},
            )
        except ValueError as error:
            raise StructureWorkerError("structure-content-parity-reassembly") from error
        if reassembled != source_payload:
            raise StructureWorkerError("structure-content-parity-reassembly")


def _in_range(cursor: str, spec: StructureRangeSpec) -> bool:
    return cursor >= spec.range_start and (not spec.range_end or cursor < spec.range_end)


def _read_object_bytes(_client: _ObjectClient, *, bucket: str, key: str) -> bytes:
    response = _client.get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise StructureWorkerError("structure-artifact-body-unavailable")
    payload = body.read()
    if not isinstance(payload, bytes):
        raise StructureWorkerError("structure-artifact-body-is-not-bytes")
    return payload


def _row_cursor(component: str, row: Mapping[str, object]) -> str:
    fields = {
        "events": ("id",),
        "event_tags": ("event_id", "tag_id"),
        "memberships": ("event_id", "market_id"),
        "group_truth": ("neg_risk_market_id",),
        "markets": ("market_id",),
        "issues": ("id",),
    }[component]
    try:
        values = tuple(row[field] for field in fields)
    except KeyError as error:
        raise StructureWorkerError(f"structure-range-cursor-unavailable:{component}") from error
    if any(isinstance(value, bool) or not isinstance(value, (str, int)) for value in values):
        raise StructureWorkerError(f"structure-range-cursor-invalid:{component}")
    return "\x00".join(str(value) for value in values)
