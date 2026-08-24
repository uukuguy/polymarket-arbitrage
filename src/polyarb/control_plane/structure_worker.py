"""One bounded, fenced worker turn for transactional Structure ranges."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import monotonic
from typing import Any, Protocol

from polyarb.config import Settings

from .alert_delivery import incident_alert_channels
from .faults import IntentionalStagingRetryFault
from .models import JobLease, JobState, StructureRangeSpec
from .postgres import IncompleteStructureGenerationError, PostgresControlPlane, StaleLeaseError
from .runtime_contract import AttemptRuntime
from .runtime_models import RuntimeDeadlineProfile
from .structure_artifact import (
    StructureBundleError,
    StructureManifestArtifact,
    StructureRangeArtifact,
    StructureShardReceipt,
    canonical_structure_bundle_bytes,
    canonical_structure_range_bytes,
    parse_structure_bundle_bytes,
    parse_structure_range_bytes,
    parse_structure_shard_bytes,
    parse_structure_shard_manifest_bytes,
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


def _runtime_profile(lease_seconds: int) -> RuntimeDeadlineProfile:
    bounded_lease = max(3, int(lease_seconds))
    heartbeat = max(1, min(30, bounded_lease // 3))
    progress = max(bounded_lease, heartbeat * 3)
    attempt = max(progress, bounded_lease * 10)
    return RuntimeDeadlineProfile(
        policy_version="runtime-v1",
        lease_seconds=bounded_lease,
        heartbeat_seconds=heartbeat,
        progress_seconds=progress,
        attempt_seconds=attempt,
    )


async def _drain_thread_task(task: asyncio.Task[Any]) -> Any:
    """Await an executor call to completion even after owner cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


async def _to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run blocking range work without abandoning its executor thread."""
    task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        try:
            await _drain_thread_task(task)
        except BaseException as error:
            raise cancellation from error
        raise


def _consume_cancellation() -> None:
    task = asyncio.current_task()
    if task is None:
        return
    while task.cancelling():
        task.uncancel()


async def _terminal_to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Drain a point-of-no-return range transaction before cancellation."""
    task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        _consume_cancellation()
        try:
            result = await _drain_thread_task(task)
        except BaseException as error:
            _consume_cancellation()
            raise error from cancellation
        _consume_cancellation()
        return result


async def _progress(
    runtime: AttemptRuntime,
    *,
    stage: str,
    current: int,
    total: int | None,
) -> None:
    await _to_thread(runtime.progress, stage=stage, current=current, total=total)


async def _heartbeat(runtime: AttemptRuntime) -> None:
    await _to_thread(runtime.heartbeat_if_due)


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
        crash_after_r2_upload: Callable[[JobLease], None] | None = None,
        retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
        acceptance_run_id: str | None = None,
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
        self._crash_after_r2_upload = crash_after_r2_upload
        self._retry_fault_before_receipt = retry_fault_before_receipt
        self._acceptance_run_id = acceptance_run_id

    async def run_once(self) -> StructureWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("structure-normalize",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        runtime = AttemptRuntime(
            store=self._control_plane,
            lease=lease,
            profile=_runtime_profile(self._lease_seconds),
            clock=self._now,
        )
        prior = await _to_thread(
            self._control_plane.structure_range_receipt, runtime.current_lease.job_key
        )
        if prior is not None:
            await _progress(runtime, stage="read-range", current=1, total=1)
            await _heartbeat(runtime)
            await _terminal_to_thread(
                self._control_plane.finish,
                runtime.current_lease,
                state=JobState.SUCCEEDED,
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="recovered")
        try:
            spec = await _to_thread(
                self._control_plane.structure_range_spec,
                runtime.current_lease.job_key,
            )
            await _progress(runtime, stage="read-range", current=1, total=1)
            await _heartbeat(runtime)
            artifact, record_count = await _to_thread(
                self._process_range,
                spec, heartbeat=runtime.heartbeat_if_due
            )
            await _progress(runtime, stage="normalize-range", current=1, total=1)
            await _heartbeat(runtime)
            await _to_thread(
                upload_structure_range_artifact,
                self._object_client,
                bucket=self._bucket,
                artifact=artifact,
            )
            await _progress(runtime, stage="upload-range", current=1, total=1)
            await _heartbeat(runtime)
            await _progress(runtime, stage="commit-range", current=1, total=1)
            await _heartbeat(runtime)
            if self._crash_after_r2_upload is not None:
                self._crash_after_r2_upload(runtime.current_lease)
            if self._retry_fault_before_receipt is not None:
                self._retry_fault_before_receipt(runtime.current_lease)
            complete = getattr(self._control_plane, "complete_structure_range", None)
            if callable(complete):
                await _terminal_to_thread(
                    complete,
                    runtime.current_lease,
                    range_digest=spec.range_digest,
                    artifact_key=artifact.key,
                    artifact_digest=artifact.sha256,
                    record_count=record_count,
                    now=self._now(),
                )
            else:
                await _terminal_to_thread(
                    self._control_plane.record_structure_range,
                    runtime.current_lease,
                    range_digest=spec.range_digest,
                    artifact_key=artifact.key,
                    artifact_digest=artifact.sha256,
                    record_count=record_count,
                    now=self._now(),
                )
                await _terminal_to_thread(
                    self._control_plane.finish,
                    runtime.current_lease,
                    state=JobState.SUCCEEDED,
                    now=self._now(),
                )
            try:
                await _terminal_to_thread(
                    self._control_plane.record_job_recovery,
                    runtime.current_lease,
                    component="structure-normalize",
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                    acceptance_run_id=self._acceptance_run_id,
                )
            except Exception:
                return StructureWorkerResult(
                    job_key=lease.job_key, outcome="succeeded:recovery-pending"
                )
            return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except (StructureBundleError, StructureWorkerError):
            await _terminal_to_thread(
                self._control_plane.finish,
                runtime.current_lease,
                state=JobState.QUARANTINED,
                error_class="StructureBundleError",
                now=self._now(),
            )
            raise
        except StaleLeaseError:
            raise
        except IntentionalStagingRetryFault as error:
            await _terminal_to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
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
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="retryable")
        except Exception as error:
            await _terminal_to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
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
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            raise

    def _process_range(
        self,
        spec: StructureRangeSpec,
        *,
        heartbeat: Callable[[], None] | None = None,
    ) -> tuple[StructureRangeArtifact, int]:
        if heartbeat is not None:
            heartbeat()
        response = self._object_client.get_object(Bucket=self._bucket, Key=spec.bundle_key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise StructureWorkerError("structure-bundle-body-unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise StructureWorkerError("structure-bundle-body-is-not-bytes")
        if heartbeat is not None:
            heartbeat()
        try:
            identity, components = parse_structure_bundle_bytes(
                payload, expected_sha256=spec.bundle_digest
            )
        except StructureBundleError:
            identity, shards = parse_structure_shard_manifest_bytes(
                payload, expected_sha256=spec.bundle_digest
            )
            if identity.source_kind != "gamma-source-window-events-v3-sharded":
                raise
            rows = self._read_v3_shard_range(spec, shards, heartbeat=heartbeat)
        else:
            selected_rows: list[dict[str, object]] = []
            for row in components[spec.component]:
                if heartbeat is not None:
                    heartbeat()
                if _in_range(_row_cursor(spec.component, row), spec):
                    selected_rows.append(row)
            rows = tuple(selected_rows)
        artifact = StructureRangeArtifact.from_bytes(
            canonical_structure_range_bytes(
                bundle_digest=spec.bundle_digest,
                component=spec.component,
                range_digest=spec.range_digest,
                rows=rows,
            )
        )
        return artifact, len(rows)

    def _read_v3_shard_range(
        self,
        spec: StructureRangeSpec,
        shards: tuple[StructureShardReceipt, ...],
        *,
        heartbeat: Callable[[], None] | None = None,
    ) -> tuple[dict[str, object], ...]:
        selected = [
            shard
            for shard in shards
            if shard.component == spec.component
            and spec.range_start <= f"shard:{shard.ordinal:08d}" < spec.range_end
        ]
        if len(selected) != 1:
            raise StructureWorkerError("structure-v3-range-shard-selection-invalid")
        shard = selected[0]
        if heartbeat is not None:
            heartbeat()
        payload = _read_object_bytes(
            self._object_client, bucket=self._bucket, key=shard.artifact_key
        )
        header, rows = parse_structure_shard_bytes(
            payload, expected_sha256=shard.artifact_digest
        )
        if header.component != spec.component or header.ordinal != shard.ordinal:
            raise StructureWorkerError("structure-v3-range-shard-identity-invalid")
        if heartbeat is not None:
            heartbeat()
        return rows


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
        monotonic_clock: Callable[[], float] = monotonic,
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
        self._monotonic_clock = monotonic_clock
        self._heartbeat_interval_seconds = max(0.1, lease_seconds / 3)

    def run_once(self) -> StructureWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("structure-certify",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        runtime = AttemptRuntime(
            store=self._control_plane,
            lease=lease,
            profile=_runtime_profile(self._lease_seconds),
            clock=self._now,
        )
        generation_key = lease.job_key.removesuffix(":certify")
        last_heartbeat = self._monotonic_clock()

        def heartbeat_if_due() -> None:
            nonlocal last_heartbeat
            current_monotonic = self._monotonic_clock()
            if current_monotonic - last_heartbeat < self._heartbeat_interval_seconds:
                return
            runtime.heartbeat()
            last_heartbeat = current_monotonic

        try:
            self._verify_content_parity(
                generation_key,
                heartbeat=heartbeat_if_due,
                progress=lambda current, total: runtime.progress(
                    stage="verify-parity", current=current, total=total
                ),
            )
            heartbeat_if_due()
            payload = self._control_plane.structure_manifest_payload(generation_key)
            artifact = StructureManifestArtifact.from_bytes(payload)
            runtime.progress(stage="build-manifest", current=1, total=1)
            heartbeat_if_due()
            upload_structure_manifest_artifact(
                self._object_client, bucket=self._bucket, artifact=artifact
            )
            runtime.progress(stage="upload-manifest", current=1, total=1)
            runtime.progress(stage="commit-certification", current=1, total=1)
            heartbeat_if_due()
            self._control_plane.certify_structure_generation(
                runtime.current_lease,
                generation_key=generation_key,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                now=self._now(),
            )
            try:
                self._control_plane.record_job_recovery(
                    runtime.current_lease,
                    component="structure-certify",
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                )
            except Exception:
                return StructureWorkerResult(
                    job_key=lease.job_key, outcome="certified:recovery-pending"
                )
            return StructureWorkerResult(job_key=lease.job_key, outcome="certified")
        except IncompleteStructureGenerationError:
            self._control_plane.finish(
                runtime.current_lease,
                state=JobState.WAITING,
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="waiting")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                runtime.current_lease,
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
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            raise

    def _verify_content_parity(
        self,
        generation_key: str,
        *,
        heartbeat: Callable[[], None],
        progress: Callable[[int, int], None],
    ) -> None:
        ranges = self._control_plane.structure_generation_receipts(generation_key)
        source_spec = ranges[0][0]
        heartbeat()
        source_payload = _read_object_bytes(
            self._object_client, bucket=self._bucket, key=source_spec.bundle_key
        )
        try:
            identity, source_components = parse_structure_bundle_bytes(
                source_payload, expected_sha256=source_spec.bundle_digest
            )
        except StructureBundleError:
            identity, shards = parse_structure_shard_manifest_bytes(
                source_payload, expected_sha256=source_spec.bundle_digest
            )
            if identity.source_kind != "gamma-source-window-events-v3-sharded":
                raise
            self._verify_v3_content_parity(
                ranges, shards, heartbeat=heartbeat, progress=progress
            )
            return
        rebuilt: dict[str, list[dict[str, object]]] = {
            component: [] for component in source_components
        }
        total_ranges = len(ranges)
        for index, (spec, receipt) in enumerate(ranges, start=1):
            heartbeat()
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
            progress(index, total_ranges)
        try:
            reassembled = canonical_structure_bundle_bytes(
                identity=identity,
                components={component: tuple(rows) for component, rows in rebuilt.items()},
            )
        except ValueError as error:
            raise StructureWorkerError("structure-content-parity-reassembly") from error
        if reassembled != source_payload:
            raise StructureWorkerError("structure-content-parity-reassembly")

    def _verify_v3_content_parity(
        self,
        ranges: Sequence[tuple[StructureRangeSpec, object]],
        shards: tuple[StructureShardReceipt, ...],
        *,
        heartbeat: Callable[[], None],
        progress: Callable[[int, int], None],
    ) -> None:
        by_identity = {(shard.component, shard.ordinal): shard for shard in shards}
        total_ranges = len(ranges)
        for index, (spec, receipt) in enumerate(ranges, start=1):
            heartbeat()
            ordinal = int(spec.range_start.removeprefix("shard:"))
            shard = by_identity.get((spec.component, ordinal))
            if shard is None or spec.range_end != f"shard:{ordinal + 1:08d}":
                raise StructureWorkerError("structure-v3-content-parity-range-identity")
            source_payload = _read_object_bytes(
                self._object_client, bucket=self._bucket, key=shard.artifact_key
            )
            _header, expected_rows = parse_structure_shard_bytes(
                source_payload, expected_sha256=shard.artifact_digest
            )
            heartbeat()
            payload = _read_object_bytes(
                self._object_client, bucket=self._bucket, key=getattr(receipt, "artifact_key")
            )
            _range_identity, actual_rows = parse_structure_range_bytes(
                payload, expected_sha256=getattr(receipt, "artifact_digest")
            )
            if actual_rows != expected_rows or len(actual_rows) != getattr(receipt, "record_count"):
                raise StructureWorkerError("structure-v3-content-parity-range-content")
            progress(index, total_ranges)


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
