"""One bounded, fenced worker turn for transactional Structure ranges."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from threading import Event, Thread
from time import monotonic
from typing import Any, Protocol

from polyarb.config import Settings

from .alert_delivery import incident_alert_channels
from .blocking_bridge import run_blocking_call
from .faults import IntentionalStagingRetryFault
from .models import JobLease, JobState, StructureRangeSpec
from .postgres import IncompleteStructureGenerationError, PostgresControlPlane, StaleLeaseError
from .runtime_contract import AttemptRuntime, ServiceStopRequested
from .runtime_deadlines import runtime_deadline_profile, runtime_policy
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


async def _to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run nonterminal blocking work under the shared service boundary."""
    return await run_blocking_call(
        call,
        *args,
        thread_name="structure-range:blocking-call",
        **kwargs,
    )


async def _terminal_to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Allow a terminal range transaction to finish only within stop grace."""
    return await run_blocking_call(
        call,
        *args,
        point_of_no_return=True,
        thread_name="structure-range:terminal-call",
        **kwargs,
    )


def _run_bounded_sync_call[SyncResult](
    call: Callable[[], SyncResult],
    *,
    heartbeat: Callable[[], None] | None,
    heartbeat_interval_seconds: float,
    attempt_timeout_seconds: float,
    monotonic_clock: Callable[[], float] = monotonic,
    terminal: bool = False,
) -> SyncResult:
    """Run one blocking call while the current thread polls its lease.

    The call owns a dedicated executor future.  Heartbeat or cancellation
    errors never abandon that future: the runner drains it before surfacing
    the error, so a late R2/DB mutation cannot outlive the worker decision.
    Terminal calls deliberately skip heartbeat polling after the caller has
    crossed its point of no return (the transaction itself remains bounded by
    the attempt/underlying timeout).
    """
    if heartbeat_interval_seconds <= 0 or attempt_timeout_seconds <= 0:
        raise ValueError("sync call deadlines must be positive")
    future: Future[SyncResult] = Future()

    def invoke() -> None:
        try:
            future.set_result(call())
        except BaseException as error:
            future.set_exception(error)

    Thread(target=invoke, name="structure-sync", daemon=True).start()
    primary_error: BaseException | None = None
    deadline = monotonic_clock() + attempt_timeout_seconds
    while True:
        remaining = deadline - monotonic_clock()
        if remaining <= 0:
            primary_error = TimeoutError("structure sync call exceeded attempt deadline")
            break
        try:
            return future.result(timeout=min(heartbeat_interval_seconds, remaining))
        except FutureTimeoutError:
            if terminal or heartbeat is None:
                continue
            try:
                heartbeat()
            except BaseException as error:
                primary_error = error
                break
        except BaseException as error:
            primary_error = error
            break

    # A worker-side timeout/fence/cancellation is only observable after
    # the underlying call has quiesced.  This is the no-late-effect fence.
    underlying_error: BaseException | None = None
    try:
        future.result()
    except BaseException as error:
        underlying_error = error
    if primary_error is None:
        raise AssertionError("sync call exited without a result or error")
    if underlying_error is not None:
        raise primary_error from underlying_error
    raise primary_error


async def _run_bounded_sync_call_async[SyncResult](
    call: Callable[[], SyncResult],
    *,
    heartbeat: Callable[[], None] | None,
    heartbeat_interval_seconds: float,
    attempt_timeout_seconds: float,
    monotonic_clock: Callable[[], float] = monotonic,
    terminal: bool = False,
) -> SyncResult:
    """Async owner wrapper for :func:`_run_bounded_sync_call`."""
    bounded_call = partial(
        _run_bounded_sync_call,
        call,
        heartbeat=heartbeat,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        attempt_timeout_seconds=attempt_timeout_seconds,
        monotonic_clock=monotonic_clock,
        terminal=terminal,
    )
    return await run_blocking_call(
        bounded_call,
        point_of_no_return=terminal,
        thread_name=(
            "structure-range:terminal-bounded-call" if terminal else "structure-range:bounded-call"
        ),
    )


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


async def _runtime_sync_call_async[SyncResult](
    runtime: AttemptRuntime,
    call: Callable[[], SyncResult],
    *,
    terminal: bool = False,
) -> SyncResult:
    profile = runtime.profile
    return await _run_bounded_sync_call_async(
        call,
        heartbeat=None if terminal else runtime.heartbeat_if_due,
        heartbeat_interval_seconds=float(profile.heartbeat_seconds),
        attempt_timeout_seconds=runtime.remaining_attempt_seconds(),
        terminal=terminal,
    )


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
        crash_after_r2_upload: Callable[[JobLease], None] | None = None,
        retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
        acceptance_run_id: str | None = None,
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
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
            profile=runtime_deadline_profile("structure-normalize", self._lease_seconds),
            clock=self._now,
        )
        prior = await _runtime_sync_call_async(
            runtime,
            lambda: self._control_plane.structure_range_receipt(runtime.current_lease.job_key),
        )
        if prior is not None:
            await _progress(runtime, stage="read-range", current=1, total=1)
            await _heartbeat(runtime)
            await _runtime_sync_call_async(
                runtime,
                lambda: self._control_plane.complete_structure_range(
                    runtime.current_lease,
                    range_digest=prior.range_digest,
                    artifact_key=prior.artifact_key,
                    artifact_digest=prior.artifact_digest,
                    record_count=prior.record_count,
                    now=self._now(),
                ),
                terminal=True,
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="recovered")
        try:
            spec = await _runtime_sync_call_async(
                runtime,
                lambda: self._control_plane.structure_range_spec(runtime.current_lease.job_key),
            )
            await _progress(runtime, stage="read-range", current=1, total=1)
            await _heartbeat(runtime)
            artifact, record_count = await _runtime_sync_call_async(
                runtime,
                lambda: self._process_range(spec),
            )
            await _progress(runtime, stage="normalize-range", current=1, total=1)
            await _heartbeat(runtime)
            await _runtime_sync_call_async(
                runtime,
                lambda: upload_structure_range_artifact(
                    self._object_client,
                    bucket=self._bucket,
                    artifact=artifact,
                ),
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
                await _runtime_sync_call_async(
                    runtime,
                    lambda: complete(
                        runtime.current_lease,
                        range_digest=spec.range_digest,
                        artifact_key=artifact.key,
                        artifact_digest=artifact.sha256,
                        record_count=record_count,
                        now=self._now(),
                    ),
                    terminal=True,
                )
            else:
                await _runtime_sync_call_async(
                    runtime,
                    lambda: self._control_plane.record_structure_range(
                        runtime.current_lease,
                        range_digest=spec.range_digest,
                        artifact_key=artifact.key,
                        artifact_digest=artifact.sha256,
                        record_count=record_count,
                        now=self._now(),
                    ),
                    terminal=True,
                )
                await _runtime_sync_call_async(
                    runtime,
                    lambda: self._control_plane.finish(
                        runtime.current_lease,
                        state=JobState.SUCCEEDED,
                        now=self._now(),
                    ),
                    terminal=True,
                )
            try:
                await _runtime_sync_call_async(
                    runtime,
                    lambda: self._control_plane.record_job_recovery(
                        runtime.current_lease,
                        component="structure-normalize",
                        channels=incident_alert_channels(Settings()),
                        now=self._now(),
                        acceptance_run_id=self._acceptance_run_id,
                    ),
                    terminal=True,
                )
            except Exception:
                return StructureWorkerResult(
                    job_key=lease.job_key, outcome="succeeded:recovery-pending"
                )
            return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except asyncio.CancelledError:
            await _terminal_to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
                error_class="ServiceStopRequested",
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="structure-normalize",
                summary="structure-normalize interrupted by service stop",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": "ServiceStopRequested",
                    "reason_code": "service-stop",
                },
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            raise
        except (StructureBundleError, StructureWorkerError):
            await _runtime_sync_call_async(
                runtime,
                lambda: self._control_plane.finish(
                    runtime.current_lease,
                    state=JobState.QUARANTINED,
                    error_class="StructureBundleError",
                    now=self._now(),
                ),
                terminal=True,
            )
            raise
        except StaleLeaseError:
            raise
        except IntentionalStagingRetryFault as error:
            error_class = type(error).__name__
            await _runtime_sync_call_async(
                runtime,
                lambda: self._control_plane.finish_retryable_with_incident(
                    runtime.current_lease,
                    error_class=error_class,
                    incident_key=f"incident:job-retry:{lease.job_key}",
                    dedupe_key=f"job-retry:{lease.job_key}",
                    component="structure-normalize",
                    summary="structure-normalize retryable failure",
                    detail={
                        "job_key": lease.job_key,
                        "lease_epoch": lease.lease_epoch,
                        "error_class": error_class,
                    },
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                ),
                terminal=True,
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="retryable")
        except Exception as error:
            error_class = type(error).__name__
            await _runtime_sync_call_async(
                runtime,
                lambda: self._control_plane.finish_retryable_with_incident(
                    runtime.current_lease,
                    error_class=error_class,
                    incident_key=f"incident:job-retry:{lease.job_key}",
                    dedupe_key=f"job-retry:{lease.job_key}",
                    component="structure-normalize",
                    summary="structure-normalize retryable failure",
                    detail={
                        "job_key": lease.job_key,
                        "lease_epoch": lease.lease_epoch,
                        "error_class": error_class,
                    },
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                ),
                terminal=True,
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
        header, rows = parse_structure_shard_bytes(payload, expected_sha256=shard.artifact_digest)
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
        monotonic_clock: Callable[[], float] = monotonic,
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._monotonic_clock = monotonic_clock
        self._heartbeat_interval_seconds = max(0.1, lease_seconds / 3)
        self._stop_requested = Event()

    def request_stop(self) -> None:
        """Stop the synchronous parity pass at its next range boundary."""
        self._stop_requested.set()

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
            profile=runtime_deadline_profile("structure-certify", self._lease_seconds),
            clock=self._now,
        )
        generation_key = lease.job_key.removesuffix(":certify")
        last_heartbeat = self._monotonic_clock()

        def heartbeat_if_due() -> None:
            nonlocal last_heartbeat
            if self._stop_requested.is_set():
                raise ServiceStopRequested("structure-certify service stop requested")
            current_monotonic = self._monotonic_clock()
            if current_monotonic - last_heartbeat < self._heartbeat_interval_seconds:
                return
            runtime.heartbeat()
            last_heartbeat = current_monotonic

        def sync_call(call: Callable[[], Any], *, terminal: bool = False) -> Any:
            profile = runtime.profile
            return _run_bounded_sync_call(
                call,
                heartbeat=None if terminal else heartbeat_if_due,
                heartbeat_interval_seconds=float(profile.heartbeat_seconds),
                attempt_timeout_seconds=runtime.remaining_attempt_seconds(),
                monotonic_clock=self._monotonic_clock,
                terminal=terminal,
            )

        def report_progress(current: int, total: int) -> None:
            sync_call(lambda: runtime.progress(stage="verify-parity", current=current, total=total))

        try:
            self._verify_content_parity(
                generation_key,
                runtime=runtime,
                heartbeat=heartbeat_if_due,
                progress=report_progress,
                sync_call=sync_call,
            )
            heartbeat_if_due()
            payload = sync_call(
                lambda: self._control_plane.structure_manifest_payload(generation_key)
            )
            artifact = StructureManifestArtifact.from_bytes(payload)
            sync_call(lambda: runtime.progress(stage="build-manifest", current=1, total=1))
            heartbeat_if_due()
            sync_call(
                lambda: upload_structure_manifest_artifact(
                    self._object_client, bucket=self._bucket, artifact=artifact
                )
            )
            sync_call(lambda: runtime.progress(stage="upload-manifest", current=1, total=1))
            sync_call(lambda: runtime.progress(stage="commit-certification", current=1, total=1))
            heartbeat_if_due()
            sync_call(
                lambda: self._control_plane.certify_structure_generation(
                    runtime.current_lease,
                    generation_key=generation_key,
                    artifact_key=artifact.key,
                    artifact_digest=artifact.sha256,
                    now=self._now(),
                ),
                terminal=True,
            )
            try:
                sync_call(
                    lambda: self._control_plane.record_job_recovery(
                        runtime.current_lease,
                        component="structure-certify",
                        channels=incident_alert_channels(Settings()),
                        now=self._now(),
                    ),
                    terminal=True,
                )
            except Exception:
                return StructureWorkerResult(
                    job_key=lease.job_key, outcome="certified:recovery-pending"
                )
            return StructureWorkerResult(job_key=lease.job_key, outcome="certified")
        except IncompleteStructureGenerationError:
            sync_call(
                lambda: self._control_plane.finish(
                    runtime.current_lease,
                    state=JobState.WAITING,
                    now=self._now(),
                ),
                terminal=True,
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="waiting")
        except StaleLeaseError:
            raise
        except Exception as error:
            error_class = type(error).__name__
            sync_call(
                lambda: self._control_plane.finish_retryable_with_incident(
                    runtime.current_lease,
                    error_class=error_class,
                    incident_key=f"incident:job-retry:{lease.job_key}",
                    dedupe_key=f"job-retry:{lease.job_key}",
                    component="structure-certify",
                    summary="structure-certify retryable failure",
                    detail={
                        "job_key": lease.job_key,
                        "lease_epoch": lease.lease_epoch,
                        "error_class": error_class,
                    },
                    channels=incident_alert_channels(Settings()),
                    now=self._now(),
                ),
                terminal=True,
            )
            raise

    def _verify_content_parity(
        self,
        generation_key: str,
        *,
        runtime: AttemptRuntime,
        heartbeat: Callable[[], None],
        progress: Callable[[int, int], None],
        sync_call: Callable[..., Any],
    ) -> None:
        ranges = sync_call(
            lambda: self._control_plane.structure_generation_receipts(generation_key)
        )
        source_spec = ranges[0][0]
        heartbeat()
        source_payload = sync_call(
            lambda: _read_object_bytes(
                self._object_client, bucket=self._bucket, key=source_spec.bundle_key
            )
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
                ranges,
                shards,
                generation_key=generation_key,
                runtime=runtime,
                heartbeat=heartbeat,
                progress=progress,
                sync_call=sync_call,
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
            payload = sync_call(
                lambda: _read_object_bytes(
                    self._object_client, bucket=self._bucket, key=receipt.artifact_key
                )
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
        generation_key: str,
        runtime: AttemptRuntime,
        heartbeat: Callable[[], None],
        progress: Callable[[int, int], None],
        sync_call: Callable[..., Any],
    ) -> None:
        by_identity = {(shard.component, shard.ordinal): shard for shard in shards}
        total_ranges = len(ranges)
        prefix_digests = _parity_prefix_digests(generation_key, ranges)
        resume_index = 0
        cursor_prefix = f"runtime-v2:{generation_key}:"
        checkpoints = sync_call(
            lambda: self._control_plane.running_checkpoints(runtime.current_lease.job_key)
        )
        for cursor, digest, artifact_key in checkpoints:
            if not cursor.startswith(cursor_prefix):
                raise StructureWorkerError("structure-parity-checkpoint-cursor")
            try:
                checkpoint_index = int(cursor.removeprefix(cursor_prefix))
            except ValueError as error:
                raise StructureWorkerError("structure-parity-checkpoint-cursor") from error
            if (
                checkpoint_index <= resume_index
                or checkpoint_index > total_ranges
                or digest != prefix_digests[checkpoint_index - 1]
                or artifact_key != str(getattr(ranges[checkpoint_index - 1][1], "artifact_key"))
            ):
                raise StructureWorkerError("structure-parity-checkpoint-proof")
            resume_index = checkpoint_index
        checkpoint_interval = runtime_policy(
            "structure-certify", lease_seconds=runtime.profile.lease_seconds
        ).checkpoint_interval
        for index, (spec, receipt) in enumerate(ranges[resume_index:], start=resume_index + 1):
            heartbeat()
            ordinal = int(spec.range_start.removeprefix("shard:"))
            shard = by_identity.get((spec.component, ordinal))
            if shard is None or spec.range_end != f"shard:{ordinal + 1:08d}":
                raise StructureWorkerError("structure-v3-content-parity-range-identity")
            shard_artifact_key = shard.artifact_key
            shard_artifact_digest = shard.artifact_digest
            source_payload = sync_call(
                lambda: _read_object_bytes(
                    self._object_client, bucket=self._bucket, key=shard_artifact_key
                )
            )
            _header, expected_rows = parse_structure_shard_bytes(
                source_payload, expected_sha256=shard_artifact_digest
            )
            heartbeat()
            payload = sync_call(
                lambda: _read_object_bytes(
                    self._object_client,
                    bucket=self._bucket,
                    key=getattr(receipt, "artifact_key"),
                )
            )
            _range_identity, actual_rows = parse_structure_range_bytes(
                payload, expected_sha256=getattr(receipt, "artifact_digest")
            )
            if actual_rows != expected_rows or len(actual_rows) != getattr(receipt, "record_count"):
                raise StructureWorkerError("structure-v3-content-parity-range-content")
            progress(index, total_ranges)
            if index % checkpoint_interval == 0 or index == total_ranges:

                def persist_checkpoint(
                    checkpoint_index: int = index,
                    checkpoint_receipt: object = receipt,
                ) -> object:
                    return self._control_plane.record_running_checkpoint(
                        runtime.current_lease,
                        checkpoint_cursor=f"{cursor_prefix}{checkpoint_index}",
                        checkpoint_digest=prefix_digests[checkpoint_index - 1],
                        artifact_key=str(getattr(checkpoint_receipt, "artifact_key")),
                        idempotency_key=(
                            "structure-parity-checkpoint:"
                            f"{runtime.current_lease.job_key}:runtime-v2:{checkpoint_index}"
                        ),
                        now=self._now(),
                    )

                sync_call(persist_checkpoint)


def _parity_prefix_digests(
    generation_key: str,
    ranges: Sequence[tuple[StructureRangeSpec, object]],
) -> tuple[str, ...]:
    """Hash immutable receipt metadata into resumable prefix proofs."""
    rolling = hashlib.sha256(f"runtime-v2:{generation_key}".encode()).digest()
    digests: list[str] = []
    for spec, receipt in ranges:
        record = json.dumps(
            {
                "artifact_digest": str(getattr(receipt, "artifact_digest")),
                "artifact_key": str(getattr(receipt, "artifact_key")),
                "component": spec.component,
                "range_digest": spec.range_digest,
                "record_count": int(getattr(receipt, "record_count")),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        rolling = hashlib.sha256(rolling + record).digest()
        digests.append(rolling.hex())
    return tuple(digests)


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
