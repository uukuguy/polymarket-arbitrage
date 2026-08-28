"""Transactional persistence primitives for M1 runtime evidence.

This module intentionally owns only cursor-level SQL.  The control-plane
repository supplies the connection/transaction boundary, while callers can
compose these helpers with a job transition and keep the state, event, and
lease mutation in one commit.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg import Cursor
from psycopg.types.json import Jsonb

from .runtime_deadlines import runtime_deadline_profile
from .runtime_models import (
    RuntimeDeadlineProfile,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeProgress,
)


class RuntimeStoreError(RuntimeError):
    """Base class for runtime persistence contract failures."""


class RuntimeFenceError(RuntimeStoreError):
    """The supplied attempt/worker/epoch is no longer current."""


class RuntimeEventConflict(RuntimeStoreError):
    """An idempotency or sequence key was reused for different evidence."""


class RuntimeProgressConflict(RuntimeStoreError):
    """A progress sequence did not strictly advance the current attempt."""


_EVENT_COLUMNS = (
    "event_id, job_key, attempt_id, lease_epoch, worker_id, event_sequence, kind, stage, "
    "progress_sequence, progress_current, progress_total, detail, occurred_at, idempotency_key"
)


def _row_value(row: object, name: str, position: int) -> Any:
    if isinstance(row, Mapping):
        return row[name]
    return row[position]  # type: ignore[index]


def _require_aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_detail(detail: Mapping[str, object]) -> dict[str, object]:
    """Normalize accepted timestamp detail fields at the storage boundary."""
    normalized = dict(detail)
    for key in ("deadline_at", "next_decision_at"):
        value = normalized.get(key)
        if value is None:
            continue
        if type(value) is not str:
            raise ValueError(f"runtime detail timestamp {key} must be an ISO string")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"runtime detail timestamp {key} is invalid") from exc
        normalized[key] = _require_aware(parsed, key).isoformat()
    return normalized


def _normalized_event(event: RuntimeEvent) -> RuntimeEvent:
    if type(event) is not RuntimeEvent:
        raise TypeError("event must be RuntimeEvent")
    return replace(
        event,
        detail=_utc_detail(event.detail),
        occurred_at=_require_aware(event.occurred_at, "occurred_at"),
    )


def _event_with(event: RuntimeEvent, **changes: object) -> RuntimeEvent:
    """Rebuild an event with a plain detail dict for RuntimeEvent validation."""
    values: dict[str, object] = {
        "job_key": event.job_key,
        "attempt_id": event.attempt_id,
        "lease_epoch": event.lease_epoch,
        "worker_id": event.worker_id,
        "event_sequence": event.event_sequence,
        "kind": event.kind,
        "stage": event.stage,
        "progress": event.progress,
        "detail": dict(event.detail),
        "occurred_at": event.occurred_at,
        "idempotency_key": event.idempotency_key,
    }
    values.update(changes)
    return RuntimeEvent(**values)  # type: ignore[arg-type]


def _event_from_row(row: object) -> RuntimeEvent:
    progress_sequence = _row_value(row, "progress_sequence", 8)
    progress_current = _row_value(row, "progress_current", 9)
    progress_total = _row_value(row, "progress_total", 10)
    progress = None
    if progress_sequence is not None:
        if progress_current is None:
            raise RuntimeStoreError("persisted runtime progress is incomplete")
        progress = RuntimeProgress(
            sequence=int(progress_sequence),
            current=int(progress_current),
            total=None if progress_total is None else int(progress_total),
            stage=str(_row_value(row, "stage", 7)),
        )
    return RuntimeEvent(
        job_key=str(_row_value(row, "job_key", 1)),
        attempt_id=str(_row_value(row, "attempt_id", 2)),
        lease_epoch=int(_row_value(row, "lease_epoch", 3)),
        worker_id=str(_row_value(row, "worker_id", 4)),
        event_sequence=int(_row_value(row, "event_sequence", 5)),
        kind=RuntimeEventKind(str(_row_value(row, "kind", 6))),
        stage=str(_row_value(row, "stage", 7)),
        progress=progress,
        detail=dict(_row_value(row, "detail", 11)),  # type: ignore[arg-type]
        occurred_at=_require_aware(
            _row_value(row, "occurred_at", 12),
            "persisted occurred_at",  # type: ignore[arg-type]
        ),
        idempotency_key=str(_row_value(row, "idempotency_key", 13)),
    )


def _event_content_equal(left: RuntimeEvent, right: RuntimeEvent) -> bool:
    """Compare all immutable event content for an exact idempotent replay."""
    return (
        left.job_key == right.job_key
        and left.attempt_id == right.attempt_id
        and left.lease_epoch == right.lease_epoch
        and left.worker_id == right.worker_id
        and left.event_sequence == right.event_sequence
        and left.kind is right.kind
        and left.stage == right.stage
        and left.progress == right.progress
        and left.detail == right.detail
        and left.occurred_at == right.occurred_at
        and left.idempotency_key == right.idempotency_key
    )


def _current_runtime_state(
    cursor: Cursor[Any],
    *,
    job_key: str,
    attempt_id: str,
    lease_epoch: int,
    worker_id: str,
    for_update: bool = True,
) -> object:
    cursor.execute(
        """
        SELECT job_key, attempt_id, lease_epoch, worker_id, stage,
               started_at, last_heartbeat_at, last_progress_at,
               progress_sequence, progress_current, progress_total,
               lease_deadline_at, heartbeat_deadline_at, progress_deadline_at,
               attempt_deadline_at, recovery_state, updated_at,
               policy_version, profile_lease_seconds,
               profile_heartbeat_seconds, profile_progress_seconds,
               profile_attempt_seconds
        FROM public.m1_job_runtime_state
        WHERE job_key = %s
        """
        + (" FOR UPDATE" if for_update else ""),
        (job_key,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeFenceError(f"runtime state is missing for {job_key}")
    if (
        str(_row_value(row, "attempt_id", 1)) != attempt_id
        or int(_row_value(row, "lease_epoch", 2)) != lease_epoch
        or str(_row_value(row, "worker_id", 3)) != worker_id
    ):
        raise RuntimeFenceError(f"runtime fence is stale for {job_key}")
    return row


def _assert_current_job_lease(
    cursor: Cursor[Any],
    *,
    job_key: str,
    lease_epoch: int,
    worker_id: str,
    now: datetime,
) -> object:
    """Lock and validate the job lease before mutating its runtime projection."""
    cursor.execute(
        """
        SELECT lease_owner, lease_epoch, lease_expires_at, state
        FROM public.m1_jobs
        WHERE job_key = %s
        FOR UPDATE
        """,
        (job_key,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeFenceError(f"job is missing for {job_key}")
    lease_owner = _row_value(row, "lease_owner", 0)
    current_epoch = _row_value(row, "lease_epoch", 1)
    lease_expires_at = _row_value(row, "lease_expires_at", 2)
    state = _row_value(row, "state", 3)
    if (
        state != "leased"
        or str(lease_owner) != worker_id
        or int(current_epoch) != lease_epoch
        or lease_expires_at is None
        or _require_aware(lease_expires_at, "lease_expires_at") <= now  # type: ignore[arg-type]
    ):
        raise RuntimeFenceError(f"job lease is no longer current for {job_key}")
    return row


def _existing_event_by_idempotency(
    cursor: Cursor[Any], idempotency_key: str
) -> RuntimeEvent | None:
    cursor.execute(
        f"SELECT {_EVENT_COLUMNS} FROM public.m1_job_runtime_events WHERE idempotency_key = %s",
        (idempotency_key,),
    )
    row = cursor.fetchone()
    return None if row is None else _event_from_row(row)


def append_runtime_event_cursor(cursor: Cursor[Any], event: RuntimeEvent) -> RuntimeEvent:
    """Append one event, returning the exact persisted row on replay.

    The current runtime fence is checked only for a new event.  This permits a
    crashed caller to replay an already committed idempotency key after a
    replacement worker has taken the lease, without allowing that old worker
    to append anything new.
    """
    normalized = _normalized_event(event)
    existing = _existing_event_by_idempotency(cursor, normalized.idempotency_key)
    if existing is not None:
        if not _event_content_equal(existing, normalized):
            raise RuntimeEventConflict(
                f"runtime idempotency key conflicts: {normalized.idempotency_key!r}"
            )
        return existing

    _assert_current_job_lease(
        cursor,
        job_key=normalized.job_key,
        lease_epoch=normalized.lease_epoch,
        worker_id=normalized.worker_id,
        now=normalized.occurred_at,
    )
    _current_runtime_state(
        cursor,
        job_key=normalized.job_key,
        attempt_id=normalized.attempt_id,
        lease_epoch=normalized.lease_epoch,
        worker_id=normalized.worker_id,
    )
    cursor.execute(
        f"SELECT {_EVENT_COLUMNS} FROM public.m1_job_runtime_events "
        "WHERE attempt_id = %s AND event_sequence = %s",
        (normalized.attempt_id, normalized.event_sequence),
    )
    sequence_row = cursor.fetchone()
    if sequence_row is not None:
        existing_sequence = _event_from_row(sequence_row)
        if not _event_content_equal(existing_sequence, normalized):
            raise RuntimeEventConflict(
                f"runtime event sequence conflicts: {normalized.attempt_id!r}/"
                f"{normalized.event_sequence}"
            )
        return existing_sequence

    progress_sequence = None if normalized.progress is None else normalized.progress.sequence
    progress_current = None if normalized.progress is None else normalized.progress.current
    progress_total = None if normalized.progress is None else normalized.progress.total
    cursor.execute(
        """
        INSERT INTO public.m1_job_runtime_events (
            event_id, job_key, attempt_id, lease_epoch, worker_id,
            event_sequence, kind, stage, progress_sequence, progress_current,
            progress_total, detail, occurred_at, idempotency_key
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(uuid4()),
            normalized.job_key,
            normalized.attempt_id,
            normalized.lease_epoch,
            normalized.worker_id,
            normalized.event_sequence,
            normalized.kind.value,
            normalized.stage,
            progress_sequence,
            progress_current,
            progress_total,
            Jsonb(dict(normalized.detail)),
            normalized.occurred_at,
            normalized.idempotency_key,
        ),
    )
    return normalized


def start_runtime_attempt_cursor(
    cursor: Cursor[Any],
    *,
    job_key: str,
    job_type: str,
    attempt_id: str,
    lease_epoch: int,
    worker_id: str,
    started_at: datetime,
    lease_deadline_at: datetime,
    lease_seconds: int | None = None,
    profile: RuntimeDeadlineProfile | None = None,
    stage: str = "started",
) -> RuntimeEvent:
    """Create/replace the current runtime projection and append ``job.started``."""
    if not all(value.strip() for value in (job_key, job_type, attempt_id, worker_id, stage)):
        raise ValueError("runtime attempt identities must be non-empty")
    if lease_epoch < 1:
        raise ValueError("lease_epoch must be positive")
    started = _require_aware(started_at, "started_at")
    lease_deadline = _require_aware(lease_deadline_at, "lease_deadline_at")
    if lease_seconds is None:
        lease_seconds = max(1, int((lease_deadline - started).total_seconds()))
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    selected_profile = (
        runtime_deadline_profile(job_type, lease_seconds) if profile is None else profile
    )
    if type(selected_profile) is not RuntimeDeadlineProfile:
        raise TypeError("profile must be RuntimeDeadlineProfile")
    heartbeat_deadline = min(
        lease_deadline, started + timedelta(seconds=selected_profile.heartbeat_seconds)
    )
    progress_deadline = min(
        started + timedelta(seconds=selected_profile.progress_seconds),
        started + timedelta(seconds=selected_profile.attempt_seconds),
    )
    attempt_deadline = started + timedelta(seconds=selected_profile.attempt_seconds)
    cursor.execute(
        """
        INSERT INTO public.m1_job_runtime_state (
            job_key, attempt_id, lease_epoch, worker_id, stage,
            started_at, last_heartbeat_at, last_progress_at,
            progress_sequence, progress_current, progress_total,
            lease_deadline_at, heartbeat_deadline_at, progress_deadline_at,
            attempt_deadline_at, recovery_state, updated_at,
            policy_version, profile_lease_seconds,
            profile_heartbeat_seconds, profile_progress_seconds,
            profile_attempt_seconds
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0, NULL, %s, %s, %s, %s,
                  'active', %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_key) DO UPDATE SET
            attempt_id = EXCLUDED.attempt_id,
            lease_epoch = EXCLUDED.lease_epoch,
            worker_id = EXCLUDED.worker_id,
            stage = EXCLUDED.stage,
            started_at = EXCLUDED.started_at,
            last_heartbeat_at = EXCLUDED.last_heartbeat_at,
            last_progress_at = EXCLUDED.last_progress_at,
            progress_sequence = EXCLUDED.progress_sequence,
            progress_current = EXCLUDED.progress_current,
            progress_total = EXCLUDED.progress_total,
            lease_deadline_at = EXCLUDED.lease_deadline_at,
            heartbeat_deadline_at = EXCLUDED.heartbeat_deadline_at,
            progress_deadline_at = EXCLUDED.progress_deadline_at,
            attempt_deadline_at = EXCLUDED.attempt_deadline_at,
            recovery_state = EXCLUDED.recovery_state,
            updated_at = EXCLUDED.updated_at,
            policy_version = EXCLUDED.policy_version,
            profile_lease_seconds = EXCLUDED.profile_lease_seconds,
            profile_heartbeat_seconds = EXCLUDED.profile_heartbeat_seconds,
            profile_progress_seconds = EXCLUDED.profile_progress_seconds,
            profile_attempt_seconds = EXCLUDED.profile_attempt_seconds
        """,
        (
            job_key,
            attempt_id,
            lease_epoch,
            worker_id,
            stage,
            started,
            started,
            started,
            lease_deadline,
            heartbeat_deadline,
            progress_deadline,
            attempt_deadline,
            started,
            selected_profile.policy_version,
            selected_profile.lease_seconds,
            selected_profile.heartbeat_seconds,
            selected_profile.progress_seconds,
            selected_profile.attempt_seconds,
        ),
    )
    return append_runtime_event_cursor(
        cursor,
        RuntimeEvent(
            job_key=job_key,
            attempt_id=attempt_id,
            lease_epoch=lease_epoch,
            worker_id=worker_id,
            event_sequence=1,
            kind=RuntimeEventKind.STARTED,
            stage=stage,
            progress=None,
            detail={
                "component": "control-plane",
                "job_type": job_type,
                "recovery_policy": "retry-job",
            },
            occurred_at=started,
            idempotency_key=f"runtime:{attempt_id}:started",
        ),
    )


def update_runtime_heartbeat_cursor(
    cursor: Cursor[Any],
    *,
    job_key: str,
    attempt_id: str,
    lease_epoch: int,
    worker_id: str,
    now: datetime,
    lease_seconds: int,
) -> dict[str, object]:
    """Renew job and runtime liveness under one current fence."""
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    observed_at = _require_aware(now, "now")
    _assert_current_job_lease(
        cursor,
        job_key=job_key,
        lease_epoch=lease_epoch,
        worker_id=worker_id,
        now=observed_at,
    )
    state = _current_runtime_state(
        cursor,
        job_key=job_key,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        worker_id=worker_id,
    )
    attempt_deadline = _require_aware(
        _row_value(state, "attempt_deadline_at", 14),
        "attempt_deadline_at",  # type: ignore[arg-type]
    )
    if observed_at >= attempt_deadline:
        raise RuntimeFenceError(f"attempt deadline has elapsed for {job_key}")
    effective_lease_deadline = min(observed_at + timedelta(seconds=lease_seconds), attempt_deadline)
    heartbeat_seconds = int(_row_value(state, "profile_heartbeat_seconds", 19))
    heartbeat_deadline = min(
        observed_at + timedelta(seconds=heartbeat_seconds), effective_lease_deadline
    )
    cursor.execute(
        """
        UPDATE public.m1_jobs
        SET lease_expires_at = %s, updated_at = %s
        WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
          AND state = 'leased' AND lease_expires_at > %s
        """,
        (
            effective_lease_deadline,
            observed_at,
            job_key,
            worker_id,
            lease_epoch,
            observed_at,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeFenceError(f"job lease is no longer current for {job_key}")
    cursor.execute(
        """
        UPDATE public.m1_job_runtime_state
        SET last_heartbeat_at = %s, heartbeat_deadline_at = %s,
            lease_deadline_at = %s, updated_at = %s
        WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s AND worker_id = %s
        """,
        (
            observed_at,
            heartbeat_deadline,
            effective_lease_deadline,
            observed_at,
            job_key,
            attempt_id,
            lease_epoch,
            worker_id,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeFenceError(f"runtime lease is no longer current for {job_key}")
    return {
        "job_key": job_key,
        "attempt_id": attempt_id,
        "lease_epoch": lease_epoch,
        "worker_id": worker_id,
        "lease_deadline_at": effective_lease_deadline,
        "heartbeat_deadline_at": heartbeat_deadline,
        "last_heartbeat_at": observed_at,
        "last_progress_at": _row_value(state, "last_progress_at", 7),
        "progress_sequence": int(_row_value(state, "progress_sequence", 8)),
    }


def update_runtime_progress_cursor(
    cursor: Cursor[Any],
    event: RuntimeEvent,
) -> RuntimeEvent:
    """Advance a fenced progress projection and append its lifecycle event."""
    normalized = _normalized_event(event)
    if normalized.progress is None:
        raise ValueError("runtime progress event requires progress")
    progress = normalized.progress
    existing = _existing_event_by_idempotency(cursor, normalized.idempotency_key)
    if existing is not None:
        normalized = _event_with(normalized, event_sequence=existing.event_sequence)
        if not _event_content_equal(existing, normalized):
            raise RuntimeEventConflict(
                f"runtime idempotency key conflicts: {normalized.idempotency_key!r}"
            )
        return existing
    observed_at = _require_aware(normalized.occurred_at, "occurred_at")
    _assert_current_job_lease(
        cursor,
        job_key=normalized.job_key,
        lease_epoch=normalized.lease_epoch,
        worker_id=normalized.worker_id,
        now=observed_at,
    )
    state = _current_runtime_state(
        cursor,
        job_key=normalized.job_key,
        attempt_id=normalized.attempt_id,
        lease_epoch=normalized.lease_epoch,
        worker_id=normalized.worker_id,
    )
    previous_sequence = int(_row_value(state, "progress_sequence", 8))
    if progress.sequence <= previous_sequence:
        raise RuntimeProgressConflict("runtime progress sequence must increase strictly")
    cursor.execute(
        "SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence "
        "FROM public.m1_job_runtime_events "
        "WHERE attempt_id = %s",
        (normalized.attempt_id,),
    )
    sequence_row = cursor.fetchone()
    if sequence_row is None:
        raise RuntimeStoreError("runtime event sequence query returned no row")
    next_event_sequence = int(_row_value(sequence_row, "next_sequence", 0))
    normalized = _event_with(normalized, event_sequence=next_event_sequence)
    attempt_deadline = _require_aware(
        _row_value(state, "attempt_deadline_at", 14),
        "attempt_deadline_at",  # type: ignore[arg-type]
    )
    if observed_at >= attempt_deadline:
        raise RuntimeFenceError(f"attempt deadline has elapsed for {normalized.job_key}")
    configured_progress_window = int(_row_value(state, "profile_progress_seconds", 20))
    progress_deadline = min(
        observed_at + timedelta(seconds=configured_progress_window),
        attempt_deadline,
    )
    cursor.execute(
        """
        UPDATE public.m1_job_runtime_state
        SET stage = %s, last_progress_at = %s,
            progress_sequence = %s, progress_current = %s, progress_total = %s,
            progress_deadline_at = %s, updated_at = %s
        WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s AND worker_id = %s
        """,
        (
            normalized.stage,
            observed_at,
            progress.sequence,
            progress.current,
            progress.total,
            progress_deadline,
            observed_at,
            normalized.job_key,
            normalized.attempt_id,
            normalized.lease_epoch,
            normalized.worker_id,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeFenceError(f"runtime lease is no longer current for {normalized.job_key}")
    return append_runtime_event_cursor(cursor, normalized)


__all__ = [
    "RuntimeEventConflict",
    "RuntimeFenceError",
    "RuntimeProgressConflict",
    "RuntimeStoreError",
    "append_runtime_event_cursor",
    "runtime_deadline_profile",
    "start_runtime_attempt_cursor",
    "update_runtime_heartbeat_cursor",
    "update_runtime_progress_cursor",
]
