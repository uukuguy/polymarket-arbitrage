"""Fenced Postgres persistence for M1 runtime recovery actions."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .recovery_models import RecoveryActionType, RecoveryDecision
from .runtime_models import RuntimeEvent, RuntimeEventKind
from .runtime_store import RuntimeEventConflict, RuntimeFenceError, append_runtime_event_cursor

ConnectionFactory = Callable[[], psycopg.Connection[Any]]

_RECOVERY_STATEMENT_TIMEOUT_MS = 2_000
_RECOVERY_LOCK_TIMEOUT_MS = 1_000
_ACTION_COLUMNS = (
    "action_id, controller_id, controller_owner_id, incident_key, target_type, target_id, "
    "action_type, expected_controller_epoch, expected_attempt_id, expected_lease_epoch, "
    "requested_at, started_at, finished_at, state, result_code, next_allowed_at, "
    "worker_id, worker_epoch, worker_lease_expires_at, detail, idempotency_key"
)
_CLOSED_RESULT_CODES = frozenset(
    {"succeeded", "failed", "stale-noop", "disabled-action"}
)


class RecoveryStoreError(RuntimeError):
    """Base error for recovery-store contract failures."""


class RecoveryActionConflict(RecoveryStoreError):
    """An idempotency key or active target was reused for conflicting content."""


@dataclass(frozen=True, slots=True)
class RuntimeControllerLease:
    controller_id: str
    owner_id: str
    lease_epoch: int
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class RecoveryActionRecord:
    action_id: str
    controller_id: str
    controller_owner_id: str
    incident_key: str | None
    target_type: str
    target_id: str
    action_type: str
    expected_controller_epoch: int
    expected_attempt_id: str
    expected_lease_epoch: int
    requested_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    state: str
    result_code: str | None
    next_allowed_at: datetime
    worker_id: str | None
    worker_epoch: int
    worker_lease_expires_at: datetime | None
    detail: dict[str, object]
    idempotency_key: str


def _require_aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _require_nonempty(**values: str) -> None:
    for name, value in values.items():
        if type(value) is not str or not value.strip():
            raise ValueError(f"{name} must be non-empty")


def _bounded_detail(detail: Mapping[str, object] | None) -> dict[str, object]:
    if detail is None:
        return {}
    if not isinstance(detail, Mapping):
        raise TypeError("detail must be a mapping")
    bounded = dict(detail)
    if len(bounded) > 20:
        raise ValueError("detail must be bounded")
    for key, value in bounded.items():
        if type(key) is not str:
            raise ValueError("detail keys must be strings")
        if type(value) in (dict, list, tuple):
            raise ValueError("detail values must be flat JSON scalars")
    encoded = json.dumps(bounded, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) > 4096:
        raise ValueError("detail must be at most 4096 bytes")
    return bounded


def _set_recovery_timeouts(cursor: psycopg.Cursor[Any]) -> None:
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(
            sql.Literal(f"{_RECOVERY_STATEMENT_TIMEOUT_MS}ms")
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(
            sql.Literal(f"{_RECOVERY_LOCK_TIMEOUT_MS}ms")
        )
    )


def _row_value(row: object, name: str, position: int) -> Any:
    if isinstance(row, Mapping):
        return row[name]
    return row[position]  # type: ignore[index]


def _action_from_row(row: object) -> RecoveryActionRecord:
    incident_key = _row_value(row, "incident_key", 3)
    result_code = _row_value(row, "result_code", 14)
    worker_id = _row_value(row, "worker_id", 16)
    return RecoveryActionRecord(
        action_id=str(_row_value(row, "action_id", 0)),
        controller_id=str(_row_value(row, "controller_id", 1)),
        controller_owner_id=str(_row_value(row, "controller_owner_id", 2)),
        incident_key=None if incident_key is None else str(incident_key),
        target_type=str(_row_value(row, "target_type", 4)),
        target_id=str(_row_value(row, "target_id", 5)),
        action_type=str(_row_value(row, "action_type", 6)),
        expected_controller_epoch=int(_row_value(row, "expected_controller_epoch", 7)),
        expected_attempt_id=str(_row_value(row, "expected_attempt_id", 8)),
        expected_lease_epoch=int(_row_value(row, "expected_lease_epoch", 9)),
        requested_at=_require_aware(_row_value(row, "requested_at", 10), "requested_at"),
        started_at=(
            None
            if _row_value(row, "started_at", 11) is None
            else _require_aware(_row_value(row, "started_at", 11), "started_at")
        ),
        finished_at=(
            None
            if _row_value(row, "finished_at", 12) is None
            else _require_aware(_row_value(row, "finished_at", 12), "finished_at")
        ),
        state=str(_row_value(row, "state", 13)),
        result_code=None if result_code is None else str(result_code),
        next_allowed_at=_require_aware(
            _row_value(row, "next_allowed_at", 15), "next_allowed_at"
        ),
        worker_id=None if worker_id is None else str(worker_id),
        worker_epoch=int(_row_value(row, "worker_epoch", 17)),
        worker_lease_expires_at=(
            None
            if _row_value(row, "worker_lease_expires_at", 18) is None
            else _require_aware(
                _row_value(row, "worker_lease_expires_at", 18), "worker_lease_expires_at"
            )
        ),
        detail=dict(_row_value(row, "detail", 19)),  # type: ignore[arg-type]
        idempotency_key=str(_row_value(row, "idempotency_key", 20)),
    )


def _fetch_action_by_idempotency(
    cursor: psycopg.Cursor[Any], idempotency_key: str
) -> RecoveryActionRecord | None:
    cursor.execute(
        f"SELECT {_ACTION_COLUMNS} FROM m1_recovery_actions WHERE idempotency_key = %s",
        (idempotency_key,),
    )
    row = cursor.fetchone()
    return None if row is None else _action_from_row(row)


def _fetch_action_by_id(
    cursor: psycopg.Cursor[Any], action_id: str, *, for_update: bool = False
) -> RecoveryActionRecord | None:
    cursor.execute(
        f"SELECT {_ACTION_COLUMNS} FROM m1_recovery_actions WHERE action_id = %s"
        + (" FOR UPDATE" if for_update else ""),
        (action_id,),
    )
    row = cursor.fetchone()
    return None if row is None else _action_from_row(row)


def _canonical_idempotency(
    *,
    controller: RuntimeControllerLease,
    incident_key: str,
    target_type: str,
    target_id: str,
    action_type: RecoveryActionType,
    expected_attempt_id: str,
    expected_lease_epoch: int,
) -> str:
    payload = {
        "action_type": action_type.value,
        "controller_id": controller.controller_id,
        "controller_epoch": controller.lease_epoch,
        "expected_attempt_id": expected_attempt_id,
        "expected_lease_epoch": expected_lease_epoch,
        "incident_key": incident_key,
        "target_id": target_id,
        "target_type": target_type,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"recovery-action:{sha256(encoded).hexdigest()}"


def _same_schedule(
    existing: RecoveryActionRecord,
    *,
    controller: RuntimeControllerLease,
    incident_key: str | None,
    target_type: str,
    target_id: str,
    action_type: RecoveryActionType,
    expected_attempt_id: str,
    expected_lease_epoch: int,
    state: str,
    result_code: str | None,
    next_allowed_at: datetime,
    detail: Mapping[str, object],
) -> bool:
    return (
        existing.controller_id == controller.controller_id
        and existing.controller_owner_id == controller.owner_id
        and existing.incident_key == incident_key
        and existing.target_type == target_type
        and existing.target_id == target_id
        and existing.action_type == action_type.value
        and existing.expected_controller_epoch == controller.lease_epoch
        and existing.expected_attempt_id == expected_attempt_id
        and existing.expected_lease_epoch == expected_lease_epoch
        and existing.state == state
        and existing.result_code == result_code
        and existing.next_allowed_at == next_allowed_at
        and existing.detail == dict(detail)
    )


def _insert_action(
    cursor: psycopg.Cursor[Any],
    *,
    action_id: str,
    controller: RuntimeControllerLease,
    incident_key: str | None,
    target_type: str,
    target_id: str,
    action_type: RecoveryActionType,
    expected_attempt_id: str,
    expected_lease_epoch: int,
    requested_at: datetime,
    state: str,
    result_code: str | None,
    next_allowed_at: datetime,
    detail: Mapping[str, object],
    idempotency_key: str,
) -> RecoveryActionRecord:
    try:
        cursor.execute(
            """
            INSERT INTO m1_recovery_actions (
                action_id, controller_id, controller_owner_id, incident_key,
                target_type, target_id, action_type, expected_controller_epoch,
                expected_attempt_id, expected_lease_epoch, requested_at, state,
                result_code, next_allowed_at, finished_at, detail, idempotency_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      CASE WHEN %s = 'completed' THEN %s ELSE NULL END, %s, %s)
            """,
            (
                action_id,
                controller.controller_id,
                controller.owner_id,
                incident_key,
                target_type,
                target_id,
                action_type.value,
                controller.lease_epoch,
                expected_attempt_id,
                expected_lease_epoch,
                requested_at,
                state,
                result_code,
                next_allowed_at,
                state,
                requested_at,
                Jsonb(dict(detail)),
                idempotency_key,
            ),
        )
    except psycopg.errors.UniqueViolation as error:
        raise RecoveryActionConflict("active recovery action conflicts") from error
    inserted = _fetch_action_by_id(cursor, action_id)
    if inserted is None:
        raise RecoveryStoreError("recovery action insert returned no row")
    return inserted


def _current_controller(
    cursor: psycopg.Cursor[Any],
    controller: RuntimeControllerLease,
    *,
    now: datetime,
) -> bool:
    cursor.execute(
        """
        SELECT owner_id, lease_epoch, lease_expires_at
        FROM m1_runtime_controller_leases
        WHERE controller_id = %s
        FOR UPDATE
        """,
        (controller.controller_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return False
    return (
        str(_row_value(row, "owner_id", 0)) == controller.owner_id
        and int(_row_value(row, "lease_epoch", 1)) == controller.lease_epoch
        and _require_aware(_row_value(row, "lease_expires_at", 2), "lease_expires_at")
        > now
    )


def claim_controller(
    connection_factory: ConnectionFactory,
    *,
    controller_id: str,
    owner_id: str,
    lease_seconds: int,
    now: datetime,
) -> RuntimeControllerLease:
    """Claim the singleton reconciler lease and advance its epoch monotonically."""
    _require_nonempty(controller_id=controller_id, owner_id=owner_id)
    observed_at = _require_aware(now, "now")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    lease_expires_at = observed_at + timedelta(seconds=lease_seconds)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        cursor.execute(
            """
            INSERT INTO m1_runtime_controller_leases (
                controller_id, owner_id, lease_epoch, lease_expires_at, claimed_at, updated_at
            ) VALUES (%s, %s, 1, %s, %s, %s)
            ON CONFLICT (controller_id) DO UPDATE
            SET owner_id = EXCLUDED.owner_id,
                lease_epoch = m1_runtime_controller_leases.lease_epoch + 1,
                lease_expires_at = EXCLUDED.lease_expires_at,
                claimed_at = EXCLUDED.claimed_at,
                updated_at = EXCLUDED.updated_at
            RETURNING controller_id, owner_id, lease_epoch, lease_expires_at
            """,
            (controller_id, owner_id, lease_expires_at, observed_at, observed_at),
        )
        row = cursor.fetchone()
        if row is None:
            raise RecoveryStoreError("controller claim returned no row")
        return RuntimeControllerLease(
            controller_id=str(row["controller_id"]),
            owner_id=str(row["owner_id"]),
            lease_epoch=int(row["lease_epoch"]),
            lease_expires_at=_require_aware(row["lease_expires_at"], "lease_expires_at"),
        )


def schedule_action(
    connection_factory: ConnectionFactory,
    *,
    controller: RuntimeControllerLease,
    decision: RecoveryDecision,
    incident_key: str,
    component: str,
    target_type: str,
    target_id: str,
    expected_attempt_id: str,
    expected_lease_epoch: int,
    recovery_budget_remaining: int,
    cooldown_seconds: int,
    channels: Sequence[str],
    now: datetime,
    detail: Mapping[str, object] | None = None,
) -> RecoveryActionRecord:
    """Schedule one fenced action or persist a durable completed stale-noop."""
    if type(controller) is not RuntimeControllerLease:
        raise TypeError("controller must be RuntimeControllerLease")
    if type(decision) is not RecoveryDecision:
        raise TypeError("decision must be RecoveryDecision")
    if decision.action is None:
        raise ValueError("decision must carry an automatic action")
    _require_nonempty(
        incident_key=incident_key,
        component=component,
        target_type=target_type,
        target_id=target_id,
        expected_attempt_id=expected_attempt_id,
    )
    observed_at = _require_aware(now, "now")
    if type(recovery_budget_remaining) is not int or recovery_budget_remaining < 0:
        raise ValueError("recovery_budget_remaining must be an exact non-negative int")
    if cooldown_seconds < 0:
        raise ValueError("cooldown_seconds must be non-negative")
    if not channels or any(not channel.strip() for channel in channels):
        raise ValueError("channels must contain non-empty values")
    if len(set(channels)) != len(channels):
        raise ValueError("channels must be unique")
    normalized_detail = _bounded_detail(detail)
    next_allowed_at = observed_at + timedelta(seconds=cooldown_seconds)
    idempotency_key = _canonical_idempotency(
        controller=controller,
        incident_key=incident_key,
        target_type=target_type,
        target_id=target_id,
        action_type=decision.action,
        expected_attempt_id=expected_attempt_id,
        expected_lease_epoch=expected_lease_epoch,
    )
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        existing = _fetch_action_by_idempotency(cursor, idempotency_key)
        if existing is not None:
            replay_incident_key = (
                None
                if existing.result_code in {"stale-noop", "disabled-action"}
                else incident_key
            )
            if not _same_schedule(
                existing,
                controller=controller,
                incident_key=replay_incident_key,
                target_type=target_type,
                target_id=target_id,
                action_type=decision.action,
                expected_attempt_id=expected_attempt_id,
                expected_lease_epoch=expected_lease_epoch,
                state=existing.state,
                result_code=existing.result_code,
                next_allowed_at=next_allowed_at,
                detail=normalized_detail,
            ):
                raise RecoveryActionConflict("recovery action idempotency conflicts")
            return existing

        stale_or_disabled = None
        if not _current_controller(cursor, controller, now=observed_at):
            stale_or_disabled = "stale-noop"

        cursor.execute(
            """
            SELECT attempt_id, lease_epoch, worker_id, stage, progress_sequence,
                   progress_current, progress_total
            FROM m1_job_runtime_state
            WHERE job_key = %s
            FOR UPDATE
            """,
            (target_id,),
        )
        runtime = cursor.fetchone()
        if (
            runtime is None
            or str(runtime["attempt_id"]) != expected_attempt_id
            or int(runtime["lease_epoch"]) != expected_lease_epoch
        ):
            stale_or_disabled = "stale-noop"
        elif recovery_budget_remaining == 0:
            stale_or_disabled = "disabled-action"
        else:
            cursor.execute(
                """
                SELECT next_allowed_at FROM m1_recovery_actions
                WHERE target_type = %s AND target_id = %s AND state = 'completed'
                  AND result_code IN ('succeeded', 'failed', 'disabled-action')
                ORDER BY next_allowed_at DESC
                LIMIT 1
                """,
                (target_type, target_id),
            )
            cooldown = cursor.fetchone()
            if cooldown is not None and _require_aware(
                cooldown["next_allowed_at"], "next_allowed_at"
            ) > observed_at:
                stale_or_disabled = "disabled-action"

        if stale_or_disabled is not None:
            return _insert_action(
                cursor,
                action_id=str(uuid4()),
                controller=controller,
                incident_key=None,
                target_type=target_type,
                target_id=target_id,
                action_type=decision.action,
                expected_attempt_id=expected_attempt_id,
                expected_lease_epoch=expected_lease_epoch,
                requested_at=observed_at,
                state="completed",
                result_code=stale_or_disabled,
                next_allowed_at=next_allowed_at,
                detail=normalized_detail,
                idempotency_key=idempotency_key,
            )

        assert runtime is not None
        cursor.execute(
            """
            INSERT INTO m1_incidents (
                incident_key, dedupe_key, component, severity, state, summary,
                opened_at, updated_at
            ) VALUES (%s, %s, %s, %s, 'open', %s, %s, %s)
            ON CONFLICT (dedupe_key) DO UPDATE
            SET severity = EXCLUDED.severity, summary = EXCLUDED.summary,
                updated_at = EXCLUDED.updated_at
            RETURNING incident_key
            """,
            (
                incident_key,
                f"recovery:{target_type}:{target_id}",
                component,
                decision.incident_severity,
                f"{component} recovery started",
                observed_at,
                observed_at,
            ),
        )
        incident = cursor.fetchone()
        if incident is None or str(incident["incident_key"]) != incident_key:
            raise RecoveryActionConflict("incident identity conflicts")

        event_id = str(uuid4())
        incident_event_idempotency = f"{idempotency_key}:incident-event"
        cursor.execute(
            """
            INSERT INTO m1_incident_events (
                incident_event_id, incident_key, kind, detail, idempotency_key, occurred_at
            ) VALUES (%s, %s, 'recovery-started', %s, %s, %s)
            """,
            (
                event_id,
                incident_key,
                Jsonb(
                    {
                        "action_type": decision.action.value,
                        "component": component,
                        "expected_controller_epoch": controller.lease_epoch,
                        "expected_lease_epoch": expected_lease_epoch,
                        "job_key": target_id,
                        "qualification_breaking": decision.qualification_breaking,
                        "reason_code": decision.reason_code,
                    }
                ),
                incident_event_idempotency,
                observed_at,
            ),
        )
        for channel in channels:
            cursor.execute(
                """
                INSERT INTO m1_alert_outbox (
                    outbox_id, incident_event_id, channel, payload, state,
                    next_attempt_at, created_at
                ) VALUES (%s, %s, %s, %s, 'pending', %s, %s)
                """,
                (
                    str(uuid4()),
                    event_id,
                    channel,
                    Jsonb({"incident_key": incident_key, "kind": "recovery-started"}),
                    observed_at,
                    observed_at,
                ),
            )

        cursor.execute(
            """
            SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
            FROM m1_job_runtime_events
            WHERE attempt_id = %s
            """,
            (expected_attempt_id,),
        )
        sequence = cursor.fetchone()
        if sequence is None:
            raise RecoveryStoreError("runtime event sequence query returned no row")
        try:
            append_runtime_event_cursor(
                cursor,
                RuntimeEvent(
                    job_key=target_id,
                    attempt_id=expected_attempt_id,
                    lease_epoch=expected_lease_epoch,
                    worker_id=str(runtime["worker_id"]),
                    event_sequence=int(sequence["next_sequence"]),
                    kind=RuntimeEventKind.RECOVERY_STARTED,
                    stage=str(runtime["stage"]),
                    progress=None,
                    detail={
                        "component": component,
                        "reason_code": "timeout",
                        "recovery_policy": "retry-job",
                        "retry_count": 0,
                    },
                    occurred_at=observed_at,
                    idempotency_key=f"{idempotency_key}:runtime-event",
                ),
            )
        except (RuntimeEventConflict, RuntimeFenceError) as error:
            raise RecoveryActionConflict("runtime recovery-started event conflicts") from error
        cursor.execute(
            """
            UPDATE m1_job_runtime_state
            SET recovery_state = 'recovering', updated_at = %s
            WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s
            """,
            (observed_at, target_id, expected_attempt_id, expected_lease_epoch),
        )
        if cursor.rowcount != 1:
            raise RecoveryActionConflict("runtime recovery state changed during scheduling")
        return _insert_action(
            cursor,
            action_id=str(uuid4()),
            controller=controller,
            incident_key=incident_key,
            target_type=target_type,
            target_id=target_id,
            action_type=decision.action,
            expected_attempt_id=expected_attempt_id,
            expected_lease_epoch=expected_lease_epoch,
            requested_at=observed_at,
            state="pending",
            result_code=None,
            next_allowed_at=next_allowed_at,
            detail=normalized_detail,
            idempotency_key=idempotency_key,
        )


def claim_action(
    connection_factory: ConnectionFactory,
    *,
    worker_id: str,
    controller: RuntimeControllerLease,
    lease_seconds: int,
    now: datetime,
) -> RecoveryActionRecord | None:
    """Claim one pending recovery action using SKIP LOCKED and controller fencing."""
    _require_nonempty(worker_id=worker_id)
    observed_at = _require_aware(now, "now")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    expires_at = observed_at + timedelta(seconds=lease_seconds)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        if not _current_controller(cursor, controller, now=observed_at):
            return None
        cursor.execute(
            f"""
            SELECT {_ACTION_COLUMNS}
            FROM m1_recovery_actions
            WHERE state = 'pending'
              AND expected_controller_epoch = %s
            ORDER BY requested_at, action_id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """,
            (controller.lease_epoch,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        action = _action_from_row(row)
        worker_epoch = action.worker_epoch + 1
        cursor.execute(
            """
            UPDATE m1_recovery_actions
            SET state = 'running', started_at = %s, worker_id = %s,
                worker_epoch = %s, worker_lease_expires_at = %s
            WHERE action_id = %s AND state = 'pending'
            """,
            (observed_at, worker_id, worker_epoch, expires_at, action.action_id),
        )
        if cursor.rowcount != 1:
            return None
        return _fetch_action_by_id(cursor, action.action_id)


def finish_action(
    connection_factory: ConnectionFactory,
    *,
    action_id: str,
    worker_id: str,
    worker_epoch: int,
    result_code: str,
    now: datetime,
    detail: Mapping[str, object] | None = None,
) -> RecoveryActionRecord:
    """Close a running recovery action under its worker/epoch fence."""
    _require_nonempty(action_id=action_id, worker_id=worker_id, result_code=result_code)
    if worker_epoch <= 0:
        raise ValueError("worker_epoch must be positive")
    if result_code not in _CLOSED_RESULT_CODES:
        raise ValueError("result_code is not in the recovery action contract")
    observed_at = _require_aware(now, "now")
    extra_detail = _bounded_detail(detail)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        action = _fetch_action_by_id(cursor, action_id, for_update=True)
        if action is None:
            raise RecoveryStoreError("recovery action is missing")
        if action.state == "completed":
            return action
        if action.worker_id != worker_id or action.worker_epoch != worker_epoch:
            return action
        merged_detail = {**action.detail, **extra_detail}
        _bounded_detail(merged_detail)
        cursor.execute(
            """
            UPDATE m1_recovery_actions
            SET state = 'completed', result_code = %s, finished_at = %s,
                detail = %s
            WHERE action_id = %s AND state = 'running'
              AND worker_id = %s AND worker_epoch = %s
            """,
            (
                result_code,
                observed_at,
                Jsonb(merged_detail),
                action_id,
                worker_id,
                worker_epoch,
            ),
        )
        if cursor.rowcount != 1:
            latest = _fetch_action_by_id(cursor, action_id)
            if latest is None:
                raise RecoveryStoreError("recovery action disappeared")
            return latest
        finished = _fetch_action_by_id(cursor, action_id)
        if finished is None:
            raise RecoveryStoreError("recovery action disappeared")
        return finished


__all__ = [
    "RecoveryActionConflict",
    "RecoveryActionRecord",
    "RecoveryStoreError",
    "RuntimeControllerLease",
    "claim_action",
    "claim_controller",
    "finish_action",
    "schedule_action",
]
