"""Fenced Postgres persistence for M1 runtime recovery actions.

Budget boundary: Task 2 has no production reset policy, so budgets are
monotonic per ``(controller_id, target_type, target_id)``.  A controller
re-claim advances its lease epoch but never resets an existing target budget.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, cast
from uuid import uuid4

import psycopg
from psycopg import Cursor, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .recovery_models import (
    RecoveryActionType,
    RecoveryBudget,
    RecoveryDecision,
    RecoveryFailureClass,
    RecoveryRuntimeState,
)
from .recovery_records import (
    _ACTION_COLUMNS,
    BudgetState,
    RecoveryActionRecord,
    RuntimeControllerLease,
    RuntimeFence,
    action_from_row,
)
from .runtime_models import RuntimeDeadlineProfile, RuntimeEventKind

ConnectionFactory = Callable[[], psycopg.Connection[Any]]

_RECOVERY_STATEMENT_TIMEOUT_MS = 2_000
_RECOVERY_LOCK_TIMEOUT_MS = 1_000
_CLOSED_RESULT_CODES = frozenset(
    {"succeeded", "failed", "stale-noop", "disabled-action"}
)
RecoveryActionCallback = Callable[[Cursor[Any], RecoveryActionRecord], object]


class RecoveryStoreError(RuntimeError):
    """Base error for recovery-store contract failures."""


class RecoveryActionConflict(RecoveryStoreError):
    """An idempotency key or active target was reused for conflicting content."""


@dataclass(frozen=True, slots=True)
class RuntimeReconcileCandidate:
    """Bounded read projection consumed by one reconciler turn.

    The candidate deliberately contains facts, not a precomputed decision.  A
    caller must pass ``runtime_state`` through :class:`RuntimeReconciler` so
    the decision remains deterministic and side-effect free.
    """

    runtime_state: RecoveryRuntimeState
    job_type: str
    job_state: str
    worker_id: str
    target_type: str
    target_id: str
    component: str
    incident_key: str
    channels: tuple[str, ...]
    cooldown_seconds: int


_RUNTIME_COMPONENTS = frozenset(
    {
        "opportunity-certify",
        "quote-admit",
        "quote-batch",
        "quote-certify",
        "structure-certify",
        "structure-fetch",
        "structure-materialize",
        "structure-normalize",
    }
)
_RUNTIME_FAILURE_CLASS_MAP: tuple[tuple[str, RecoveryFailureClass], ...] = (
    ("integrity", RecoveryFailureClass.INTEGRITY),
    ("authentication", RecoveryFailureClass.AUTHENTICATION),
    ("credential", RecoveryFailureClass.CREDENTIAL),
    ("schema", RecoveryFailureClass.SCHEMA),
    ("capacity", RecoveryFailureClass.CAPACITY),
)


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
    encoded = json.dumps(
        bounded,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    if len(encoded) > 4096:
        raise ValueError("detail must be at most 4096 bytes")
    return bounded


def _set_recovery_timeouts(
    cursor: psycopg.Cursor[Any],
    *,
    now: datetime | None = None,
    deadlines: Sequence[datetime] = (),
) -> None:
    statement_timeout_ms = _RECOVERY_STATEMENT_TIMEOUT_MS
    if now is not None and deadlines:
        remaining_ms = min(int((deadline - now).total_seconds() * 1000) for deadline in deadlines)
        statement_timeout_ms = min(statement_timeout_ms, max(1, remaining_ms - 1))
    lock_timeout_ms = min(_RECOVERY_LOCK_TIMEOUT_MS, statement_timeout_ms)
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(
            sql.Literal(f"{statement_timeout_ms}ms")
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(
            sql.Literal(f"{lock_timeout_ms}ms")
        )
    )


def _fetch_action_by_idempotency(
    cursor: psycopg.Cursor[Any],
    idempotency_key: str,
    *,
    for_update: bool = False,
) -> RecoveryActionRecord | None:
    cursor.execute(
        f"SELECT {_ACTION_COLUMNS} FROM m1_recovery_actions WHERE idempotency_key = %s"
        + (" FOR UPDATE" if for_update else ""),
        (idempotency_key,),
    )
    row = cursor.fetchone()
    return None if row is None else action_from_row(row)


def _fetch_action_by_id(
    cursor: psycopg.Cursor[Any],
    action_id: str,
    *,
    for_update: bool = False,
) -> RecoveryActionRecord | None:
    cursor.execute(
        f"SELECT {_ACTION_COLUMNS} FROM m1_recovery_actions WHERE action_id = %s"
        + (" FOR UPDATE" if for_update else ""),
        (action_id,),
    )
    row = cursor.fetchone()
    return None if row is None else action_from_row(row)


# ---------------------------------------------------------------------------
# Controller lease


def claim_controller(
    connection_factory: ConnectionFactory,
    *,
    controller_id: str,
    owner_id: str,
    lease_seconds: int,
    now: datetime,
) -> RuntimeControllerLease:
    """Claim the named reconciler lease and advance its epoch monotonically."""
    _require_nonempty(controller_id=controller_id, owner_id=owner_id)
    observed_at = _require_aware(now, "now")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    expires_at = observed_at + timedelta(seconds=lease_seconds)
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
            (controller_id, owner_id, expires_at, observed_at, observed_at),
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


def renew_controller(
    connection_factory: ConnectionFactory,
    *,
    controller: RuntimeControllerLease,
    lease_seconds: int,
    now: datetime,
) -> RuntimeControllerLease:
    """Renew one exact controller epoch without fencing its pending actions.

    ``claim_controller`` advances the epoch and is therefore appropriate for a
    fresh process.  A long-lived service must renew the same epoch between
    turns; otherwise an action queued in the previous turn would be stranded
    behind the new epoch before its executor can claim it.
    """
    if type(controller) is not RuntimeControllerLease:
        raise TypeError("controller must be RuntimeControllerLease")
    observed_at = _require_aware(now, "now")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    expires_at = observed_at + timedelta(seconds=lease_seconds)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        cursor.execute(
            """
            UPDATE m1_runtime_controller_leases
            SET lease_expires_at = %s, updated_at = %s
            WHERE controller_id = %s AND owner_id = %s AND lease_epoch = %s
              AND lease_expires_at > %s
            RETURNING controller_id, owner_id, lease_epoch, lease_expires_at
            """,
            (
                expires_at,
                observed_at,
                controller.controller_id,
                controller.owner_id,
                controller.lease_epoch,
                observed_at,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            raise RecoveryStoreError("controller lease is stale or expired")
        return RuntimeControllerLease(
            controller_id=str(row["controller_id"]),
            owner_id=str(row["owner_id"]),
            lease_epoch=int(row["lease_epoch"]),
            lease_expires_at=_require_aware(row["lease_expires_at"], "lease_expires_at"),
        )


def _controller_is_current(
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
    return bool(
        row is not None
        and str(row["owner_id"]) == controller.owner_id
        and int(row["lease_epoch"]) == controller.lease_epoch
        and _require_aware(row["lease_expires_at"], "lease_expires_at") > now
    )


# ---------------------------------------------------------------------------
# Read-only controller dashboard and reconciler facts


def _safe_text(value: object, *, limit: int = 256) -> str:
    """Return bounded printable text for operator projections."""
    text = str(value).replace("\x00", "")
    text = "".join(character if character.isprintable() else " " for character in text)
    if any(
        marker in text.casefold()
        for marker in ("authorization", "api_key", "apikey", "password", "secret", "token=")
    ):
        return "<redacted>"
    return text[:limit]


def _safe_timestamp(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_aware(value, field_name).isoformat()  # type: ignore[arg-type]


def _safe_action_detail(detail: Mapping[str, object]) -> dict[str, object]:
    """Project only the bounded, non-secret detail keys used by dashboards."""
    allowed = (
        "action_type",
        "budget_remaining",
        "component",
        "cooldown_seconds",
        "next_check_at",
        "qualification_breaking",
        "reason_code",
        "severity",
    )
    projected: dict[str, object] = {}
    for key in allowed:
        value = detail.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or type(value) is int:
            projected[key] = value
        elif isinstance(value, str):
            projected[key] = _safe_text(value)
    return projected


def _action_projection(action: RecoveryActionRecord) -> dict[str, object]:
    """Render one action without exposing arbitrary persisted JSON."""
    return {
        "action_id": _safe_text(action.action_id),
        "controller_id": _safe_text(action.controller_id),
        "controller_owner_id": _safe_text(action.controller_owner_id),
        "incident_key": None if action.incident_key is None else _safe_text(action.incident_key),
        "target_type": _safe_text(action.target_type),
        "target_id": _safe_text(action.target_id),
        "action_type": _safe_text(action.action_type),
        "expected_controller_epoch": action.expected_controller_epoch,
        "expected_attempt_id": _safe_text(action.expected_attempt_id),
        "expected_lease_epoch": action.expected_lease_epoch,
        "requested_at": action.requested_at.isoformat(),
        "started_at": None if action.started_at is None else action.started_at.isoformat(),
        "finished_at": None if action.finished_at is None else action.finished_at.isoformat(),
        "state": _safe_text(action.state),
        "result_code": None if action.result_code is None else _safe_text(action.result_code),
        "next_allowed_at": action.next_allowed_at.isoformat(),
        "action_lease": {
            "worker_id": None if action.worker_id is None else _safe_text(action.worker_id),
            "worker_epoch": action.worker_epoch,
            "expires_at": (
                None
                if action.worker_lease_expires_at is None
                else action.worker_lease_expires_at.isoformat()
            ),
        },
        "detail": _safe_action_detail(action.detail),
    }


def read_runtime_controller_status(
    connection_factory: ConnectionFactory,
    *,
    controller_id: str,
    now: datetime,
    sample_limit: int = 20,
) -> dict[str, object]:
    """Read the reconciler dashboard in a read-only transaction.

    This function intentionally has no claim, schedule, or action-worker
    calls.  A missing controller row is represented as ``None`` so a fresh
    authority remains distinguishable from a failed database read.
    """
    _require_nonempty(controller_id=controller_id)
    observed_at = _require_aware(now, "now")
    if not 1 <= sample_limit <= 100:
        raise ValueError("sample_limit must be in 1..100")
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        _set_recovery_timeouts(cursor)
        cursor.execute(
            """
            SELECT controller_id, owner_id, lease_epoch, lease_expires_at,
                   claimed_at, updated_at
            FROM m1_runtime_controller_leases
            WHERE controller_id = %s
            """,
            (controller_id,),
        )
        lease_row = cursor.fetchone()
        controller: dict[str, object] | None
        if lease_row is None:
            controller = None
        else:
            lease_expires_at = _require_aware(lease_row["lease_expires_at"], "lease_expires_at")
            controller = {
                "controller_id": _safe_text(lease_row["controller_id"]),
                "owner_id": _safe_text(lease_row["owner_id"]),
                "lease_epoch": int(lease_row["lease_epoch"]),
                "lease_expires_at": lease_expires_at.isoformat(),
                "lease_active": lease_expires_at > observed_at,
                "lease_remaining_seconds": max(
                    0, int((lease_expires_at - observed_at).total_seconds())
                ),
                "claimed_at": _safe_timestamp(lease_row["claimed_at"], "claimed_at"),
                "updated_at": _safe_timestamp(lease_row["updated_at"], "updated_at"),
            }

        cursor.execute(
            """
            SELECT incident_key, component, severity, state, summary,
                   opened_at, updated_at
            FROM m1_incidents
            WHERE state IN ('open', 'acknowledged')
              AND (component LIKE 'runtime%%' OR component LIKE 'recovery%%'
                   OR incident_key LIKE 'recovery:%%')
            ORDER BY updated_at DESC, incident_key DESC
            LIMIT %s
            """,
            (sample_limit,),
        )
        incidents = [
            {
                "incident_key": _safe_text(row["incident_key"]),
                "component": _safe_text(row["component"]),
                "severity": _safe_text(row["severity"]),
                "state": _safe_text(row["state"]),
                "summary": _safe_text(row["summary"]),
                "opened_at": _safe_timestamp(row["opened_at"], "opened_at"),
                "updated_at": _safe_timestamp(row["updated_at"], "updated_at"),
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT target_type, target_id, max_actions, remaining_actions,
                   last_next_allowed_at, updated_at
            FROM m1_recovery_target_budgets
            WHERE controller_id = %s
            ORDER BY updated_at DESC, target_type, target_id
            LIMIT %s
            """,
            (controller_id, sample_limit),
        )
        budgets = [
            {
                "target_type": _safe_text(row["target_type"]),
                "target_id": _safe_text(row["target_id"]),
                "max_actions": int(row["max_actions"]),
                "remaining_actions": int(row["remaining_actions"]),
                "last_next_allowed_at": _safe_timestamp(
                    row["last_next_allowed_at"], "last_next_allowed_at"
                ),
                "updated_at": _safe_timestamp(row["updated_at"], "updated_at"),
            }
            for row in cursor.fetchall()
        ]

        cursor.execute(
            f"""
            SELECT {_ACTION_COLUMNS}
            FROM m1_recovery_actions
            WHERE controller_id = %s AND state IN ('pending', 'running')
            ORDER BY requested_at ASC, action_id ASC
            LIMIT %s
            """,
            (controller_id, sample_limit),
        )
        active_actions = [action_from_row(row) for row in cursor.fetchall()]

        cursor.execute(
            f"""
            SELECT {_ACTION_COLUMNS}
            FROM m1_recovery_actions
            WHERE controller_id = %s AND state = 'completed'
            ORDER BY finished_at DESC NULLS LAST, action_id DESC
            LIMIT %s
            """,
            (controller_id, sample_limit),
        )
        recent_completed = [action_from_row(row) for row in cursor.fetchall()]

        action_views = [_action_projection(action) for action in active_actions]
        completed_views = [_action_projection(action) for action in recent_completed]
        next_checks = [
            action.next_allowed_at
            for action in active_actions
            if action.next_allowed_at > observed_at
        ]
        next_checks.extend(
            action.worker_lease_expires_at
            for action in active_actions
            if action.worker_lease_expires_at is not None
            and action.worker_lease_expires_at > observed_at
        )
        next_check = min(next_checks).isoformat() if next_checks else None
        last_outcome = completed_views[0] if completed_views else None
        return {
            "read_at": observed_at.isoformat(),
            "controller": controller,
            "active_runtime_incidents": incidents,
            "recovery_budget": budgets,
            "actions": {
                "pending": [
                    view for action, view in zip(active_actions, action_views, strict=True)
                    if action.state == "pending"
                ],
                "running": [
                    view for action, view in zip(active_actions, action_views, strict=True)
                    if action.state == "running"
                ],
                "recent_completed": completed_views,
            },
            "last_outcome": last_outcome,
            "next_check_at": next_check,
        }


def _runtime_failure_class(value: object) -> RecoveryFailureClass | None:
    if not isinstance(value, str):
        return None
    normalized = value.casefold()
    for marker, failure_class in _RUNTIME_FAILURE_CLASS_MAP:
        if marker in normalized:
            return failure_class
    return None


def _runtime_deadline_profile(row: Mapping[str, object]) -> RuntimeDeadlineProfile:
    started_at = _require_aware(cast(datetime, row["started_at"]), "started_at")
    heartbeat_at = _require_aware(
        cast(datetime, row["last_heartbeat_at"]), "last_heartbeat_at"
    )
    progress_at = _require_aware(cast(datetime, row["last_progress_at"]), "last_progress_at")
    lease_deadline = _require_aware(
        cast(datetime, row["lease_deadline_at"]), "lease_deadline_at"
    )
    heartbeat_deadline = _require_aware(
        cast(datetime, row["heartbeat_deadline_at"]), "heartbeat_deadline_at"
    )
    progress_deadline = _require_aware(
        cast(datetime, row["progress_deadline_at"]), "progress_deadline_at"
    )
    attempt_deadline = _require_aware(
        cast(datetime, row["attempt_deadline_at"]), "attempt_deadline_at"
    )
    lease_seconds = max(3, int((lease_deadline - started_at).total_seconds()))
    heartbeat_seconds = max(
        1,
        min(
            max(1, lease_seconds // 3),
            int((heartbeat_deadline - heartbeat_at).total_seconds()),
        ),
    )
    progress_seconds = max(1, int((progress_deadline - progress_at).total_seconds()))
    attempt_seconds = max(progress_seconds, int((attempt_deadline - started_at).total_seconds()))
    return RuntimeDeadlineProfile(
        policy_version="runtime-deadline-derived-v1",
        lease_seconds=lease_seconds,
        heartbeat_seconds=heartbeat_seconds,
        progress_seconds=progress_seconds,
        attempt_seconds=attempt_seconds,
    )


def read_runtime_reconcile_states(
    connection_factory: ConnectionFactory,
    *,
    controller_id: str,
    now: datetime,
    sample_limit: int = 100,
) -> tuple[RuntimeReconcileCandidate, ...]:
    """Read bounded non-terminal runtime facts without changing any row."""
    _require_nonempty(controller_id=controller_id)
    _require_aware(now, "now")
    if not 1 <= sample_limit <= 100:
        raise ValueError("sample_limit must be in 1..100")
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        _set_recovery_timeouts(cursor)
        cursor.execute(
            """
            SELECT j.job_key, j.job_type, j.state AS job_state, j.attempt_count,
                   j.last_error_class, r.attempt_id, r.lease_epoch, r.worker_id,
                   r.stage, r.started_at, r.last_heartbeat_at, r.last_progress_at,
                   r.progress_sequence, r.progress_current, r.progress_total,
                   r.lease_deadline_at, r.heartbeat_deadline_at,
                   r.progress_deadline_at, r.attempt_deadline_at, r.recovery_state,
                   c.state AS circuit_state, c.opened_at AS circuit_opened_at,
                   c.next_probe_at AS circuit_next_probe_at,
                   a.error_class AS attempt_error_class,
                   b.remaining_actions
            FROM m1_job_runtime_state AS r
            JOIN m1_jobs AS j ON j.job_key = r.job_key
            LEFT JOIN m1_job_circuits AS c ON c.job_key = j.job_key
            LEFT JOIN m1_job_attempts AS a ON a.attempt_id = r.attempt_id
            LEFT JOIN m1_recovery_target_budgets AS b
              ON b.controller_id = %s
             AND b.target_type = CASE WHEN c.state = 'open' THEN 'circuit' ELSE 'job' END
             AND b.target_id = j.job_key
            WHERE j.state NOT IN ('succeeded', 'quarantined')
              AND r.recovery_state <> 'terminal'
            ORDER BY r.updated_at ASC, j.job_key ASC
            LIMIT %s
            """,
            (controller_id, sample_limit),
        )
        rows = cursor.fetchall()

    candidates: list[RuntimeReconcileCandidate] = []
    for row in rows:
        job_type = _safe_text(row["job_type"], limit=64)
        if job_type not in _RUNTIME_COMPONENTS:
            # Runtime stage registration is closed; an unknown type is
            # observable in the normal operational snapshot but is not fed to
            # an automatic recovery action by this bounded controller.
            continue
        job_state = _safe_text(row["job_state"], limit=32)
        circuit_open = row["circuit_state"] == "open"
        owner_is_current = job_state == "leased" or circuit_open
        remaining = 3 if row["remaining_actions"] is None else int(row["remaining_actions"])
        circuit_opened_at = row["circuit_opened_at"]
        cooldown_seconds = 60
        if (
            circuit_open
            and circuit_opened_at is not None
            and row["circuit_next_probe_at"] is not None
        ):
            cooldown_seconds = max(
                0,
                int(
                    (
                        _require_aware(row["circuit_next_probe_at"], "circuit_next_probe_at")
                        - _require_aware(circuit_opened_at, "circuit_opened_at")
                    ).total_seconds()
                ),
            )
        target_type = "circuit" if circuit_open else "job"
        target_id = str(row["job_key"])
        candidates.append(
            RuntimeReconcileCandidate(
                runtime_state=RecoveryRuntimeState(
                    job_key=target_id,
                    attempt_id=str(row["attempt_id"]),
                    lease_epoch=int(row["lease_epoch"]),
                    owner_is_current=owner_is_current,
                    profile=_runtime_deadline_profile(row),
                    attempt_started_at=_require_aware(row["started_at"], "started_at"),
                    last_heartbeat_at=_require_aware(
                        row["last_heartbeat_at"], "last_heartbeat_at"
                    ),
                    last_progress_at=_require_aware(
                        row["last_progress_at"], "last_progress_at"
                    ),
                    lease_expires_at=_require_aware(row["lease_deadline_at"], "lease_deadline_at"),
                    retry_count=max(0, int(row["attempt_count"])),
                    recovery_budget=RecoveryBudget(max(0, remaining)),
                    failure_class=_runtime_failure_class(
                        row["attempt_error_class"] or row["last_error_class"]
                    ),
                    open_circuit=circuit_open,
                    circuit_opened_at=(
                        None
                        if circuit_opened_at is None
                        else _require_aware(circuit_opened_at, "circuit_opened_at")
                    ),
                    circuit_cooldown_seconds=cooldown_seconds,
                ),
                job_type=job_type,
                job_state=job_state,
                worker_id=_safe_text(row["worker_id"], limit=128),
                target_type=target_type,
                target_id=target_id,
                component=job_type,
                incident_key=f"recovery:{target_type}:{target_id}",
                channels=("dashboard", "telegram"),
                cooldown_seconds=cooldown_seconds,
            )
        )
    return tuple(candidates)


# ---------------------------------------------------------------------------
# Budget and cooldown authority


def _lock_target_budget(
    cursor: psycopg.Cursor[Any],
    *,
    controller_id: str,
    target_type: str,
    target_id: str,
    initial_budget: int,
    now: datetime,
) -> BudgetState:
    cursor.execute(
        """
        INSERT INTO m1_recovery_target_budgets (
            controller_id, target_type, target_id, max_actions, remaining_actions,
            last_next_allowed_at, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, NULL, %s, %s)
        ON CONFLICT (controller_id, target_type, target_id) DO NOTHING
        """,
        (
            controller_id,
            target_type,
            target_id,
            initial_budget,
            initial_budget,
            now,
            now,
        ),
    )
    cursor.execute(
        """
        SELECT max_actions, remaining_actions, last_next_allowed_at
        FROM m1_recovery_target_budgets
        WHERE controller_id = %s AND target_type = %s AND target_id = %s
        FOR UPDATE
        """,
        (controller_id, target_type, target_id),
    )
    row = cursor.fetchone()
    if row is None:
        raise RecoveryStoreError("recovery budget row is missing")
    return BudgetState(
        max_actions=int(row["max_actions"]),
        remaining_actions=int(row["remaining_actions"]),
        last_next_allowed_at=(
            None
            if row["last_next_allowed_at"] is None
            else _require_aware(row["last_next_allowed_at"], "last_next_allowed_at")
        ),
    )


def _consume_budget_and_cooldown(
    cursor: psycopg.Cursor[Any],
    *,
    controller_id: str,
    target_type: str,
    target_id: str,
    next_allowed_at: datetime,
    now: datetime,
) -> None:
    cursor.execute(
        """
        UPDATE m1_recovery_target_budgets
        SET remaining_actions = remaining_actions - 1,
            last_next_allowed_at = GREATEST(
                COALESCE(last_next_allowed_at, '-infinity'::timestamptz),
                %s
            ),
            updated_at = %s
        WHERE controller_id = %s AND target_type = %s AND target_id = %s
          AND remaining_actions > 0
        """,
        (next_allowed_at, now, controller_id, target_type, target_id),
    )
    if cursor.rowcount != 1:
        raise RecoveryActionConflict("recovery budget changed during scheduling")


# ---------------------------------------------------------------------------
# Scheduling and evidence emission


def _canonical_idempotency(
    *,
    controller: RuntimeControllerLease,
    target_type: str,
    target_id: str,
    expected_attempt_id: str,
    expected_lease_epoch: int,
) -> str:
    payload = {
        "controller_id": controller.controller_id,
        "controller_epoch": controller.lease_epoch,
        "expected_attempt_id": expected_attempt_id,
        "expected_lease_epoch": expected_lease_epoch,
        "target_id": target_id,
        "target_type": target_type,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"recovery-action:{sha256(encoded).hexdigest()}"


def _normalized_channels(channels: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted(channels))


def _encoded_channels(channels: Sequence[str]) -> str:
    return json.dumps(
        list(_normalized_channels(channels)),
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _schedule_detail(
    *,
    decision: RecoveryDecision,
    incident_key: str,
    component: str,
    channels: Sequence[str],
    recovery_budget_remaining: int,
    cooldown_seconds: int,
    detail: Mapping[str, object] | None,
) -> dict[str, object]:
    normalized_detail: dict[str, object] = {
        "action_type": decision.action.value if decision.action is not None else "",
        "budget_remaining": recovery_budget_remaining,
        "channels": _encoded_channels(channels),
        "component": component,
        "cooldown_seconds": cooldown_seconds,
        "incident_key": incident_key,
        "next_check_at": _require_aware(decision.next_check_at, "next_check_at").isoformat(),
        "qualification_breaking": decision.qualification_breaking,
        "reason_code": decision.reason_code,
        "severity": decision.incident_severity,
    }
    for key, value in _bounded_detail(detail).items():
        normalized_detail[f"detail.{key}"] = value
    return _bounded_detail(normalized_detail)


def _same_scheduled_action(
    existing: RecoveryActionRecord,
    *,
    controller: RuntimeControllerLease,
    incident_key: str | None,
    target_type: str,
    target_id: str,
    action_type: RecoveryActionType,
    expected_attempt_id: str,
    expected_lease_epoch: int,
    detail: Mapping[str, object],
) -> bool:
    persisted_detail = existing.detail
    if (
        existing.result_code == "stale-noop"
        and persisted_detail.get("stale_reason") == "active-target-authoritative"
    ):
        persisted_detail = {
            key: value
            for key, value in persisted_detail.items()
            if key not in {"stale_reason", "active_action_id"}
        }
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
        and persisted_detail == dict(detail)
    )


def _lock_runtime_fence(
    cursor: psycopg.Cursor[Any],
    *,
    target_id: str,
) -> RuntimeFence | None:
    cursor.execute(
        """
        SELECT attempt_id, lease_epoch, worker_id, stage
        FROM m1_job_runtime_state
        WHERE job_key = %s
        FOR UPDATE
        """,
        (target_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return RuntimeFence(
        attempt_id=str(row["attempt_id"]),
        lease_epoch=int(row["lease_epoch"]),
        worker_id=str(row["worker_id"]),
        stage=str(row["stage"]),
    )


def _fetch_active_action_for_target(
    cursor: psycopg.Cursor[Any],
    *,
    target_type: str,
    target_id: str,
) -> RecoveryActionRecord | None:
    """Lock the authoritative pending/running action for one target, if any."""
    cursor.execute(
        f"""
        SELECT {_ACTION_COLUMNS}
        FROM m1_recovery_actions
        WHERE target_type = %s AND target_id = %s
          AND state IN ('pending', 'running')
        ORDER BY requested_at, action_id
        LIMIT 1
        FOR UPDATE
        """,
        (target_type, target_id),
    )
    row = cursor.fetchone()
    return None if row is None else action_from_row(row)


def _insert_action_once(
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
) -> RecoveryActionRecord | None:
    cursor.execute(
        """
        INSERT INTO m1_recovery_actions (
            action_id, controller_id, controller_owner_id, incident_key,
            target_type, target_id, action_type, expected_controller_epoch,
            expected_attempt_id, expected_lease_epoch, requested_at, state,
            result_code, next_allowed_at, finished_at, detail, idempotency_key
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  CASE WHEN %s = 'completed' THEN %s ELSE NULL END, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING action_id
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
    row = cursor.fetchone()
    if row is None:
        return None
    return _fetch_action_by_id(cursor, str(row["action_id"]))


def _existing_replay_or_conflict(
    cursor: psycopg.Cursor[Any],
    *,
    idempotency_key: str,
    controller: RuntimeControllerLease,
    incident_key: str | None,
    target_type: str,
    target_id: str,
    action_type: RecoveryActionType,
    expected_attempt_id: str,
    expected_lease_epoch: int,
    detail: Mapping[str, object],
) -> RecoveryActionRecord:
    existing = _fetch_action_by_idempotency(cursor, idempotency_key, for_update=True)
    if existing is None:
        raise RecoveryActionConflict("recovery action idempotency raced without a row")
    expected_incident_key = (
        None
        if existing.result_code in {"stale-noop", "disabled-action"}
        else incident_key
    )
    if not _same_scheduled_action(
        existing,
        controller=controller,
        incident_key=expected_incident_key,
        target_type=target_type,
        target_id=target_id,
        action_type=action_type,
        expected_attempt_id=expected_attempt_id,
        expected_lease_epoch=expected_lease_epoch,
        detail=detail,
    ):
        raise RecoveryActionConflict("recovery action idempotency conflicts")
    return existing


def _append_recovery_started_event(
    cursor: psycopg.Cursor[Any],
    *,
    runtime: RuntimeFence,
    target_id: str,
    component: str,
    decision: RecoveryDecision,
    now: datetime,
    idempotency_key: str,
) -> None:
    detail = {
        "action_type": decision.action.value if decision.action is not None else "",
        "component": component,
        "reason_code": decision.reason_code,
        "recovery_policy": decision.action.value if decision.action is not None else "",
        "retry_count": 0,
    }
    _bounded_detail(detail)
    cursor.execute(
        """
        SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
        FROM m1_job_runtime_events
        WHERE attempt_id = %s
        """,
        (runtime.attempt_id,),
    )
    sequence = cursor.fetchone()
    if sequence is None:
        raise RecoveryStoreError("runtime event sequence query returned no row")
    event_sequence = int(sequence["next_sequence"])
    cursor.execute(
        """
        INSERT INTO m1_job_runtime_events (
            event_id, job_key, attempt_id, lease_epoch, worker_id,
            event_sequence, kind, stage, progress_sequence, progress_current,
            progress_total, detail, occurred_at, idempotency_key
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, %s, %s, %s)
        """,
        (
            str(uuid4()),
            target_id,
            runtime.attempt_id,
            runtime.lease_epoch,
            runtime.worker_id,
            event_sequence,
            RuntimeEventKind.RECOVERY_STARTED.value,
            runtime.stage,
            Jsonb(detail),
            now,
            f"{idempotency_key}:runtime-event",
        ),
    )
    cursor.execute(
        """
        UPDATE m1_job_runtime_state
        SET recovery_state = 'recovering', updated_at = %s
        WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s
        """,
        (now, target_id, runtime.attempt_id, runtime.lease_epoch),
    )
    if cursor.rowcount != 1:
        raise RecoveryActionConflict("runtime recovery state changed during scheduling")


def _record_recovery_incident(
    cursor: psycopg.Cursor[Any],
    *,
    incident_key: str,
    component: str,
    target_type: str,
    target_id: str,
    decision: RecoveryDecision,
    controller: RuntimeControllerLease,
    expected_lease_epoch: int,
    channels: Sequence[str],
    now: datetime,
    idempotency_key: str,
) -> None:
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
            now,
            now,
        ),
    )
    incident = cursor.fetchone()
    if incident is None or str(incident["incident_key"]) != incident_key:
        raise RecoveryActionConflict("incident identity conflicts")

    event_id = str(uuid4())
    cursor.execute(
        """
        INSERT INTO m1_incident_events (
            incident_event_id, incident_key, kind, detail, idempotency_key, occurred_at
        ) VALUES (%s, %s, 'recovery-started', %s, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING incident_event_id
        """,
        (
            event_id,
            incident_key,
            Jsonb(
                {
                    "action_type": decision.action.value if decision.action else "",
                    "component": component,
                    "expected_controller_epoch": controller.lease_epoch,
                    "expected_lease_epoch": expected_lease_epoch,
                    "job_key": target_id,
                    "qualification_breaking": decision.qualification_breaking,
                    "reason_code": decision.reason_code,
                }
            ),
            f"{idempotency_key}:incident-event",
            now,
        ),
    )
    row = cursor.fetchone()
    if row is not None:
        event_id = str(row["incident_event_id"])
    else:
        cursor.execute(
            """
            SELECT incident_event_id
            FROM m1_incident_events
            WHERE idempotency_key = %s
            """,
            (f"{idempotency_key}:incident-event",),
        )
        existing = cursor.fetchone()
        if existing is None:
            raise RecoveryActionConflict("incident event idempotency raced")
        event_id = str(existing["incident_event_id"])

    for channel in channels:
        cursor.execute(
            """
            INSERT INTO m1_alert_outbox (
                outbox_id, incident_event_id, channel, payload, state,
                next_attempt_at, created_at
            ) VALUES (%s, %s, %s, %s, 'pending', %s, %s)
            ON CONFLICT (incident_event_id, channel) DO NOTHING
            """,
            (
                str(uuid4()),
                event_id,
                channel,
                Jsonb({"incident_key": incident_key, "kind": "recovery-started"}),
                now,
                now,
            ),
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
    """Schedule one fenced action or persist a durable completed stale/disabled action."""
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
    schedule_detail = _schedule_detail(
        decision=decision,
        incident_key=incident_key,
        component=component,
        channels=channels,
        recovery_budget_remaining=recovery_budget_remaining,
        cooldown_seconds=cooldown_seconds,
        detail=normalized_detail,
    )
    next_allowed_at = observed_at + timedelta(seconds=cooldown_seconds)
    idempotency_key = _canonical_idempotency(
        controller=controller,
        target_type=target_type,
        target_id=target_id,
        expected_attempt_id=expected_attempt_id,
        expected_lease_epoch=expected_lease_epoch,
    )

    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        existing = _fetch_action_by_idempotency(cursor, idempotency_key, for_update=True)
        if existing is not None:
            replay_incident_key = (
                None
                if existing.result_code in {"stale-noop", "disabled-action"}
                else incident_key
            )
            if not _same_scheduled_action(
                existing,
                controller=controller,
                incident_key=replay_incident_key,
                target_type=target_type,
                target_id=target_id,
                action_type=decision.action,
                expected_attempt_id=expected_attempt_id,
                expected_lease_epoch=expected_lease_epoch,
                detail=schedule_detail,
            ):
                raise RecoveryActionConflict("recovery action idempotency conflicts")
            return existing

        runtime = _lock_runtime_fence(cursor, target_id=target_id)
        controller_current = _controller_is_current(cursor, controller, now=observed_at)

        result_code: str | None = None
        if not controller_current:
            result_code = "stale-noop"
        elif (
            runtime is None
            or runtime.attempt_id != expected_attempt_id
            or runtime.lease_epoch != expected_lease_epoch
        ):
            result_code = "stale-noop"

        budget: BudgetState | None = None
        if result_code is None:
            active = _fetch_active_action_for_target(
                cursor,
                target_type=target_type,
                target_id=target_id,
            )
            if active is not None:
                if active.idempotency_key == idempotency_key:
                    if not _same_scheduled_action(
                        active,
                        controller=controller,
                        incident_key=incident_key,
                        target_type=target_type,
                        target_id=target_id,
                        action_type=decision.action,
                        expected_attempt_id=expected_attempt_id,
                        expected_lease_epoch=expected_lease_epoch,
                        detail=schedule_detail,
                    ):
                        raise RecoveryActionConflict("recovery action idempotency conflicts")
                    return active
                stale_detail = _bounded_detail(
                    {
                        **schedule_detail,
                        "stale_reason": "active-target-authoritative",
                        "active_action_id": active.action_id,
                    }
                )
                stale = _insert_action_once(
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
                    result_code="stale-noop",
                    next_allowed_at=next_allowed_at,
                    detail=stale_detail,
                    idempotency_key=idempotency_key,
                )
                if stale is None:
                    return _existing_replay_or_conflict(
                        cursor,
                        idempotency_key=idempotency_key,
                        controller=controller,
                        incident_key=incident_key,
                        target_type=target_type,
                        target_id=target_id,
                        action_type=decision.action,
                        expected_attempt_id=expected_attempt_id,
                        expected_lease_epoch=expected_lease_epoch,
                        detail=stale_detail,
                    )
                return stale
            budget = _lock_target_budget(
                cursor,
                controller_id=controller.controller_id,
                target_type=target_type,
                target_id=target_id,
                initial_budget=recovery_budget_remaining,
                now=observed_at,
            )
            if budget.remaining_actions <= 0:
                result_code = "disabled-action"
            elif (
                budget.last_next_allowed_at is not None
                and budget.last_next_allowed_at > observed_at
            ):
                result_code = "disabled-action"

        state = "pending" if result_code is None else "completed"
        action = _insert_action_once(
            cursor,
            action_id=str(uuid4()),
            controller=controller,
            incident_key=None if result_code is not None else incident_key,
            target_type=target_type,
            target_id=target_id,
            action_type=decision.action,
            expected_attempt_id=expected_attempt_id,
            expected_lease_epoch=expected_lease_epoch,
            requested_at=observed_at,
            state=state,
            result_code=result_code,
            next_allowed_at=next_allowed_at,
            detail=schedule_detail,
            idempotency_key=idempotency_key,
        )
        if action is None:
            return _existing_replay_or_conflict(
                cursor,
                idempotency_key=idempotency_key,
                controller=controller,
                incident_key=incident_key,
                target_type=target_type,
                target_id=target_id,
                action_type=decision.action,
                expected_attempt_id=expected_attempt_id,
                expected_lease_epoch=expected_lease_epoch,
                detail=schedule_detail,
            )
        if result_code is not None:
            return action

        assert runtime is not None
        assert budget is not None
        _consume_budget_and_cooldown(
            cursor,
            controller_id=controller.controller_id,
            target_type=target_type,
            target_id=target_id,
            next_allowed_at=next_allowed_at,
            now=observed_at,
        )
        _append_recovery_started_event(
            cursor,
            runtime=runtime,
            target_id=target_id,
            component=component,
            decision=decision,
            now=observed_at,
            idempotency_key=idempotency_key,
        )
        _record_recovery_incident(
            cursor,
            incident_key=incident_key,
            component=component,
            target_type=target_type,
            target_id=target_id,
            decision=decision,
            controller=controller,
            expected_lease_epoch=expected_lease_epoch,
            channels=channels,
            now=observed_at,
            idempotency_key=idempotency_key,
        )
        return action


# ---------------------------------------------------------------------------
# Action worker lease and completion


def claim_action(
    connection_factory: ConnectionFactory,
    *,
    worker_id: str,
    controller: RuntimeControllerLease,
    lease_seconds: int,
    now: datetime,
) -> RecoveryActionRecord | None:
    """Claim one pending or expired-running action using SKIP LOCKED."""
    _require_nonempty(worker_id=worker_id)
    observed_at = _require_aware(now, "now")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    expires_at = observed_at + timedelta(seconds=lease_seconds)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        if not _controller_is_current(cursor, controller, now=observed_at):
            return None
        cursor.execute(
            f"""
            SELECT {_ACTION_COLUMNS}
            FROM m1_recovery_actions
            WHERE controller_id = %s
              AND expected_controller_epoch = %s
              AND (
                  state = 'pending'
                  OR (state = 'running' AND worker_lease_expires_at <= %s)
              )
            ORDER BY requested_at, action_id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """,
            (controller.controller_id, controller.lease_epoch, observed_at),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        action = action_from_row(row)
        worker_epoch = action.worker_epoch + 1
        cursor.execute(
            """
            UPDATE m1_recovery_actions
            SET state = 'running', started_at = %s, worker_id = %s,
                worker_epoch = %s, worker_lease_expires_at = %s
            WHERE action_id = %s
              AND (
                  state = 'pending'
                  OR (state = 'running' AND worker_lease_expires_at <= %s)
              )
            """,
            (
                observed_at,
                worker_id,
                worker_epoch,
                expires_at,
                action.action_id,
                observed_at,
            ),
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
    """Close a running recovery action under exact worker/epoch/lease fencing."""
    _require_nonempty(action_id=action_id, worker_id=worker_id, result_code=result_code)
    if worker_epoch <= 0:
        raise ValueError("worker_epoch must be positive")
    if result_code not in _CLOSED_RESULT_CODES:
        raise ValueError("result_code is not in the recovery action contract")
    observed_at = _require_aware(now, "now")
    normalized_detail = _bounded_detail(detail)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        action = _fetch_action_by_id(cursor, action_id, for_update=True)
        if action is None:
            raise RecoveryStoreError("recovery action is missing")
        if action.state == "completed":
            if action.result_code == result_code and action.detail == normalized_detail:
                return action
            raise RecoveryActionConflict("finish replay conflicts")
        if (
            action.worker_id != worker_id
            or action.worker_epoch != worker_epoch
            or action.worker_lease_expires_at is None
            or action.worker_lease_expires_at <= observed_at
        ):
            return action
        cursor.execute(
            """
            UPDATE m1_recovery_actions
            SET state = 'completed', result_code = %s, finished_at = %s,
                detail = %s
            WHERE action_id = %s AND state = 'running'
              AND worker_id = %s AND worker_epoch = %s
              AND worker_lease_expires_at > %s
            """,
            (
                result_code,
                observed_at,
                Jsonb(normalized_detail),
                action_id,
                worker_id,
                worker_epoch,
                observed_at,
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


def execute_claimed_action(
    connection_factory: ConnectionFactory,
    *,
    action_id: str,
    worker_id: str,
    worker_epoch: int,
    controller: RuntimeControllerLease,
    now: datetime,
    callback: RecoveryActionCallback,
) -> RecoveryActionRecord:
    """Run a claimed action and close its ledger row in one transaction.

    The action row is the outer lock for the worker lease.  The controller and
    exact runtime/job rows are then locked before the callback is invoked.  A
    callback exception rolls back both business mutation and terminal action
    state, leaving the action reclaimable after its worker lease expires.
    """
    _require_nonempty(action_id=action_id, worker_id=worker_id)
    if worker_epoch <= 0:
        raise ValueError("worker_epoch must be positive")
    if type(controller) is not RuntimeControllerLease:
        raise TypeError("controller must be RuntimeControllerLease")
    if not callable(callback):
        raise TypeError("callback must be callable")
    observed_at = _require_aware(now, "now")
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_recovery_timeouts(cursor)
        action = _fetch_action_by_id(cursor, action_id, for_update=True)
        if action is None:
            raise RecoveryStoreError("recovery action is missing")
        if (
            action.state != "running"
            or action.worker_id != worker_id
            or action.worker_epoch != worker_epoch
            or action.worker_lease_expires_at is None
            or action.worker_lease_expires_at <= observed_at
        ):
            return action
        _set_recovery_timeouts(
            cursor,
            now=observed_at,
            deadlines=(action.worker_lease_expires_at,),
        )

        controller_current = (
            action.controller_id == controller.controller_id
            and action.controller_owner_id == controller.owner_id
            and action.expected_controller_epoch == controller.lease_epoch
            and _controller_is_current(cursor, controller, now=observed_at)
        )
        if not controller_current:
            return _complete_action_cursor(
                cursor,
                action=action,
                result_code="stale-noop",
            )

        if action.target_type in {"job", "circuit"} and not _action_runtime_fence_current(
            cursor, action=action, now=observed_at
        ):
            return _complete_action_cursor(
                cursor,
                action=action,
                result_code="stale-noop",
            )

        raw_result = callback(cursor, action)
        result_code = _closed_result_code(raw_result)
        return _complete_action_cursor(
            cursor,
            action=action,
            result_code=result_code,
        )


def _closed_result_code(value: object) -> str:
    if isinstance(value, str) and value in _CLOSED_RESULT_CODES:
        return value
    if value is True or value is False:
        return "succeeded" if value else "failed"
    raise RecoveryStoreError("recovery callback returned no bounded result code")


def _complete_action_cursor(
    cursor: psycopg.Cursor[Any],
    *,
    action: RecoveryActionRecord,
    result_code: str,
) -> RecoveryActionRecord:
    if result_code not in _CLOSED_RESULT_CODES:
        raise RecoveryStoreError("recovery result code is not in the action contract")
    detail = _bounded_detail({"postcondition": result_code})
    cursor.execute(
        """
        UPDATE m1_recovery_actions
        SET state = 'completed', result_code = %s, finished_at = clock_timestamp(), detail = %s
        WHERE action_id = %s AND state = 'running'
          AND worker_id = %s AND worker_epoch = %s
          AND worker_lease_expires_at > clock_timestamp()
        """,
        (
            result_code,
            Jsonb(detail),
            action.action_id,
            action.worker_id,
            action.worker_epoch,
        ),
    )
    if cursor.rowcount != 1:
        raise RecoveryActionConflict("action worker lease changed during terminal finish")
    finished = _fetch_action_by_id(cursor, action.action_id)
    if finished is None:
        raise RecoveryStoreError("recovery action disappeared after terminal finish")
    return finished


def _action_runtime_fence_current(
    cursor: psycopg.Cursor[Any],
    *,
    action: RecoveryActionRecord,
    now: datetime,
) -> bool:
    cursor.execute(
        """
        SELECT j.state, j.lease_owner, j.lease_epoch, j.lease_expires_at,
               r.attempt_id, r.lease_epoch AS runtime_epoch
        FROM m1_jobs AS j
        JOIN m1_job_runtime_state AS r ON r.job_key = j.job_key
        WHERE j.job_key = %s
        FOR UPDATE
        """,
        (action.target_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return False
    assert action.worker_lease_expires_at is not None
    deadlines: list[datetime] = [action.worker_lease_expires_at]
    if row["lease_expires_at"] is not None and _require_aware(
        row["lease_expires_at"], "lease_expires_at"
    ) > now:
        deadlines.append(_require_aware(row["lease_expires_at"], "lease_expires_at"))
    _set_recovery_timeouts(cursor, now=now, deadlines=tuple(deadlines))
    if (
        str(row["attempt_id"]) != action.expected_attempt_id
        or int(row["runtime_epoch"]) != action.expected_lease_epoch
        or int(row["lease_epoch"]) != action.expected_lease_epoch
    ):
        return False
    if action.action_type in {"heartbeat-job", "cancel-job"}:
        return bool(
            row["state"] == "leased"
            and row["lease_owner"] is not None
            and row["lease_expires_at"] is not None
            and _require_aware(row["lease_expires_at"], "lease_expires_at") > now
        )
    if action.action_type == "reclaim-job":
        return bool(
            row["state"] == "leased"
            and row["lease_owner"] is not None
            and row["lease_expires_at"] is not None
            and _require_aware(row["lease_expires_at"], "lease_expires_at") <= now
        )
    return str(row["state"]) in {"retryable", "runnable", "leased"}


__all__ = [
    "RecoveryActionConflict",
    "RecoveryActionRecord",
    "RecoveryStoreError",
    "RuntimeReconcileCandidate",
    "RuntimeControllerLease",
    "claim_action",
    "claim_controller",
    "execute_claimed_action",
    "finish_action",
    "read_runtime_controller_status",
    "read_runtime_reconcile_states",
    "renew_controller",
    "schedule_action",
]
