"""Typed records and row materialization for runtime recovery actions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

_ACTION_COLUMNS = (
    "action_id, controller_id, controller_owner_id, incident_key, target_type, target_id, "
    "action_type, expected_controller_epoch, expected_attempt_id, expected_lease_epoch, "
    "requested_at, started_at, finished_at, state, result_code, next_allowed_at, "
    "worker_id, worker_epoch, worker_lease_expires_at, detail, idempotency_key"
)


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


@dataclass(frozen=True, slots=True)
class RuntimeFence:
    attempt_id: str
    lease_epoch: int
    worker_id: str
    stage: str


@dataclass(frozen=True, slots=True)
class BudgetState:
    max_actions: int
    remaining_actions: int
    last_next_allowed_at: datetime | None


def _require_aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _row_value(row: object, name: str, position: int) -> Any:
    if isinstance(row, Mapping):
        return row[name]
    return row[position]  # type: ignore[index]


def _optional_aware(row: object, name: str, position: int) -> datetime | None:
    value = _row_value(row, name, position)
    return None if value is None else _require_aware(value, name)


def action_from_row(row: object) -> RecoveryActionRecord:
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
        started_at=_optional_aware(row, "started_at", 11),
        finished_at=_optional_aware(row, "finished_at", 12),
        state=str(_row_value(row, "state", 13)),
        result_code=None if result_code is None else str(result_code),
        next_allowed_at=_require_aware(
            _row_value(row, "next_allowed_at", 15),
            "next_allowed_at",
        ),
        worker_id=None if worker_id is None else str(worker_id),
        worker_epoch=int(_row_value(row, "worker_epoch", 17)),
        worker_lease_expires_at=_optional_aware(row, "worker_lease_expires_at", 18),
        detail=dict(_row_value(row, "detail", 19)),  # type: ignore[arg-type]
        idempotency_key=str(_row_value(row, "idempotency_key", 20)),
    )
