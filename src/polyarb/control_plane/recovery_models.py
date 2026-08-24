"""Bounded recovery decisions for the M1 runtime reconciler."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal

from .runtime_models import RuntimeDeadlineProfile


class RecoveryActionType(StrEnum):
    HEARTBEAT_JOB = "heartbeat-job"
    CANCEL_JOB = "cancel-job"
    RETRY_JOB = "retry-job"
    RECLAIM_JOB = "reclaim-job"
    PROBE_CIRCUIT = "probe-circuit"
    RESTART_WORKER_PROCESS = "restart-worker-process"
    RESTART_MACHINE = "restart-machine"


class RecoveryFailureClass(StrEnum):
    INTEGRITY = "integrity"
    AUTHENTICATION = "authentication"
    SCHEMA = "schema"
    CREDENTIAL = "credential"
    CAPACITY = "capacity"


RECOVERY_REASON_CODES = frozenset(
    {
        "job.healthy",
        "job.lease-at-risk",
        "job.progress-stalled",
        "job.heartbeat-missing-fence",
        "job.heartbeat-missing",
        "job.lease-expired",
        "job.attempt-deadline",
        "circuit.probe-due",
        "circuit.cooldown",
        "recovery.budget-exhausted",
        "recovery.stale-fence",
        "failure.integrity",
        "failure.authentication",
        "failure.schema",
        "failure.credential",
        "failure.capacity",
    }
)
type _ReasonPolicy = tuple[
    RecoveryActionType | None,
    Literal["warning", "critical"],
    bool,
]
_REASON_POLICY: dict[str, _ReasonPolicy] = {
    "job.healthy": (None, "warning", False),
    "job.lease-at-risk": (RecoveryActionType.HEARTBEAT_JOB, "warning", False),
    "job.progress-stalled": (RecoveryActionType.CANCEL_JOB, "warning", False),
    "job.heartbeat-missing-fence": (None, "critical", True),
    "job.heartbeat-missing": (RecoveryActionType.RECLAIM_JOB, "critical", True),
    "job.lease-expired": (RecoveryActionType.RECLAIM_JOB, "critical", True),
    "job.attempt-deadline": (RecoveryActionType.CANCEL_JOB, "critical", True),
    "circuit.probe-due": (RecoveryActionType.PROBE_CIRCUIT, "warning", False),
    "circuit.cooldown": (None, "warning", False),
    "recovery.budget-exhausted": (None, "critical", True),
    "recovery.stale-fence": (None, "critical", True),
    "failure.integrity": (None, "critical", True),
    "failure.authentication": (None, "critical", True),
    "failure.schema": (None, "critical", True),
    "failure.credential": (None, "critical", True),
    "failure.capacity": (None, "critical", True),
}
_NO_ACTION_REASON_CODES = frozenset(
    {
        "job.healthy",
        "job.heartbeat-missing-fence",
        "circuit.cooldown",
        "recovery.budget-exhausted",
        "recovery.stale-fence",
        "failure.integrity",
        "failure.authentication",
        "failure.schema",
        "failure.credential",
        "failure.capacity",
    }
)
_HUMAN_ONLY_REASON_CODES = frozenset(
    {
        "failure.integrity",
        "failure.authentication",
        "failure.schema",
        "failure.credential",
        "failure.capacity",
    }
)


def require_timezone_aware(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def require_exact_non_negative_int(value: int, *, field_name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an exact int")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def require_exact_positive_int(value: int, *, field_name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an exact int")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


@dataclass(frozen=True, slots=True)
class RecoveryBudget:
    remaining_actions: int

    def __post_init__(self) -> None:
        require_exact_non_negative_int(
            self.remaining_actions,
            field_name="remaining_actions",
        )


@dataclass(frozen=True, slots=True)
class RecoveryRuntimeState:
    job_key: str
    attempt_id: str
    lease_epoch: int
    owner_is_current: bool
    profile: RuntimeDeadlineProfile
    attempt_started_at: datetime
    last_heartbeat_at: datetime
    last_progress_at: datetime
    lease_expires_at: datetime
    retry_count: int
    recovery_budget: RecoveryBudget
    failure_class: RecoveryFailureClass | None = None
    open_circuit: bool = False
    circuit_opened_at: datetime | None = None
    circuit_cooldown_seconds: int = 60

    def __post_init__(self) -> None:
        if not self.job_key or not self.attempt_id:
            raise ValueError("runtime recovery identities must be non-empty")
        require_exact_positive_int(self.lease_epoch, field_name="lease_epoch")
        if type(self.owner_is_current) is not bool:
            raise TypeError("owner_is_current must be bool")
        if type(self.profile) is not RuntimeDeadlineProfile:
            raise TypeError("profile must be RuntimeDeadlineProfile")
        require_timezone_aware(self.attempt_started_at, field_name="attempt_started_at")
        require_timezone_aware(self.last_heartbeat_at, field_name="last_heartbeat_at")
        require_timezone_aware(self.last_progress_at, field_name="last_progress_at")
        require_timezone_aware(self.lease_expires_at, field_name="lease_expires_at")
        require_exact_non_negative_int(self.retry_count, field_name="retry_count")
        if type(self.recovery_budget) is not RecoveryBudget:
            raise TypeError("recovery_budget must be RecoveryBudget")
        if self.failure_class is not None and type(self.failure_class) is not RecoveryFailureClass:
            raise TypeError("failure_class must be RecoveryFailureClass or None")
        if type(self.open_circuit) is not bool:
            raise TypeError("open_circuit must be bool")
        if self.circuit_opened_at is not None:
            require_timezone_aware(self.circuit_opened_at, field_name="circuit_opened_at")
        require_exact_non_negative_int(
            self.circuit_cooldown_seconds,
            field_name="circuit_cooldown_seconds",
        )
        if self.open_circuit and self.circuit_opened_at is None:
            raise ValueError("open circuit requires circuit_opened_at")
        if not self.open_circuit and self.circuit_opened_at is not None:
            raise ValueError("closed circuit cannot carry circuit_opened_at")


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    action: RecoveryActionType | None
    reason_code: str
    incident_severity: Literal["warning", "critical"]
    qualification_breaking: bool
    next_check_at: datetime

    def __post_init__(self) -> None:
        if self.action is not None and type(self.action) is not RecoveryActionType:
            raise TypeError("action must be RecoveryActionType or None")
        if self.reason_code not in RECOVERY_REASON_CODES:
            raise ValueError("reason_code must be one of the bounded recovery codes")
        if self.incident_severity not in {"warning", "critical"}:
            raise ValueError("incident_severity must be warning or critical")
        if type(self.qualification_breaking) is not bool:
            raise TypeError("qualification_breaking must be bool")
        require_timezone_aware(self.next_check_at, field_name="next_check_at")
        if self.reason_code in _NO_ACTION_REASON_CODES and self.action is not None:
            raise ValueError("reason_code with no automatic action cannot carry automatic action")
        expected_action, expected_severity, expected_breaking = _REASON_POLICY[self.reason_code]
        if self.action is not expected_action:
            raise ValueError("action must exactly match reason_code")
        if self.reason_code in _HUMAN_ONLY_REASON_CODES:
            if self.incident_severity != "critical" or not self.qualification_breaking:
                raise ValueError("human-only failures must be critical and qualification-breaking")
        if (
            self.incident_severity != expected_severity
            or self.qualification_breaking is not expected_breaking
        ):
            raise ValueError("incident severity and qualification impact must match reason_code")


__all__ = [
    "RECOVERY_REASON_CODES",
    "RecoveryActionType",
    "RecoveryBudget",
    "RecoveryDecision",
    "RecoveryFailureClass",
    "RecoveryRuntimeState",
    "require_timezone_aware",
]
