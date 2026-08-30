from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from math import isfinite
from typing import Any, NoReturn


class RuntimeEventKind(StrEnum):
    STARTED = "job.started"
    STAGE_CHANGED = "job.stage-changed"
    LEASE_AT_RISK = "job.lease-at-risk"
    PROGRESS_STALLED = "job.progress-stalled"
    RETRYABLE_FAILED = "job.retryable-failed"
    RETRY_SCHEDULED = "job.retry-scheduled"
    RECOVERY_STARTED = "job.recovery-started"
    RECOVERED = "job.recovered"
    TERMINAL_FAILED = "job.terminal-failed"
    SUCCEEDED = "job.succeeded"


@dataclass(frozen=True, slots=True)
class RuntimeDeadlineProfile:
    policy_version: str
    lease_seconds: int
    heartbeat_seconds: int
    progress_seconds: int
    attempt_seconds: int

    def __post_init__(self) -> None:
        values = (
            self.lease_seconds,
            self.heartbeat_seconds,
            self.progress_seconds,
            self.attempt_seconds,
        )
        if not self.policy_version or any(value <= 0 for value in values):
            raise ValueError("runtime deadline profile values must be positive")
        if self.heartbeat_seconds * 3 > self.lease_seconds:
            raise ValueError("heartbeat must run at least three times per lease")
        if self.progress_seconds > self.attempt_seconds:
            raise ValueError("progress deadline cannot exceed attempt deadline")

    @property
    def missed_heartbeat_incident_seconds(self) -> int:
        return self.heartbeat_seconds * 3


@dataclass(frozen=True, slots=True)
class RuntimeProgress:
    sequence: int
    current: int
    total: int | None
    stage: str

    def __post_init__(self) -> None:
        if self.sequence < 0 or self.current < 0 or not self.stage:
            raise ValueError("runtime progress values are invalid")
        if self.total is not None and (self.total < 0 or self.current > self.total):
            raise ValueError("current cannot exceed total")


_MAX_DETAIL_KEYS = 20
_MAX_DETAIL_BYTES = 4096
_DETAIL_CODE_REGISTRIES: dict[str, frozenset[str]] = {
    "action_type": frozenset(
        {
            "cancel-job",
            "heartbeat-job",
            "probe-circuit",
            "reclaim-job",
            "restart-machine",
            "restart-worker-process",
            "retry-job",
        }
    ),
    "component": frozenset(
        {
            "control-plane",
            "opportunity-certify",
            "quote-admit",
            "quote-batch",
            "quote-certify",
            "structure-certify",
            "structure-fetch",
            "structure-materialize",
            "structure-normalize",
        }
    ),
    "data_product": frozenset({"market-snapshot", "structure-sync"}),
    "deadline_kind": frozenset({"attempt", "heartbeat", "lease", "progress"}),
    "failure_signature": frozenset(
        {
            "database.unavailable",
            "progress.stalled",
            "service.interrupted",
            "upstream.malformed",
            "upstream.timeout",
            "upstream.transport",
            "validation.failed",
        }
    ),
    "job_type": frozenset(
        {
            "opportunity-certify",
            "quote-admit",
            "quote-batch",
            "quote-certify",
            "quote-scan",
            "structure-certify",
            "structure-fetch",
            "structure-materialize",
            "structure-normalize",
        }
    ),
    "qualification_impact": frozenset(
        {"blocked", "delayed", "invalidated", "none", "qualified", "restored"}
    ),
    "reason_code": frozenset(
        {
            "checkpoint.advance",
            "circuit.cooldown",
            "circuit.probe-due",
            "failure.authentication",
            "failure.capacity",
            "failure.credential",
            "failure.integrity",
            "failure.schema",
            "freshness.quote",
            "invalid-input",
            "integrity.conflict",
            "job.attempt-deadline",
            "job.heartbeat-missing",
            "job.heartbeat-missing-fence",
            "job.healthy",
            "job.lease-at-risk",
            "job.lease-expired",
            "job.progress-stalled",
            "publication.superseded",
            "recovery.budget-exhausted",
            "recovery.stale-fence",
            "service-stop",
            "timeout",
        }
    ),
    "recovery_policy": frozenset(
        {
            "cancel-job",
            "exponential-backoff",
            "heartbeat-job",
            "probe-circuit",
            "reclaim-job",
            "restart-machine",
            "restart-worker-process",
            "retry-job",
            "retry-same-input",
            "retry-soon",
        }
    ),
    "result_code": frozenset({"failed", "ok"}),
}
_DETAIL_CODE_KEYS = frozenset(
    {
        "component",
        "data_product",
        "deadline_kind",
        "failure_signature",
        "job_type",
        "qualification_impact",
        "reason_code",
        "recovery_policy",
        "result_code",
        "action_type",
    }
)
_DETAIL_SECONDS_KEYS = frozenset({"backoff_seconds", "freshness_seconds"})
_DETAIL_COUNT_KEYS = frozenset({"retry_count"})
_DETAIL_TIMESTAMP_KEYS = frozenset({"deadline_at", "next_decision_at"})
_RUNTIME_EVENT_DETAIL_KEYS = {
    RuntimeEventKind.STARTED: frozenset(
        {
            "component",
            "data_product",
            "job_type",
            "recovery_policy",
        }
    ),
    RuntimeEventKind.STAGE_CHANGED: frozenset(
        {
            "component",
            "data_product",
            "reason_code",
            "result_code",
        }
    ),
    RuntimeEventKind.LEASE_AT_RISK: frozenset(
        {
            "component",
            "deadline_at",
            "deadline_kind",
            "freshness_seconds",
            "qualification_impact",
            "recovery_policy",
        }
    ),
    RuntimeEventKind.PROGRESS_STALLED: frozenset(
        {
            "component",
            "data_product",
            "deadline_at",
            "deadline_kind",
            "failure_signature",
            "freshness_seconds",
            "qualification_impact",
            "recovery_policy",
        }
    ),
    RuntimeEventKind.RETRYABLE_FAILED: frozenset(
        {
            "component",
            "failure_signature",
            "qualification_impact",
            "reason_code",
            "recovery_policy",
            "retry_count",
        }
    ),
    RuntimeEventKind.RETRY_SCHEDULED: frozenset(
        {
            "backoff_seconds",
            "next_decision_at",
            "reason_code",
            "recovery_policy",
            "retry_count",
        }
    ),
    RuntimeEventKind.RECOVERY_STARTED: frozenset(
        {
            "action_type",
            "component",
            "reason_code",
            "recovery_policy",
            "retry_count",
        }
    ),
    RuntimeEventKind.RECOVERED: frozenset(
        {
            "component",
            "qualification_impact",
            "result_code",
            "retry_count",
        }
    ),
    RuntimeEventKind.TERMINAL_FAILED: frozenset(
        {
            "component",
            "failure_signature",
            "qualification_impact",
            "reason_code",
            "result_code",
        }
    ),
    RuntimeEventKind.SUCCEEDED: frozenset(
        {
            "component",
            "data_product",
            "freshness_seconds",
            "qualification_impact",
            "result_code",
        }
    ),
}


class _FrozenRuntimeDetail(dict[str, object]):
    """JSON-serializable immutable dict used for runtime event evidence detail."""

    __slots__ = ()

    def _immutable(self) -> NoReturn:
        raise TypeError("runtime event detail is immutable")

    def __setitem__(self, key: str, value: object) -> None:
        self._immutable()

    def __delitem__(self, key: str) -> None:
        self._immutable()

    def clear(self) -> None:
        self._immutable()

    def pop(self, *args: Any, **kwargs: Any) -> Any:
        self._immutable()

    def popitem(self) -> tuple[str, object]:
        self._immutable()

    def setdefault(self, *args: Any, **kwargs: Any) -> Any:
        self._immutable()

    def update(self, *args: Any, **kwargs: Any) -> None:
        self._immutable()

    def __ior__(self, value: object) -> _FrozenRuntimeDetail:
        self._immutable()


def _invalid_detail_value(key: str) -> ValueError:
    return ValueError(f"runtime event detail value is invalid for {key}")


def _require_detail_code(key: str, value: object) -> str:
    registry = _DETAIL_CODE_REGISTRIES[key]
    if type(value) is not str or value not in registry:
        raise _invalid_detail_value(key)
    return value


def _require_detail_seconds(key: str, value: object) -> int | float:
    if type(value) is int and value >= 0:
        return value
    if type(value) is float and isfinite(value) and value >= 0:
        return value
    raise _invalid_detail_value(key)


def _require_detail_count(key: str, value: object) -> int:
    if type(value) is int and value >= 0:
        return value
    raise _invalid_detail_value(key)


def _require_detail_timestamp(key: str, value: object) -> str:
    if type(value) is not str:
        raise _invalid_detail_value(key)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _invalid_detail_value(key) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _invalid_detail_value(key)
    return value


def _normalize_detail_scalar(key: str, value: object) -> str | int | float:
    if type(value) in (dict, list, tuple):
        raise ValueError("runtime event detail values must be flat JSON scalars")
    if key in _DETAIL_CODE_KEYS:
        return _require_detail_code(key, value)
    if key in _DETAIL_SECONDS_KEYS:
        return _require_detail_seconds(key, value)
    if key in _DETAIL_COUNT_KEYS:
        return _require_detail_count(key, value)
    if key in _DETAIL_TIMESTAMP_KEYS:
        return _require_detail_timestamp(key, value)
    raise ValueError(f"runtime event detail key has no validator: {key}")


def validate_runtime_detail_bounds(
    detail: dict[str, object], *, validate_encoded_size: bool = True
) -> None:
    """Validate shared runtime detail size/shape bounds before specialized checks."""
    if type(detail) is not dict:
        raise TypeError("detail root must be a dict")
    if len(detail) > _MAX_DETAIL_KEYS:
        raise ValueError("runtime event detail is not bounded")
    for key, value in detail.items():
        if type(key) is not str:
            raise ValueError("runtime event detail must be JSON-compatible")
        if type(value) in (dict, list, tuple):
            raise ValueError("runtime event detail values must be flat JSON scalars")
    if validate_encoded_size:
        try:
            encoded_detail = json.dumps(
                detail,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("runtime event detail must be JSON-compatible") from exc
        if len(encoded_detail) > _MAX_DETAIL_BYTES:
            raise ValueError("runtime event detail JSON must be at most 4096 bytes")


def _freeze_runtime_detail(
    kind: RuntimeEventKind,
    detail: dict[str, object],
) -> _FrozenRuntimeDetail:
    allowed_keys = _RUNTIME_EVENT_DETAIL_KEYS[kind]
    detail_keys = frozenset(detail)
    unknown_keys = detail_keys - allowed_keys
    if unknown_keys:
        raise ValueError(
            f"runtime event detail keys are not allowed for {kind.value}: {sorted(unknown_keys)!r}"
        )
    frozen = _FrozenRuntimeDetail()
    for key, value in detail.items():
        if type(key) is not str:
            raise ValueError("runtime event detail must be JSON-compatible")
        dict.__setitem__(frozen, key, _normalize_detail_scalar(key, value))
    return frozen


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    job_key: str
    attempt_id: str
    lease_epoch: int
    worker_id: str
    event_sequence: int
    kind: RuntimeEventKind
    stage: str
    progress: RuntimeProgress | None
    detail: dict[str, object]
    occurred_at: datetime
    idempotency_key: str

    def __post_init__(self) -> None:
        if type(self.kind) is not RuntimeEventKind:
            raise TypeError("kind must be RuntimeEventKind")
        if self.progress is not None and type(self.progress) is not RuntimeProgress:
            raise TypeError("progress must be RuntimeProgress or None")
        if type(self.detail) is not dict:
            raise TypeError("detail root must be a dict")
        identities = (
            self.job_key,
            self.attempt_id,
            self.worker_id,
            self.stage,
            self.idempotency_key,
        )
        if any(not value for value in identities):
            raise ValueError("runtime event identities must be non-empty")
        if self.lease_epoch < 1 or self.event_sequence < 1:
            raise ValueError("runtime event sequences must be positive")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("runtime event time must be timezone-aware")
        validate_runtime_detail_bounds(self.detail, validate_encoded_size=False)
        frozen_detail = _freeze_runtime_detail(self.kind, self.detail)
        try:
            encoded_detail = json.dumps(
                frozen_detail,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("runtime event detail must be JSON-compatible") from exc
        if len(encoded_detail) > _MAX_DETAIL_BYTES:
            raise ValueError("runtime event detail JSON must be at most 4096 bytes")
        object.__setattr__(self, "detail", frozen_detail)


__all__ = [
    "RuntimeDeadlineProfile",
    "RuntimeEvent",
    "RuntimeEventKind",
    "RuntimeProgress",
    "validate_runtime_detail_bounds",
]
