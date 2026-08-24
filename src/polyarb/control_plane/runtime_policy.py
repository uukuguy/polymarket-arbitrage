"""Pure, closed-taxonomy policy for immutable M1 runtime observations.

The sampler records facts; this module decides whether a fact breaks the
qualification contract.  It deliberately has no database, network, clock, or
mutation dependency so the exact decision can be replayed after the fact.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from .soak_evidence import SoakEvidenceError, _validated

_LEASE_EXPIRED: Final = "lease.expired"
_CIRCUIT_OPEN: Final = "circuit.open"
_MACHINE_MISSING: Final = "machine.missing"
_MACHINE_UNHEALTHY: Final = "machine.unhealthy"
_API_UNAVAILABLE: Final = "api.unavailable"
_PROGRESS_REGRESSED: Final = "progress.regressed"
_EVIDENCE_GAP: Final = "evidence.gap"

# This is a deliberately closed vocabulary.  A raw exception, response body,
# machine name, or user-controlled string must never become a reason code.
RUNTIME_REASON_CODES: Final = frozenset(
    {
        _LEASE_EXPIRED,
        _CIRCUIT_OPEN,
        _MACHINE_MISSING,
        _MACHINE_UNHEALTHY,
        _API_UNAVAILABLE,
        _PROGRESS_REGRESSED,
        _EVIDENCE_GAP,
    }
)
_REASON_ORDER: Final = (
    _LEASE_EXPIRED,
    _CIRCUIT_OPEN,
    _MACHINE_MISSING,
    _MACHINE_UNHEALTHY,
    _API_UNAVAILABLE,
    _PROGRESS_REGRESSED,
    _EVIDENCE_GAP,
)

_COUNTER_FIELDS: Final = (
    "expired_leases",
    "open_circuit_count",
    "successful_job_count",
)


@dataclass(frozen=True, slots=True)
class RuntimeRuleResult:
    """One immutable policy decision for one normalized observation."""

    observed_at: datetime
    severity: str
    breaking: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("runtime result time must be timezone-aware")
        if self.severity not in {"healthy", "breaking"}:
            raise ValueError("runtime result severity is invalid")
        if any(code not in RUNTIME_REASON_CODES for code in self.reason_codes):
            raise ValueError("runtime result contains an unknown reason code")
        if tuple(sorted(set(self.reason_codes), key=_REASON_ORDER.index)) != self.reason_codes:
            raise ValueError("runtime result reason codes must be unique and ordered")
        if self.breaking != bool(self.reason_codes) or (
            (self.severity == "breaking") != self.breaking
        ):
            raise ValueError("runtime result severity and breaking flag disagree")


@dataclass(frozen=True, slots=True)
class _NormalizedObservation:
    """Validated facts used by the pure classifier, not a public API."""

    observed_at: datetime
    control_api_status: object
    machine_states: dict[str, str]
    expired_leases: int
    open_circuit_count: int
    successful_job_count: int | None


def _utc_timestamp(value: object, *, field: str = "observed_at") -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise SoakEvidenceError(f"{field} is invalid") from error
    else:
        raise SoakEvidenceError(f"{field} is invalid")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SoakEvidenceError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _counter(value: object, field: str) -> int:
    """Accept only an exact Python integer with a non-negative value."""
    if type(value) is not int:
        raise SoakEvidenceError(f"{field} is invalid")
    parsed = value
    if parsed < 0:
        raise SoakEvidenceError(f"{field} must be non-negative")
    return parsed


def _canonical_sort_key(record: Mapping[str, object]) -> str:
    """Produce a deterministic tie-breaker for equal observation instants."""

    def default(value: object) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError(f"unsupported evidence value: {type(value).__name__}")

    try:
        return json.dumps(
            dict(record),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
            default=default,
        )
    except (TypeError, ValueError) as error:
        raise SoakEvidenceError("observation contains non-canonical evidence") from error


def _normalized_observation(record: Mapping[str, object]) -> _NormalizedObservation:
    if not isinstance(record, Mapping):
        raise SoakEvidenceError("soak observation must be an object")

    # Database-backed V2 rows carry the canonical digest.  Reuse the existing
    # strict validator when those markers are present; direct policy tests can
    # use the smaller explicit fact shape without manufacturing a digest.
    if "kind" in record or "snapshot_sha256" in record:
        if "kind" not in record or "snapshot_sha256" not in record:
            raise SoakEvidenceError("soak record shape is invalid")
        payload = _validated(record)
    else:
        payload = dict(record)

    observed_at = _utc_timestamp(payload.get("observed_at"))
    machine_states = payload.get("machine_states")
    if not isinstance(machine_states, Mapping) or not machine_states:
        raise SoakEvidenceError("machine states are invalid")
    normalized_states: dict[str, str] = {}
    for machine_id, state in machine_states.items():
        if (
            not isinstance(machine_id, str)
            or not machine_id
            or not isinstance(state, str)
            or not state
        ):
            raise SoakEvidenceError("machine states are invalid")
        normalized_states[machine_id] = state

    counters: dict[str, int] = {}
    for field in _COUNTER_FIELDS[:2]:
        if field not in payload:
            raise SoakEvidenceError(f"{field} is missing")
        counters[field] = _counter(payload[field], field)
    successful = (
        None
        if "successful_job_count" not in payload
        else _counter(payload["successful_job_count"], "successful_job_count")
    )
    return _NormalizedObservation(
        observed_at=observed_at,
        control_api_status=payload.get("control_api_status", payload.get("status")),
        machine_states=normalized_states,
        expired_leases=counters["expired_leases"],
        open_circuit_count=counters["open_circuit_count"],
        successful_job_count=successful,
    )


def _as_record(value: Mapping[str, object] | _NormalizedObservation) -> _NormalizedObservation:
    if isinstance(value, _NormalizedObservation):
        return value
    return _normalized_observation(value)


def evaluate_soak_observation(
    observation: Mapping[str, object] | _NormalizedObservation,
    baseline: Mapping[str, object] | _NormalizedObservation | None = None,
    *,
    previous: Mapping[str, object] | _NormalizedObservation | None = None,
    max_gap_seconds: float | None = None,
) -> RuntimeRuleResult:
    """Classify one observation against immutable baseline/previous facts.

    The first observation is intentionally treated as the baseline: existing
    historical runs may already contain stale leases or open circuits, and a
    replay must flag *new* degradation rather than rewriting history.  The
    ``previous`` sample is used for monotonic progress and gap checks.
    """
    current = _as_record(observation)
    reference = current if baseline is None else _as_record(baseline)
    prior = None if previous is None else _as_record(previous)
    reasons: set[str] = set()

    if current.expired_leases > reference.expired_leases:
        reasons.add(_LEASE_EXPIRED)
    if current.open_circuit_count > reference.open_circuit_count:
        reasons.add(_CIRCUIT_OPEN)
    expected_machine_ids = set(reference.machine_states)
    if set(current.machine_states) != expected_machine_ids:
        reasons.add(_MACHINE_MISSING)
    if any(state != "started" for state in current.machine_states.values()):
        reasons.add(_MACHINE_UNHEALTHY)
    if current.control_api_status != "available":
        reasons.add(_API_UNAVAILABLE)
    progress_reference = reference if prior is None else prior
    if (
        current.successful_job_count is not None
        and progress_reference.successful_job_count is not None
        and current.successful_job_count < progress_reference.successful_job_count
    ):
        reasons.add(_PROGRESS_REGRESSED)
    if max_gap_seconds is not None:
        if not math.isfinite(max_gap_seconds) or max_gap_seconds <= 0:
            raise ValueError("max_gap_seconds must be finite and positive")
        if prior is not None and (
            current.observed_at - prior.observed_at
        ).total_seconds() > max_gap_seconds:
            reasons.add(_EVIDENCE_GAP)

    ordered = tuple(code for code in _REASON_ORDER if code in reasons)
    return RuntimeRuleResult(
        observed_at=current.observed_at,
        severity="breaking" if ordered else "healthy",
        breaking=bool(ordered),
        reason_codes=ordered,
    )


__all__ = [
    "RUNTIME_REASON_CODES",
    "RuntimeRuleResult",
    "evaluate_soak_observation",
]
