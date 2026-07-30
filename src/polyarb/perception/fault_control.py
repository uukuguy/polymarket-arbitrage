"""Pure typed contracts for dormant upstream fault control.

This module deliberately has no persistence or web-framework dependencies.
Invalid or unavailable control state must never prevent the real data-plane
call from running.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any
from uuid import UUID

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RELEASE_RE = re.compile(r"^[0-9a-f]{40}$")
_FAULT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_GROUP_TARGET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SUPERVISOR_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CALL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_INCIDENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COVERAGE_ID_RE = re.compile(r"^coverage-[0-9a-f]{64}$")
_CONTAINMENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CLEANUP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RECOVERY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_VERDICT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_MACHINE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COMPONENTS = frozenset({"candidate", "discovery", "reconciliation", "notification"})
_SENSITIVE_MARKERS = (
    "authorization",
    "body",
    "cookie",
    "header",
    "password",
    "response",
    "secret",
    "token",
)


class FaultCallClass(StrEnum):
    GAMMA_DISCOVERY_EVENT_PAGE = "gamma-discovery-event-page"
    GAMMA_RECONCILIATION_EVENT_PAGE = "gamma-reconciliation-event-page"
    CLOB_CANDIDATE_BOOK_BATCH = "clob-candidate-book-batch"
    TELEGRAM_OPPORTUNITY_CARD = "telegram-opportunity-card"


class FaultKind(StrEnum):
    GAMMA_TIMEOUT = "gamma-timeout"
    GAMMA_PARTIAL = "gamma-partial"
    GAMMA_MALFORMED = "gamma-malformed"
    GAMMA_CURSOR = "gamma-cursor"
    CLOB_MISSING_LEG = "clob-missing-leg"
    CLOB_429 = "clob-429"
    CLOB_LATENCY = "clob-latency"
    TELEGRAM_FAILURE = "telegram-failure"


class FaultRecoveryWriter(StrEnum):
    DISCOVERY_BATCH = "discovery-batch"
    RECONCILIATION_CHECKPOINT = "reconciliation-checkpoint"
    CANDIDATE_SUCCESS = "candidate-success"
    TELEGRAM_DELIVERY = "telegram-delivery"


class FaultEventState(StrEnum):
    AUTHORIZED = "authorized"
    ARMED = "armed"
    INJECTED = "injected"
    DETECTED = "detected"
    CONTAINED = "contained"
    RECOVERED = "recovered"
    CLEANED = "cleaned"
    VERIFIED = "verified"
    REJECTED = "rejected"
    EXPIRED = "expired"
    ABANDONED = "abandoned"
    CLEANUP_FAILED = "cleanup-failed"
    RECOVERY_TIMEOUT = "recovery-timeout"
    EVIDENCE_INVALID = "evidence-invalid"
    ESCALATED = "escalated"


class FaultEventAction(StrEnum):
    CLEANUP_REQUESTED = "cleanup-requested"
    CLEANUP_CONFIRMED = "cleanup-confirmed"


FAULT_CALL_CLASS_BY_KIND: Mapping[FaultKind, FaultCallClass] = {
    FaultKind.GAMMA_TIMEOUT: FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
    FaultKind.GAMMA_PARTIAL: FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
    FaultKind.GAMMA_MALFORMED: FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
    FaultKind.GAMMA_CURSOR: FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
    FaultKind.CLOB_MISSING_LEG: FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
    FaultKind.CLOB_429: FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
    FaultKind.CLOB_LATENCY: FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
    FaultKind.TELEGRAM_FAILURE: FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
}

FAULT_COMPONENT_BY_CALL_CLASS: Mapping[FaultCallClass, str] = {
    FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE: "discovery",
    FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE: "reconciliation",
    FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH: "candidate",
    FaultCallClass.TELEGRAM_OPPORTUNITY_CARD: "notification",
}

FAULT_PARAMETER_RULES: Mapping[FaultKind, Mapping[str, tuple[int, int]]] = {
    FaultKind.GAMMA_TIMEOUT: {"delay_ms": (1, 30_000)},
    FaultKind.GAMMA_PARTIAL: {"keep_events": (0, 99)},
    FaultKind.GAMMA_MALFORMED: {},
    FaultKind.GAMMA_CURSOR: {},
    FaultKind.CLOB_MISSING_LEG: {"leg_index": (0, 499)},
    FaultKind.CLOB_429: {},
    FaultKind.CLOB_LATENCY: {"delay_ms": (1, 30_000)},
    FaultKind.TELEGRAM_FAILURE: {},
}


def _canonical_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value


def canonical_json(value: object) -> str:
    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def fault_call_binding_digest(
    *,
    fault_id: str,
    kind: str,
    call_class: str,
    target_key: str,
    runtime: Mapping[str, object],
    call_id: str,
) -> str:
    return canonical_digest(
        {
            "call_class": call_class,
            "call_id": call_id,
            "fault_id": fault_id,
            "kind": kind,
            "runtime": dict(runtime),
            "target_key": target_key,
        }
    )


def normalize_target(call_class: FaultCallClass, target_key: str) -> str:
    try:
        typed_class = FaultCallClass(call_class)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid-call-class") from exc
    if not isinstance(target_key, str):
        raise ValueError("invalid-target")
    target = target_key.strip()
    if not target or len(target) > 128:
        raise ValueError("invalid-target")
    lowered = target.lower()
    if (
        "://" in target
        or "/" in target
        or "?" in target
        or "=" in target
        or any(word in lowered for word in _SENSITIVE_MARKERS)
    ):
        raise ValueError("invalid-target")
    if typed_class is FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE:
        if target != "discovery":
            raise ValueError("cross-class-target")
    elif typed_class is FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE:
        if target != "reconciliation":
            raise ValueError("cross-class-target")
    elif typed_class is FaultCallClass.TELEGRAM_OPPORTUNITY_CARD:
        if not target.isdecimal():
            raise ValueError("invalid-notification-target")
    elif not _GROUP_TARGET_RE.fullmatch(target):
        raise ValueError("invalid-group-target")
    return target


def normalize_parameters(kind: FaultKind, parameters: Mapping[str, object]) -> Mapping[str, int]:
    try:
        typed_kind = FaultKind(kind)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid-fault-kind") from exc
    if not isinstance(parameters, Mapping):
        raise ValueError("invalid-parameters")
    rules = FAULT_PARAMETER_RULES[typed_kind]
    if set(parameters) != set(rules):
        raise ValueError("invalid-parameter-keys")
    normalized: dict[str, int] = {}
    for key, (minimum, maximum) in rules.items():
        value = parameters[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("invalid-parameter-value")
        if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
            raise ValueError("invalid-parameter-value")
        integer = int(value)
        if not minimum <= integer <= maximum:
            raise ValueError("parameter-out-of-bounds")
        normalized[key] = integer
    return MappingProxyType(normalized)


def _normalize_field_identifier(
    value: str,
    *,
    pattern: re.Pattern[str],
    reason: str,
) -> str:
    if not isinstance(value, str):
        raise ValueError(reason)
    normalized = value.strip()
    if not pattern.fullmatch(normalized) or any(
        marker in normalized.lower() for marker in _SENSITIVE_MARKERS
    ):
        raise ValueError(reason)
    return normalized


def normalize_fault_id(value: str) -> str:
    return _normalize_field_identifier(
        value,
        pattern=_FAULT_ID_RE,
        reason="invalid-fault-id",
    )


def normalize_fault_call_id(value: str) -> str:
    """Normalize one opaque fault-call identity without weakening event evidence."""
    return _normalize_field_identifier(
        value,
        pattern=_CALL_ID_RE,
        reason="invalid-fault-call-id",
    )


def normalize_supervisor_run_id(value: str) -> str:
    return _normalize_field_identifier(
        value,
        pattern=_SUPERVISOR_RUN_ID_RE,
        reason="invalid-supervisor-run-id",
    )


_EVIDENCE_ID_PATTERNS: Mapping[str, re.Pattern[str]] = {
    "call_id": _CALL_ID_RE,
    "incident_id": _INCIDENT_ID_RE,
    "coverage_id": _COVERAGE_ID_RE,
    "containment_id": _CONTAINMENT_ID_RE,
    "cleanup_id": _CLEANUP_ID_RE,
    "recovery_id": _RECOVERY_ID_RE,
    "verdict_id": _VERDICT_ID_RE,
    "call_binding_digest": _DIGEST_RE,
    "memory_cleared_at_ms": re.compile(r"(?:0|[1-9][0-9]{0,15})"),
    "receipt_persisted_at_ms": re.compile(r"(?:0|[1-9][0-9]{0,15})"),
}


_EVIDENCE_KEYS: Mapping[FaultEventState, frozenset[str]] = {
    FaultEventState.AUTHORIZED: frozenset({"reason"}),
    FaultEventState.ARMED: frozenset({"runtime_identity_digest", "ownership_digest"}),
    FaultEventState.INJECTED: frozenset({"call_id", "call_binding_digest"}),
    FaultEventState.DETECTED: frozenset({"incident_id", "coverage_id"}),
    FaultEventState.CONTAINED: frozenset({"containment_id"}),
    FaultEventState.CLEANED: frozenset(
        {"cleanup_id", "memory_cleared_at_ms", "receipt_persisted_at_ms"}
    ),
    FaultEventState.RECOVERED: frozenset({"recovery_id"}),
    FaultEventState.VERIFIED: frozenset({"verdict_id", "verdict_digest"}),
    FaultEventState.REJECTED: frozenset({"reason"}),
    FaultEventState.EXPIRED: frozenset({"reason"}),
    FaultEventState.ABANDONED: frozenset({"reason"}),
    FaultEventState.CLEANUP_FAILED: frozenset({"reason"}),
    FaultEventState.RECOVERY_TIMEOUT: frozenset({"reason"}),
    FaultEventState.EVIDENCE_INVALID: frozenset({"reason"}),
    FaultEventState.ESCALATED: frozenset({"reason"}),
}
_EVIDENCE_REASONS: Mapping[FaultEventState, frozenset[str]] = {
    FaultEventState.AUTHORIZED: frozenset({"accepted"}),
    FaultEventState.REJECTED: frozenset(
        {
            "fault-already-active",
            "nonce-replay",
            "runtime-mismatch",
            "runtime-unavailable",
        }
    ),
    FaultEventState.EXPIRED: frozenset({"intent-expired"}),
    FaultEventState.ABANDONED: frozenset({"runtime-replaced", "process-relinquished"}),
    FaultEventState.CLEANUP_FAILED: frozenset({"cleanup-failed"}),
    FaultEventState.RECOVERY_TIMEOUT: frozenset({"recovery-timeout"}),
    FaultEventState.EVIDENCE_INVALID: frozenset({"evidence-invalid"}),
    FaultEventState.ESCALATED: frozenset({"escalated"}),
}


def normalize_evidence(
    state: FaultEventState,
    evidence: Mapping[str, object],
) -> Mapping[str, str]:
    typed_state = FaultEventState(state)
    if not isinstance(evidence, Mapping) or not set(evidence) <= _EVIDENCE_KEYS[typed_state]:
        raise ValueError("invalid-evidence")
    normalized: dict[str, str] = {}
    for key, value in evidence.items():
        if not isinstance(value, str):
            raise ValueError("invalid-evidence")
        if key.endswith("_digest"):
            _validate_digest(value, "invalid-evidence")
            normalized[key] = value
        elif key == "reason":
            if value not in _EVIDENCE_REASONS[typed_state]:
                raise ValueError("invalid-evidence")
            normalized[key] = value
        else:
            normalized[key] = _normalize_field_identifier(
                value,
                pattern=_EVIDENCE_ID_PATTERNS[key],
                reason="invalid-evidence",
            )
    if typed_state in {
        FaultEventState.ARMED,
        FaultEventState.INJECTED,
        FaultEventState.CLEANED,
        FaultEventState.VERIFIED,
    } and set(normalized) != _EVIDENCE_KEYS[typed_state]:
        raise ValueError("invalid-evidence")
    if typed_state is FaultEventState.DETECTED and len(normalized) != 1:
        raise ValueError("invalid-evidence")
    return MappingProxyType(normalized)


def _validate_digest(value: str, reason: str) -> None:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError(reason)


@dataclass(frozen=True, slots=True)
class FaultRuntimeIdentity:
    component: str
    release_id: str
    machine_id: str
    boot_id: UUID

    def __post_init__(self) -> None:
        if self.component not in _COMPONENTS:
            raise ValueError("invalid-component")
        if not isinstance(self.release_id, str) or not _RELEASE_RE.fullmatch(self.release_id):
            raise ValueError("invalid-release-id")
        if not isinstance(self.machine_id, str) or not _MACHINE_RE.fullmatch(self.machine_id):
            raise ValueError("invalid-machine-id")
        if not isinstance(self.boot_id, UUID) or self.boot_id.version != 4:
            raise ValueError("invalid-boot-id")


@dataclass(frozen=True, slots=True)
class FaultRecoveryReceipt:
    fault_id: str
    kind: FaultKind
    call_class: FaultCallClass
    component: str
    runtime: FaultRuntimeIdentity
    writer: FaultRecoveryWriter
    writer_id: int | str
    writer_occurred_at_ms: int

    def __post_init__(self) -> None:
        try:
            fault_id = normalize_fault_id(self.fault_id)
            kind = FaultKind(self.kind)
            call_class = FaultCallClass(self.call_class)
            writer = FaultRecoveryWriter(self.writer)
        except (TypeError, ValueError) as error:
            raise ValueError("invalid-recovery-receipt") from error
        if (
            self.component not in _COMPONENTS
            or not isinstance(self.runtime, FaultRuntimeIdentity)
            or self.runtime.component != self.component
            or isinstance(self.writer_occurred_at_ms, bool)
            or not isinstance(self.writer_occurred_at_ms, int)
            or self.writer_occurred_at_ms < 0
        ):
            raise ValueError("invalid-recovery-receipt")
        if writer in {
            FaultRecoveryWriter.DISCOVERY_BATCH,
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
        }:
            if (
                isinstance(self.writer_id, bool)
                or not isinstance(self.writer_id, int)
                or self.writer_id < 1
            ):
                raise ValueError("invalid-recovery-receipt")
        elif not isinstance(self.writer_id, str) or not _RECOVERY_ID_RE.fullmatch(self.writer_id):
            raise ValueError("invalid-recovery-receipt")
        object.__setattr__(self, "fault_id", fault_id)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "call_class", call_class)
        object.__setattr__(self, "writer", writer)


@dataclass(frozen=True, slots=True)
class FaultIntentRequest:
    fault_id: str
    kind: FaultKind
    call_class: FaultCallClass
    target_key: str
    parameters: Mapping[str, object]
    ttl_ms: int
    runtime: FaultRuntimeIdentity

    def __post_init__(self) -> None:
        object.__setattr__(self, "fault_id", normalize_fault_id(self.fault_id))
        kind = FaultKind(self.kind)
        call_class = FaultCallClass(self.call_class)
        if FAULT_CALL_CLASS_BY_KIND[kind] is not call_class:
            raise ValueError("fault-call-class-mismatch")
        if FAULT_COMPONENT_BY_CALL_CLASS[call_class] != self.runtime.component:
            raise ValueError("component-call-class-mismatch")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "call_class", call_class)
        object.__setattr__(self, "target_key", normalize_target(call_class, self.target_key))
        object.__setattr__(self, "parameters", normalize_parameters(kind, self.parameters))
        if isinstance(self.ttl_ms, bool) or not isinstance(self.ttl_ms, int):
            raise ValueError("invalid-ttl")
        if not 1_000 <= self.ttl_ms <= 120_000:
            raise ValueError("invalid-ttl")


@dataclass(frozen=True, slots=True)
class FaultAuthorization:
    nonce_digest: str
    authorization_digest: str

    def __post_init__(self) -> None:
        _validate_digest(self.nonce_digest, "invalid-nonce-digest")
        _validate_digest(self.authorization_digest, "invalid-authorization-digest")


@dataclass(frozen=True, slots=True)
class FaultIntent(FaultIntentRequest):
    nonce_digest: str
    accepted_at_ms: int
    ownership_capability: FaultOwnershipCapability | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        super(FaultIntent, self).__post_init__()
        _validate_digest(self.nonce_digest, "invalid-nonce-digest")
        if (
            isinstance(self.accepted_at_ms, bool)
            or not isinstance(self.accepted_at_ms, int)
            or self.accepted_at_ms < 0
        ):
            raise ValueError("invalid-accepted-at")
        if self.ownership_capability is not None:
            if self.ownership_capability.fault_id != self.fault_id:
                raise ValueError("ownership-fault-mismatch")
            if self.ownership_capability.runtime != self.runtime:
                raise ValueError("ownership-runtime-mismatch")


@dataclass(frozen=True, slots=True)
class FaultOwnershipCapability:
    fault_id: str
    runtime: FaultRuntimeIdentity
    token: str = field(repr=False)

    def __post_init__(self) -> None:
        normalize_fault_id(self.fault_id)
        _validate_digest(self.token, "invalid-ownership-token")


@dataclass(frozen=True, slots=True)
class FaultCall:
    call_class: FaultCallClass
    target_key: str

    def __post_init__(self) -> None:
        call_class = FaultCallClass(self.call_class)
        object.__setattr__(self, "call_class", call_class)
        object.__setattr__(self, "target_key", normalize_target(call_class, self.target_key))


@dataclass(frozen=True, slots=True)
class ActiveFault:
    intent: FaultIntent
    claimed_at_ms: int
    expires_monotonic: float
    consumed: bool = False


@dataclass(frozen=True, slots=True)
class FaultDecision:
    inject: bool
    fault_id: str | None = None
    kind: FaultKind | None = None
    parameters: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class IntentAdmission:
    fault_id: str
    accepted: bool
    reason: str


@dataclass(frozen=True, slots=True)
class FaultEvent:
    event_id: int
    fault_id: str
    sequence: int
    state: FaultEventState | None
    action: FaultEventAction | None
    occurred_at_ms: int
    evidence: Mapping[str, object]
    previous_hash: str
    event_hash: str


@dataclass(frozen=True, slots=True)
class FaultHistory:
    fault_id: str
    valid: bool
    reason: str
    intent: FaultIntent | None
    events: tuple[FaultEvent, ...]


@dataclass(frozen=True, slots=True)
class FaultProjection:
    fault_id: str
    available: bool
    active: bool
    state: FaultEventState | None
    reason: str
    intent: FaultIntent | None = None


@dataclass(frozen=True, slots=True)
class FaultAuthoritySnapshot:
    available: bool
    reason: str
    runtime: FaultRuntimeIdentity | None = None
    projection: FaultProjection | None = None
    history: FaultHistory | None = None


class FaultController:
    """Single-process, single-use in-memory admission controller."""

    def __init__(
        self,
        *,
        runtime: FaultRuntimeIdentity,
        monotonic: Callable[[], float],
    ) -> None:
        self.runtime = runtime
        self._monotonic = monotonic
        self._active: ActiveFault | None = None
        self._frozen = False

    @property
    def active(self) -> ActiveFault | None:
        return self._active

    @property
    def frozen(self) -> bool:
        return self._frozen

    def admit(self, intent: FaultIntent, *, claimed_at_ms: int) -> None:
        if self._frozen:
            raise RuntimeError("fault-admission-frozen")
        if self._active is not None:
            raise RuntimeError("fault-already-active")
        if not isinstance(intent, FaultIntent) or intent.runtime != self.runtime:
            raise ValueError("runtime-mismatch")
        if (
            isinstance(claimed_at_ms, bool)
            or not isinstance(claimed_at_ms, int)
            or claimed_at_ms < 0
        ):
            raise ValueError("invalid-claimed-at")
        elapsed_ms = claimed_at_ms - intent.accepted_at_ms
        remaining_ms = intent.ttl_ms - elapsed_ms
        if elapsed_ms < 0:
            raise ValueError("claim-before-acceptance")
        if remaining_ms <= 0:
            raise ValueError("intent-expired")
        self._active = ActiveFault(
            intent=intent,
            claimed_at_ms=claimed_at_ms,
            expires_monotonic=self._monotonic() + remaining_ms / 1_000,
        )

    def consume(self, call: FaultCall) -> FaultDecision:
        if not isinstance(call, FaultCall):
            return FaultDecision(False)
        active = self._active
        if active is None or active.consumed or self._monotonic() >= active.expires_monotonic:
            return FaultDecision(False)
        intent = active.intent
        if call.call_class is not intent.call_class or call.target_key != intent.target_key:
            return FaultDecision(False)
        self._active = ActiveFault(
            intent=active.intent,
            claimed_at_ms=active.claimed_at_ms,
            expires_monotonic=active.expires_monotonic,
            consumed=True,
        )
        return FaultDecision(
            True,
            intent.fault_id,
            intent.kind,
            MappingProxyType(dict(intent.parameters)),
        )

    async def execute(
        self,
        call: object,
        real_call: Callable[[], Awaitable[Any] | Any],
    ) -> Any:
        """Validate control input fail-open and execute the supplied real call."""
        try:
            self.consume(call)  # adapters introduced later interpret decisions
        except Exception:
            pass
        result = real_call()
        return await result if inspect.isawaitable(result) else result

    def clear(self, fault_id: str, *, receipt_writer: Callable[[str], None]) -> None:
        active = self._active
        if active is None or active.intent.fault_id != fault_id:
            raise ValueError("fault-not-active")
        self._active = None
        try:
            receipt_writer(fault_id)
        except BaseException:
            self._frozen = True
            raise
