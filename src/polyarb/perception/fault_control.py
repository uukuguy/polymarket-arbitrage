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
from typing import Any
from uuid import UUID

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RELEASE_RE = re.compile(r"^[0-9a-f]{40}$")
_SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_MACHINE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COMPONENTS = frozenset({"candidate", "discovery", "reconciliation", "notification"})


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


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


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
        or any(word in lowered for word in ("token", "secret", "cookie", "authorization"))
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
    elif not _SAFE_KEY_RE.fullmatch(target):
        raise ValueError("invalid-group-target")
    return target


def normalize_parameters(kind: FaultKind, parameters: Mapping[str, object]) -> dict[str, int]:
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
    return normalized


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
class FaultIntentRequest:
    fault_id: str
    kind: FaultKind
    call_class: FaultCallClass
    target_key: str
    parameters: Mapping[str, object]
    ttl_ms: int
    runtime: FaultRuntimeIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.fault_id, str) or not _SAFE_KEY_RE.fullmatch(self.fault_id):
            raise ValueError("invalid-fault-id")
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

    def __post_init__(self) -> None:
        super(FaultIntent, self).__post_init__()
        _validate_digest(self.nonce_digest, "invalid-nonce-digest")
        if (
            isinstance(self.accepted_at_ms, bool)
            or not isinstance(self.accepted_at_ms, int)
            or self.accepted_at_ms < 0
        ):
            raise ValueError("invalid-accepted-at")


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
    parameters: Mapping[str, int] = field(default_factory=dict)


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
    state: FaultEventState
    action: str | None
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
        self._active = ActiveFault(
            intent=intent,
            claimed_at_ms=claimed_at_ms,
            expires_monotonic=self._monotonic() + intent.ttl_ms / 1_000,
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
        return FaultDecision(True, intent.fault_id, intent.kind, dict(intent.parameters))

    async def execute(
        self,
        call: object,
        real_call: Callable[[], Awaitable[Any] | Any],
    ) -> Any:
        """Validate control input fail-open and execute the supplied real call."""
        try:
            self.consume(call)  # adapters introduced later interpret decisions
        except (TypeError, ValueError, RuntimeError):
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
