"""Pure rolling qualification policy for the M1 runtime.

The qualification engine is deliberately a small virtual-time state machine.
It consumes already durable facts and never reads a clock, a database, or a
service.  This makes a qualification decision replayable from an old evidence
stream and, importantly, keeps a broken epoch immutable while a new epoch is
opened after recovery.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any, Final, Self


class QualificationState(StrEnum):
    """Lifecycle of one qualification epoch."""

    ACCUMULATING = "accumulating"
    INVALIDATED = "invalidated"
    RECOVERING = "recovering"
    QUALIFIED = "qualified"


BREAKING_REASONS: Final[frozenset[str]] = frozenset(
    {
        "fence.mutated-stale",
        "integrity.conflict",
        "progress.regressed",
        "identity.policy",
        "identity.release",
        "identity.config",
        "identity.role",
    }
)

BLOCKING_REASONS: Final[frozenset[str]] = frozenset(
    {
        "lease.expired",
        "freshness.structure",
        "freshness.quote",
        "freshness.opportunity",
        "evidence.gap",
        "incident.p1-slo",
        "recovery.human-intervention",
        "recovery.signature-budget",
        "recovery.slo",
    }
)
_HARD_BLOCKING_REASONS: Final[frozenset[str]] = frozenset(
    {
        "evidence.gap",
        "incident.p1-slo",
        "recovery.human-intervention",
        "recovery.signature-budget",
        "recovery.slo",
    }
)

CONTAINED_REASONS: Final[frozenset[str]] = frozenset(
    {
        "recovery.heartbeat",
        "recovery.retry",
        "recovery.reclaim",
        "recovery.machine-replacement",
        "recovery.process-replacement",
        "recovery.circuit-probe",
    }
)

_NON_BREAKING_REASONS: Final[frozenset[str]] = frozenset(
    {
        "healthy",
        "observation.healthy",
        "progress",
        "recovery.started",
        "recovery.confirmed",
    }
)
_KNOWN_REASONS: Final[frozenset[str]] = (
    BREAKING_REASONS | BLOCKING_REASONS | CONTAINED_REASONS | _NON_BREAKING_REASONS
)
_REASON_ALIASES: Final[dict[str, str]] = {
    "ok": "healthy",
    "observation.ok": "healthy",
    "policy.changed": "identity.policy",
    "policy.version-changed": "identity.policy",
    "policy-version-change": "identity.policy",
    "release.changed": "identity.release",
    "release.change": "identity.release",
    "config.changed": "identity.config",
    "config.change": "identity.config",
    "incident.p1": "incident.p1-slo",
    "progress.count-regressed": "progress.regressed",
    "recovery.signature-budget-exceeded": "recovery.signature-budget",
    "recovery.process-restart": "recovery.process-replacement",
    "recovery.confirmation": "recovery.confirmed",
}
_FRESHNESS_PRODUCTS: Final[frozenset[str]] = frozenset({"structure", "quote", "opportunity"})


class QualificationError(ValueError):
    """Base class for malformed or non-replayable qualification input."""


class QualificationFactConflict(QualificationError):
    """The same fact ID was replayed with different immutable contents."""


class QualificationTerminalError(QualificationError):
    """A new fact attempted to mutate a sealed or invalidated epoch."""


def _aware(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise QualificationError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _non_negative(value: int, *, field: str) -> None:
    if type(value) is not int or value < 0:
        raise QualificationError(f"{field} must be a non-negative integer")


def _positive(value: int, *, field: str) -> None:
    if type(value) is not int or value <= 0:
        raise QualificationError(f"{field} must be a positive integer")


def _identity_tuple(value: str | Sequence[str] | None, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    else:
        values = tuple(value)
    if not values or any(type(item) is not str or not item for item in values):
        raise QualificationError(f"{field} must contain non-empty strings")
    if len(set(values)) != len(values):
        raise QualificationError(f"{field} must not contain duplicates")
    return tuple(sorted(values))


def _digest_payload(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _normal_reason(value: str) -> str:
    if type(value) is not str or not value:
        raise QualificationError("reason must be a known non-empty string")
    normalized = _REASON_ALIASES.get(value, value)
    if normalized not in _KNOWN_REASONS:
        raise QualificationError(f"unknown qualification reason: {value}")
    return normalized


@dataclass(frozen=True, slots=True)
class QualificationFact:
    """One immutable, bounded fact consumed by :class:`RollingQualificationPolicy`.

    A fact is intentionally richer than a reason string.  Runtime recovery can
    therefore remain contained only when its measured duration and postcondition
    are present in the same replay input.  Optional identity fields mean a
    producer can omit values that are unchanged; a supplied value is always
    checked against the epoch identity.
    """

    fact_id: str
    observed_at: datetime
    reason: str = "healthy"
    policy_version: str | None = None
    release_id: str | None = None
    config_id: str | None = None
    role_identity: str | tuple[str, ...] | None = None
    epoch_id: str | None = None
    signature: str | None = None
    progress_count: int | None = None
    successful_count: int | None = None
    count: int | None = None
    evidence_gap_seconds: int | None = None
    freshness_seconds: int | None = None
    freshness_slo_seconds: int | None = None
    freshness_product: str | None = None
    recovery_duration_seconds: int | None = None
    recovery_slo_seconds: int | None = None
    recovery_confirmed: bool = False
    resolved: bool = True
    evidence_complete: bool = True

    def __post_init__(self) -> None:
        if type(self.fact_id) is not str or not self.fact_id:
            raise QualificationError("fact_id must be a non-empty string")
        object.__setattr__(self, "observed_at", _aware(self.observed_at, field="observed_at"))
        object.__setattr__(self, "reason", _normal_reason(self.reason))
        for field in ("policy_version", "release_id", "config_id", "epoch_id", "signature"):
            value = getattr(self, field)
            if value is not None and (type(value) is not str or not value):
                raise QualificationError(f"{field} must be a non-empty string when provided")
        if self.role_identity is not None:
            object.__setattr__(
                self,
                "role_identity",
                _identity_tuple(self.role_identity, field="role_identity"),
            )
        for field in (
            "progress_count",
            "successful_count",
            "count",
            "evidence_gap_seconds",
            "freshness_seconds",
            "freshness_slo_seconds",
            "recovery_duration_seconds",
            "recovery_slo_seconds",
        ):
            value = getattr(self, field)
            if value is not None:
                _non_negative(value, field=field)
        for field in ("recovery_confirmed", "resolved", "evidence_complete"):
            if type(getattr(self, field)) is not bool:
                raise QualificationError(f"{field} must be a boolean")
        if self.freshness_product is not None:
            if self.freshness_product not in _FRESHNESS_PRODUCTS:
                raise QualificationError("freshness_product is not a supported data product")
        if self.reason.startswith("freshness."):
            product = self.reason.partition(".")[2]
            if product not in _FRESHNESS_PRODUCTS:
                raise QualificationError("freshness reason is not a supported data product")
        if self.reason in CONTAINED_REASONS:
            if self.recovery_duration_seconds is None or self.recovery_slo_seconds is None:
                raise QualificationError(
                    "contained recovery facts require duration and recovery SLO"
                )
        if self.reason == "recovery.confirmed" and not self.recovery_confirmed:
            raise QualificationError("recovery.confirmed requires recovery_confirmed=True")

    @property
    def digest(self) -> str:
        """Return a deterministic digest used for duplicate-fact fencing."""

        return _digest_payload(
            {
                "fact_id": self.fact_id,
                "observed_at": self.observed_at.isoformat(),
                "reason": self.reason,
                "policy_version": self.policy_version,
                "release_id": self.release_id,
                "config_id": self.config_id,
                "role_identity": self.role_identity,
                "epoch_id": self.epoch_id,
                "signature": self.signature,
                "progress_count": self.progress_count,
                "successful_count": self.successful_count,
                "count": self.count,
                "evidence_gap_seconds": self.evidence_gap_seconds,
                "freshness_seconds": self.freshness_seconds,
                "freshness_slo_seconds": self.freshness_slo_seconds,
                "freshness_product": self.freshness_product,
                "recovery_duration_seconds": self.recovery_duration_seconds,
                "recovery_slo_seconds": self.recovery_slo_seconds,
                "recovery_confirmed": self.recovery_confirmed,
                "resolved": self.resolved,
                "evidence_complete": self.evidence_complete,
            }
        )

    @classmethod
    def healthy(cls, fact_id: str, observed_at: datetime, **kwargs: Any) -> Self:
        return cls(fact_id=fact_id, observed_at=observed_at, reason="healthy", **kwargs)

    @classmethod
    def breaking(cls, fact_id: str, observed_at: datetime, reason: str, **kwargs: Any) -> Self:
        return cls(fact_id=fact_id, observed_at=observed_at, reason=reason, **kwargs)

    @classmethod
    def contained_recovery(
        cls,
        fact_id: str,
        observed_at: datetime,
        *,
        reason: str = "recovery.retry",
        **kwargs: Any,
    ) -> Self:
        return cls(fact_id=fact_id, observed_at=observed_at, reason=reason, **kwargs)


def qualification_fact_payload(fact: QualificationFact) -> dict[str, object]:
    """Return the canonical JSON-safe persistence payload for one fact."""
    if type(fact) is not QualificationFact:
        raise TypeError("fact must be QualificationFact")
    return {
        "fact_id": fact.fact_id,
        "observed_at": fact.observed_at.isoformat(),
        "reason": fact.reason,
        "policy_version": fact.policy_version,
        "release_id": fact.release_id,
        "config_id": fact.config_id,
        "role_identity": None if fact.role_identity is None else list(fact.role_identity),
        "epoch_id": fact.epoch_id,
        "signature": fact.signature,
        "progress_count": fact.progress_count,
        "successful_count": fact.successful_count,
        "count": fact.count,
        "evidence_gap_seconds": fact.evidence_gap_seconds,
        "freshness_seconds": fact.freshness_seconds,
        "freshness_slo_seconds": fact.freshness_slo_seconds,
        "freshness_product": fact.freshness_product,
        "recovery_duration_seconds": fact.recovery_duration_seconds,
        "recovery_slo_seconds": fact.recovery_slo_seconds,
        "recovery_confirmed": fact.recovery_confirmed,
        "resolved": fact.resolved,
        "evidence_complete": fact.evidence_complete,
    }


@dataclass(frozen=True, slots=True)
class QualificationDecision:
    """Immutable snapshot after applying zero or more qualification facts."""

    state: QualificationState
    epoch_id: str
    started_at: datetime
    policy_version: str
    release_id: str
    config_id: str
    role_identity: tuple[str, ...]
    last_fact_at: datetime | None = None
    invalidated_at: datetime | None = None
    invalidation_reason: str | None = None
    qualified_at: datetime | None = None
    previous_epoch_id: str | None = None
    facts: tuple[QualificationFact, ...] = ()
    fact_digests: tuple[tuple[str, str], ...] = ()
    contained_recoveries: tuple[str, ...] = ()
    max_gap_seconds: int = 0
    coverage_seconds: int = 0
    progress_count: int | None = None
    successful_count: int | None = None
    signature_counts: tuple[tuple[str, int], ...] = ()
    recovery_confirmed_at: datetime | None = None
    pending_recovery_started: bool = False
    eligibility_reason: str | None = None

    def __post_init__(self) -> None:
        if type(self.state) is not QualificationState:
            raise QualificationError("state must be QualificationState")
        for field in ("epoch_id", "policy_version", "release_id", "config_id"):
            value = getattr(self, field)
            if type(value) is not str or not value:
                raise QualificationError(f"{field} must be a non-empty string")
        object.__setattr__(self, "started_at", _aware(self.started_at, field="started_at"))
        if self.role_identity:
            object.__setattr__(
                self,
                "role_identity",
                _identity_tuple(self.role_identity, field="role_identity"),
            )
        else:
            raise QualificationError("role_identity must not be empty")
        for field in ("last_fact_at", "invalidated_at", "qualified_at", "recovery_confirmed_at"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field, _aware(value, field=field))
        if self.previous_epoch_id is not None and not self.previous_epoch_id:
            raise QualificationError("previous_epoch_id must be non-empty when provided")
        if self.invalidation_reason is not None:
            object.__setattr__(
                self,
                "invalidation_reason",
                _normal_reason(self.invalidation_reason),
            )
            if self.invalidation_reason not in BREAKING_REASONS:
                raise QualificationError("invalidation_reason must be breaking")
        if self.state is QualificationState.INVALIDATED:
            if self.invalidated_at is None or self.invalidation_reason is None:
                raise QualificationError("invalidated decision needs exact breaker metadata")
        elif self.invalidated_at is not None or self.invalidation_reason is not None:
            raise QualificationError("only invalidated decisions may carry invalidation metadata")
        for field in ("max_gap_seconds", "coverage_seconds", "progress_count", "successful_count"):
            value = getattr(self, field)
            if value is not None:
                _non_negative(value, field=field)
        if type(self.pending_recovery_started) is not bool:
            raise QualificationError("pending_recovery_started must be a boolean")
        if self.eligibility_reason is not None:
            normalized_eligibility_reason = _normal_reason(self.eligibility_reason)
            if normalized_eligibility_reason not in BLOCKING_REASONS and (
                normalized_eligibility_reason != "recovery.started"
            ):
                raise QualificationError("eligibility_reason must pause or block eligibility")
            object.__setattr__(
                self,
                "eligibility_reason",
                normalized_eligibility_reason,
            )
        facts = tuple(self.facts)
        if any(type(fact) is not QualificationFact for fact in facts):
            raise QualificationError("facts must contain QualificationFact values")
        ids = tuple(fact.fact_id for fact in facts)
        if len(set(ids)) != len(ids):
            raise QualificationError("facts must have unique IDs")
        if any(fact.observed_at < self.started_at for fact in facts):
            raise QualificationError("facts cannot precede epoch start")
        if any(first.observed_at > second.observed_at for first, second in zip(facts, facts[1:])):
            raise QualificationError("facts must be ordered")
        expected_digests = tuple((fact.fact_id, fact.digest) for fact in facts)
        if self.fact_digests and tuple(self.fact_digests) != expected_digests:
            raise QualificationError("fact digests do not match facts")
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "fact_digests", expected_digests)
        if facts:
            pending_recovery_started = False
            eligibility_reason: str | None = None
            for fact in facts:
                if fact.reason == "recovery.started" or fact.reason in BLOCKING_REASONS:
                    pending_recovery_started = True
                    eligibility_reason = fact.reason
                elif fact.reason in CONTAINED_REASONS or (
                    fact.reason == "recovery.confirmed" and fact.recovery_confirmed
                ):
                    pending_recovery_started = False
                    eligibility_reason = None
            pending_recovery_started = (
                pending_recovery_started or self.pending_recovery_started
            )
            eligibility_reason = self.eligibility_reason or eligibility_reason
            object.__setattr__(self, "pending_recovery_started", pending_recovery_started)
            object.__setattr__(self, "eligibility_reason", eligibility_reason)
        if self.pending_recovery_started != (self.eligibility_reason is not None):
            raise QualificationError(
                "pending recovery and eligibility reason must agree"
            )
        if self.last_fact_at is None and facts:
            object.__setattr__(self, "last_fact_at", facts[-1].observed_at)
        if self.state is QualificationState.QUALIFIED and self.qualified_at is None:
            raise QualificationError("qualified decision needs exact certificate boundary")
        if self.state is not QualificationState.QUALIFIED and self.qualified_at is not None:
            raise QualificationError("only qualified decisions may carry qualified_at")
        if self.state is QualificationState.RECOVERING:
            if self.previous_epoch_id is None:
                raise QualificationError("recovering decision needs previous_epoch_id")
            if (
                self.last_fact_at is not None
                or facts
                or self.contained_recoveries
                or self.max_gap_seconds != 0
                or self.coverage_seconds != 0
                or self.progress_count is not None
                or self.successful_count is not None
                or self.signature_counts
                or self.recovery_confirmed_at is not None
                or self.pending_recovery_started
                or self.eligibility_reason is not None
            ):
                raise QualificationError(
                    "recovering decision may carry only previous epoch identity"
                )
        signatures = tuple(self.signature_counts)
        if len({signature for signature, _count in signatures}) != len(signatures):
            raise QualificationError("signature counts must have unique signatures")
        for signature, count in signatures:
            if not signature:
                raise QualificationError("signature counts require non-empty signatures")
            _non_negative(count, field="signature count")
        object.__setattr__(self, "signature_counts", signatures)

    @classmethod
    def initial(
        cls,
        *,
        started_at: datetime,
        epoch_id: str,
        policy_version: str,
        release_id: str,
        config_id: str,
        role_identity: str | Sequence[str],
    ) -> Self:
        return cls(
            state=QualificationState.ACCUMULATING,
            epoch_id=epoch_id,
            started_at=started_at,
            policy_version=policy_version,
            release_id=release_id,
            config_id=config_id,
            role_identity=_identity_tuple(role_identity, field="role_identity"),
        )

    @property
    def fact_ids(self) -> tuple[str, ...]:
        return tuple(fact.fact_id for fact in self.facts)

    @property
    def identity(self) -> tuple[str, str, str, tuple[str, ...]]:
        return (self.policy_version, self.release_id, self.config_id, self.role_identity)

    @property
    def status(self) -> str:
        return self.state.value

    @property
    def is_terminal(self) -> bool:
        return self.state in {QualificationState.INVALIDATED, QualificationState.QUALIFIED}

    @property
    def certificate_eligible(self) -> bool:
        return self.state is QualificationState.QUALIFIED

    @property
    def eligibility_state(self) -> str:
        if self.state is QualificationState.INVALIDATED:
            return "invalidated"
        if self.state is QualificationState.RECOVERING:
            return "blocked"
        if self.state is QualificationState.QUALIFIED:
            return "qualified"
        if self.eligibility_reason in _HARD_BLOCKING_REASONS:
            return "blocked"
        if self.pending_recovery_started:
            return "paused"
        return "eligible"


class RollingQualificationPolicy:
    """Apply ordered facts to an immutable virtual-time qualification epoch."""

    DEFAULT_POLICY_VERSION: Final[str] = "m1-rolling-qualification-v2"

    def __init__(
        self,
        *,
        policy_version: str = DEFAULT_POLICY_VERSION,
        release_id: str = "release-unknown",
        config_id: str = "config-unknown",
        role_identity: str | Sequence[str] = ("m1",),
        required_seconds: int = 86_400,
        max_gap_seconds: int = 900,
        signature_budget: int = 3,
        repeated_signature_budget: int | None = None,
    ) -> None:
        if type(policy_version) is not str or not policy_version:
            raise QualificationError("policy_version must be a non-empty string")
        for value, field in (
            (release_id, "release_id"),
            (config_id, "config_id"),
        ):
            if type(value) is not str or not value:
                raise QualificationError(f"{field} must be a non-empty string")
        _positive(required_seconds, field="required_seconds")
        _positive(max_gap_seconds, field="max_gap_seconds")
        _non_negative(signature_budget, field="signature_budget")
        if repeated_signature_budget is not None:
            _non_negative(repeated_signature_budget, field="repeated_signature_budget")
            if signature_budget != 3 and signature_budget != repeated_signature_budget:
                raise QualificationError("signature budget aliases disagree")
            signature_budget = repeated_signature_budget
        roles = _identity_tuple(role_identity, field="role_identity")
        if not roles:
            raise QualificationError("role_identity must not be empty")
        self.policy_version = policy_version
        self.release_id = release_id
        self.config_id = config_id
        self.role_identity = roles
        self.required_seconds = required_seconds
        self.max_gap_seconds = max_gap_seconds
        self.signature_budget = signature_budget

    def new_epoch(
        self,
        *,
        started_at: datetime,
        epoch_id: str | None = None,
        policy_version: str | None = None,
        release_id: str | None = None,
        config_id: str | None = None,
        role_identity: str | Sequence[str] | None = None,
        previous_epoch_id: str | None = None,
    ) -> QualificationDecision:
        started = _aware(started_at, field="started_at")
        selected_policy = policy_version or self.policy_version
        selected_release = release_id or self.release_id
        selected_config = config_id or self.config_id
        selected_roles = _identity_tuple(role_identity or self.role_identity, field="role_identity")
        if not epoch_id:
            epoch_id = self._derive_epoch_id(
                started,
                selected_policy,
                selected_release,
                selected_config,
                selected_roles,
                previous_epoch_id=previous_epoch_id,
            )
        return QualificationDecision.initial(
            started_at=started,
            epoch_id=epoch_id,
            policy_version=selected_policy,
            release_id=selected_release,
            config_id=selected_config,
            role_identity=selected_roles,
        )

    initial = new_epoch
    accumulating = new_epoch

    def recovering(
        self,
        previous_epoch: str | QualificationDecision,
        *,
        started_at: datetime,
    ) -> QualificationDecision:
        """Create a fresh recovery boundary without mutating its old epoch."""

        started = _aware(started_at, field="started_at")
        if isinstance(previous_epoch, QualificationDecision):
            if previous_epoch.state is not QualificationState.INVALIDATED:
                raise QualificationError("recovery must derive from an invalidated epoch")
            previous_id = previous_epoch.epoch_id
            return QualificationDecision(
                state=QualificationState.RECOVERING,
                epoch_id=self._derive_epoch_id(
                    started,
                    previous_epoch.policy_version,
                    previous_epoch.release_id,
                    previous_epoch.config_id,
                    previous_epoch.role_identity,
                    previous_epoch_id=previous_id,
                ),
                started_at=started,
                policy_version=previous_epoch.policy_version,
                release_id=previous_epoch.release_id,
                config_id=previous_epoch.config_id,
                role_identity=previous_epoch.role_identity,
                previous_epoch_id=previous_id,
            )
        if type(previous_epoch) is not str or not previous_epoch:
            raise QualificationError("previous_epoch must be a non-empty epoch ID")
        return replace(
            self.new_epoch(
                started_at=started,
                previous_epoch_id=previous_epoch,
            ),
            state=QualificationState.RECOVERING,
            previous_epoch_id=previous_epoch,
        )

    def apply(self, state: QualificationDecision, fact: QualificationFact) -> QualificationDecision:
        """Apply exactly one fact, returning a new immutable decision.

        Existing fact IDs are replay-safe.  A same-ID/different-digest replay
        fails closed, while any new fact after a sealed epoch is rejected.
        """

        if type(state) is not QualificationDecision:
            raise QualificationError("state must be QualificationDecision")
        if type(fact) is not QualificationFact:
            raise QualificationError("fact must be QualificationFact")
        known = dict(state.fact_digests)
        previous_digest = known.get(fact.fact_id)
        if previous_digest is not None:
            if previous_digest != fact.digest:
                raise QualificationFactConflict(
                    f"fact ID {fact.fact_id!r} has conflicting contents"
                )
            return state
        if state.state is QualificationState.QUALIFIED:
            raise QualificationTerminalError("qualified epoch is immutable")
        if fact.observed_at < state.started_at:
            raise QualificationError("facts must be ordered after epoch start")
        if state.last_fact_at is not None and fact.observed_at < state.last_fact_at:
            raise QualificationError("facts must be ordered by observed_at")

        if state.state is QualificationState.INVALIDATED:
            raise QualificationTerminalError("invalidated epoch is immutable")

        if state.state is QualificationState.RECOVERING:
            if self._is_recovery_confirmation(fact):
                return self._open_recovered_epoch(state, fact)
            raise QualificationError("recovering epoch awaits recovery confirmation")

        breaking_reason = self._breaking_reason(state, fact)
        if breaking_reason is not None:
            appended = self._append_fact(state, fact)
            return replace(
                appended,
                state=QualificationState.INVALIDATED,
                invalidated_at=fact.observed_at,
                invalidation_reason=breaking_reason,
                qualified_at=None,
                pending_recovery_started=False,
                eligibility_reason=None,
            )

        blocking_reason = self._blocking_reason(state, fact)
        appended = self._append_fact(
            state,
            fact,
            blocking_reason=blocking_reason,
        )
        if blocking_reason is not None or fact.reason == "recovery.started":
            return appended
        if self._has_pending_recovery_start(state):
            if fact.reason not in CONTAINED_REASONS and not self._is_recovery_confirmation(fact):
                return appended
        if appended.coverage_seconds >= self.required_seconds and fact.evidence_complete:
            surplus = appended.coverage_seconds - self.required_seconds
            boundary = fact.observed_at - timedelta(seconds=surplus)
            return replace(
                appended,
                state=QualificationState.QUALIFIED,
                qualified_at=boundary,
                coverage_seconds=self.required_seconds,
            )
        return appended

    def apply_many(
        self,
        state: QualificationDecision,
        facts: Iterable[QualificationFact],
    ) -> QualificationDecision:
        result = state
        for fact in facts:
            result = self.apply(result, fact)
        return result

    def _breaking_reason(
        self,
        state: QualificationDecision,
        fact: QualificationFact,
    ) -> str | None:
        if fact.epoch_id is not None and fact.epoch_id != state.epoch_id:
            return "fence.mutated-stale"
        for field, expected, actual, reason in (
            ("policy_version", state.policy_version, fact.policy_version, "identity.policy"),
            ("release_id", state.release_id, fact.release_id, "identity.release"),
            ("config_id", state.config_id, fact.config_id, "identity.config"),
        ):
            if actual is not None and actual != expected:
                return reason
        if fact.role_identity is not None and tuple(fact.role_identity) != state.role_identity:
            return "identity.role"
        if fact.reason in BREAKING_REASONS:
            return fact.reason
        return None

    def _blocking_reason(
        self,
        state: QualificationDecision,
        fact: QualificationFact,
    ) -> str | None:
        if fact.reason in BLOCKING_REASONS:
            return fact.reason
        if not fact.evidence_complete:
            return "evidence.gap"
        if (
            fact.evidence_gap_seconds is not None
            and fact.evidence_gap_seconds > self.max_gap_seconds
        ):
            return "evidence.gap"
        anchor = state.last_fact_at or state.started_at
        gap = (fact.observed_at - anchor).total_seconds()
        if gap > self.max_gap_seconds:
            return "evidence.gap"
        if (
            fact.freshness_seconds is not None
            and fact.freshness_slo_seconds is not None
            and fact.freshness_seconds > fact.freshness_slo_seconds
        ):
            product = fact.freshness_product or "structure"
            return f"freshness.{product}"
        if fact.reason in CONTAINED_REASONS:
            assert fact.recovery_duration_seconds is not None
            assert fact.recovery_slo_seconds is not None
            if fact.recovery_duration_seconds > fact.recovery_slo_seconds:
                return "recovery.slo"
            if not fact.resolved:
                return "incident.p1-slo"
            signature = fact.signature
            if signature is not None:
                counts = dict(state.signature_counts)
                if counts.get(signature, 0) + 1 > self.signature_budget:
                    return "recovery.signature-budget"
        return None

    def _append_fact(
        self,
        current: QualificationDecision,
        fact: QualificationFact,
        *,
        next_state: QualificationState | None = None,
        blocking_reason: str | None = None,
    ) -> QualificationDecision:
        gap = 0.0
        gap_anchor = current.last_fact_at or current.started_at
        if gap_anchor is not None:
            gap = (fact.observed_at - gap_anchor).total_seconds()
        max_gap = max(current.max_gap_seconds, int(gap))
        coverage_delta = 0
        if not current.pending_recovery_started and blocking_reason is None:
            coverage_delta = max(0, int(gap))
            if fact.reason in CONTAINED_REASONS:
                assert fact.recovery_duration_seconds is not None
                coverage_delta = max(0, coverage_delta - fact.recovery_duration_seconds)
        coverage = current.coverage_seconds + coverage_delta
        signatures = dict(current.signature_counts)
        if fact.reason in CONTAINED_REASONS and fact.signature is not None:
            signatures[fact.signature] = signatures.get(fact.signature, 0) + 1
        contained = current.contained_recoveries
        if fact.reason in CONTAINED_REASONS:
            contained = (*contained, fact.fact_id)
        pending_recovery_started = current.pending_recovery_started
        eligibility_reason = current.eligibility_reason
        if fact.reason == "recovery.started" or blocking_reason is not None:
            pending_recovery_started = True
            eligibility_reason = blocking_reason or "recovery.started"
        elif fact.reason in CONTAINED_REASONS or self._is_recovery_confirmation(fact):
            pending_recovery_started = False
            eligibility_reason = None
        progress_count = current.progress_count
        if fact.progress_count is not None:
            progress_count = fact.progress_count
        elif fact.count is not None:
            progress_count = fact.count
        successful_count = current.successful_count
        if fact.successful_count is not None:
            successful_count = fact.successful_count
        return replace(
            current,
            state=next_state or current.state,
            last_fact_at=fact.observed_at,
            facts=(*current.facts, fact),
            fact_digests=(*current.fact_digests, (fact.fact_id, fact.digest)),
            contained_recoveries=contained,
            max_gap_seconds=max_gap,
            coverage_seconds=coverage,
            progress_count=progress_count,
            successful_count=successful_count,
            signature_counts=tuple(sorted(signatures.items())),
            pending_recovery_started=pending_recovery_started,
            eligibility_reason=eligibility_reason,
        )

    def _open_recovered_epoch(
        self,
        recovering: QualificationDecision,
        fact: QualificationFact,
    ) -> QualificationDecision:
        self._check_recovery_identity(recovering, fact)
        previous_epoch_id = recovering.previous_epoch_id or recovering.epoch_id
        new_epoch_id = self._derive_epoch_id(
            fact.observed_at,
            fact.policy_version or recovering.policy_version,
            fact.release_id or recovering.release_id,
            fact.config_id or recovering.config_id,
            fact.role_identity or recovering.role_identity,
            previous_epoch_id=previous_epoch_id,
            recovery_fact=fact,
        )
        roles = fact.role_identity if fact.role_identity is not None else recovering.role_identity
        return QualificationDecision(
            state=QualificationState.ACCUMULATING,
            epoch_id=new_epoch_id,
            started_at=fact.observed_at,
            policy_version=fact.policy_version or recovering.policy_version,
            release_id=fact.release_id or recovering.release_id,
            config_id=fact.config_id or recovering.config_id,
            role_identity=_identity_tuple(roles, field="role_identity"),
            last_fact_at=fact.observed_at,
            previous_epoch_id=previous_epoch_id,
            facts=(fact,),
            fact_digests=((fact.fact_id, fact.digest),),
            recovery_confirmed_at=fact.observed_at,
        )

    @staticmethod
    def _check_recovery_identity(
        recovering: QualificationDecision,
        fact: QualificationFact,
    ) -> None:
        for field, expected, actual in (
            ("policy_version", recovering.policy_version, fact.policy_version),
            ("release_id", recovering.release_id, fact.release_id),
            ("config_id", recovering.config_id, fact.config_id),
        ):
            if actual is not None and actual != expected:
                raise QualificationError(f"recovery confirmation identity conflict: {field}")
        if fact.role_identity is not None and fact.role_identity != recovering.role_identity:
            raise QualificationError("recovery confirmation identity conflict: role_identity")

    @staticmethod
    def _is_recovery_confirmation(fact: QualificationFact) -> bool:
        return fact.reason == "recovery.confirmed" and fact.recovery_confirmed

    @staticmethod
    def _has_pending_recovery_start(state: QualificationDecision) -> bool:
        return state.pending_recovery_started

    @staticmethod
    def _derive_epoch_id(
        started_at: datetime,
        policy_version: str,
        release_id: str,
        config_id: str,
        role_identity: Sequence[str],
        *,
        previous_epoch_id: str | None = None,
        recovery_fact: QualificationFact | None = None,
    ) -> str:
        payload: dict[str, object] = {
            "started_at": started_at.isoformat(),
            "policy_version": policy_version,
            "release_id": release_id,
            "config_id": config_id,
            "role_identity": tuple(role_identity),
            "previous_epoch_id": previous_epoch_id,
            "recovery_fact": None if recovery_fact is None else recovery_fact.digest,
        }
        return "epoch-" + _digest_payload(payload)[:24]


__all__ = [
    "BLOCKING_REASONS",
    "BREAKING_REASONS",
    "CONTAINED_REASONS",
    "QualificationDecision",
    "QualificationError",
    "QualificationFact",
    "QualificationFactConflict",
    "QualificationState",
    "QualificationTerminalError",
    "RollingQualificationPolicy",
]
