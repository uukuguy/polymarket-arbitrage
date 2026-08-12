"""Typed, dependency-free boundary objects for the M1 control plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256


class JobState(StrEnum):
    """States allowed by the durable M1 jobs state machine."""

    RUNNABLE = "runnable"
    LEASED = "leased"
    RETRYABLE = "retryable"
    CHECKPOINTED = "checkpointed"
    SUCCEEDED = "succeeded"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class QuoteBatchLeg:
    """Frozen market identity needed to turn a token book into a Quote row."""

    neg_risk_market_id: str
    market_id: str
    condition_id: str
    slug: str | None
    yes_token_id: str
    event_id: str = ""
    membership_hash: str = ""

    def __post_init__(self) -> None:
        for field in ("neg_risk_market_id", "market_id", "condition_id", "yes_token_id"):
            _require_identity(getattr(self, field), field)
        if self.slug is not None and not self.slug.strip():
            raise ValueError("slug must be non-empty when present")


@dataclass(frozen=True, slots=True)
class QuoteBatchReceipt:
    """Authenticated result of one immutable Quote range."""

    job_key: str
    quote_digest: str
    artifact_key: str
    artifact_digest: str
    successful_response_count: int

    def __post_init__(self) -> None:
        _require_identity(self.job_key, "job_key")
        for field in ("quote_digest", "artifact_digest"):
            if len(getattr(self, field)) != 64:
                raise ValueError(f"{field} must be a sha256 digest")
        _require_identity(self.artifact_key, "artifact_key")
        if self.successful_response_count < 0:
            raise ValueError("successful_response_count must be non-negative")


@dataclass(frozen=True, slots=True)
class StructureRangeSpec:
    """One frozen key range extracted from an immutable Structure bundle."""

    bundle_key: str
    bundle_digest: str
    component: str
    ordinal: int
    range_start: str
    range_end: str
    range_digest: str

    @classmethod
    def create(
        cls,
        *,
        bundle_key: str,
        bundle_digest: str,
        component: str,
        ordinal: int,
        range_start: str,
        range_end: str,
    ) -> StructureRangeSpec:
        if not bundle_key or component not in {
            "events", "event_tags", "memberships", "group_truth", "markets", "issues"
        }:
            raise ValueError("invalid Structure range identity")
        if len(bundle_digest) != 64 or ordinal < 0:
            raise ValueError("invalid Structure range digest or ordinal")
        if range_end and range_start >= range_end:
            raise ValueError("Structure range end must follow its start")
        range_digest = sha256(
            f"{component}\n{range_start}\n{range_end}".encode()
        ).hexdigest()
        return cls(
            bundle_key=bundle_key,
            bundle_digest=bundle_digest,
            component=component,
            ordinal=ordinal,
            range_start=range_start,
            range_end=range_end,
            range_digest=range_digest,
        )

    @property
    def generation_key(self) -> str:
        return f"structure:{self.bundle_digest}"

    @property
    def job_key(self) -> str:
        return f"{self.generation_key}:normalize:{self.component}:{self.ordinal}"

    @property
    def input_identity(self) -> str:
        return f"{self.generation_key}:{self.component}:{self.ordinal}:{self.range_digest}"


@dataclass(frozen=True, slots=True)
class QuoteBatchSpec:
    """One immutable, deterministic Quote token range."""

    structure_receipt_digest: str
    universe_hash: str
    ordinal: int
    token_ids: tuple[str, ...]
    token_range_digest: str
    legs: tuple[QuoteBatchLeg, ...] = ()

    @classmethod
    def from_tokens(
        cls,
        *,
        structure_receipt_digest: str,
        universe_hash: str,
        ordinal: int,
        token_ids: tuple[str, ...],
    ) -> QuoteBatchSpec:
        for field, value in (
            ("structure_receipt_digest", structure_receipt_digest),
            ("universe_hash", universe_hash),
        ):
            if len(value) != 64:
                raise ValueError(f"{field} must be a sha256 digest")
        if ordinal < 0:
            raise ValueError("ordinal must be non-negative")
        normalized = tuple(sorted(set(token_ids)))
        if not normalized or any(not token_id for token_id in normalized):
            raise ValueError("token_ids must contain non-empty values")
        token_range_digest = sha256("\n".join(normalized).encode()).hexdigest()
        return cls(
            structure_receipt_digest=structure_receipt_digest,
            universe_hash=universe_hash,
            ordinal=ordinal,
            token_ids=normalized,
            token_range_digest=token_range_digest,
        )

    @classmethod
    def from_legs(
        cls,
        *,
        structure_receipt_digest: str,
        universe_hash: str,
        ordinal: int,
        legs: tuple[QuoteBatchLeg, ...],
    ) -> QuoteBatchSpec:
        """Build a range while preserving the exact market mapping for takeover."""
        by_token: dict[str, QuoteBatchLeg] = {}
        for leg in legs:
            if leg.yes_token_id in by_token:
                raise ValueError("legs must have one unambiguous entry per yes_token_id")
            by_token[leg.yes_token_id] = leg
        normalized_legs = tuple(by_token[token_id] for token_id in sorted(by_token))
        base = cls.from_tokens(
            structure_receipt_digest=structure_receipt_digest,
            universe_hash=universe_hash,
            ordinal=ordinal,
            token_ids=tuple(by_token),
        )
        return cls(
            structure_receipt_digest=base.structure_receipt_digest,
            universe_hash=base.universe_hash,
            ordinal=base.ordinal,
            token_ids=base.token_ids,
            token_range_digest=base.token_range_digest,
            legs=normalized_legs,
        )

    @property
    def generation_key(self) -> str:
        return f"quote:{self.structure_receipt_digest}"

    @property
    def job_key(self) -> str:
        return f"{self.generation_key}:batch:{self.ordinal}"

    @property
    def input_identity(self) -> str:
        return (
            f"quote:{self.structure_receipt_digest}:{self.universe_hash}:"
            f"{self.ordinal}:{self.token_range_digest}"
        )


def _require_identity(value: str, field: str) -> None:
    if not value.strip():
        raise ValueError(f"{field} must be non-empty")


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class JobLease:
    job_key: str
    job_type: str
    input_identity: str
    lease_owner: str
    lease_epoch: int
    lease_expires_at: datetime
    checkpoint_cursor: str | None
    checkpoint_digest: str | None
    state: JobState = JobState.LEASED

    def __post_init__(self) -> None:
        for field in ("job_key", "job_type", "input_identity", "lease_owner"):
            _require_identity(getattr(self, field), field)
        if self.lease_epoch < 1:
            raise ValueError("lease_epoch must be positive")
        _require_aware(self.lease_expires_at, "lease_expires_at")
        if self.state is not JobState.LEASED:
            raise ValueError("JobLease state must be leased")


@dataclass(frozen=True, slots=True)
class CheckpointReceipt:
    receipt_id: str
    job_key: str
    lease_epoch: int
    idempotency_key: str
    checkpoint_cursor: str
    checkpoint_digest: str
    committed_at: datetime

    def __post_init__(self) -> None:
        for field in (
            "receipt_id",
            "job_key",
            "idempotency_key",
            "checkpoint_cursor",
            "checkpoint_digest",
        ):
            _require_identity(getattr(self, field), field)
        if self.lease_epoch < 1:
            raise ValueError("lease_epoch must be positive")
        _require_aware(self.committed_at, "committed_at")
