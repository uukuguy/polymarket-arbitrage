"""Typed, dependency-free boundary objects for the M1 control plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class JobState(StrEnum):
    """States allowed by the durable M1 jobs state machine."""

    RUNNABLE = "runnable"
    LEASED = "leased"
    RETRYABLE = "retryable"
    CHECKPOINTED = "checkpointed"
    SUCCEEDED = "succeeded"
    QUARANTINED = "quarantined"


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
