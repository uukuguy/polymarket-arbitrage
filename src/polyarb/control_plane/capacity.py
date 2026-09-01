"""Pure capacity policy shared by M1 runtime recovery surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CapacityState = Literal["healthy", "warning", "critical", "exhausted"]


@dataclass(frozen=True, slots=True)
class CapacityVerdict:
    state: CapacityState
    used_bytes: int
    budget_bytes: int
    used_percent: int
    reason_code: str


def classify_database_capacity(
    *, used_bytes: int, budget_bytes: int, provider_read_only: bool
) -> CapacityVerdict:
    """Classify one explicit database budget without provider-specific inference."""
    if used_bytes < 0 or budget_bytes <= 0:
        raise ValueError("capacity bytes must be non-negative with a positive budget")

    used_percent = used_bytes * 100 // budget_bytes
    if provider_read_only:
        state: CapacityState = "exhausted"
        reason_code = "provider-read-only"
    elif used_percent >= 85:
        state = "exhausted"
        reason_code = "budget-exhausted"
    elif used_percent >= 75:
        state = "critical"
        reason_code = "budget-critical"
    elif used_percent >= 60:
        state = "warning"
        reason_code = "budget-warning"
    else:
        state = "healthy"
        reason_code = "within-budget"
    return CapacityVerdict(
        state=state,
        used_bytes=used_bytes,
        budget_bytes=budget_bytes,
        used_percent=used_percent,
        reason_code=reason_code,
    )
