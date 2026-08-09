"""Deterministic capacity-watermark policy for M1 resident maintenance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CapacityState = Literal["normal", "pressure", "critical", "exhaustion-imminent"]


@dataclass(frozen=True)
class CapacityPolicy:
    pressure_free_percent: float
    critical_free_percent: float
    exhaustion_free_percent: float
    recovery_hold_ms: int

    def __post_init__(self) -> None:
        if not (
            0.0 < self.exhaustion_free_percent < self.critical_free_percent
            < self.pressure_free_percent < 100.0
        ):
            raise ValueError("invalid-capacity-watermarks")
        if self.recovery_hold_ms < 0:
            raise ValueError("invalid-capacity-recovery-hold")

    def transition(
        self,
        previous: CapacityState | None,
        *,
        previous_state_started_at_ms: int | None = None,
        last_recovery_receipt_at_ms: int | None = None,
        free_percent: float,
        now_ms: int,
    ) -> CapacityState:
        if not 0.0 <= free_percent <= 100.0:
            raise ValueError("invalid-capacity-free-percent")
        if now_ms < 0:
            raise ValueError("invalid-capacity-time")
        if (
            last_recovery_receipt_at_ms is not None
            and (
                type(last_recovery_receipt_at_ms) is not int
                or last_recovery_receipt_at_ms < 0
                or last_recovery_receipt_at_ms > now_ms
            )
        ):
            raise ValueError("invalid-capacity-recovery-receipt")
        if free_percent <= self.exhaustion_free_percent:
            return "exhaustion-imminent"
        if free_percent <= self.critical_free_percent:
            return "critical"
        if free_percent <= self.pressure_free_percent:
            return "pressure"
        if (
            previous in {"pressure", "critical", "exhaustion-imminent"}
            and previous_state_started_at_ms is not None
            and (
                now_ms - previous_state_started_at_ms < self.recovery_hold_ms
                or last_recovery_receipt_at_ms is None
                or last_recovery_receipt_at_ms < previous_state_started_at_ms
            )
        ):
            return previous
        return "normal"


__all__ = ["CapacityPolicy", "CapacityState"]
