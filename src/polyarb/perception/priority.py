"""Deterministic and calibratable Discovery priority scoring."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

EDGE_WEIGHT = Decimal("0.35")
ACTIVITY_WEIGHT = Decimal("0.20")
LIQUIDITY_WEIGHT = Decimal("0.15")
CHANGE_WEIGHT = Decimal("0.15")
AGE_WEIGHT = Decimal("0.15")
MILLISECONDS_PER_MINUTE = Decimal("60000")


@dataclass(frozen=True)
class GroupScheduleInput:
    group_id: str
    gross_edge_bps: Decimal
    activity_rank: Decimal
    liquidity_rank: Decimal
    change_rank: Decimal
    last_visited_at_ms: int | None
    first_discovered_at_ms: int = 0


@dataclass(frozen=True)
class PriorityComponents:
    gross_edge_bps: Decimal
    activity_rank: Decimal
    liquidity_rank: Decimal
    change_rank: Decimal
    age_rank: Decimal
    score: Decimal
    reason: str


def priority_components(
    value: GroupScheduleInput,
    *,
    now_ms: int,
) -> PriorityComponents:
    """Return persisted score inputs and output.

    Age rank is elapsed minutes and intentionally has no upper clamp.  That is
    the deterministic anti-starvation guarantee: any continuously deferred
    group eventually outranks a finite recent score.
    """
    age_anchor_ms = (
        value.last_visited_at_ms
        if value.last_visited_at_ms is not None
        else value.first_discovered_at_ms
    )
    age_rank = Decimal(max(0, now_ms - age_anchor_ms)) / MILLISECONDS_PER_MINUTE
    score = (
        value.gross_edge_bps * EDGE_WEIGHT
        + value.activity_rank * ACTIVITY_WEIGHT
        + value.liquidity_rank * LIQUIDITY_WEIGHT
        + value.change_rank * CHANGE_WEIGHT
        + age_rank * AGE_WEIGHT
    )
    reason = (
        "weighted-edge-activity-liquidity-change-age:"
        "0.35,0.20,0.15,0.15,0.15"
    )
    return PriorityComponents(
        gross_edge_bps=value.gross_edge_bps,
        activity_rank=value.activity_rank,
        liquidity_rank=value.liquidity_rank,
        change_rank=value.change_rank,
        age_rank=age_rank,
        score=score,
        reason=reason,
    )


def priority_score(value: GroupScheduleInput, *, now_ms: int) -> Decimal:
    return priority_components(value, now_ms=now_ms).score
