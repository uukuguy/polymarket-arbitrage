from __future__ import annotations

from decimal import Decimal

import pytest

from polyarb.perception.priority import GroupScheduleInput, priority_score


def _schedule_input(
    *,
    last_visited_at_ms: int | None,
    gross_edge_bps: str,
) -> GroupScheduleInput:
    return GroupScheduleInput(
        group_id="g-1",
        gross_edge_bps=Decimal(gross_edge_bps),
        activity_rank=Decimal("0"),
        liquidity_rank=Decimal("0"),
        change_rank=Decimal("0"),
        last_visited_at_ms=last_visited_at_ms,
    )


def test_priority_score_uses_decimal_and_explicit_documented_weights() -> None:
    item = GroupScheduleInput(
        group_id="g-1",
        gross_edge_bps=Decimal("10.1"),
        activity_rank=Decimal("20.2"),
        liquidity_rank=Decimal("30.3"),
        change_rank=Decimal("40.4"),
        last_visited_at_ms=9_940_000,
    )

    assert priority_score(item, now_ms=10_000_000) == Decimal("18.330")


def test_old_unvisited_group_eventually_outranks_recent_low_edge_group() -> None:
    old = _schedule_input(last_visited_at_ms=0, gross_edge_bps="0")
    recent = _schedule_input(last_visited_at_ms=9_900_000, gross_edge_bps="50")

    assert priority_score(old, now_ms=10_000_000) > priority_score(
        recent,
        now_ms=10_000_000,
    )


def test_never_visited_group_receives_age_from_first_discovery() -> None:
    item = GroupScheduleInput(
        group_id="g-1",
        gross_edge_bps=Decimal("0"),
        activity_rank=Decimal("0"),
        liquidity_rank=Decimal("0"),
        change_rank=Decimal("0"),
        last_visited_at_ms=None,
        first_discovered_at_ms=1_000,
    )

    assert priority_score(item, now_ms=61_000) == Decimal("0.15")


@pytest.mark.parametrize(
    "field,value",
    [
        ("activity_rank", Decimal("-1")),
        ("liquidity_rank", Decimal("101")),
        ("change_rank", Decimal("NaN")),
        ("gross_edge_bps", Decimal("Infinity")),
    ],
)
def test_priority_inputs_reject_invalid_or_unbounded_ranks(field, value) -> None:
    kwargs = {
        "group_id": "g-1",
        "gross_edge_bps": Decimal("0"),
        "activity_rank": Decimal("0"),
        "liquidity_rank": Decimal("0"),
        "change_rank": Decimal("0"),
        "last_visited_at_ms": None,
    }
    kwargs[field] = value
    with pytest.raises(ValueError, match="priority"):
        GroupScheduleInput(**kwargs)
