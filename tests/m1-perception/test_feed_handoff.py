from __future__ import annotations

from collections.abc import Mapping

import pytest

from polyarb.routing.feed_handoff import (
    FeedAvailability,
    decide_feed_availability,
)


def _decision(**overrides: int | float | None) -> FeedAvailability:
    values: dict[str, int | float | None] = {
        "source_snapshot_id": 10,
        "latest_structure_snapshot_id": 10,
        "quote_age_seconds": 10.0,
        "universe_age_seconds": 10.0,
        "handoff_age_seconds": 0.0,
    }
    values.update(overrides)
    return decide_feed_availability(**values)


def test_current_certified_feed_is_available() -> None:
    assert _decision() == FeedAvailability(True, False, None)


def test_previous_feed_is_available_at_both_exact_hard_boundaries() -> None:
    assert _decision(
        latest_structure_snapshot_id=11,
        quote_age_seconds=300.0,
        handoff_age_seconds=300.0,
    ) == FeedAvailability(
        True,
        True,
        "source-snapshot-refreshing-serving-previous",
    )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    (
        ({"latest_structure_snapshot_id": None}, "source-truth-unavailable"),
        ({"latest_structure_snapshot_id": 9}, "source-revision-ahead"),
        ({"quote_age_seconds": 300.1}, "stale-quote"),
        ({"universe_age_seconds": 50_400.1}, "stale-universe"),
        (
            {"latest_structure_snapshot_id": 11, "handoff_age_seconds": None},
            "source-snapshot-mismatch",
        ),
        (
            {"latest_structure_snapshot_id": 11, "handoff_age_seconds": 300.1},
            "source-snapshot-mismatch",
        ),
    ),
)
def test_unavailable_feed_reasons(
    overrides: Mapping[str, int | float | None],
    reason: str,
) -> None:
    assert _decision(**overrides) == FeedAvailability(False, False, reason)
