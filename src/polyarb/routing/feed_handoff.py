"""Shared fail-closed policy for serving one certified Quote feed revision."""

from __future__ import annotations

from dataclasses import dataclass

from polyarb.routing.opportunity_scanner import (
    QUOTE_SLA_SECONDS,
    UNIVERSE_SLA_SECONDS,
)


@dataclass(frozen=True)
class FeedAvailability:
    """Whether one immutable certified feed may be served right now."""

    available: bool
    refreshing: bool
    reason: str | None


def decide_feed_availability(
    *,
    source_snapshot_id: int,
    latest_structure_snapshot_id: int | None,
    quote_age_seconds: float,
    universe_age_seconds: float,
    handoff_age_seconds: float | None,
) -> FeedAvailability:
    """Evaluate current/previous revision serving without reading mutable state."""

    def unavailable(reason: str) -> FeedAvailability:
        return FeedAvailability(False, False, reason)

    if latest_structure_snapshot_id is None:
        return unavailable("source-truth-unavailable")
    if source_snapshot_id > latest_structure_snapshot_id:
        return unavailable("source-revision-ahead")
    if quote_age_seconds > QUOTE_SLA_SECONDS:
        return unavailable("stale-quote")
    if universe_age_seconds > UNIVERSE_SLA_SECONDS:
        return unavailable("stale-universe")
    if source_snapshot_id == latest_structure_snapshot_id:
        return FeedAvailability(True, False, None)
    if handoff_age_seconds is None or handoff_age_seconds > QUOTE_SLA_SECONDS:
        return unavailable("source-snapshot-mismatch")
    return FeedAvailability(
        True,
        True,
        "source-snapshot-refreshing-serving-previous",
    )
