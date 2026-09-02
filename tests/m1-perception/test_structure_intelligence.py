from __future__ import annotations

from polyarb.control_plane.structure_intelligence import build_structure_intelligence


def test_bundle_aggregates_event_market_tag_and_group_truth() -> None:
    bundle = build_structure_intelligence(
        generation_key="structure:current",
        rows_by_component={
            "events": (
                {
                    "id": "event-a",
                    "title": "Will it rain?",
                    "slug": "rain",
                    "active": True,
                    "closed": False,
                    "end_time_ms": 1_800_000_000_000,
                    "liquidity_usd": 123.5,
                    "volume_usd": 456.0,
                },
            ),
            "markets": (
                {"market_id": "market-a", "event_id": "event-a", "active": True, "closed": False},
                {"market_id": "market-b", "event_id": "event-a", "active": False, "closed": True},
            ),
            "event_tags": ({"event_id": "event-a", "tag_label": "Weather", "tag_slug": "weather"},),
            "group_truth": (
                {
                    "neg_risk_market_id": "group-a",
                    "event_id": "event-a",
                    "neg_risk_type": "standard",
                    "expected_member_count": 2,
                    "active_named_count": 1,
                    "quality": "complete-supported",
                    "reason": None,
                },
            ),
        },
    )

    event = bundle.events[0]
    assert event.event_id == "event-a"
    assert event.payload["title"] == "Will it rain?"
    assert event.payload["tags"] == ["Weather"]
    assert event.payload["market_count"] == 2
    assert event.payload["active_market_count"] == 1
    assert event.payload["closed_market_count"] == 1
    assert event.payload["neg_risk_quality"] == "complete-supported"
    assert event.payload["missing_fields"] == []
    assert bundle.groups[0].group_id == "group-a"
    assert bundle.groups[0].payload["event_id"] == "event-a"


def test_bundle_marks_absent_business_fields_as_missing_not_zero() -> None:
    bundle = build_structure_intelligence(
        generation_key="structure:current",
        rows_by_component={"events": ({"id": "event-a"},)},
    )

    event = bundle.events[0]
    assert event.payload["liquidity"] is None
    assert event.payload["volume"] is None
    assert event.payload["end_time_ms"] is None
    assert set(event.payload["missing_fields"]) >= {"liquidity", "volume", "end_time_ms"}
