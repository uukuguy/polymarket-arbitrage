"""Contracts for the canonical M1 business brief."""

from __future__ import annotations

import pytest

from polyarb.control_plane.business_brief import (
    BusinessBriefUnavailable,
    build_business_brief,
    render_business_brief,
)


def _status_fixture() -> dict[str, object]:
    return {
        "status": "available",
        "structure": {"generation": "structure-1", "record_count": 12},
        "quote": {"generation": "quote-1", "record_count": 12},
        "qualification": {
            "eligibility_state": "paused",
            "eligibility_reason": "freshness.quote",
        },
        "open_incidents": [],
        "runtime_incidents": [],
        "recovery_actions": [],
        "runtime_watchdog": {"current": None, "recent_events": []},
    }


def _opportunities_fixture() -> dict[str, object]:
    return {
        "status": "available",
        "current_opportunity_count": 6,
        "items": [
            {"group_id": f"group-{index}", "gross_edge_bps": index}
            for index in range(1, 7)
        ],
    }


def test_builds_available_brief_from_canonical_authorities() -> None:
    status = _status_fixture()
    opportunities = _opportunities_fixture()

    brief = build_business_brief(status, opportunities)

    assert brief == {
        "status": "available",
        "conclusion": {
            "eligibility_state": "paused",
            "eligibility_reason": "freshness.quote",
            "escalate": True,
        },
        "structure": status["structure"],
        "quote": status["quote"],
        "opportunities": {
            "count": 6,
            "items": opportunities["items"][:5],
        },
        "incidents": {
            "open": [],
            "runtime": [],
            "recovery_actions": [],
            "watchdog": status["runtime_watchdog"],
        },
    }
    assert brief["opportunities"]["count"] == 6
    assert len(brief["opportunities"]["items"]) == 5


def test_renderer_emits_five_business_sections() -> None:
    text = render_business_brief(build_business_brief(_status_fixture(), _opportunities_fixture()))

    assert "今日结论" in text
    assert "市场覆盖（Structure）" in text
    assert "报价（Quote）" in text
    assert "资格与机会" in text
    assert "异常与恢复" in text


@pytest.mark.parametrize(
    ("status", "opportunities"),
    [
        (_status_fixture(), {"status": "unavailable"}),
        (
            {
                key: value
                for key, value in _status_fixture().items()
                if key != "qualification"
            },
            _opportunities_fixture(),
        ),
    ],
)
def test_rejects_unavailable_or_malformed_authority(
    status: dict[str, object], opportunities: dict[str, object]
) -> None:
    with pytest.raises(BusinessBriefUnavailable):
        build_business_brief(status, opportunities)
