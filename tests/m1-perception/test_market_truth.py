from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from polyarb.perception.market_truth import (
    INVALID_EVENT_MEMBER_REASON,
    INVALID_NEG_RISK_FLAGS_REASON,
    MISSING_EVENT_MEMBERSHIP_REASON,
    NEG_RISK_ENABLEMENT_CONFLICT_REASON,
    EventMember,
    GroupTruth,
    SourceCoverage,
    market_truth_mismatch_reason,
    membership_hash,
)
from polyarb.snapshot.normalizer import normalize_events

_MISSING = object()


def _michigan_event() -> dict:
    active = [
        {
            "id": str(969760 + i),
            "groupItemTitle": title,
            "active": True,
            "closed": False,
            "negRiskOther": False,
        }
        for i, title in enumerate(
            [
                "Kent Benham",
                "Fred Heurtebise",
                "Mike Rogers",
                "Genevieve Scott",
                "Bernadette Smith",
                "Andrew Kamal",
            ]
        )
    ]
    other = [
        {
            "id": "969766",
            "groupItemTitle": "Other",
            "active": False,
            "closed": False,
            "negRiskOther": True,
        }
    ]
    reserved = [
        {
            "id": str(969767 + i),
            "groupItemTitle": f"Candidate {chr(65 + i)}",
            "active": False,
            "closed": False,
            "negRiskOther": False,
        }
        for i in range(26)
    ]
    return {
        "id": "111080",
        "slug": "michigan-republican-senate-primary-winner-954",
        "title": "Michigan Republican Senate Primary Winner",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": True,
        "negRiskMarketID": "group-mi",
        "markets": active + other + reserved,
        "tags": [],
    }


def _standard_event() -> dict:
    return {
        "id": "e-standard",
        "slug": "standard-winner",
        "title": "Standard Winner",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "group-standard",
        "markets": [
            {
                "id": "market-a",
                "groupItemTitle": "Candidate A",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            },
            {
                "id": "market-b",
                "groupItemTitle": "Candidate B",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            },
        ],
        "tags": [],
    }


def test_augmented_event_is_complete_but_unsupported() -> None:
    _, _, _, members, groups = normalize_events([_michigan_event()])
    assert len(members) == 33
    assert members[6].member_kind == "other"
    assert all(member.member_kind == "inactive-reserved" for member in members[7:])
    assert groups == [
        GroupTruth(
            event_id="111080",
            group_id="group-mi",
            neg_risk_type="augmented",
            expected_member_count=33,
            active_named_count=6,
            membership_hash=membership_hash("111080", "group-mi", members),
            quality="complete-unsupported",
            reason="augmented-neg-risk-not-supported",
        )
    ]


def test_standard_event_with_all_open_named_members_is_supported() -> None:
    _, _, _, members, groups = normalize_events([_standard_event()])
    assert members == [
        EventMember("e-standard", "group-standard", "market-a", "named", True, False),
        EventMember("e-standard", "group-standard", "market-b", "named", True, False),
    ]
    assert groups == [
        GroupTruth(
            event_id="e-standard",
            group_id="group-standard",
            neg_risk_type="standard",
            expected_member_count=2,
            active_named_count=2,
            membership_hash=membership_hash("e-standard", "group-standard", members),
            quality="complete-supported",
            reason=None,
        )
    ]


def test_closed_active_member_remains_named_but_blocks_standard_support() -> None:
    event = _standard_event()
    event["markets"][0]["closed"] = True
    _, _, _, members, groups = normalize_events([event])
    assert members[0].member_kind == "named"
    assert groups[0].active_named_count == 2
    assert groups[0].quality == "complete-unsupported"


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("negRisk", _MISSING),
        ("negRisk", "false"),
        ("enableNegRisk", _MISSING),
        ("enableNegRisk", 1),
        ("negRiskAugmented", _MISSING),
        ("negRiskAugmented", "false"),
    ],
)
def test_missing_or_non_boolean_neg_risk_flags_fail_closed(
    field: str,
    invalid_value: object,
) -> None:
    event = _standard_event()
    if invalid_value is _MISSING:
        event.pop(field)
    else:
        event[field] = invalid_value
    _, _, _, members, groups = normalize_events([event])
    assert len(members) == 2
    assert groups[0].quality == "incomplete-source"
    assert groups[0].reason == INVALID_NEG_RISK_FLAGS_REASON


@pytest.mark.parametrize(
    ("neg_risk", "enabled"),
    [(True, False), (False, True), (False, False)],
)
def test_contradictory_neg_risk_enablement_fails_closed(
    neg_risk: bool,
    enabled: bool,
) -> None:
    event = _standard_event()
    event["negRisk"] = neg_risk
    event["enableNegRisk"] = enabled
    _, _, _, members, groups = normalize_events([event])
    assert len(members) == 2
    assert groups[0].quality == "incomplete-source"
    assert groups[0].reason == NEG_RISK_ENABLEMENT_CONFLICT_REASON


def test_ordinary_non_neg_risk_event_without_hint_has_no_group_truth() -> None:
    event = _standard_event()
    event["negRisk"] = False
    event.pop("enableNegRisk")
    event.pop("negRiskAugmented")
    event.pop("negRiskMarketID")
    _, _, _, members, groups = normalize_events([event])
    assert members == []
    assert groups == []


@pytest.mark.parametrize("markets", [None, []])
def test_missing_or_empty_neg_risk_membership_is_incomplete_source(
    markets: object,
) -> None:
    event = _standard_event()
    event["markets"] = markets
    _, _, _, members, groups = normalize_events([event])
    assert members == []
    assert groups == [
        GroupTruth(
            event_id="e-standard",
            group_id="group-standard",
            neg_risk_type="standard",
            expected_member_count=0,
            active_named_count=0,
            membership_hash=membership_hash("e-standard", "group-standard", []),
            quality="incomplete-source",
            reason=MISSING_EVENT_MEMBERSHIP_REASON,
        )
    ]


@pytest.mark.parametrize(
    "invalid_member",
    [
        "not-a-dict",
        {"active": True, "closed": False, "negRiskOther": False},
        {"id": "   ", "active": True, "closed": False, "negRiskOther": False},
    ],
)
def test_invalid_member_shape_or_id_fails_closed_but_keeps_valid_members(
    invalid_member: object,
) -> None:
    event = _standard_event()
    event["markets"].append(invalid_member)
    _, _, _, members, groups = normalize_events([event])
    assert [member.market_id for member in members] == ["market-a", "market-b"]
    assert groups[0].expected_member_count == 2
    assert groups[0].quality == "incomplete-source"
    assert groups[0].reason == INVALID_EVENT_MEMBER_REASON


@pytest.mark.parametrize("field", ["active", "closed", "negRiskOther"])
@pytest.mark.parametrize("invalid_value", [None, 0, 1, "false"])
def test_non_boolean_member_status_fails_closed(
    field: str,
    invalid_value: object,
) -> None:
    event = _standard_event()
    event["markets"][0][field] = invalid_value
    _, _, _, members, groups = normalize_events([event])
    assert [member.market_id for member in members] == ["market-b"]
    assert groups[0].expected_member_count == 1
    assert groups[0].quality == "incomplete-source"
    assert groups[0].reason == INVALID_EVENT_MEMBER_REASON


@pytest.mark.parametrize("invalid_id", [False, 7, [], {}, "   "])
def test_non_string_or_blank_market_id_fails_closed(invalid_id: object) -> None:
    event = _standard_event()
    event["markets"][0]["id"] = invalid_id
    _, _, market_to_event, members, groups = normalize_events([event])
    assert market_to_event == {"market-b": "e-standard"}
    assert [member.market_id for member in members] == ["market-b"]
    assert groups[0].expected_member_count == 1
    assert groups[0].quality == "incomplete-source"
    assert groups[0].reason == INVALID_EVENT_MEMBER_REASON


@pytest.mark.parametrize("invalid_id", [False, 7, [], {}, "   "])
def test_non_string_or_blank_event_id_is_rejected(invalid_id: object) -> None:
    event = _standard_event()
    event["id"] = invalid_id
    events, _, market_to_event, members, groups = normalize_events([event])
    assert events == []
    assert market_to_event == {}
    assert members == []
    assert groups == []


@pytest.mark.parametrize("invalid_id", [False, 7, [], {}, "   "])
def test_non_string_or_blank_group_id_is_rejected(invalid_id: object) -> None:
    event = _standard_event()
    event["negRiskMarketID"] = invalid_id
    events, _, market_to_event, members, groups = normalize_events([event])
    assert len(events) == 1
    assert market_to_event == {
        "market-a": "e-standard",
        "market-b": "e-standard",
    }
    assert members == []
    assert groups == []


def test_authoritative_ids_are_stripped_before_use() -> None:
    event = _standard_event()
    event["id"] = "  e-standard  "
    event["negRiskMarketID"] = "  group-standard  "
    event["markets"][0]["id"] = "  market-a  "
    _, _, market_to_event, members, groups = normalize_events([event])
    assert market_to_event["market-a"] == "e-standard"
    assert members[0] == EventMember(
        "e-standard",
        "group-standard",
        "market-a",
        "named",
        True,
        False,
    )
    assert groups[0].event_id == "e-standard"
    assert groups[0].group_id == "group-standard"


def test_standard_group_hash_is_order_independent() -> None:
    left = [
        EventMember("e1", "g1", "m1", "named", True, False),
        EventMember("e1", "g1", "m2", "named", True, False),
    ]
    assert membership_hash("e1", "g1", left) == membership_hash("e1", "g1", list(reversed(left)))


def test_publication_completeness_requires_only_active_open_event_members() -> None:
    active = EventMember("e1", "g1", "m-active", "named", True, False)
    inactive = EventMember("e1", "g1", "m-inactive", "inactive-reserved", False, False)
    closed = EventMember("e1", "g1", "m-closed", "named", True, True)
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=3,
        active_named_count=2,
        membership_hash=membership_hash("e1", "g1", [active, inactive, closed]),
        quality="complete-unsupported",
        reason="standard-neg-risk-has-non-tradable-members",
    )

    assert (
        market_truth_mismatch_reason(
            [active, inactive, closed],
            [truth],
            [
                {
                    "market_id": "m-active",
                    "event_id": "e1",
                    "neg_risk_market_id": "g1",
                    "neg_risk": True,
                    "active": True,
                    "closed": False,
                }
            ],
        )
        is None
    )


def test_complete_unsupported_group_members_are_not_required_in_published_view() -> None:
    member = EventMember("e1", "g1", "m-active", "named", True, False)
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=1,
        active_named_count=1,
        membership_hash=membership_hash("e1", "g1", [member]),
        quality="complete-unsupported",
        reason="active-member-absent-from-market-keyset",
    )

    assert market_truth_mismatch_reason([member], [truth], []) is None


def test_truth_contracts_are_immutable() -> None:
    member = EventMember("e1", "g1", "m1", "named", True, False)
    with pytest.raises(FrozenInstanceError):
        member.active = False  # type: ignore[misc]


def test_source_coverage_factories_build_consistent_states() -> None:
    assert SourceCoverage.complete(10, 3) == SourceCoverage(
        completed=True,
        market_items=10,
        event_items=3,
        failure_source=None,
        failure_reason=None,
    )
    assert SourceCoverage.incomplete("markets", 2, 100, "  http-422  ") == SourceCoverage(
        completed=False,
        market_items=2,
        event_items=100,
        failure_source="markets",
        failure_reason="http-422",
    )


def test_source_coverage_rejects_invalid_counts() -> None:
    for value in (True, 1.5, "1"):
        with pytest.raises(TypeError):
            SourceCoverage.complete(value, 0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        SourceCoverage.complete(-1, 0)


def test_source_coverage_rejects_inconsistent_direct_states() -> None:
    with pytest.raises(ValueError):
        SourceCoverage(True, 1, 1, "markets", "unexpected failure")
    with pytest.raises(ValueError):
        SourceCoverage(False, 1, 1, None, None)
    with pytest.raises(ValueError):
        SourceCoverage(False, 1, 1, "events", "   ")
    with pytest.raises(ValueError):
        SourceCoverage(False, 1, 1, "clob", "failed")  # type: ignore[arg-type]


def test_source_coverage_caps_failure_reason_at_200_characters() -> None:
    coverage = SourceCoverage.incomplete("events", 1, 2, f"  {'x' * 250}  ")
    assert coverage.failure_reason == "x" * 200
