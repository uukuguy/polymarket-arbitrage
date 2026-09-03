from __future__ import annotations

from polyarb.control_plane.analysis_candidates import build_group_candidate


def test_complete_current_group_with_positive_bundle_edge_is_positive_candidate() -> None:
    candidate = build_group_candidate(
        group={"quality": "complete-supported", "expected_member_count": 2},
        event={"is_open": True, "end_time_ms": 1_800_000_000_000},
        quotes=(
            {"terminal_state": "executable", "best_ask_price": 0.45, "best_ask_size": 20},
            {"terminal_state": "executable", "best_ask_price": 0.45, "best_ask_size": 15},
        ),
        evaluated_at_ms=1_700_000_000_000,
    )

    assert candidate == {
        "candidate_state": "positive-edge",
        "bundle_cost": 0.9,
        "gross_edge_bps": 1000.0,
        "max_bundle_size": 15.0,
    }


def test_ended_or_incomplete_group_cannot_be_positive_candidate() -> None:
    ended = build_group_candidate(
        group={"quality": "complete-supported", "expected_member_count": 1},
        event={"is_open": True, "end_time_ms": 1},
        quotes=({"terminal_state": "executable", "best_ask_price": 0.1, "best_ask_size": 10},),
        evaluated_at_ms=2,
    )
    incomplete = build_group_candidate(
        group={"quality": "complete-supported", "expected_member_count": 2},
        event={"is_open": True, "end_time_ms": 10},
        quotes=({"terminal_state": "executable", "best_ask_price": 0.1, "best_ask_size": 10},),
        evaluated_at_ms=2,
    )

    assert ended["candidate_state"] == "expired-or-closed"
    assert incomplete["candidate_state"] == "incomplete-coverage"
