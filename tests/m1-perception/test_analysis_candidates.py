from __future__ import annotations

from pathlib import Path

from polyarb.control_plane.analysis_candidates import build_group_candidate, candidate_payload


def test_complete_current_group_with_positive_bundle_edge_is_positive_candidate() -> None:
    candidate = build_group_candidate(
        group={"quality": "complete-supported", "event_id": "event-1", "expected_member_count": 2},
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
        group={"quality": "complete-supported", "event_id": "event-1", "expected_member_count": 1},
        event={"is_open": True, "end_time_ms": 1},
        quotes=({"terminal_state": "executable", "best_ask_price": 0.1, "best_ask_size": 10},),
        evaluated_at_ms=2,
    )
    incomplete = build_group_candidate(
        group={"quality": "complete-supported", "event_id": "event-1", "expected_member_count": 2},
        event={"is_open": True, "end_time_ms": 10},
        quotes=({"terminal_state": "executable", "best_ask_price": 0.1, "best_ask_size": 10},),
        evaluated_at_ms=2,
    )

    assert ended["candidate_state"] == "expired-or-closed"
    assert incomplete["candidate_state"] == "incomplete-coverage"


def test_candidate_payload_is_compact_group_level_fact() -> None:
    payload = candidate_payload(
        group_id="group-1",
        group={"quality": "complete-supported", "event_id": "event-1", "expected_member_count": 2},
        event={"is_open": True, "end_time_ms": 2_000, "title": "A market", "slug": "a-market"},
        quotes=(
            {"terminal_state": "executable", "best_ask_price": 0.45, "best_ask_size": 20},
            {"terminal_state": "executable", "best_ask_price": 0.45, "best_ask_size": 15},
        ),
        evaluated_at_ms=1_000,
    )

    assert payload == {
        "group_id": "group-1",
        "event_id": "event-1",
        "candidate_state": "positive-edge",
        "quality": "complete-supported",
        "expected_member_count": 2,
        "quoted_member_count": 2,
        "event": {"title": "A market", "slug": "a-market", "is_open": True, "end_time_ms": 2_000},
        "bundle_cost": 0.9,
        "gross_edge_bps": 1000.0,
        "max_bundle_size": 15.0,
    }


def test_candidate_source_page_does_not_bind_a_duplicate_generation_filter() -> None:
    source = Path("src/polyarb/control_plane/postgres.py").read_text()
    start = source.index("WITH selected_groups AS")
    candidate_query = source[start : source.index("rows = cursor.fetchall()", start)]

    assert "WHERE groups.generation_key = %s" not in candidate_query
