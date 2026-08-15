from polyarb.control_plane.models import QuoteBatchLeg
from polyarb.control_plane.opportunity_projection import build_opportunity_rows


def test_complete_authenticated_quote_group_projects_only_positive_buy_all_edge() -> None:
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    rows = build_opportunity_rows(
        legs=legs,
        quotes=(
            {
                "yes_token_id": "token-a", "terminal_state": "executable",
                "best_ask_price": 0.4, "best_ask_size": 4,
            },
            {
                "yes_token_id": "token-b", "terminal_state": "executable",
                "best_ask_price": 0.5, "best_ask_size": 3,
            },
        ),
        structure_observed_at_ms=1,
        quote_started_at_ms=2,
        quote_quoted_at_ms=3,
    )

    assert rows == ({
        "group_id": "group-a", "event_id": "event-a", "membership_hash": "m-a",
        "bundle_cost": 0.9, "gross_edge_bps": 1000.0, "max_bundle_size": 3.0,
        "legs": [
            {
                "market_id": "market-a", "condition_id": "condition-a", "slug": "a",
                "yes_token_id": "token-a", "ask_price": 0.4, "ask_size": 4.0,
            },
            {
                "market_id": "market-b", "condition_id": "condition-b", "slug": "b",
                "yes_token_id": "token-b", "ask_price": 0.5, "ask_size": 3.0,
            },
        ],
        "structure_observed_at_ms": 1, "quote_started_at_ms": 2, "quote_quoted_at_ms": 3,
    },)
