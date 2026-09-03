from __future__ import annotations

from polyarb.control_plane.quote_discovery import (
    decode_discovery_cursor,
    encode_discovery_cursor,
    quote_discovery,
)


def test_executable_deep_non_neutral_quote_is_a_research_lead() -> None:
    evidence = quote_discovery(
        {
            "terminal_state": "executable",
            "best_ask_price": 0.70,
            "best_ask_size": 57.0,
        }
    )

    assert evidence["executable_notional_usd"] == 39.9
    assert evidence["price_extremity_bps"] == 2000.0
    assert evidence["score"] > 0
    assert evidence["reasons"] == [
        "meaningful-executable-depth",
        "non-neutral-yes-price",
    ]


def test_non_executable_or_invalid_quote_is_explicitly_demoted() -> None:
    assert quote_discovery({"terminal_state": "missing-book"}) == {
        "executable_notional_usd": 0.0,
        "price_extremity_bps": 0.0,
        "score": 0.0,
        "reasons": ["not-executable"],
    }
    assert quote_discovery(
        {
            "terminal_state": "executable",
            "best_ask_price": 1.2,
            "best_ask_size": 5,
        }
    )["reasons"] == ["missing-or-invalid-quote"]


def test_discovery_cursor_round_trips_and_rejects_malformed_input() -> None:
    cursor = encode_discovery_cursor(7770.0, 39.9, "token:abc")

    assert decode_discovery_cursor(cursor) == (7770.0, 39.9, "token:abc")
    assert decode_discovery_cursor("not-a-cursor") is None
