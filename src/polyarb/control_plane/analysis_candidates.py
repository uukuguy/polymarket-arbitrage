"""Pure, bounded group-level candidate facts for M1 business analysis."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from math import isfinite

MAX_CANDIDATE_PAYLOAD_OCTETS = 2_048


def build_group_candidate(
    *,
    group: Mapping[str, object],
    event: Mapping[str, object],
    quotes: Sequence[Mapping[str, object]],
    evaluated_at_ms: int,
) -> dict[str, object]:
    """Classify one group without granting it Certified-opportunity authority."""
    if group.get("quality") != "complete-supported":
        return {"candidate_state": "context-unavailable"}
    if not isinstance(group.get("event_id"), str) or not event:
        return {"candidate_state": "context-unavailable"}
    end_time_ms = event.get("end_time_ms")
    is_event_active = (
        event.get("is_open") is True
        and isinstance(end_time_ms, int)
        and end_time_ms > evaluated_at_ms
    )
    if not is_event_active:
        return {"candidate_state": "expired-or-closed"}
    expected = group.get("expected_member_count")
    if not isinstance(expected, int) or expected <= 0 or len(quotes) != expected:
        return {"candidate_state": "incomplete-coverage"}
    legs = [_quote_leg(quote) for quote in quotes]
    if any(leg is None for leg in legs):
        return {"candidate_state": "incomplete-coverage"}
    prepared = [leg for leg in legs if leg is not None]
    bundle_cost = round(sum(ask for ask, _size in prepared), 8)
    gross_edge_bps = round((1.0 - bundle_cost) * 10_000, 8)
    max_bundle_size = min(size for _ask, size in prepared)
    return {
        "candidate_state": "positive-edge" if gross_edge_bps > 0 else "no-edge",
        "bundle_cost": bundle_cost,
        "gross_edge_bps": gross_edge_bps,
        "max_bundle_size": max_bundle_size,
    }


def candidate_payload(
    *,
    group_id: str,
    group: Mapping[str, object],
    event: Mapping[str, object],
    quotes: Sequence[Mapping[str, object]],
    evaluated_at_ms: int,
) -> dict[str, object]:
    """Create the bounded dashboard fact for one group-level analysis result."""
    fact = build_group_candidate(
        group=group, event=event, quotes=quotes, evaluated_at_ms=evaluated_at_ms
    )
    payload: dict[str, object] = {
        "group_id": group_id,
        "event_id": _text(group.get("event_id"), maximum=160),
        "candidate_state": fact["candidate_state"],
        "quality": _text(group.get("quality"), maximum=64),
        "expected_member_count": group.get("expected_member_count"),
        "quoted_member_count": len(quotes),
        "event": {
            "title": _text(event.get("title"), maximum=320),
            "slug": _text(event.get("slug"), maximum=160),
            "is_open": event.get("is_open"),
            "end_time_ms": event.get("end_time_ms"),
        },
    }
    for key in ("bundle_cost", "gross_edge_bps", "max_bundle_size"):
        if key in fact:
            payload[key] = fact[key]
    if fact["candidate_state"] == "positive-edge":
        payload["executable_economic_value"] = round(
            float(fact["gross_edge_bps"])
            * float(fact["bundle_cost"])
            * float(fact["max_bundle_size"])
            / 10_000,
            8,
        )
    octets = len(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
    if octets > MAX_CANDIDATE_PAYLOAD_OCTETS:
        raise ValueError("analysis-candidate-payload-out-of-bounds")
    return payload


def _quote_leg(quote: Mapping[str, object]) -> tuple[float, float] | None:
    if quote.get("terminal_state") != "executable":
        return None
    ask, size = quote.get("best_ask_price"), quote.get("best_ask_size")
    if not _finite_number(ask) or not _finite_number(size):
        return None
    if not 0 <= float(ask) <= 1 or float(size) <= 0:
        return None
    return float(ask), float(size)


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(float(value))
    )


def _text(value: object, *, maximum: int) -> str | None:
    return value[:maximum] if isinstance(value, str) and value else None
