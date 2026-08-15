"""Pure, fail-closed projection of authenticated Quote batches into opportunities."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from decimal import Decimal
from math import isfinite

from .models import QuoteBatchLeg


class OpportunityProjectionError(ValueError):
    """An authenticated Quote artifact cannot produce a complete opportunity row."""


def parse_quote_batch_bytes(
    payload: bytes, *, expected_digest: str
) -> tuple[dict[str, object], ...]:
    """Authenticate and decode one canonical Quote artifact without CLOB access."""
    if hashlib.sha256(payload).hexdigest() != expected_digest:
        raise OpportunityProjectionError("quote-artifact-digest-mismatch")
    try:
        records = [json.loads(line) for line in payload.splitlines()]
        header, quotes = records[0], records[1:]
        if not isinstance(header, dict) or set(header) != {
            "structure_receipt_digest", "token_range_digest", "universe_hash"
        }:
            raise ValueError("header")
        if not quotes or not all(isinstance(quote, dict) for quote in quotes):
            raise ValueError("quotes")
        if any(not isinstance(quote.get("yes_token_id"), str) for quote in quotes):
            raise ValueError("token")
        if len({str(quote["yes_token_id"]) for quote in quotes}) != len(quotes):
            raise ValueError("duplicate")
    except (IndexError, ValueError, TypeError, json.JSONDecodeError) as error:
        raise OpportunityProjectionError("quote-artifact-malformed") from error
    return tuple(quotes)


def build_opportunity_rows(
    *,
    legs: Sequence[QuoteBatchLeg],
    quotes: Sequence[Mapping[str, object]],
    structure_observed_at_ms: int,
    quote_started_at_ms: int,
    quote_quoted_at_ms: int,
) -> tuple[dict[str, object], ...]:
    """Return positive buy-all opportunities from a complete frozen quote universe."""
    if structure_observed_at_ms < 0 or not 0 <= quote_started_at_ms <= quote_quoted_at_ms:
        raise OpportunityProjectionError("opportunity-projection-time-invalid")
    by_token = {str(quote.get("yes_token_id")): quote for quote in quotes}
    groups: dict[str, list[QuoteBatchLeg]] = defaultdict(list)
    for leg in legs:
        groups[leg.neg_risk_market_id].append(leg)
    rows: list[dict[str, object]] = []
    for group_id, group_legs in sorted(groups.items()):
        event_ids = {leg.event_id for leg in group_legs}
        memberships = {leg.membership_hash for leg in group_legs}
        if len(group_legs) < 2 or len(event_ids) != 1 or len(memberships) != 1:
            continue
        prepared: list[dict[str, object]] = []
        for leg in group_legs:
            quote = by_token.get(leg.yes_token_id)
            if quote is None or quote.get("terminal_state") != "executable":
                prepared = []
                break
            price = _positive(quote.get("best_ask_price"), maximum=1)
            size = _positive(quote.get("best_ask_size"))
            if price is None or size is None:
                prepared = []
                break
            prepared.append(
                {
                    "market_id": leg.market_id,
                    "condition_id": leg.condition_id,
                    "slug": leg.slug or "",
                    "yes_token_id": leg.yes_token_id,
                    "ask_price": float(price),
                    "ask_size": float(size),
                }
            )
        if not prepared:
            continue
        bundle_cost = sum((Decimal(str(leg["ask_price"])) for leg in prepared), Decimal(0))
        edge = (Decimal(1) - bundle_cost) * Decimal(10_000)
        if edge <= 0:
            continue
        rows.append(
            {
                "group_id": group_id,
                "event_id": next(iter(event_ids)),
                "membership_hash": next(iter(memberships)),
                "bundle_cost": float(bundle_cost),
                "gross_edge_bps": float(edge),
                "max_bundle_size": min(float(leg["ask_size"]) for leg in prepared),
                "legs": prepared,
                "structure_observed_at_ms": structure_observed_at_ms,
                "quote_started_at_ms": quote_started_at_ms,
                "quote_quoted_at_ms": quote_quoted_at_ms,
            }
        )
    return tuple(rows)


def _positive(value: object, *, maximum: int | None = None) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:  # pragma: no cover - Decimal's exception types vary
        return None
    if not parsed.is_finite() or parsed <= 0 or (maximum is not None and parsed > maximum):
        return None
    if not isfinite(float(parsed)):
        return None
    return parsed
