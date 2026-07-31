"""Classify opportunity-feed HTTP responses without exposing server details."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal
from math import isfinite
from typing import Literal

from polyarb.routing.opportunity_scanner import BOUNDED_REJECTION_REASONS

DiagnosticKind = Literal[
    "available-zero",
    "available-opportunities",
    "stale-snapshot",
    "stale-quote-run",
    "stale-universe",
    "feed-unavailable",
    "invalid-response",
]

_STALE_SNAPSHOT_ERROR = re.compile(
    r"^snapshot age (?P<age>\d+(?:\.\d+)?)s exceeds (?P<limit>\d+(?:\.\d+)?)s$"
)
_STALE_QUOTE_RUN_ERROR = re.compile(
    r"^quote age (?P<age>\d+(?:\.\d+)?)s exceeds (?P<limit>\d+(?:\.\d+)?)s$"
)
_STALE_UNIVERSE_ERROR = re.compile(
    r"^universe age (?P<age>\d+(?:\.\d+)?)s exceeds (?P<limit>\d+(?:\.\d+)?)s$"
)


@dataclass(frozen=True)
class OpportunityFeedDiagnostic:
    """A stable, operator-safe summary of one opportunity-feed response."""

    kind: DiagnosticKind
    http_status: int
    reason: str
    count: int | None = None
    age_seconds: float | None = None
    max_age_seconds: float | None = None
    snapshot_age_seconds: float | None = None
    max_snapshot_age_seconds: float | None = None
    quote_age_seconds: float | None = None
    max_quote_age_seconds: float | None = None
    universe_age_seconds: float | None = None
    max_universe_age_seconds: float | None = None
    strategy: str | None = None
    profit_basis: str | None = None
    source_snapshot_id: int | None = None
    universe_hash: str | None = None
    quote_run_id: int | None = None
    rejections: dict[str, int] | None = None

    @property
    def exit_code(self) -> int:
        return 0 if self.kind in {"available-zero", "available-opportunities"} else 2

    def to_dict(self) -> dict[str, object]:
        return {
            key: value
            for key, value in {
                "kind": self.kind,
                "http_status": self.http_status,
                "reason": self.reason,
                "count": self.count,
                "age_seconds": self.age_seconds,
                "max_age_seconds": self.max_age_seconds,
                "snapshot_age_seconds": self.snapshot_age_seconds,
                "max_snapshot_age_seconds": self.max_snapshot_age_seconds,
                "quote_age_seconds": self.quote_age_seconds,
                "max_quote_age_seconds": self.max_quote_age_seconds,
                "universe_age_seconds": self.universe_age_seconds,
                "max_universe_age_seconds": self.max_universe_age_seconds,
                "strategy": self.strategy,
                "profit_basis": self.profit_basis,
                "source_snapshot_id": self.source_snapshot_id,
                "universe_hash": self.universe_hash,
                "quote_run_id": self.quote_run_id,
                "rejections": self.rejections,
            }.items()
            if value is not None
        }


def diagnose_opportunity_feed(http_status: int, body: str) -> OpportunityFeedDiagnostic:
    """Return a bounded diagnostic for an opportunity-feed status and body."""
    if http_status == 503:
        return _diagnose_service_unavailable(body)
    if http_status != 200:
        return OpportunityFeedDiagnostic(
            kind="feed-unavailable",
            http_status=http_status,
            reason="non-success-status",
        )

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return OpportunityFeedDiagnostic(
            kind="invalid-response",
            http_status=http_status,
            reason="invalid-json",
        )

    if not _is_valid_success_payload(payload):
        return OpportunityFeedDiagnostic(
            kind="invalid-response",
            http_status=http_status,
            reason="invalid-schema",
        )

    count = payload["count"]
    kind: DiagnosticKind = "available-zero" if count == 0 else "available-opportunities"
    return OpportunityFeedDiagnostic(
        kind=kind,
        http_status=http_status,
        reason="valid-empty-feed" if count == 0 else "valid-feed",
        count=count,
        strategy=payload["strategy"],
        profit_basis=payload["profit_basis"],
        source_snapshot_id=payload["source_snapshot_id"],
        universe_hash=payload["universe_hash"],
        quote_run_id=payload["quote_run_id"],
        rejections=dict(payload["rejections"]),
    )


def _diagnose_service_unavailable(body: str) -> OpportunityFeedDiagnostic:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        payload = None

    error = payload.get("error") if isinstance(payload, dict) else None
    match = _STALE_SNAPSHOT_ERROR.fullmatch(error) if isinstance(error, str) else None
    if match is not None:
        age = float(match["age"])
        limit = float(match["limit"])
        return OpportunityFeedDiagnostic(
            kind="stale-snapshot",
            http_status=503,
            reason="snapshot-age-exceeded",
            age_seconds=age,
            max_age_seconds=limit,
            snapshot_age_seconds=age,
            max_snapshot_age_seconds=limit,
        )
    match = _STALE_QUOTE_RUN_ERROR.fullmatch(error) if isinstance(error, str) else None
    if match is not None:
        age = float(match["age"])
        limit = float(match["limit"])
        return OpportunityFeedDiagnostic(
            kind="stale-quote-run",
            http_status=503,
            reason="quote-age-exceeded",
            age_seconds=age,
            max_age_seconds=limit,
            quote_age_seconds=age,
            max_quote_age_seconds=limit,
        )
    match = _STALE_UNIVERSE_ERROR.fullmatch(error) if isinstance(error, str) else None
    if match is not None:
        age = float(match["age"])
        limit = float(match["limit"])
        return OpportunityFeedDiagnostic(
            kind="stale-universe",
            http_status=503,
            reason="universe-age-exceeded",
            age_seconds=age,
            max_age_seconds=limit,
            universe_age_seconds=age,
            max_universe_age_seconds=limit,
        )
    return OpportunityFeedDiagnostic(
        kind="feed-unavailable",
        http_status=503,
        reason="non-success-status",
    )


def _is_valid_success_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False

    strategy = payload.get("strategy")
    profit_basis = payload.get("profit_basis")
    coverage = payload.get("coverage")
    source_snapshot_id = payload.get("source_snapshot_id")
    universe_hash = payload.get("universe_hash")
    quote_run_id = payload.get("quote_run_id")
    quote_sla_seconds = payload.get("quote_sla_seconds")
    count = payload.get("count")
    rejections = payload.get("rejections")
    opportunities = payload.get("opportunities")
    return (
        strategy == "neg-risk-buy-all"
        and profit_basis == "gross-before-fees"
        and coverage == "verified-standard-neg-risk"
        and type(source_snapshot_id) is int
        and source_snapshot_id > 0
        and isinstance(universe_hash, str)
        and bool(universe_hash)
        and type(quote_run_id) is int
        and quote_run_id > 0
        and type(quote_sla_seconds) is int
        and quote_sla_seconds == 300
        and type(count) is int
        and count >= 0
        and _is_valid_rejections(rejections)
        and isinstance(opportunities, list)
        and len(opportunities) == count
        and all(
            _is_valid_opportunity(
                item,
                source_snapshot_id=source_snapshot_id,
                universe_hash=universe_hash,
                quote_run_id=quote_run_id,
                quote_sla_seconds=quote_sla_seconds,
            )
            for item in opportunities
        )
    )


def _is_valid_rejections(rejections: object) -> bool:
    return isinstance(rejections, dict) and all(
        isinstance(reason, str)
        and reason in BOUNDED_REJECTION_REASONS
        and type(count) is int
        and count >= 0
        for reason, count in rejections.items()
    )


def _is_valid_opportunity(
    opportunity: object,
    *,
    source_snapshot_id: int,
    universe_hash: str,
    quote_run_id: int,
    quote_sla_seconds: int,
) -> bool:
    if not isinstance(opportunity, dict):
        return False
    if not (
        _has_nonempty_strings(
            opportunity,
            ("event_id", "group_id", "membership_hash"),
        )
        and opportunity.get("quality") == "complete-supported"
        and type(opportunity.get("quote_run_id")) is int
        and opportunity["quote_run_id"] == quote_run_id
        and _optional_identity_matches(
            opportunity,
            fields=("snapshot_id", "universe_snapshot_id", "source_snapshot_id"),
            expected=source_snapshot_id,
        )
        and _optional_identity_matches(
            opportunity,
            fields=("universe_hash",),
            expected=universe_hash,
        )
    ):
        return False

    quote_age = _finite_number(opportunity.get("quote_age_seconds"))
    sum_asks = _finite_number(opportunity.get("sum_asks"))
    gross_edge_bps = _finite_number(opportunity.get("gross_edge_bps"))
    executable_quantity = _finite_number(opportunity.get("executable_quantity"))
    gross_profit = _finite_number(opportunity.get("gross_profit"))
    if (
        quote_age is None
        or not 0 <= quote_age <= quote_sla_seconds
        or sum_asks is None
        or not 0 < sum_asks < 1
        or gross_edge_bps is None
        or gross_edge_bps <= 0
        or executable_quantity is None
        or executable_quantity <= 0
        or gross_profit is None
        or gross_profit <= 0
        or not _optional_finite_nonnegative(
            opportunity,
            ("snapshot_age_seconds", "universe_age_seconds"),
        )
    ):
        return False

    legs = opportunity.get("legs")
    if not isinstance(legs, list) or len(legs) < 2:
        return False
    parsed_legs = [_valid_leg_values(leg) for leg in legs]
    if any(values is None for values in parsed_legs):
        return False
    complete_legs = [values for values in parsed_legs if values is not None]
    identities = [values[0] for values in complete_legs]
    if (
        len(set(identities)) != len(identities)
        or len({identity[0] for identity in identities}) != len(identities)
        or len({identity[1] for identity in identities}) != len(identities)
        or len({identity[2] for identity in identities}) != len(identities)
    ):
        return False
    ask_prices = [values[1] for values in complete_legs]
    ask_sizes = [values[2] for values in complete_legs]
    expected_sum = sum(ask_prices, Decimal(0))
    expected_edge = (Decimal(1) - expected_sum) * Decimal(10_000)
    expected_quantity = min(ask_sizes)
    expected_profit = expected_quantity * (Decimal(1) - expected_sum)
    return (
        Decimal(str(sum_asks)) == expected_sum
        and Decimal(str(gross_edge_bps)) == expected_edge
        and Decimal(str(executable_quantity)) == expected_quantity
        and Decimal(str(gross_profit)) == expected_profit
    )


def _has_nonempty_strings(payload: dict, fields: tuple[str, ...]) -> bool:
    return all(
        isinstance(payload.get(field), str) and bool(payload[field].strip())
        for field in fields
    )


def _optional_identity_matches(
    payload: dict,
    *,
    fields: tuple[str, ...],
    expected: object,
) -> bool:
    return all(
        field not in payload
        or (
            type(payload[field]) is type(expected)
            and payload[field] == expected
        )
        for field in fields
    )


def _optional_finite_nonnegative(
    payload: dict,
    fields: tuple[str, ...],
) -> bool:
    for field in fields:
        if field not in payload:
            continue
        value = _finite_number(payload[field])
        if value is None or value < 0:
            return False
    return True


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        numeric = float(value)
    except OverflowError:
        return None
    return numeric if isfinite(numeric) else None


def _valid_leg_values(
    leg: object,
) -> tuple[tuple[str, str, str], Decimal, Decimal] | None:
    if not isinstance(leg, dict):
        return None
    if not _has_nonempty_strings(
        leg,
        ("market_id", "condition_id", "yes_token_id"),
    ):
        return None
    if "slug" in leg and not isinstance(leg["slug"], str):
        return None
    ask_price = _finite_number(leg.get("ask_price"))
    ask_size = _finite_number(leg.get("ask_size"))
    if ask_price is None or not 0 < ask_price <= 1 or ask_size is None or ask_size <= 0:
        return None
    return (
        (
            leg["market_id"],
            leg["condition_id"],
            leg["yes_token_id"],
        ),
        Decimal(str(ask_price)),
        Decimal(str(ask_size)),
    )
