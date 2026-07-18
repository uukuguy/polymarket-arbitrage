"""Classify opportunity-feed HTTP responses without exposing server details."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal

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
    quote_sla_seconds = payload.get("quote_sla_seconds")
    universe_sla_seconds = payload.get("universe_sla_seconds")
    count = payload.get("count")
    opportunities = payload.get("opportunities")
    return (
        isinstance(strategy, str)
        and bool(strategy)
        and isinstance(profit_basis, str)
        and bool(profit_basis)
        and coverage == "known-universe"
        and type(quote_sla_seconds) is int
        and quote_sla_seconds == 300
        and type(universe_sla_seconds) is int
        and universe_sla_seconds == 50_400
        and type(count) is int
        and count >= 0
        and isinstance(opportunities, list)
        and len(opportunities) == count
    )
