"""Fail-closed neg-risk buy-all opportunity discovery from an M1 snapshot."""

from __future__ import annotations

import sqlite3
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal
from math import isfinite
from pathlib import Path
from types import MappingProxyType

from polyarb.routing.neg_risk_quote_store import (
    NegRiskQuoteStore,
    QuoteUniverseUnavailableError,
)

QUOTE_SLA_SECONDS = 300
QUOTE_WARN_SECONDS = 240
UNIVERSE_SLA_SECONDS = 50_400
BOUNDED_REJECTION_REASONS = frozenset(
    {
        "augmented-neg-risk-not-supported",
        "event-membership-member-invalid",
        "event-membership-missing-or-empty",
        "event-neg-risk-enablement-conflict",
        "event-neg-risk-flags-invalid",
        "incomplete-quotes",
        "incomplete-source",
        "invalid-identity",
        "market-id-conflict-across-events",
        "membership-market-mismatch",
        "neg-risk-group-not-supported",
        "standard-neg-risk-has-non-tradable-members",
    }
)


class StaleSnapshotError(RuntimeError):
    """The source snapshot is too old to support an executable claim."""


class QuoteRunUnavailableError(RuntimeError):
    """No atomically complete quote run is available to scan."""


class StaleQuoteRunError(RuntimeError):
    """The complete quote run is too old to support an executable claim."""


class StaleUniverseError(RuntimeError):
    """The quote run's known universe is too old to support an executable claim."""


@dataclass(frozen=True)
class OpportunityLeg:
    market_id: str
    condition_id: str
    slug: str
    yes_token_id: str
    ask_price: float
    ask_size: float


@dataclass(frozen=True)
class NegRiskOpportunity:
    group_id: str
    snapshot_id: int
    snapshot_age_seconds: float
    sum_asks: float
    gross_edge_bps: float
    executable_quantity: float
    gross_profit: float
    legs: tuple[OpportunityLeg, ...]
    quote_run_id: int | None = None
    quote_age_seconds: float | None = None
    universe_snapshot_id: int | None = None
    universe_age_seconds: float | None = None
    event_id: str | None = None
    membership_hash: str | None = None
    quality: str | None = None

    def to_dict(self) -> dict:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True)
class OpportunityScanResult:
    opportunities: tuple[NegRiskOpportunity, ...]
    rejections: Mapping[str, int]
    source_snapshot_id: int
    universe_hash: str
    quote_run_id: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rejections",
            MappingProxyType(dict(self.rejections)),
        )


def scan_neg_risk_buy_all(
    db_path: Path | str,
    *,
    min_edge_bps: float = 0,
    max_snapshot_age_s: float | None = None,
    limit: int = 50,
) -> list[NegRiskOpportunity]:
    """Return executable buy-all bundles ordered by gross edge."""
    if not isfinite(min_edge_bps):
        raise ValueError("min_edge_bps must be finite")
    if max_snapshot_age_s is not None and (
        not isfinite(max_snapshot_age_s) or max_snapshot_age_s < 0
    ):
        raise ValueError("max_snapshot_age_s must be finite and non-negative")
    connection = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        snapshot = connection.execute(
            "SELECT id, taken_at_ms FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if snapshot is None:
            return []
        snapshot_id, taken_at_ms = int(snapshot[0]), int(snapshot[1])
        age_seconds = max(0.0, time.time() - taken_at_ms / 1000)
        if max_snapshot_age_s is not None and age_seconds > max_snapshot_age_s:
            raise StaleSnapshotError(
                f"snapshot age {age_seconds:.1f}s exceeds {max_snapshot_age_s:.1f}s"
            )
        rows = connection.execute(
            "SELECT neg_risk_market_id, market_id, condition_id, slug, "
            "yes_token_id, best_ask_price, best_ask_size, active, closed, incomplete "
            "FROM markets WHERE snapshot_id = ? "
            "AND neg_risk_market_id IS NOT NULL "
            "ORDER BY neg_risk_market_id, market_id",
            (snapshot_id,),
        ).fetchall()
    finally:
        connection.close()

    groups: dict[str, list[tuple]] = {}
    for row in rows:
        groups.setdefault(str(row[0]), []).append(row)

    opportunities: list[NegRiskOpportunity] = []
    threshold = Decimal(str(min_edge_bps))
    for group_id, group_rows in groups.items():
        if len(group_rows) < 2:
            continue
        legs: list[OpportunityLeg] = []
        valid = True
        for row in group_rows:
            _, market_id, condition_id, slug, token_id, ask, size, active, closed, incomplete = row
            if (
                not active
                or closed
                or incomplete
                or not token_id
                or ask is None
                or size is None
                or not (0 < float(ask) <= 1)
                or float(size) <= 0
            ):
                valid = False
                break
            legs.append(
                OpportunityLeg(
                    market_id=str(market_id),
                    condition_id=str(condition_id),
                    slug=str(slug or ""),
                    yes_token_id=str(token_id),
                    ask_price=float(ask),
                    ask_size=float(size),
                )
            )
        if not valid:
            continue
        sum_asks_decimal = sum((Decimal(str(leg.ask_price)) for leg in legs), Decimal(0))
        edge_bps = (Decimal(1) - sum_asks_decimal) * Decimal(10_000)
        if edge_bps < threshold or edge_bps <= 0:
            continue
        quantity = min(leg.ask_size for leg in legs)
        gross_profit = Decimal(str(quantity)) * (Decimal(1) - sum_asks_decimal)
        opportunities.append(
            NegRiskOpportunity(
                group_id=group_id,
                snapshot_id=snapshot_id,
                snapshot_age_seconds=age_seconds,
                sum_asks=float(sum_asks_decimal),
                gross_edge_bps=float(edge_bps),
                executable_quantity=quantity,
                gross_profit=float(gross_profit),
                legs=tuple(legs),
            )
        )
    opportunities.sort(key=lambda item: (-item.gross_edge_bps, item.group_id))
    return opportunities[: max(0, limit)]


def scan_neg_risk_quote_run(
    db_path: Path | str,
    *,
    min_edge_bps: float = 0,
    max_quote_age_s: float = 300,
    max_universe_age_s: float = 50_400,
    limit: int = 50,
    now_s: Callable[[], float] = time.time,
) -> list[NegRiskOpportunity]:
    """Compatibility wrapper returning candidates from one verified quote run."""
    return list(
        scan_verified_neg_risk_quote_run(
            db_path,
            min_edge_bps=min_edge_bps,
            max_quote_age_s=max_quote_age_s,
            max_universe_age_s=max_universe_age_s,
            limit=limit,
            now_s=now_s,
        ).opportunities
    )


def scan_verified_neg_risk_quote_run(
    db_path: Path | str,
    *,
    min_edge_bps: float = 0,
    max_quote_age_s: float = 300,
    max_universe_age_s: float = 50_400,
    limit: int = 50,
    now_s: Callable[[], float] = time.time,
) -> OpportunityScanResult:
    """Return verified candidates and bounded per-group rejection counts.

    A run's persisted terminal rows are its complete known universe.  We do
    not consult snapshot best-asks here: doing so could mix observations from
    different collection runs and turn a stale/missing quote into an apparent
    executable opportunity.
    """
    _validate_non_negative_finite(min_edge_bps, "min_edge_bps")
    _validate_non_negative_finite(max_quote_age_s, "max_quote_age_s")
    _validate_non_negative_finite(max_universe_age_s, "max_universe_age_s")
    if type(limit) is not int or limit < 0:
        raise ValueError("limit must be a non-negative integer")

    store = NegRiskQuoteStore(db_path)
    projection = store.latest_complete_projection()
    if projection is None:
        raise QuoteRunUnavailableError("quote run unavailable")
    source_universe = store.verified_universe_for_snapshot(
        projection.universe_snapshot_id
    )
    if source_universe.universe_hash != projection.universe_hash:
        raise QuoteUniverseUnavailableError(
            "verified source identity does not match quote run"
        )

    now = now_s()
    quote_age_seconds = max(0.0, now - projection.quoted_at_ms / 1000)
    if quote_age_seconds > max_quote_age_s:
        raise StaleQuoteRunError(
            f"quote age {quote_age_seconds:.1f}s exceeds {max_quote_age_s:.1f}s"
        )
    universe_age_seconds = max(0.0, now - projection.universe_taken_at_ms / 1000)
    if universe_age_seconds > max_universe_age_s:
        raise StaleUniverseError(
            f"universe age {universe_age_seconds:.1f}s exceeds {max_universe_age_s:.1f}s"
        )

    groups: dict[str, list[object]] = {}
    for quote in projection.quotes:
        groups.setdefault(quote.neg_risk_market_id, []).append(quote)

    rejections = Counter(
        _bounded_rejection_reason(rejection.reason)
        for rejection in source_universe.rejections
    )
    opportunities: list[NegRiskOpportunity] = []
    threshold = Decimal(str(min_edge_bps))
    for group_id, group_quotes in groups.items():
        if len(group_quotes) < 2:
            rejections["invalid-identity"] += 1
            continue
        event_ids = {quote.event_id for quote in group_quotes}
        membership_hashes = {quote.membership_hash for quote in group_quotes}
        if (
            len(event_ids) != 1
            or len(membership_hashes) != 1
            or not next(iter(event_ids)).strip()
            or not next(iter(membership_hashes)).strip()
        ):
            rejections["invalid-identity"] += 1
            continue
        legs: list[OpportunityLeg] = []
        for quote in group_quotes:
            if quote.terminal_state != "executable":
                break
            # The quote store validates executable values at write time.  Keep
            # this boundary defensive in case a legacy database bypassed it.
            if (
                quote.best_ask_price is None
                or quote.best_ask_size is None
                or not (0 < float(quote.best_ask_price) <= 1)
                or float(quote.best_ask_size) <= 0
            ):
                break
            legs.append(
                OpportunityLeg(
                    market_id=quote.market_id,
                    condition_id=quote.condition_id,
                    slug=quote.slug or "",
                    yes_token_id=quote.yes_token_id,
                    ask_price=float(quote.best_ask_price),
                    ask_size=float(quote.best_ask_size),
                )
            )
        if len(legs) != len(group_quotes):
            rejections["incomplete-quotes"] += 1
            continue

        sum_asks_decimal = sum((Decimal(str(leg.ask_price)) for leg in legs), Decimal(0))
        edge_bps = (Decimal(1) - sum_asks_decimal) * Decimal(10_000)
        if edge_bps < threshold or edge_bps <= 0:
            continue
        quantity = min(leg.ask_size for leg in legs)
        gross_profit = Decimal(str(quantity)) * (Decimal(1) - sum_asks_decimal)
        opportunities.append(
            NegRiskOpportunity(
                group_id=group_id,
                snapshot_id=projection.universe_snapshot_id,
                snapshot_age_seconds=universe_age_seconds,
                sum_asks=float(sum_asks_decimal),
                gross_edge_bps=float(edge_bps),
                executable_quantity=quantity,
                gross_profit=float(gross_profit),
                legs=tuple(legs),
                quote_run_id=projection.run_id,
                quote_age_seconds=quote_age_seconds,
                universe_snapshot_id=projection.universe_snapshot_id,
                universe_age_seconds=universe_age_seconds,
                event_id=next(iter(event_ids)),
                membership_hash=next(iter(membership_hashes)),
                quality="complete-supported",
            )
        )
    opportunities.sort(key=lambda item: (-item.gross_edge_bps, item.group_id))
    return OpportunityScanResult(
        opportunities=tuple(opportunities[:limit]),
        rejections={
            reason: count
            for reason, count in sorted(rejections.items())
            if count > 0
        },
        source_snapshot_id=projection.universe_snapshot_id,
        universe_hash=projection.universe_hash,
        quote_run_id=projection.run_id,
    )


def _bounded_rejection_reason(reason: str) -> str:
    return reason if reason in BOUNDED_REJECTION_REASONS else "invalid-identity"


def _validate_non_negative_finite(value: float, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{name} must be finite and non-negative")
