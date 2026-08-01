"""Fail-closed neg-risk buy-all opportunity discovery from an M1 snapshot."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Literal

from polyarb.routing.neg_risk_quote_store import (
    CompleteQuoteProjection,
    NegRiskQuoteStore,
)
from polyarb.routing.quote_timing import QUOTE_AGE_SLA_SECONDS
from polyarb.storage.sqlite_store import (
    StructureGenerationReadError,
    structure_read_transaction,
)

QUOTE_SLA_SECONDS = int(QUOTE_AGE_SLA_SECONDS)
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


@dataclass(frozen=True)
class GroupAssessment:
    """One group classification from exactly one certified Quote projection."""

    group_id: str
    event_id: str | None
    membership_hash: str | None
    status: Literal["observe", "no-edge", "unavailable"]
    reason: str | None
    bundle_cost: float | None
    gross_edge_bps: float | None
    max_bundle_size: float | None
    legs: tuple[OpportunityLeg, ...]
    structure_revision: int
    quote_run_id: int
    quoted_at_ms: int


@dataclass(frozen=True)
class AssessmentResult:
    """Complete classification result; callers choose their own projection."""

    assessments: tuple[GroupAssessment, ...]
    rejections: Mapping[str, int]

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
    structure_generation_read_mode: str = "legacy",
) -> list[NegRiskOpportunity]:
    """Return executable buy-all bundles ordered by gross edge."""
    if not isfinite(min_edge_bps):
        raise ValueError("min_edge_bps must be finite")
    if max_snapshot_age_s is not None and (
        not isfinite(max_snapshot_age_s) or max_snapshot_age_s < 0
    ):
        raise ValueError("max_snapshot_age_s must be finite and non-negative")
    try:
        with structure_read_transaction(
            db_path,
            mode=structure_generation_read_mode,
            legacy_latest_snapshot=True,
        ) as read:
            snapshot_id, taken_at_ms = read.snapshot_id, read.taken_at_ms
            age_seconds = max(0.0, time.time() - taken_at_ms / 1000)
            if max_snapshot_age_s is not None and age_seconds > max_snapshot_age_s:
                raise StaleSnapshotError(
                    f"snapshot age {age_seconds:.1f}s exceeds {max_snapshot_age_s:.1f}s"
                )
            rows = read.connection.execute(
                "SELECT neg_risk_market_id, market_id, condition_id, slug, "
                "yes_token_id, best_ask_price, best_ask_size, active, closed, incomplete "
                f"FROM {read.table('markets')} WHERE snapshot_id = ? "
                "AND neg_risk_market_id IS NOT NULL "
                "ORDER BY neg_risk_market_id, market_id",
                (snapshot_id,),
            ).fetchall()
    except StructureGenerationReadError:
        if structure_generation_read_mode == "generation":
            raise
        return []

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
    return scan_certified_neg_risk_quote_projection(
        projection,
        min_edge_bps=min_edge_bps,
        max_quote_age_s=max_quote_age_s,
        max_universe_age_s=max_universe_age_s,
        limit=limit,
        now_s=now_s,
    )


def assess_certified_neg_risk_quote_projection(
    projection: CompleteQuoteProjection,
    *,
    min_edge_bps: float = 0,
    max_quote_age_s: float = 300,
    max_universe_age_s: float = 50_400,
    now_s: Callable[[], float] = time.time,
) -> AssessmentResult:
    """Classify every group in one complete, fresh Quote projection.

    Unlike the legacy candidate scan, this keeps the distinction between a
    valid group below threshold and a group whose quote evidence is incomplete.
    """
    _validate_non_negative_finite(min_edge_bps, "min_edge_bps")
    _validate_non_negative_finite(max_quote_age_s, "max_quote_age_s")
    _validate_non_negative_finite(max_universe_age_s, "max_universe_age_s")

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

    expected_by_group: dict[str, list[object]] = {}
    for source_leg in projection.source_universe.legs:
        expected_by_group.setdefault(source_leg.neg_risk_market_id, []).append(source_leg)
    quotes_by_group: dict[str, list[object]] = {}
    for quote in projection.quotes:
        quotes_by_group.setdefault(quote.neg_risk_market_id, []).append(quote)

    threshold = Decimal(str(min_edge_bps))
    assessments: list[GroupAssessment] = []
    for group_id in sorted(expected_by_group):
        expected = expected_by_group[group_id]
        quotes = quotes_by_group.get(group_id, [])
        event_ids = {leg.event_id for leg in expected}
        membership_hashes = {leg.membership_hash for leg in expected}
        event_id = next(iter(event_ids), None)
        membership_hash = next(iter(membership_hashes), None)
        unavailable_reason: str | None = None
        if (
            len(expected) < 2
            or len(event_ids) != 1
            or len(membership_hashes) != 1
            or not event_id
            or not membership_hash
        ):
            unavailable_reason = "invalid-identity"
        elif {leg.yes_token_id for leg in expected} != {
            quote.yes_token_id for quote in quotes
        }:
            unavailable_reason = "incomplete-quotes"

        legs: list[OpportunityLeg] = []
        if unavailable_reason is None:
            for quote in quotes:
                if (
                    quote.event_id != event_id
                    or quote.membership_hash != membership_hash
                    or quote.terminal_state != "executable"
                    or quote.best_ask_price is None
                    or quote.best_ask_size is None
                    or not (0 < float(quote.best_ask_price) <= 1)
                    or float(quote.best_ask_size) <= 0
                ):
                    unavailable_reason = "incomplete-quotes"
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

        if unavailable_reason is not None:
            assessments.append(
                GroupAssessment(
                    group_id=group_id,
                    event_id=event_id,
                    membership_hash=membership_hash,
                    status="unavailable",
                    reason=unavailable_reason,
                    bundle_cost=None,
                    gross_edge_bps=None,
                    max_bundle_size=None,
                    legs=(),
                    structure_revision=projection.universe_snapshot_id,
                    quote_run_id=projection.run_id,
                    quoted_at_ms=projection.quoted_at_ms,
                )
            )
            continue

        bundle_cost = sum((Decimal(str(leg.ask_price)) for leg in legs), Decimal(0))
        gross_edge_bps = (Decimal(1) - bundle_cost) * Decimal(10_000)
        assessments.append(
            GroupAssessment(
                group_id=group_id,
                event_id=event_id,
                membership_hash=membership_hash,
                status="observe" if gross_edge_bps >= threshold else "no-edge",
                reason=None,
                bundle_cost=float(bundle_cost),
                gross_edge_bps=float(gross_edge_bps),
                max_bundle_size=min(leg.ask_size for leg in legs),
                legs=tuple(legs),
                structure_revision=projection.universe_snapshot_id,
                quote_run_id=projection.run_id,
                quoted_at_ms=projection.quoted_at_ms,
            )
        )

    rejections = Counter(
        _bounded_rejection_reason(rejection.reason)
        for rejection in projection.source_universe.rejections
    )
    return AssessmentResult(
        assessments=tuple(assessments),
        rejections={reason: count for reason, count in sorted(rejections.items()) if count},
    )


def scan_certified_neg_risk_quote_projection(
    projection: CompleteQuoteProjection,
    *,
    min_edge_bps: float = 0,
    max_quote_age_s: float = 300,
    max_universe_age_s: float = 50_400,
    limit: int = 50,
    now_s: Callable[[], float] = time.time,
) -> OpportunityScanResult:
    """Scan one immutable projection already certified by the quote worker."""
    _validate_non_negative_finite(min_edge_bps, "min_edge_bps")
    _validate_non_negative_finite(max_quote_age_s, "max_quote_age_s")
    _validate_non_negative_finite(max_universe_age_s, "max_universe_age_s")
    if type(limit) is not int or limit < 0:
        raise ValueError("limit must be a non-negative integer")

    source_universe = projection.source_universe

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
