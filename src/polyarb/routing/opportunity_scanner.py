"""Fail-closed neg-risk buy-all opportunity discovery from an M1 snapshot."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from math import isfinite
from pathlib import Path


class StaleSnapshotError(RuntimeError):
    """The source snapshot is too old to support an executable claim."""


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

    def to_dict(self) -> dict:
        return asdict(self)


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
        sum_asks_decimal = sum(
            (Decimal(str(leg.ask_price)) for leg in legs), Decimal(0)
        )
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
