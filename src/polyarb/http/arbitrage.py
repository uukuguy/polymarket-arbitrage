"""Read-only production surface for M1→M2 opportunity discovery."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from math import isfinite
from pathlib import Path

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.http.health import read_market_truth_health
from polyarb.routing.neg_risk_quote_store import QuoteUniverseUnavailableError
from polyarb.routing.opportunity_scanner import (
    QUOTE_SLA_SECONDS,
    UNIVERSE_SLA_SECONDS,
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
)

_SOURCE_TRUTH_READ_TIMEOUT_S = 1.0


def _last_complete_snapshot_id(db_path: Path) -> int | None:
    return read_market_truth_health(
        db_path,
        time.time(),
    ).last_complete_snapshot_id


def _select_cached_opportunities(
    feed,
    *,
    min_edge_bps: float,
    limit: int,
    now_s: float,
):
    projection = feed.projection
    result = feed.opportunity_scan
    if result is None:
        raise QuoteRunUnavailableError("quote run unavailable")
    quote_age_seconds = max(0.0, now_s - projection.quoted_at_ms / 1000)
    if quote_age_seconds > QUOTE_SLA_SECONDS:
        raise StaleQuoteRunError(
            f"quote age {quote_age_seconds:.1f}s exceeds {QUOTE_SLA_SECONDS:.1f}s"
        )
    universe_age_seconds = max(
        0.0,
        now_s - projection.universe_taken_at_ms / 1000,
    )
    if universe_age_seconds > UNIVERSE_SLA_SECONDS:
        raise StaleUniverseError(
            "universe age "
            f"{universe_age_seconds:.1f}s exceeds {UNIVERSE_SLA_SECONDS:.1f}s"
        )
    selected = tuple(
        item
        for item in result.opportunities
        if item.gross_edge_bps >= min_edge_bps
    )[:limit]
    return result, selected, quote_age_seconds, universe_age_seconds


async def opportunities(request: Request) -> JSONResponse:
    try:
        min_edge_bps = float(request.query_params.get("min_edge_bps", "0"))
        limit = min(100, max(1, int(request.query_params.get("limit", "20"))))
        if not isfinite(min_edge_bps) or min_edge_bps < 0:
            raise ValueError
    except ValueError:
        return JSONResponse({"error": "invalid numeric query"}, status_code=400)
    runtime = getattr(request.app.state, "quote_worker_runtime", None)
    feed = (
        runtime.certified_feed()
        if runtime is not None
        else None
    )
    if feed is None or feed.opportunity_scan is None:
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
    try:
        source_snapshot_id = await asyncio.wait_for(
            asyncio.to_thread(
                _last_complete_snapshot_id,
                request.app.state.sqlite_store.db_path,
            ),
            timeout=_SOURCE_TRUTH_READ_TIMEOUT_S,
        )
        if source_snapshot_id != feed.projection.universe_snapshot_id:
            raise QuoteUniverseUnavailableError("source-snapshot-mismatch")
        result, selected, quote_age_seconds, universe_age_seconds = (
            _select_cached_opportunities(
                feed,
                min_edge_bps=min_edge_bps,
                limit=limit,
                now_s=time.time(),
            )
        )
    except (QuoteUniverseUnavailableError, QuoteRunUnavailableError):
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
    except StaleQuoteRunError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except StaleUniverseError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except (sqlite3.Error, TimeoutError, ValueError):
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
    return JSONResponse(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "verified-standard-neg-risk",
            "source_snapshot_id": result.source_snapshot_id,
            "universe_hash": result.universe_hash,
            "quote_run_id": result.quote_run_id,
            "quote_sla_seconds": QUOTE_SLA_SECONDS,
            "count": len(selected),
            "rejections": dict(result.rejections),
            "opportunities": [
                {
                    **item.to_dict(),
                    "snapshot_age_seconds": universe_age_seconds,
                    "quote_age_seconds": quote_age_seconds,
                    "universe_age_seconds": universe_age_seconds,
                }
                for item in selected
            ],
        }
    )
