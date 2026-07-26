"""Read-only production surface for M1→M2 opportunity discovery."""

from __future__ import annotations

import sqlite3
from math import isfinite

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.routing.neg_risk_quote_store import QuoteUniverseUnavailableError
from polyarb.routing.opportunity_scanner import (
    QUOTE_SLA_SECONDS,
    UNIVERSE_SLA_SECONDS,
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
    scan_verified_neg_risk_quote_run,
)


async def opportunities(request: Request) -> JSONResponse:
    try:
        min_edge_bps = float(request.query_params.get("min_edge_bps", "0"))
        limit = min(100, max(1, int(request.query_params.get("limit", "20"))))
        if not isfinite(min_edge_bps) or min_edge_bps < 0:
            raise ValueError
    except ValueError:
        return JSONResponse({"error": "invalid numeric query"}, status_code=400)
    try:
        result = scan_verified_neg_risk_quote_run(
            request.app.state.sqlite_store.db_path,
            min_edge_bps=min_edge_bps,
            max_quote_age_s=QUOTE_SLA_SECONDS,
            max_universe_age_s=UNIVERSE_SLA_SECONDS,
            limit=limit,
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
    except (sqlite3.Error, ValueError):
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
            "count": len(result.opportunities),
            "rejections": dict(result.rejections),
            "opportunities": [item.to_dict() for item in result.opportunities],
        }
    )
