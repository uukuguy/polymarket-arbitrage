"""Read-only production surface for M1→M2 opportunity discovery."""

from __future__ import annotations

import sqlite3
from math import isfinite

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.routing.opportunity_scanner import (
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
    scan_neg_risk_quote_run,
)


async def opportunities(request: Request) -> JSONResponse:
    try:
        min_edge_bps = float(request.query_params.get("min_edge_bps", "0"))
        limit = min(100, max(1, int(request.query_params.get("limit", "20"))))
        if not isfinite(min_edge_bps):
            raise ValueError
    except ValueError:
        return JSONResponse({"error": "invalid numeric query"}, status_code=400)
    try:
        found = scan_neg_risk_quote_run(
            request.app.state.sqlite_store.db_path,
            min_edge_bps=min_edge_bps,
            max_quote_age_s=300,
            max_universe_age_s=50_400,
            limit=limit,
        )
    except QuoteRunUnavailableError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except StaleQuoteRunError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except StaleUniverseError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except (sqlite3.Error, ValueError) as error:
        return JSONResponse({"error": f"snapshot database: {error}"}, status_code=503)
    return JSONResponse(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "known-universe",
            "quote_sla_seconds": 300,
            "universe_sla_seconds": 50_400,
            "count": len(found),
            "opportunities": [item.to_dict() for item in found],
        }
    )
