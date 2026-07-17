"""Read-only production surface for M1→M2 opportunity discovery."""

from __future__ import annotations

import sqlite3
from math import isfinite

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.routing.opportunity_scanner import (
    StaleSnapshotError,
    scan_neg_risk_buy_all,
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
        found = scan_neg_risk_buy_all(
            request.app.state.sqlite_store.db_path,
            min_edge_bps=min_edge_bps,
            max_snapshot_age_s=900,
            limit=limit,
        )
    except StaleSnapshotError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except (sqlite3.Error, ValueError) as error:
        return JSONResponse({"error": f"snapshot database: {error}"}, status_code=503)
    return JSONResponse(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "count": len(found),
            "opportunities": [item.to_dict() for item in found],
        }
    )
