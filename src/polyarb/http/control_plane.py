"""Independent HTTP projection for the durable M1 operator control plane."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import psycopg
from starlette.requests import Request
from starlette.responses import JSONResponse

_SAMPLE_LIMIT = 20
_READ_TIMEOUT_S = 5.5


async def control_plane_status(request: Request) -> JSONResponse:
    """Return durable operator evidence or a typed unavailable response.

    This path deliberately cannot fall back to SQLite: a data-plane outage is
    precisely the event operators need to inspect from an independent source.
    """
    control_plane: Any | None = getattr(request.app.state, "control_plane", None)
    if control_plane is None:
        return JSONResponse(
            {"status": "unavailable", "reason": "control-plane-read-unavailable"},
            status_code=503,
        )
    try:
        snapshot = await asyncio.wait_for(
            asyncio.to_thread(
                control_plane.operational_snapshot,
                now=datetime.now(UTC),
                sample_limit=_SAMPLE_LIMIT,
            ),
            timeout=_READ_TIMEOUT_S,
        )
    except (TimeoutError, OSError, RuntimeError, TypeError, ValueError, psycopg.Error):
        return JSONResponse(
            {"status": "unavailable", "reason": "control-plane-read-unavailable"},
            status_code=503,
        )
    return JSONResponse({"status": "available", **snapshot})
