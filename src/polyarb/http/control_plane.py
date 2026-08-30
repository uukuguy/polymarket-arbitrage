"""Independent HTTP projection for the durable M1 operator control plane."""

from __future__ import annotations

from typing import Any

import psycopg
from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.control_plane.blocking_bridge import run_blocking_call_with_timeout
from polyarb.control_plane.db_deadlines import CONTROL_PLANE_DB_POLICY

_SAMPLE_LIMIT = 20


def _database_pool_snapshot(control_plane: Any) -> dict[str, object] | None:
    reader = getattr(control_plane, "database_pool_snapshot", None)
    if not callable(reader):
        return None
    try:
        snapshot = reader()
    except Exception:
        return None
    return snapshot if isinstance(snapshot, dict) and snapshot else None


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
        snapshot = await run_blocking_call_with_timeout(
            control_plane.operational_snapshot,
            sample_limit=_SAMPLE_LIMIT,
            timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
            thread_name="control-plane-api:status-read",
        )
    except (TimeoutError, OSError, RuntimeError, TypeError, ValueError, psycopg.Error):
        payload: dict[str, object] = {
            "status": "unavailable",
            "reason": "control-plane-read-unavailable",
        }
        if (pool_snapshot := _database_pool_snapshot(control_plane)) is not None:
            payload["database_pool"] = pool_snapshot
        return JSONResponse(
            payload,
            status_code=503,
        )
    return JSONResponse({"status": "available", **snapshot})
