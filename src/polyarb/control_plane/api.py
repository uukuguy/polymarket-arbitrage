"""Independent, Postgres-only operator read service for transactional M1."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from typing import Any

import psycopg
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from polyarb.http.control_plane import control_plane_status

from .postgres import PostgresControlPlane


async def control_plane_healthz(request: Request) -> JSONResponse:
    """Expose readiness only when the durable authority is readable."""
    response = await control_plane_status(request)
    if response.status_code != 200:
        return response
    return JSONResponse({"status": "ok", "control_plane": "available"})


async def current_opportunities(request: Request) -> JSONResponse:
    """Serve only the certified transactional opportunity projection.

    This intentionally has no SQLite fallback: absence of a Postgres projection
    is an unavailable authority, not evidence of zero opportunities.
    """
    control_plane = getattr(request.app.state, "control_plane", None)
    if control_plane is None or not hasattr(control_plane, "current_opportunities"):
        return JSONResponse(
            {"status": "unavailable", "reason": "opportunity-projection-unavailable"},
            status_code=503,
        )
    try:
        limit = int(request.query_params.get("limit", "50"))
        after_group_id = request.query_params.get("after_group_id", "")
        if not 1 <= limit <= 500 or len(after_group_id) > 256 or "\x00" in after_group_id:
            raise ValueError("invalid-opportunity-page")
        return JSONResponse(
            control_plane.current_opportunities(limit=limit, after_group_id=after_group_id)
        )
    except ValueError:
        return JSONResponse({"error": "invalid opportunity page"}, status_code=400)
    except Exception:
        return JSONResponse(
            {"status": "unavailable", "reason": "opportunity-projection-unavailable"},
            status_code=503,
        )


def create_control_plane_app(*, control_plane: Any | None) -> Starlette:
    """Create an HTTP app with no SQLite, scheduler, or data-worker dependency."""
    app = Starlette(
        routes=[
            Route("/healthz", control_plane_healthz, methods=["GET"]),
            Route("/perception/control-plane", control_plane_status, methods=["GET"]),
            Route("/perception/opportunities", current_opportunities, methods=["GET"]),
        ]
    )
    app.state.control_plane = control_plane
    return app


def _build_control_plane(dsn: str) -> PostgresControlPlane:
    """Build the standalone API's Postgres client with a bounded connect."""
    return PostgresControlPlane(lambda: psycopg.connect(dsn, connect_timeout=5))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="control-plane-api")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args(argv)
    dsn = os.environ.get("POLYARB_SUPABASE_DB_DSN", "").strip()
    if not dsn:
        parser.error("POLYARB_SUPABASE_DB_DSN is required")
    default_port = int(os.environ.get("POLYARB_HTTP_PORT", "8080"))
    port = args.port or default_port
    if port <= 0:
        parser.error("--port must be positive")
    control_plane = _build_control_plane(dsn)
    uvicorn.run(
        create_control_plane_app(control_plane=control_plane),
        host=args.host,
        port=port,
        log_config=None,
        access_log=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
