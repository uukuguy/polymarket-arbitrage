"""Independent, Postgres-only operator read service for transactional M1."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from typing import Any, cast

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from polyarb.config import Settings
from polyarb.http.control_plane import control_plane_status

from .blocking_bridge import run_blocking_call_with_timeout
from .db_deadlines import (
    CONTROL_PLANE_API_OPERATIONAL_POOL_MAX_SIZE,
    CONTROL_PLANE_API_READINESS_POOL_MAX_SIZE,
    CONTROL_PLANE_DB_POLICY,
    CONTROL_PLANE_HEALTH_DB_POLICY,
)
from .db_role_contract import scoped_connection_factory
from .postgres import PostgresControlPlane


async def control_plane_healthz(_request: Request) -> JSONResponse:
    """Keep Fly routing attached while the HTTP process can answer requests."""
    return JSONResponse({"status": "ok", "service": "control-plane-api"})


async def control_plane_health(request: Request) -> JSONResponse:
    """Expose strict readiness only when the durable authority is readable."""
    control_plane = getattr(request.app.state, "control_plane", None)
    if control_plane is None or not hasattr(control_plane, "readiness"):
        return JSONResponse(
            {"status": "unavailable", "reason": "control-plane-read-unavailable"},
            status_code=503,
        )
    try:
        ready = await run_blocking_call_with_timeout(
            control_plane.readiness,
            timeout_seconds=CONTROL_PLANE_HEALTH_DB_POLICY.request_timeout_seconds,
            thread_name="control-plane-api:readiness",
        )
    except Exception:
        return JSONResponse(
            {"status": "unavailable", "reason": "control-plane-read-unavailable"},
            status_code=503,
        )
    if ready is not True:
        return JSONResponse(
            {"status": "unavailable", "reason": "control-plane-read-unavailable"},
            status_code=503,
        )
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
        projection = await run_blocking_call_with_timeout(
            control_plane.current_opportunities,
            limit=limit,
            after_group_id=after_group_id,
            timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
            thread_name="control-plane-api:opportunity-read",
        )
        return JSONResponse(projection)
    except ValueError:
        return JSONResponse({"error": "invalid opportunity page"}, status_code=400)
    except Exception:
        return JSONResponse(
            {"status": "unavailable", "reason": "opportunity-projection-unavailable"},
            status_code=503,
        )


async def business_overview(request: Request) -> JSONResponse:
    """Serve one authority-owned M1 business snapshot without response composition."""
    control_plane = getattr(request.app.state, "control_plane", None)
    if control_plane is None or not hasattr(control_plane, "business_overview"):
        return JSONResponse(
            {"status": "unavailable", "reason": "business-overview-unavailable"},
            status_code=503,
        )
    try:
        overview = await run_blocking_call_with_timeout(
            control_plane.business_overview,
            timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
            thread_name="control-plane-api:business-overview-read",
        )
    except Exception:
        return JSONResponse(
            {"status": "unavailable", "reason": "business-overview-unavailable"},
            status_code=503,
        )
    return JSONResponse(overview)


async def business_research_page(request: Request) -> JSONResponse:
    """Transport one bounded, generation-bound Structure or Quote research page."""
    product = request.path_params["product"]
    method_name = {
        "quotes": "business_quote_page",
        "analysis": "business_analysis_page",
    }.get(product, f"business_{product.removesuffix('s')}_page")
    control_plane = getattr(request.app.state, "control_plane", None)
    reader = None if control_plane is None else getattr(control_plane, method_name, None)
    if not callable(reader):
        return JSONResponse(
            {"status": "unavailable", "reason": "business-research-unavailable"},
            status_code=503,
        )
    try:
        raw_limit = request.query_params.get("limit", "50")
        limit = int(raw_limit)
        generation_key = request.query_params.get("generation_key")
        after = request.query_params.get("after", "")
        if not 1 <= limit <= 200 or len(after) > 256 or "\x00" in after:
            raise ValueError("invalid-business-research-page")
        if generation_key is not None and (not generation_key or len(generation_key) > 256 or "\x00" in generation_key):
            raise ValueError("invalid-business-research-page")
        page = await run_blocking_call_with_timeout(
            reader,
            generation_key=generation_key,
            limit=limit,
            after=after,
            timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
            thread_name=f"control-plane-api:business-{product}-read",
        )
        return JSONResponse(page)
    except ValueError:
        return JSONResponse({"error": "invalid business research page"}, status_code=400)
    except Exception:
        return JSONResponse(
            {"status": "unavailable", "reason": "business-research-unavailable"},
            status_code=503,
        )


async def structure_intelligence(request: Request) -> JSONResponse:
    """Serve a bounded business view, never the raw Structure recovery index."""
    view = request.path_params["view"]
    method_name = f"structure_intelligence_{view}"
    control_plane = getattr(request.app.state, "control_plane", None)
    reader = None if control_plane is None else getattr(control_plane, method_name, None)
    if not callable(reader):
        return JSONResponse(
            {"status": "unavailable", "reason": "structure-intelligence-unavailable"},
            status_code=503,
        )
    try:
        generation_key = request.query_params.get("generation_key")
        if generation_key is not None and (not generation_key or len(generation_key) > 256 or "\x00" in generation_key):
            raise ValueError("invalid-structure-intelligence-page")
        if view == "summary":
            result = await run_blocking_call_with_timeout(
                reader,
                generation_key=generation_key,
                timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
                thread_name="control-plane-api:structure-intelligence-summary",
            )
        else:
            limit = int(request.query_params.get("limit", "50"))
            after = request.query_params.get("after", "")
            if not 1 <= limit <= 200 or len(after) > 256 or "\x00" in after:
                raise ValueError("invalid-structure-intelligence-page")
            if view == "events":
                raw_open_only = request.query_params.get("open_only")
                if raw_open_only not in (None, "true", "false"):
                    raise ValueError("invalid-structure-intelligence-page")
                result = await run_blocking_call_with_timeout(
                    reader,
                    generation_key=generation_key,
                    limit=limit,
                    after=after,
                    open_only=None if raw_open_only is None else raw_open_only == "true",
                    timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
                    thread_name="control-plane-api:structure-intelligence-events",
                )
            else:
                quality = request.query_params.get("quality")
                if quality is not None and (not quality or len(quality) > 128 or "\x00" in quality):
                    raise ValueError("invalid-structure-intelligence-page")
                result = await run_blocking_call_with_timeout(
                    reader,
                    generation_key=generation_key,
                    limit=limit,
                    after=after,
                    quality=quality,
                    timeout_seconds=CONTROL_PLANE_DB_POLICY.request_timeout_seconds,
                    thread_name="control-plane-api:structure-intelligence-groups",
                )
        return JSONResponse(result)
    except ValueError:
        return JSONResponse({"error": "invalid structure intelligence page"}, status_code=400)
    except Exception:
        return JSONResponse(
            {"status": "unavailable", "reason": "structure-intelligence-unavailable"},
            status_code=503,
        )
def create_control_plane_app(*, control_plane: Any | None) -> Starlette:
    """Create an HTTP app with no SQLite, scheduler, or data-worker dependency."""
    app = Starlette(
        routes=[
            Route("/healthz", control_plane_healthz, methods=["GET"]),
            Route("/health", control_plane_health, methods=["GET"]),
            Route("/perception/control-plane", control_plane_status, methods=["GET"]),
            Route("/perception/opportunities", current_opportunities, methods=["GET"]),
            Route("/perception/business-overview", business_overview, methods=["GET"]),
            Route("/perception/business/structure/{view:str}", structure_intelligence, methods=["GET"]),
            Route("/perception/business/{product:str}", business_research_page, methods=["GET"]),
        ]
    )
    app.state.control_plane = control_plane
    return app


def _build_control_plane(dsn: str) -> PostgresControlPlane:
    """Build the API with the same bounded session contract as every daemon."""
    return PostgresControlPlane(
        cast(
            Any,
            scoped_connection_factory(
                dsn,
                pool_max_size=CONTROL_PLANE_API_OPERATIONAL_POOL_MAX_SIZE,
            ),
        ),
        readiness_connection_factory=cast(
            Any,
            scoped_connection_factory(
                dsn,
                deadline_policy=CONTROL_PLANE_HEALTH_DB_POLICY,
                pool_max_size=CONTROL_PLANE_API_READINESS_POOL_MAX_SIZE,
            ),
        ),
        database_capacity_budget_bytes=Settings().m1_database_capacity_budget_bytes,
    )


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
    try:
        uvicorn.run(
            create_control_plane_app(control_plane=control_plane),
            host=args.host,
            port=port,
            log_config=None,
            access_log=False,
        )
    finally:
        control_plane.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
