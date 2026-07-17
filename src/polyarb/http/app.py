"""Starlette app factory for the L1 daemon HTTP server.

Phase 02 Plan 02 — D-21 / D-22.
Phase 02.1 Plan 02 — D-03: /control/* HMAC-protected routes.
Phase 02.1 Plan 03 — D-05: /healthz Fly-friendly always-200 probe.

create_app() wires:
- /health  (public, IETF strict 三态 — Better Stack alarm target)
- /healthz (public, ALWAYS HTTP 200 — Fly platform probe target)
- /scan    (HMAC-protected, P1 trust-split)
- /control/* (HMAC-protected, same secret per D-22)

Middleware:
- ScanAuthMiddleware / ControlAuthMiddleware both bypass /health and /healthz
  via path guards inside their respective middlewares. /healthz must stay
  public-no-HMAC because Fly platform probe is unauthenticated.

app.state stashes scheduler + sqlite_store + settings for route handlers.

Source: starlette.io (RESEARCH.md §9 lines 1372-1398)
"""
from __future__ import annotations

from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Route

from polyarb.http.arbitrage import opportunities
from polyarb.http.control import (
    ControlAuthMiddleware,
    control_status,
    pause,
    unpause,
)
from polyarb.http.health import health, healthz
from polyarb.http.scan import scan, scan_auth_middleware


class ScanAuthMiddleware(BaseHTTPMiddleware):
    """Wraps scan_auth_middleware as a Starlette BaseHTTPMiddleware class.

    Starlette's Middleware() factory requires a class; this adapter bridges
    the functional scan_auth_middleware to the class-based interface.
    The secret is passed at construction time from settings.scan_shared_secret.
    """

    def __init__(self, app: Any, secret: str) -> None:
        super().__init__(app)
        self._secret = secret

    async def dispatch(self, request: Any, call_next: Any) -> Any:
        return await scan_auth_middleware(request, call_next, secret=self._secret)


def create_app(*, scheduler: Any, sqlite_store: Any, settings: Any) -> Starlette:
    """Factory: build Starlette app with /health + /scan routes.

    Args:
        scheduler: SnapshotScheduler instance (stashed on app.state)
        sqlite_store: SQLiteStore instance (stashed on app.state)
        settings: Settings instance (provides scan_shared_secret + db_path etc.)

    Returns:
        Configured Starlette application ready for uvicorn.
    """
    # Extract secret value for HMAC middleware
    secret = settings.scan_shared_secret.get_secret_value()

    middleware = [
        Middleware(ScanAuthMiddleware, secret=secret),
        # D-03: /control/* HMAC, same secret per D-22.
        Middleware(ControlAuthMiddleware, secret=secret),
    ]
    routes = [
        Route("/health", health, methods=["GET"]),
        # D-05 Phase 02.1: Fly probe target (always 200).
        Route("/healthz", healthz, methods=["GET"]),
        Route("/arbitrage/opportunities", opportunities, methods=["GET"]),
        Route("/scan", scan, methods=["POST"]),
        Route("/control/unpause", unpause, methods=["POST"]),       # D-03 Phase 02.1
        Route("/control/pause", pause, methods=["POST"]),           # stub 501, Phase 03+ 填实现
        Route("/control/status", control_status, methods=["GET"]),  # stub 501, Phase 03+ 填实现
    ]

    app = Starlette(routes=routes, middleware=middleware)

    # Stash dependencies on app.state for handlers to access
    app.state.scheduler = scheduler
    app.state.sqlite_store = sqlite_store
    app.state.settings = settings

    return app
