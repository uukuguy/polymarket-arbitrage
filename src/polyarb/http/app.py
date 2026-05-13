"""Starlette app factory for the L1 daemon HTTP server.

Phase 02 Plan 02 — D-21 / D-22.

create_app() wires /health (public, IETF三态) + /scan (HMAC-protected, P1 trust-split).

Middleware:
- ScanAuthMiddleware wraps scan_auth_middleware in BaseHTTPMiddleware so it applies
  globally. The middleware itself bypasses /health (path check inside).

app.state stashes scheduler + sqlite_store + settings for route handlers.

Source: starlette.io (RESEARCH.md §9 lines 1372-1398)
"""
from __future__ import annotations

from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Route

from polyarb.http.health import health
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
    ]
    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/scan", scan, methods=["POST"]),
    ]

    app = Starlette(routes=routes, middleware=middleware)

    # Stash dependencies on app.state for handlers to access
    app.state.scheduler = scheduler
    app.state.sqlite_store = sqlite_store
    app.state.settings = settings

    return app
