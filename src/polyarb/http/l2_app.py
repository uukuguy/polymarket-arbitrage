"""Starlette app factory for the L2 daemon HTTP server.

Phase 03 Plan 03 — D-06: separate process boundary for L2 orderbook tracking.

Routes registered at Plan 03 boundary:
- GET /health   (public, IETF strict 三态 — Better Stack alarm target)
- GET /healthz  (public, ALWAYS HTTP 200 — Fly platform probe target — BUG-6)

No /control/* or /scan endpoints at Plan 03 — those are L1-only.
Plan 04 will pass a real WsConsumer via the ws_consumer kwarg.
Plan 05 will pass a real EventListener via the event_listener kwarg.
Plan 03 ships placeholders (default None) — health checks handle gracefully.

app.state stashes sqlite_store + settings + ws_consumer + event_listener
for route handlers.
"""
from __future__ import annotations

from typing import Any

from starlette.applications import Starlette
from starlette.routing import Route

from polyarb.http.l2_health import health, healthz


def create_l2_app(
    *,
    sqlite_store: Any,
    settings: Any,
    ws_consumer: Any | None = None,
    event_listener: Any | None = None,
) -> Starlette:
    """Build L2 Starlette app — /health (IETF strict) + /healthz (always 200).

    Plan 03 boundary: ws_consumer / event_listener default None.
    Plan 04 wires real WsConsumer; Plan 05 wires real EventListener.

    Args:
        sqlite_store: SQLiteStore instance (stashed on app.state).
        settings: Settings instance (provides db_path, version, release_id, etc.).
        ws_consumer: Plan 04 WsConsumer instance, or None at Plan 03.
        event_listener: Plan 05 EventListener instance, or None at Plan 03.

    Returns:
        Configured Starlette application ready for uvicorn.
    """
    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/healthz", healthz, methods=["GET"]),
    ]
    app = Starlette(routes=routes)
    # Stash dependencies on app.state for handlers to access
    app.state.sqlite_store = sqlite_store
    app.state.settings = settings
    app.state.ws_consumer = ws_consumer
    app.state.event_listener = event_listener
    return app
