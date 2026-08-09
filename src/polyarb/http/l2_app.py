"""Starlette app factory for the L2 daemon HTTP server.

Phase 03 Plan 03 — D-06: separate process boundary for L2 orderbook tracking.

Routes registered at Plan 03 boundary:
- GET /health   (public, IETF strict 三态 — Better Stack alarm target)
- GET /healthz  (public, ALWAYS HTTP 200 — Fly platform probe target — BUG-6)

Phase 04.1 Plan 03 — G-03 (D-03.4): L2's FIRST HMAC-gated admin endpoint.
    POST /control/chaos/ws-test-kill — in-band chaos primitive (no restart).
    Reuses ControlAuthMiddleware from polyarb.http.control (path-guard /control/*).
    /health + /healthz automatically bypass the guard (not under /control/).

app.state stashes sqlite_store + settings + ws_consumer + event_listener
for route handlers.
"""

from __future__ import annotations

from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Route

from polyarb.http.control import ControlAuthMiddleware
from polyarb.http.l2_console import l2_console
from polyarb.http.l2_control import ws_test_kill_handler
from polyarb.http.l2_health import health, healthz

_LOCAL_BOUNDARY_RUNTIME = object()


def create_l2_app(
    *,
    sqlite_store: Any,
    settings: Any,
    ws_consumer: Any | None = None,
    event_listener: Any | None = None,
    evidence_runtime: Any = _LOCAL_BOUNDARY_RUNTIME,
) -> Starlette:
    """Build L2 Starlette app — /health + /healthz + /control/chaos/ws-test-kill.

    Plan 03 boundary: ws_consumer / event_listener default None.
    Plan 04 wires real WsConsumer; Plan 05 wires real EventListener.
    Plan 04.1-03 adds the first HMAC admin route on L2.

    Args:
        sqlite_store: SQLiteStore instance (stashed on app.state).
        settings: Settings instance (provides db_path, version, release_id, etc.).
        ws_consumer: Plan 04 WsConsumer instance, or None at Plan 03.
        event_listener: Plan 05 EventListener instance, or None at Plan 03.

    Returns:
        Configured Starlette application ready for uvicorn.

    Security note (T-04.1-08):
        ControlAuthMiddleware path-guard is hard-coded to /control — only routes
        under /control/* are HMAC-checked. /health + /healthz are exempt (they do
        not start with /control). Missing/wrong X-Signature → 401 before handler.
    """
    secret = settings.scan_shared_secret.get_secret_value()

    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/healthz", healthz, methods=["GET"]),
        Route("/console", l2_console, methods=["GET"]),
        # Phase 04.1 G-03: in-band chaos endpoint — HMAC-gated by middleware below.
        # Route lives under /control/ so ControlAuthMiddleware's path-guard covers it.
        Route(
            "/control/chaos/ws-test-kill",
            ws_test_kill_handler,
            methods=["POST"],
        ),
    ]
    middleware = [
        # Reuse L1 ControlAuthMiddleware (control.py) unchanged.
        # Path-guard: only /control/* requests are HMAC-verified.
        # /health + /healthz pass through without a signature check.
        Middleware(ControlAuthMiddleware, secret=secret),
    ]
    app = Starlette(routes=routes, middleware=middleware)
    # Stash dependencies on app.state for handlers to access
    app.state.sqlite_store = sqlite_store
    app.state.settings = settings
    app.state.ws_consumer = ws_consumer
    app.state.event_listener = event_listener
    # Omission is reserved for explicit local/legacy fixtures and warns.  A
    # configured L2 caller passes its exact runtime; explicitly passing None is
    # therefore a fail-closed wiring error rather than an implicit opt-out.
    app.state.l3_evidence_runtime_required = evidence_runtime is not _LOCAL_BOUNDARY_RUNTIME
    app.state.l3_evidence_runtime = (
        None if evidence_runtime is _LOCAL_BOUNDARY_RUNTIME else evidence_runtime
    )
    return app
