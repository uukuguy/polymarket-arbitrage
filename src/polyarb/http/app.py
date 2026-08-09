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

import os
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Route

from polyarb.http.arbitrage import opportunities
from polyarb.http.control import (
    ControlAuthMiddleware,
    build_market_map,
    control_status,
    pause,
    queue_perception_discovery,
    queue_perception_reconciliation,
    scan_neg_risk_map,
    unpause,
)
from polyarb.http.health import health, healthz
from polyarb.http.market_map import (
    market_map,
    opportunity_history,
    opportunity_watch_status,
)
from polyarb.http.opportunity_read_health import (
    BoundedReadLane,
    OpportunityReadHealth,
)
from polyarb.http.perception import (
    perception_console,
    perception_discovery,
    perception_group_history,
    perception_group_timeline,
    perception_groups,
    perception_incident_history,
    perception_incidents,
    perception_opportunities,
    perception_qualification,
    perception_recent_incidents,
    perception_reconciliation,
    perception_resources,
    perception_status,
)
from polyarb.http.perception_faults import (
    arm_fault,
    cleanup_fault,
    export_fault,
    fault_runtime,
    fault_status,
    finalize_fault,
)
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


def create_app(
    *,
    scheduler: Any,
    sqlite_store: Any,
    settings: Any,
    quote_worker_runtime: Any | None = None,
    quote_worker: Any | None = None,
    opportunity_watcher: Any | None = None,
    candidate_watcher_runtime: Any | None = None,
) -> Starlette:
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
        Route(
            "/arbitrage/opportunities/{opportunity_id}/history",
            opportunity_history,
            methods=["GET"],
        ),
        Route("/market-map", market_map, methods=["GET"]),
        Route("/opportunity-watch/status", opportunity_watch_status, methods=["GET"]),
        Route("/perception/status", perception_status, methods=["GET"]),
        Route("/perception/console", perception_console, methods=["GET"]),
        Route(
            "/perception/opportunities",
            perception_opportunities,
            methods=["GET"],
        ),
        Route("/perception/groups", perception_groups, methods=["GET"]),
        Route(
            "/perception/groups/{group_id:path}/timeline",
            perception_group_timeline,
            methods=["GET"],
        ),
        Route(
            "/perception/groups/{group_id:path}/history",
            perception_group_history,
            methods=["GET"],
        ),
        Route("/perception/discovery", perception_discovery, methods=["GET"]),
        Route("/perception/reconciliation", perception_reconciliation, methods=["GET"]),
        Route(
            "/perception/incidents/recent",
            perception_recent_incidents,
            methods=["GET"],
        ),
        Route(
            "/perception/incidents/{incident_id}/history",
            perception_incident_history,
            methods=["GET"],
        ),
        Route("/perception/incidents", perception_incidents, methods=["GET"]),
        Route("/perception/qualification", perception_qualification, methods=["GET"]),
        Route("/perception/resources", perception_resources, methods=["GET"]),
        Route("/perception/faults/runtime", fault_runtime, methods=["GET"]),
        Route("/perception/faults/{fault_id}", fault_status, methods=["GET"]),
        Route("/scan", scan, methods=["POST"]),
        Route("/control/market-map/build", build_market_map, methods=["POST"]),
        Route("/control/neg-risk/scan", scan_neg_risk_map, methods=["POST"]),
        Route(
            "/control/perception/discovery",
            queue_perception_discovery,
            methods=["POST"],
        ),
        Route(
            "/control/perception/reconciliation",
            queue_perception_reconciliation,
            methods=["POST"],
        ),
        Route("/control/perception/faults/arm", arm_fault, methods=["POST"]),
        Route(
            "/perception/faults/{fault_id}/export",
            export_fault,
            methods=["GET"],
        ),
        Route(
            "/control/perception/faults/cleanup",
            cleanup_fault,
            methods=["POST"],
        ),
        Route(
            "/control/perception/faults/{fault_id}/finalize",
            finalize_fault,
            methods=["POST"],
        ),
        Route("/control/unpause", unpause, methods=["POST"]),  # D-03 Phase 02.1
        Route("/control/pause", pause, methods=["POST"]),  # stub 501, Phase 03+ 填实现
        Route("/control/status", control_status, methods=["GET"]),  # stub 501, Phase 03+ 填实现
    ]

    source_truth_lane = BoundedReadLane("opportunity-source-truth")
    lifecycle_lane = BoundedReadLane("opportunity-lifecycle")
    perception_read_lane = BoundedReadLane("perception-read")

    @asynccontextmanager
    async def opportunity_read_lifespan(_app: Starlette):
        try:
            yield
        finally:
            source_truth_lane.shutdown()
            lifecycle_lane.shutdown()
            perception_read_lane.shutdown()

    app = Starlette(
        routes=routes,
        middleware=middleware,
        lifespan=opportunity_read_lifespan,
    )

    # Stash dependencies on app.state for handlers to access
    app.state.scheduler = scheduler
    app.state.sqlite_store = sqlite_store
    app.state.settings = settings
    app.state.machine_id = os.environ.get("FLY_MACHINE_ID", "local")
    app.state.boot_id = str(uuid4())
    app.state.quote_worker_runtime = quote_worker_runtime
    app.state.quote_worker = quote_worker
    app.state.opportunity_watcher = opportunity_watcher
    app.state.opportunity_read_health = OpportunityReadHealth()
    app.state.opportunity_source_truth_lane = source_truth_lane
    app.state.opportunity_lifecycle_lane = lifecycle_lane
    app.state.perception_read_lane = perception_read_lane
    # Slice B stores the exact runtime object mutated by Candidate Watcher.
    # Public HTTP exposure belongs to Task 6; keeping it on app.state now
    # preserves chain-truth without adding a premature route.
    app.state.candidate_watcher_runtime = candidate_watcher_runtime

    return app
