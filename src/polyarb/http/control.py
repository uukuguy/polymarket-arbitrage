"""POST /control/* endpoints + HMAC X-Signature middleware.

Phase 02.1 Plan 02 — D-03 / D-04 / D-22 / T-02.1-8-01..03 / BUG-8.

Purpose:
    Bug #8 from Phase 02 chaos: when daemon hits 3 consecutive snapshot
    failures, scheduler enters PAUSED. Returning to RUNNING in prod
    previously required SSH + ``sqlite3`` UPDATE + restart (~30s,
    error-prone). This module adds a HMAC-signed HTTP entry-point so a
    single ``make unpause-prod`` resumes the daemon.

    The underlying ``SnapshotScheduler.unpause()`` (``scheduler.py:192``)
    is already implemented (sets state=RUNNING + counter=0 +
    ``_persist_counter``); per codegraph it has 0 callers. This module
    is the new HTTP caller.

Routes (registered in ``http/app.py``):
    POST /control/unpause   — D-03 functional
    POST /control/pause     — 501 stub (forward-compat surface, D-07 boundary)
    GET  /control/status    — 501 stub (forward-compat surface, D-07 boundary)

Security (per D-22 + threat register):
    - HMAC-SHA256 of request body, keyed by ``POLYARB_SCAN_SHARED_SECRET``
      (same secret as /scan per D-22, RESEARCH Area 4 assumption A2 LOW risk).
    - ``hmac.compare_digest`` (constant-time, no timing oracle — T-02.1-8-01).
    - Missing X-Signature header → 401 (T-02.1-8-02).
    - Path guard: only ``/control/*`` is auth-checked. ``/health``, ``/healthz``,
      ``/scan`` bypass this middleware entirely (RESEARCH Pitfall 1, D-22).

Module independence (per RESEARCH Area 3 recommendation B):
    This module does NOT import from ``polyarb.http.scan``. The HMAC
    verification logic is inlined to keep ``/scan`` (scanner trigger)
    and ``/control/*`` (scheduler state machine) modules independent
    in semantics, ownership, and future evolution.

Sync vs async (per RESEARCH Pitfall 4):
    ``SnapshotScheduler.unpause()`` is a ``def`` method, NOT ``async def``.
    Call it WITHOUT ``await`` — otherwise Python raises
    ``TypeError: object NoneType can't be used in 'await' expression``.
"""

from __future__ import annotations

import hashlib
import hmac
import sqlite3
import time
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.daemon.scheduler import SchedulerState


async def control_auth_middleware(request: Request, call_next: Any, *, secret: str) -> Any:
    """HMAC-of-body auth gate for /control/*; bypass everything else.

    Path guard scope:
        Only requests whose path starts with ``/control`` are auth-checked.
        ``/health`` (IETF strict), ``/healthz`` (Fly probe — Plan 02.1-03),
        ``/scan`` (covered by its own middleware) all bypass cleanly.
        This avoids the RESEARCH Pitfall 1 trap of writing
        ``if path != "/scan"`` which would wrongly intercept /health on this
        middleware.

    Auth steps (identical to /scan pattern by design — same secret per D-22):
        1. Reject missing X-Signature → 401.
        2. Strip ``sha256=`` prefix if present (Stripe/GitHub webhook style).
        3. Read body, compute HMAC-SHA256, ``hmac.compare_digest`` constant-time.
        4. Re-inject the consumed body so the downstream handler can read it.
    """
    if not request.url.path.startswith("/control"):
        return await call_next(request)

    received_sig = request.headers.get("X-Signature")
    if not received_sig:
        return JSONResponse({"error": "missing X-Signature header"}, status_code=401)

    # Accept both `sha256=<hex>` (Stripe/GitHub) and bare `<hex>` (backward compat).
    if received_sig.startswith("sha256="):
        received_sig = received_sig[len("sha256=") :]

    body = await request.body()
    if request.url.path.startswith("/control/perception/"):
        timestamp = request.headers.get("X-Perception-Timestamp", "")
        nonce = request.headers.get("X-Perception-Nonce", "")
        try:
            timestamp_s = int(timestamp)
        except ValueError:
            return JSONResponse({"error": "invalid control authentication"}, status_code=401)
        if (
            str(timestamp_s) != timestamp
            or abs(int(time.time()) - timestamp_s) > 300
            or not 16 <= len(nonce) <= 128
            or not nonce.isalnum()
        ):
            return JSONResponse({"error": "invalid control authentication"}, status_code=401)
        canonical = b"\n".join(
            (
                timestamp.encode(),
                nonce.encode(),
                request.method.encode(),
                request.url.path.encode(),
                body,
            )
        )
        expected_sig = hmac.new(
            secret.encode("utf-8"), canonical, hashlib.sha256
        ).hexdigest()
    else:
        expected_sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    # Constant-time compare — T-02.1-8-01 / T-02.1-8-03 mitigation.
    if not hmac.compare_digest(received_sig, expected_sig):
        return JSONResponse({"error": "invalid X-Signature"}, status_code=401)

    if request.url.path.startswith("/control/perception/"):
        try:
            db_path = request.app.state.sqlite_store.db_path
            con = sqlite3.connect(db_path, timeout=0.25, isolation_level=None)
            try:
                con.execute("PRAGMA busy_timeout=250")
                con.execute("BEGIN IMMEDIATE")
                con.execute(
                    "DELETE FROM neg_risk_operator_auth_nonces WHERE nonce IN ("
                    "SELECT nonce FROM neg_risk_operator_auth_nonces "
                    "WHERE accepted_at_ms<? ORDER BY accepted_at_ms LIMIT 500)",
                    ((int(time.time()) - 600) * 1_000,),
                )
                con.execute(
                    "INSERT INTO neg_risk_operator_auth_nonces("
                    "nonce,request_path,request_timestamp_s,accepted_at_ms"
                    ") VALUES(?,?,?,?)",
                    (nonce, request.url.path, timestamp_s, int(time.time() * 1_000)),
                )
                con.execute("COMMIT")
            finally:
                con.close()
        except sqlite3.IntegrityError:
            return JSONResponse({"error": "invalid control authentication"}, status_code=401)
        except sqlite3.Error:
            return JSONResponse(
                {"status": "unavailable", "reason": "control-auth-store-unavailable"},
                status_code=409,
            )

    # Re-inject body for downstream handler (handlers may or may not read it,
    # but ASGI semantics require the consumed body to be re-presented).
    async def _receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    request._receive = _receive  # type: ignore[assignment]

    return await call_next(request)


class ControlAuthMiddleware(BaseHTTPMiddleware):
    """Starlette BaseHTTPMiddleware adapter for ``control_auth_middleware``.

    Starlette's ``Middleware()`` factory needs a class. The secret is captured
    at construction time from ``settings.scan_shared_secret.get_secret_value()``
    in ``create_app()`` and held as an instance attribute.
    """

    def __init__(self, app: Any, secret: str) -> None:
        super().__init__(app)
        self._secret = secret

    async def dispatch(self, request: Any, call_next: Any) -> Any:
        return await control_auth_middleware(request, call_next, secret=self._secret)


async def unpause(request: Request) -> JSONResponse:
    """POST /control/unpause — resume a PAUSED scheduler (D-03 + D-04).

    Behavior:
        - scheduler.state == PAUSED → call scheduler.unpause() (sync, no await
          per Pitfall 4), then return 200 with ``status=ok`` + ``state=RUNNING``
          + ``failure_counter=0`` (D-04 contract).
        - scheduler.state != PAUSED → 200 with ``status=already_running``
          (idempotent; T-02.1-8-04 — replay/repeat is safe).

    Response shape exposes ``failure_counter`` to give ops debug context
    (T-02.1-8-05 accepted — value is only useful inside HMAC-protected channel).
    """
    scheduler = request.app.state.scheduler
    if scheduler.state != SchedulerState.PAUSED:
        return JSONResponse(
            {
                "status": "already_running",
                "state": scheduler.state.value
                if hasattr(scheduler.state, "value")
                else str(scheduler.state),
            },
            status_code=200,
        )

    # Sync call — scheduler.unpause() is `def`, not `async def` (Pitfall 4).
    scheduler.unpause()

    state_repr = (
        scheduler.state.value if hasattr(scheduler.state, "value") else str(scheduler.state)
    )
    return JSONResponse(
        {
            "status": "ok",
            "message": "scheduler unpaused",
            "state": state_repr,
            "failure_counter": scheduler._failure_counter,
        },
        status_code=200,
    )


async def build_market_map(request: Request) -> JSONResponse:
    """Queue one normal Structure cycle without reviving a PAUSED scheduler."""
    scheduler = getattr(request.app.state, "scheduler", None)
    state = getattr(scheduler, "state", None)
    state_value = getattr(state, "value", state)
    if scheduler is None or state_value == SchedulerState.PAUSED.value:
        return JSONResponse({"error": "unavailable"}, status_code=409)
    queued = scheduler.request_now()
    return JSONResponse(
        {"status": "queued" if queued else "already_queued"},
        status_code=202 if queued else 200,
    )


async def scan_neg_risk_map(request: Request) -> JSONResponse:
    """Queue one normal global Quote cycle; disabled workers remain unavailable."""
    worker = getattr(request.app.state, "quote_worker", None)
    if worker is None:
        return JSONResponse({"error": "unavailable"}, status_code=409)
    queued = worker.request_now()
    return JSONResponse(
        {"status": "queued" if queued else "already_queued"},
        status_code=202 if queued else 200,
    )


async def queue_perception_discovery(request: Request) -> JSONResponse:
    return await _queue_perception_component(request, "discovery")


async def queue_perception_reconciliation(request: Request) -> JSONResponse:
    return await _queue_perception_component(request, "reconciliation")


async def _queue_perception_component(request: Request, component: str) -> JSONResponse:
    """Persist one coalescing wake-up; never invoke or revive a producer."""
    import asyncio

    from polyarb.perception.store import OpportunityPerceptionStore

    enabled_flag = (
        "opportunity_discovery_enabled"
        if component == "discovery"
        else "opportunity_reconciliation_enabled"
    )
    if not bool(getattr(request.app.state.settings, enabled_flag, False)):
        return JSONResponse(
            {"status": "unavailable", "reason": "component-disabled"},
            status_code=409,
        )
    nonce = request.headers["X-Perception-Nonce"]
    store = OpportunityPerceptionStore(
        request.app.state.sqlite_store.db_path,
        busy_timeout_ms=250,
    )
    try:
        queued = await asyncio.wait_for(
            asyncio.to_thread(
                store.queue_operator_wakeup,
                component,
                request_nonce=nonce,
                occurred_at_ms=int(time.time() * 1_000),
            ),
            timeout=1.0,
        )
    except RuntimeError as error:
        reason = (
            "component-escalated"
            if str(error) == "component-escalated"
            else "component-unavailable"
        )
        return JSONResponse({"status": "unavailable", "reason": reason}, status_code=409)
    except (TimeoutError, sqlite3.Error, ValueError):
        return JSONResponse(
            {"status": "unavailable", "reason": "component-unavailable"},
            status_code=409,
        )
    return JSONResponse(
        {"status": "queued" if queued else "already_queued"},
        status_code=202 if queued else 200,
    )


async def pause(request: Request) -> JSONResponse:
    """POST /control/pause — stub (Phase 02.1 D-07 strict scope).

    Returns 501 Not Implemented. Forward-compat router surface — Phase 03+
    decides whether to implement (current Phase 02.1 explicitly defers per
    D-07 boundary).
    """
    return JSONResponse({"error": "not implemented"}, status_code=501)


async def control_status(request: Request) -> JSONResponse:
    """GET /control/status — stub (Phase 02.1 D-07 strict scope).

    Returns 501 Not Implemented. Forward-compat router surface — Phase 03+
    decides shape (likely a subset of /health that exposes scheduler.state +
    counter + last_tick_at_ms, but body shape not locked in this phase).
    """
    return JSONResponse({"error": "not implemented"}, status_code=501)
