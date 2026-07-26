"""POST /control/chaos/ws-test-kill — HMAC-gated L2 chaos endpoint.

Phase 04.1 Plan 03 — G-03 fix: in-band chaos primitive redesign.

Purpose:
    Phase 04 chaos triggered the WS kill via ``flyctl secrets set
    POLYARB_WS_TEST_KILL=1``, which RESTARTS the Fly machine. The pre-storm
    60-asset process was killed before the storm, making Pitfall 4 (watchdog
    false-trip on a healthy long-lived process) unobservable (04-SOAK-LOG §G-03).

    This endpoint flips a PROCESS-LOCAL atomic flag on the RUNNING process —
    no restart, no env-unset required. The storm runs against the same 60-asset
    process that survived the bootstrap phase.

Security (T-04.1-01..08, see 04.1-03-PLAN.md threat_model):
    - HMAC-SHA256 of request body, keyed by ``POLYARB_SCAN_SHARED_SECRET``
      (same secret as /control/unpause — reuses ``ControlAuthMiddleware``
      from ``polyarb.http.control`` unchanged, path-guard covers /control/*).
    - ``hmac.compare_digest`` constant-time (T-04.1-01 mitigation, delegated
      to the middleware — this handler does NOT re-verify).
    - Missing/invalid X-Signature → 401 (enforced by middleware before handler).
    - Response body exposes only ``{"status":"ok","ws_test_kill":<bool>}`` —
      no secret material, no config dump (T-04.1-04 whitelist discipline).

Prod safety invariant (T-04.1-07, CI-enforced):
    fly-l2.toml MUST NOT contain POLYARB_WS_TEST_KILL. The env var only seeds
    the cold-start flag value (False in prod since fly-l2.toml is clean). The
    flag defaults False on every (re)start — a natural restart resets it.

CHAOS-ONLY. The endpoint clears the flag in-band (``{"enabled":false}``) so
cleanup does NOT require ``flyctl secrets unset`` (which would restart the machine
again, defeating the G-03 fix). Always clear via ``make chaos-ws-kill ON=0``.

Routes (registered in l2_app.py):
    POST /control/chaos/ws-test-kill

Handler does NOT import from polyarb.http.control (module independence) —
the HMAC gate is entirely in the middleware, not in this handler.
"""

from __future__ import annotations

from loguru import logger
from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.daemon.ws_consumer import get_ws_test_kill, set_ws_test_kill


async def ws_test_kill_handler(request: Request) -> JSONResponse:
    """POST /control/chaos/ws-test-kill — flip the process-local WS-kill flag.

    Body: ``{"enabled": true}`` to set, ``{"enabled": false}`` to clear.
    HMAC verification is enforced by ``ControlAuthMiddleware`` (wired in
    l2_app.py) — this handler only runs after the middleware has validated
    the X-Signature. It does NOT re-verify.

    Response (200):
        ``{"status": "ok", "ws_test_kill": <bool>}``

    Threat model:
        T-04.1-01 (spoofing) — mitigated by middleware HMAC gate.
        T-04.1-02 (tampering) — HMAC covers body bytes; any tamper invalidates sig.
        T-04.1-04 (info disclosure) — response is minimal whitelist only.
        T-04.1-05 (DoS leave-killed) — ``{"enabled":false}`` clears in-band.
        T-04.1-06 (replay) — idempotent; accepted (same rationale as /control/unpause).
        T-04.1-08 (path-guard bypass) — route is under /control/* so middleware covers it.
    """
    try:
        body = await request.json()
        raw_enabled = body.get("enabled", False)
    except Exception as e:
        logger.warning(f"ws_test_kill_handler: bad JSON body: {e!r}")
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    # WR-02 (04.1 code review): require a genuine JSON boolean. A chaos kill-switch
    # must never silently invert — `bool("false")` is True, so reject any non-bool
    # (string/int/null) rather than coerce. The Makefile sends real booleans.
    if not isinstance(raw_enabled, bool):
        logger.warning(
            f"ws_test_kill_handler: 'enabled' must be a JSON boolean, got "
            f"{type(raw_enabled).__name__}={raw_enabled!r}"
        )
        return JSONResponse(
            {"error": "'enabled' must be a JSON boolean (true/false)"},
            status_code=400,
        )
    enabled: bool = raw_enabled

    set_ws_test_kill(enabled)
    flag = get_ws_test_kill()
    logger.warning(
        f"ws_test_kill_handler: chaos flag flipped — enabled={enabled} "
        f"(current={flag}). THIS MUST NOT APPEAR IN PRODUCTION."
    )
    return JSONResponse({"status": "ok", "ws_test_kill": flag})
