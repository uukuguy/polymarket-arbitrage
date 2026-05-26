"""L2 daemon /health (IETF strict) + /healthz (always-200) endpoints.

Phase 03 Plan 03 — D-06: separate process (polyarb-l2) needs its own health surface.

Phase 02.1 P5 helper-first refactor: both /health and /healthz call
`_build_l2_health_checks(...)`. The HTTP status wrapping differs:
- /health  → 503 when overall == "fail" (Better Stack alarm signal)
- /healthz → ALWAYS 200 (Fly proxy keeps routing — BUG-6 invariant)

Sub-check scaffolding (Plan 03 ships skeleton; Plan 04/05/06 wire data):
- ws:connection_state         (Plan 04 wires WsConsumer.current_state)
- ws:last_event_age_seconds   (Plan 04 wires WsConsumer.last_event_at_s)
- event_bus:listener_state    (Plan 05 wires EventListener.is_listening)
- mirror:l2_tob_age_seconds   (Plan 06 wires when settings.l2_mirror_enabled)

T-03-03-04 mitigation: serviceId is the literal "polyarb-l2" — never "polyarb-l1".
T-03-03-06 mitigation: response body whitelists fields; never dict(settings) dump.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger
from starlette.requests import Request
from starlette.responses import JSONResponse

HEALTH_CONTENT_TYPE = "application/health+json"

# WS age thresholds (seconds)
_WS_AGE_PASS_S = 30      # < 30s → pass
_WS_AGE_WARN_S = 120     # 30-120s → warn; > 120s → fail

# RECONNECTING age threshold for "too long" (Phase 02.1 D-05)
_RECONNECTING_FAIL_S = 60

# L2 mirror age thresholds — Phase 03.1 Plan 02 (B-3 fix): thresholds NOW
# read from Settings (settings.l2_tob_age_warn_s / l2_tob_age_fail_s) so the
# Plan 07 chaos knob can lower them via env override to flip /health within
# 60s instead of waiting 10 minutes. The constants below remain as compatibility
# fallbacks ONLY if Settings somehow lacks the fields (defensive).
_MIRROR_PASS_S_DEFAULT = 300   # default warn threshold (override via settings.l2_tob_age_warn_s)
_MIRROR_FAIL_S_DEFAULT = 600   # default fail threshold (override via settings.l2_tob_age_fail_s)


def _utc_now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format with Z suffix."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _severity(a: str, b: str) -> str:
    """Return worst of two health statuses (fail > warn > pass)."""
    order = {"pass": 0, "warn": 1, "fail": 2}
    return a if order.get(a, 0) >= order.get(b, 0) else b


def _build_l2_health_checks(
    store: Any,
    settings: Any,
    ws_consumer: Any | None,
    event_listener: Any | None,
    now_s: float,
) -> tuple[dict[str, list[dict[str, Any]]], str]:
    """Compute all L2 sub-checks and the overall status.

    Shared by /health (IETF strict — fail → 503) and /healthz (always 200).
    Phase 02.1 P5 helper-first refactor pattern.

    Args:
        store: SQLiteStore instance (read-only).
        settings: Settings instance — drives optional mirror checks.
        ws_consumer: Plan 04 WsConsumer instance, or None at Plan 03 boundary.
        event_listener: Plan 05 EventListener instance, or None at Plan 03 boundary.
        now_s: current epoch seconds (passed in so /health and /healthz agree
               on age values within a single request handler invocation).

    Returns:
        (checks_dict, overall_status) where overall_status in {"pass","warn","fail"}.
    """
    checks: dict[str, list[dict[str, Any]]] = {}
    overall = "pass"

    # ── Check 1: ws:connection_state ───────────────────────────────────────
    if ws_consumer is None:
        checks["ws:connection_state"] = [{
            "componentId": "ws-consumer",
            "componentType": "websocket",
            "observedValue": "not_configured",
            "status": "warn",
            "output": "ws_consumer not yet wired (Plan 04 deliverable)",
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, "warn")
    else:
        state = getattr(ws_consumer, "current_state", "UNKNOWN")
        last_at = getattr(ws_consumer, "last_event_at_s", now_s)
        try:
            age = now_s - float(last_at)
        except (TypeError, ValueError):
            age = 0.0

        if state == "CONNECTED":
            ws_state_status = "pass"
        elif state == "RECONNECTING":
            ws_state_status = "fail" if age > _RECONNECTING_FAIL_S else "warn"
        elif state == "WAITING_FOR_EVENT":
            ws_state_status = "warn"
        else:
            ws_state_status = "fail"

        checks["ws:connection_state"] = [{
            "componentId": "ws-consumer",
            "componentType": "websocket",
            "observedValue": state,
            "status": ws_state_status,
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, ws_state_status)

    # ── Check 2: ws:last_event_age_seconds ─────────────────────────────────
    if ws_consumer is not None:
        last_at = getattr(ws_consumer, "last_event_at_s", None)
        if last_at is None:
            age_status = "warn"
            age_val: float | None = None
        else:
            try:
                age_val = now_s - float(last_at)
            except (TypeError, ValueError):
                age_val = None
            if age_val is None:
                age_status = "warn"
            elif age_val < _WS_AGE_PASS_S:
                age_status = "pass"
            elif age_val < _WS_AGE_WARN_S:
                age_status = "warn"
            else:
                age_status = "fail"
        checks["ws:last_event_age_seconds"] = [{
            "componentId": "ws-consumer",
            "componentType": "websocket",
            "observedValue": round(age_val, 1) if age_val is not None else None,
            "observedUnit": "s",
            "status": age_status,
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, age_status)

    # ── Check 3: event_bus:listener_state ──────────────────────────────────
    if event_listener is None:
        checks["event_bus:listener_state"] = [{
            "componentId": "event-listener",
            "componentType": "asyncpg-listener",
            "observedValue": "not_configured",
            "status": "warn",
            "output": "event_listener not yet wired (Plan 05 deliverable)",
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, "warn")
    else:
        is_listening = bool(getattr(event_listener, "is_listening", False))
        listener_status = "pass" if is_listening else "warn"
        checks["event_bus:listener_state"] = [{
            "componentId": "event-listener",
            "componentType": "asyncpg-listener",
            "observedValue": "listening" if is_listening else "reconnecting",
            "status": listener_status,
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, listener_status)

    # ── Check 4: mirror:l2_tob_age_seconds (only when l2_mirror_enabled) ───
    # Phase 03.1 Plan 02 (B-3): chain-truth wiring. settings.l2_mirror_enabled
    # auto-detects from supabase secrets; thresholds come from Settings (env-
    # overridable via POLYARB_L2_TOB_AGE_WARN_S / POLYARB_L2_TOB_AGE_FAIL_S).
    # Mapping: age < warn → pass; warn <= age < fail → warn; age >= fail → fail;
    # cold-start (getter returns None) → warn (do NOT fail on first boot).
    if getattr(settings, "l2_mirror_enabled", False):
        warn_s = int(getattr(settings, "l2_tob_age_warn_s", _MIRROR_PASS_S_DEFAULT))
        fail_s = int(getattr(settings, "l2_tob_age_fail_s", _MIRROR_FAIL_S_DEFAULT))
        try:
            getter = getattr(store, "get_l2_tob_last_mirror_at_s", None)
            last_mirror_at = getter() if callable(getter) else None
            if last_mirror_at is None:
                mirror_status = "warn"
                mirror_age: float | None = None
                mirror_output: str | None = "cold-start: never mirrored"
            else:
                mirror_age = now_s - float(last_mirror_at)
                if mirror_age >= fail_s:
                    mirror_status = "fail"
                elif mirror_age >= warn_s:
                    mirror_status = "warn"
                else:
                    mirror_status = "pass"
                mirror_output = (
                    f"last mirror push {mirror_age:.0f}s ago "
                    f"(warn>={warn_s}s, fail>={fail_s}s)"
                )
        except Exception as e:
            logger.warning(f"L2 mirror age check failed (fail-soft): {e!r}")
            mirror_status = "warn"
            mirror_age = None
            mirror_output = f"check error: {e!r}"
        checks["mirror:l2_tob_age_seconds"] = [{
            "componentId": "supabase-l2-mirror",
            "componentType": "datastore",
            "observedValue": round(mirror_age, 1) if mirror_age is not None else None,
            "observedUnit": "s",
            "status": mirror_status,
            "output": mirror_output,
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, mirror_status)

    # ── Check 5: chaos:ws_test_kill_flag (Phase 03.1-06 W-5 chain-truth) ────
    # Plan 03 codified the rule: every fail-soft / chaos primitive MUST surface
    # to /health (feedback_code-vs-chain-truth-2026-05). POLYARB_WS_TEST_KILL=1
    # forces the WS consumer to drop the connection on next message — it's the
    # quintessential chaos primitive, and operators MUST be able to see it via
    # curl /health, not just by grep'ing flyctl logs.
    #
    # Status is 'warn' (not 'fail'): the flag itself doesn't trip overall=fail;
    # let downstream sub-checks (ws:connection_state going RECONNECTING,
    # mirror:l2_tob_age_seconds going stale) drive overall fail. The chaos
    # sub-check is purely a visibility surface — "yes, the flag is set".
    if os.getenv("POLYARB_WS_TEST_KILL") == "1":
        checks["chaos:ws_test_kill_flag"] = [{
            "componentId": "ws-consumer",
            "componentType": "system",
            "observedValue": "1",
            "status": "warn",
            "output": (
                "POLYARB_WS_TEST_KILL=1 — CHAOS MODE active; "
                "should never appear in production"
            ),
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, "warn")

    return checks, overall


def _build_l2_health_body(
    overall: str,
    checks: dict[str, list[dict[str, Any]]],
    settings: Any,
) -> dict[str, Any]:
    """Shared IETF body shape — same for /health and /healthz (D-06 full mirror).

    T-03-03-06 mitigation: explicit whitelist of fields. Never dict(settings).
    Never include: db_path, secrets, DSN, tokens, service_role, scan_shared_secret.
    """
    return {
        "status": overall,
        "version": getattr(settings, "version", "0.0.0"),
        "releaseId": getattr(settings, "release_id", "unknown"),
        "serviceId": "polyarb-l2",
        "description": "Polymarket L2 orderbook tracking daemon — WS market channel + event bus",
        "checks": checks,
    }


async def health(request: Request) -> JSONResponse:
    """GET /health — IETF strict health response (Better Stack alarm target).

    Returns 200 for pass/warn, 503 for fail.

    Reads ws_consumer / event_listener from app.state (may be None at Plan 03 boundary).
    """
    store = request.app.state.sqlite_store
    settings = request.app.state.settings
    ws_consumer = getattr(request.app.state, "ws_consumer", None)
    event_listener = getattr(request.app.state, "event_listener", None)

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer, event_listener, time.time()
    )
    body = _build_l2_health_body(overall, checks, settings)
    http_status = 503 if overall == "fail" else 200
    return JSONResponse(body, status_code=http_status, media_type=HEALTH_CONTENT_TYPE)


async def healthz(request: Request) -> JSONResponse:
    """GET /healthz — Fly-friendly probe. ALWAYS HTTP 200 (BUG-6 invariant).

    Same JSON body schema as /health (D-06 full mirror). The underlying check
    status is exposed in body["status"], but the HTTP code is always 200 so
    Fly platform's [http_service.checks] never marks the machine unhealthy.
    """
    store = request.app.state.sqlite_store
    settings = request.app.state.settings
    ws_consumer = getattr(request.app.state, "ws_consumer", None)
    event_listener = getattr(request.app.state, "event_listener", None)

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer, event_listener, time.time()
    )
    body = _build_l2_health_body(overall, checks, settings)
    # KEY: ignore overall when deciding HTTP code — always 200 (BUG-6).
    return JSONResponse(body, status_code=200, media_type=HEALTH_CONTENT_TYPE)
