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

# Phase 04 Plan 02 Task 3 — candidates:supabase_fetch_age_seconds thresholds.
# The default refresh debounce is 60s (REFRESH_DEBOUNCE_S), so:
#   warn at 2× debounce (120s)  → "fetch slipping a window"
#   fail at 10× debounce (600s) → "sustained Supabase outage; freeze candidate set"
# These are reasonable defaults; expose via settings only if a future chaos
# knob needs them (no env override today, keep API surface narrow).
_CANDIDATES_FETCH_WARN_S = 120
_CANDIDATES_FETCH_FAIL_S = 600


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

    # ── Check 2b: ws:subscribed_count ──────────────────────────────────────
    # Phase 04 Plan 04 Task 2 — operator-visibility surface for the
    # candidate set size driving WS subscriptions. Needed by
    # `make chaos-l2-inj4-throughput` as the precondition gate: a candidate
    # count of <= 3 means Plan 02's Supabase data-source swap is not
    # effective in prod (only bootstrap asset_ids), so the throughput chaos
    # would degrade to the Phase 03.1 Inj L2-4 logic-only test all over
    # again. The Makefile aborts when this number is <= 3.
    #
    # Status is purely informational (pass) — health pass/fail for the WS
    # connection itself is driven by ws:connection_state above. This
    # sub-check is read by chaos/operator tooling, not the alerting path.
    if ws_consumer is not None:
        try:
            subscribed = getattr(ws_consumer, "subscribed_assets", []) or []
            sub_count = len(subscribed)
        except Exception:  # noqa: BLE001 — fail-soft on /health read
            sub_count = 0
        checks["ws:subscribed_count"] = [{
            "componentId": "ws-consumer",
            "componentType": "websocket",
            "observedValue": sub_count,
            "observedUnit": "assets",
            "status": "pass",
            "output": f"{sub_count} assets currently subscribed",
            "time": _utc_now_iso(),
        }]

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

    # ── Check 4: mirror:l2_tob_age_seconds — D-08 three-branch (GAP-200) ──
    # Phase 04 Plan 03: three-branch chain-truth gate. Inverse of Phase 03.1
    # L4 lesson (feedback_code-vs-chain-truth-2026-05): the previous binary
    # `if l2_mirror_enabled:` gate made a config mistake (URL set but key
    # forgotten) silently absent from /health. Now:
    #   (a) supabase_url empty (key irrelevant) → no sub-check
    #       (Supabase not configured at all — backwards-compat, correct).
    #   (b) supabase_url SET but service_key EMPTY → status=fail, surface
    #       the operator config mistake on /health (chain-truth).
    #   (c) both set → existing age-based pass/warn/fail logic (unchanged).
    #
    # Note: config.py model_validator still sets l2_mirror_enabled iff BOTH
    # url AND key non-empty (the AND-gate at line 238). In case (b),
    # l2_mirror_enabled remains False — only /health PRESENTATION changes.
    _supabase_url = getattr(settings, "supabase_url", "")
    _service_key_val = ""
    try:
        _service_key_val = settings.supabase_service_key.get_secret_value()
    except AttributeError:
        # service_key is not a SecretStr (defensive — possible under test mocks)
        pass

    if _supabase_url and not _service_key_val:
        # Case (b): URL configured but service_key missing — operator mistake.
        # GAP-200: surface as a /health fail so the misconfiguration is
        # observable, not silent. Output names the missing field (no secret
        # material leaked — T-04-04 in plan threat model).
        checks["mirror:l2_tob_age_seconds"] = [{
            "componentId": "supabase-l2-mirror",
            "componentType": "datastore",
            "observedValue": None,
            "observedUnit": "s",
            "status": "fail",
            "output": "mirror disabled by config (service_key empty)",
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, "fail")
    elif getattr(settings, "l2_mirror_enabled", False):
        # Case (c): both url + key set — existing age sub-check (unchanged
        # body from Phase 03.1 Plan 02). Settings drive thresholds so the
        # Plan 07 chaos knob can lower them via env override.
        # Mapping: age < warn → pass; warn <= age < fail → warn;
        # age >= fail → fail; cold-start (getter returns None) → warn.
        warn_s = int(getattr(settings, "l2_tob_age_warn_s", _MIRROR_PASS_S_DEFAULT))
        fail_s = int(getattr(settings, "l2_tob_age_fail_s", _MIRROR_FAIL_S_DEFAULT))
        try:
            getter = getattr(store, "get_l2_tob_last_mirror_at_s", None)
            last_mirror_at: Any = getter() if callable(getter) else None
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
    # else: case (a) — supabase_url also empty → no sub-check (correct,
    # operator opted out of Supabase entirely; reporting fail would be a
    # false alarm).

    # ── Check 4b: candidates:supabase_fetch_age_seconds — D-01 chain-truth ─
    # Phase 04 Plan 02 Task 3. The Supabase markets_latest fetch driving
    # `compute_candidates` is fail-soft (last-known rows on failure). Without
    # a /health surface, a sustained outage would silently freeze the
    # candidate set — the exact Inj L2-2 dead-code-gate failure pattern
    # (feedback_code-vs-chain-truth-2026-05). This sub-check reads a field
    # the fetch path REALLY mutates: `_last_fetch_success_at_s` updated on
    # every successful fetch via `_record_fetch_success()`. Cold-start
    # (None) is `warn`, NOT `fail` — boot must not be a health alarm.
    #
    # Gated identically to the mirror sub-check above:
    #   case (a) supabase_url empty                  → skip (no signal needed)
    #   case (b) supabase_url set, service_key empty → registered as warn
    #            (candidates path will not run at all; mirror gate already
    #             surfaces the config mistake as fail — we add a soft warn
    #             here too so the candidates row exists in /health output)
    #   case (c) both set → real age-based pass/warn/fail logic
    if _supabase_url:
        # Read the write-side state via the public getter (chain-truth §1.6 —
        # this is not a dead-code config flag; it is the same field
        # _record_fetch_success() writes after every successful fetch).
        try:
            from polyarb.observation.l2_candidate_refresh import (
                get_last_fetch_success_at_s,
            )

            last_fetch_at = get_last_fetch_success_at_s()
        except Exception as e:  # noqa: BLE001 — fail-soft on /health read
            logger.warning(f"candidates fetch age check failed (fail-soft): {e!r}")
            last_fetch_at = None
            fetch_status = "warn"
            fetch_age: float | None = None
            fetch_output: str | None = f"check error: {e!r}"
        else:
            if not _service_key_val:
                # Case (b): URL configured but no key → candidates path never runs.
                # mirror sub-check already FAILs above; we warn here so the
                # candidates row is present in /health output (operator visibility).
                fetch_status = "warn"
                fetch_age = None
                fetch_output = "supabase service_key empty — candidates fetch disabled"
            elif last_fetch_at is None:
                # Cold-start: never fetched. Warn (NOT fail) on boot.
                fetch_status = "warn"
                fetch_age = None
                fetch_output = "cold-start: never fetched"
            else:
                fetch_age = now_s - float(last_fetch_at)
                if fetch_age >= _CANDIDATES_FETCH_FAIL_S:
                    fetch_status = "fail"
                elif fetch_age >= _CANDIDATES_FETCH_WARN_S:
                    fetch_status = "warn"
                else:
                    fetch_status = "pass"
                fetch_output = (
                    f"last fetch {fetch_age:.0f}s ago "
                    f"(warn>={_CANDIDATES_FETCH_WARN_S}s, "
                    f"fail>={_CANDIDATES_FETCH_FAIL_S}s)"
                )
        checks["candidates:supabase_fetch_age_seconds"] = [{
            "componentId": "l2-candidate-refresh",
            "componentType": "candidate-set",
            "observedValue": round(fetch_age, 1) if fetch_age is not None else None,
            "observedUnit": "s",
            "status": fetch_status,
            "output": fetch_output,
            "time": _utc_now_iso(),
        }]
        overall = _severity(overall, fetch_status)

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

    # ── Check 6: process:rss_kb — G-04 (04.1) current-process RSS ───────────
    # Phase 04 chaos read /proc/1/status (PID-1-hallpass bug — 04-SOAK-LOG
    # §G-04): PID 1 is the Fly init/shim, not the Python daemon. psutil.Process()
    # (no arg) = os.getpid() = THIS daemon. Informational (pass) — observability
    # only, never trips overall (D-04.4). Lazy-import so a missing dep degrades
    # to warn, not a /health import crash.
    try:
        import psutil  # lazy — runtime dep (pyproject), degrade-soft if absent
        rss_kb = psutil.Process().memory_info().rss / 1024
        rss_status = "pass"
        rss_output = f"current-process RSS {rss_kb:.0f} kB (psutil.Process, not PID 1)"
    except Exception as e:  # noqa: BLE001 — fail-soft on /health read
        rss_kb = None
        rss_status = "warn"
        rss_output = f"rss read unavailable (fail-soft): {e!r}"
    checks["process:rss_kb"] = [{
        "componentId": "l2-daemon",
        "componentType": "system",
        "observedValue": round(rss_kb, 1) if rss_kb is not None else None,
        "observedUnit": "kB",
        "status": rss_status,
        "output": rss_output,
        "time": _utc_now_iso(),
    }]
    # NOTE: intentionally NO `overall = _severity(overall, rss_status)` —
    # informational (D-04.4); even the fail-soft warn must not alarm /health.

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
