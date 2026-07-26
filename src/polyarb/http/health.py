"""IETF draft-inadarei-api-health-check-06 compliant /health endpoint + Fly-friendly /healthz.

Phase 02 Plan 02 — D-12 / D-13 / D-16.
Phase 02.1 Plan 03 — D-05 / D-06: separate /healthz (always 200) from /health (IETF strict).

Two endpoints, ONE underlying check logic (see _build_health_checks()):

- GET /health (IETF strict) — Better Stack external probe target.
    * 200 for pass/warn, 503 for fail.
    * 503 is the alarm signal — Better Stack uptime probe interprets it as down.

- GET /healthz (Fly-friendly) — Fly platform machine probe target (per fly.toml).
    * ALWAYS 200 regardless of underlying check status.
    * Status is reported in the JSON body, but Fly proxy reads only the HTTP code.
    * This prevents Fly from withdrawing the machine from the routing pool when
      daemon is PAUSED / Supabase mirror is stale / R2 upload failed —
      observed during Phase 02 Inj 2 and confirmed in Plan 02.1-02 Inj 4
      ("could not find a good candidate within 40 attempts at load balancing").

Three-state health: pass | warn | fail
- pass  → all checks green
- warn  → at least one check degraded, none failed
- fail  → at least one check failed

Plan 02 checks (Plan 03 adds supabase/r2):
1. snapshot:last_success_age_seconds
   - pass  < 14h  (subset cron interval 12h + 2h buffer)
   - warn  14-25h
   - fail  > 25h  OR no snapshot at all
2. snapshot:last_status
   - maps SnapshotStatus.OK → pass, DEGRADED → warn, FAILED → fail
   - if no snapshot → omitted from checks (age check already reports fail)
3. supabase:mirror_age_seconds (when settings.supabase_mirror_enabled)
4. r2:upload_recent_success (when settings.r2_enabled)

Overall = worst-of all sub-checks (fail > warn > pass).

Security note (T-02-09 / T-02.1-6-01): both /health and /healthz are intentionally
PUBLIC (no HMAC). Better Stack uptime probe + Fly platform probe both need
unauthenticated access. Response exposes only snapshot age + status enum —
no DB schema, no IPs, no secrets. (D-22 amendment + D-06.)

Source: datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06
        RESEARCH.md §8 / 02.1-RESEARCH.md Area 1-2
"""

from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

HEALTH_CONTENT_TYPE = "application/health+json"

# Age thresholds in seconds
_PASS_AGE_S = 14 * 3600  # < 14h → pass
_WARN_AGE_S = 25 * 3600  # 14-25h → warn; > 25h → fail

# Supabase mirror thresholds
_MIRROR_WARN_S = 25 * 3600
_MIRROR_FAIL_S = 48 * 3600


def _utc_now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format with Z suffix."""
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _severity(a: str, b: str) -> str:
    """Return worst of two health statuses (fail > warn > pass)."""
    order = {"pass": 0, "warn": 1, "fail": 2}
    return a if order.get(a, 0) >= order.get(b, 0) else b


def _build_health_checks(
    store: Any,
    settings: Any,
    now_s: float,
    quote_worker_runtime: Any | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], str]:
    """Compute all health sub-checks and the overall status.

    Shared by /health (IETF strict — fail → 503) and /healthz (always 200).
    Both endpoints render the same JSON body shape; only the HTTP status code
    differs (per D-06 — full-mirror body schema, do NOT diverge check logic).

    Args:
        store: SQLiteStore instance (read-only).
        settings: Settings instance — drives optional mirror/r2 checks.
        now_s: current epoch seconds (passed in so /health and /healthz agree
               on age values within a single request handler invocation).

    Returns:
        (checks_dict, overall_status) where overall_status in {"pass","warn","fail"}.
    """
    checks: dict[str, list[dict[str, Any]]] = {}
    overall = "pass"

    # ── Check 1: snapshot age ─────────────────────────────────────────────
    last_snapshot = store.get_latest_snapshot()

    if last_snapshot is None:
        age_s: float | None = None
        age_status = "fail"
        overall = _severity(overall, "fail")
    else:
        taken_at_ms = last_snapshot["taken_at_ms"]
        age_s = now_s - taken_at_ms / 1000.0
        if age_s < _PASS_AGE_S:
            age_status = "pass"
        elif age_s < _WARN_AGE_S:
            age_status = "warn"
        else:
            age_status = "fail"
        overall = _severity(overall, age_status)

    checks["snapshot:last_success_age_seconds"] = [
        {
            "componentId": "scheduler",
            "componentType": "component",
            "observedValue": round(age_s, 1) if age_s is not None else None,
            "observedUnit": "s",
            "status": age_status,
            "time": _utc_now_iso(),
        }
    ]

    # ── Check 2: last snapshot status ─────────────────────────────────────
    if last_snapshot is not None:
        # notes column carries status string written by orchestrator (ok/degraded/failed)
        # is_valid=True → snapshot completed (OK or DEGRADED); is_valid=False → FAILED
        notes = (last_snapshot.get("notes") or "").lower()
        if "degraded" in notes:
            last_status_val = "DEGRADED"
            status_check = "warn"
        elif not last_snapshot.get("is_valid", True):
            last_status_val = "FAILED"
            status_check = "fail"
        else:
            last_status_val = "OK"
            status_check = "pass"

        overall = _severity(overall, status_check)
        checks["snapshot:last_status"] = [
            {
                "componentId": "orchestrator",
                "observedValue": last_status_val,
                "status": status_check,
                "time": _utc_now_iso(),
            }
        ]

    # ── Check 3: Supabase mirror age (only if mirror enabled) ────────────
    # supabase:mirror_age_seconds — seconds since last successful Supabase push.
    # Uses supabase_mirror_at_ms column added in Plan 03.
    # warn if > 25h (cron interval 12h + 13h buffer); fail if > 48h.
    if settings.supabase_mirror_enabled:
        if last_snapshot is not None and last_snapshot.get("supabase_mirror_at_ms") is not None:
            mirror_age_s: float | None = now_s - last_snapshot["supabase_mirror_at_ms"] / 1000.0
            if mirror_age_s < _MIRROR_WARN_S:
                mirror_status = "pass"
            elif mirror_age_s < _MIRROR_FAIL_S:
                mirror_status = "warn"
            else:
                mirror_status = "fail"
        else:
            # No mirror timestamp yet (mirror disabled or first run) → warn (not fail)
            mirror_age_s = None
            mirror_status = "warn"
        overall = _severity(overall, mirror_status)
        checks["supabase:mirror_age_seconds"] = [
            {
                "componentId": "supabase-mirror",
                "componentType": "datastore",
                "observedValue": round(mirror_age_s, 1) if mirror_age_s is not None else None,
                "observedUnit": "s",
                "status": mirror_status,
                "time": _utc_now_iso(),
            }
        ]

    # ── Check 4: R2 upload recent success (only if R2 enabled) ───────────
    # r2:upload_recent_success — True if last snapshot has a parquet_r2_url.
    # Uses parquet_r2_url column added in Plan 03.
    if settings.r2_enabled:
        if last_snapshot is not None and last_snapshot.get("parquet_r2_url"):
            r2_status = "pass"
            r2_value: bool = True
        elif last_snapshot is not None:
            # Snapshot exists but no R2 URL — warn (first run or upload failed)
            r2_status = "warn"
            r2_value = False
        else:
            r2_status = "warn"
            r2_value = False
        overall = _severity(overall, r2_status)
        checks["r2:upload_recent_success"] = [
            {
                "componentId": "r2-archive",
                "componentType": "system",
                "observedValue": r2_value,
                "status": r2_status,
                "time": _utc_now_iso(),
            }
        ]

    # ── Check 5: production opportunity quote freshness ──────────────────
    if settings.neg_risk_quote_worker_enabled:
        from polyarb.routing.neg_risk_quote_store import NegRiskQuoteStore
        from polyarb.routing.opportunity_scanner import (
            QUOTE_SLA_SECONDS,
            QUOTE_WARN_SECONDS,
        )

        quote_error_kind: str | None = None
        try:
            quote_run = NegRiskQuoteStore(store.db_path).latest_complete_run()
        except sqlite3.Error as error:
            quote_run = None
            quote_error_kind = type(error).__name__
        if quote_run is None:
            quote_age_s: float | None = None
            quote_status = "fail"
        else:
            quote_age_s = max(0.0, now_s - quote_run.quoted_at_ms / 1000.0)
            if quote_age_s < QUOTE_WARN_SECONDS:
                quote_status = "pass"
            elif quote_age_s <= QUOTE_SLA_SECONDS:
                quote_status = "warn"
            else:
                quote_status = "fail"
        overall = _severity(overall, quote_status)
        checks["quote_feed:last_complete_age_seconds"] = [
            {
                "componentId": "neg-risk-quote-worker",
                "componentType": "component",
                "observedValue": (round(quote_age_s, 1) if quote_age_s is not None else None),
                "observedUnit": "s",
                "status": quote_status,
                "output": (
                    f"quote-store-unreadable:{quote_error_kind}"
                    if quote_error_kind is not None
                    else None
                ),
                "time": _utc_now_iso(),
            }
        ]

        if quote_worker_runtime is not None:
            runtime = quote_worker_runtime.snapshot()
            if runtime.state in {"cold-start", "error"}:
                collector_status = "warn"
            elif runtime.state == "stopped":
                collector_status = "fail"
            else:
                collector_status = "pass"
            overall = _severity(overall, collector_status)
            checks["quote_feed:collector_state"] = [
                {
                    "componentId": "neg-risk-quote-worker",
                    "componentType": "component",
                    "observedValue": runtime.state,
                    "status": collector_status,
                    "output": runtime.last_error_kind,
                    "time": _utc_now_iso(),
                }
            ]

    return checks, overall


def _build_health_body(
    overall: str,
    checks: dict[str, list[dict[str, Any]]],
    settings: Any,
) -> dict[str, Any]:
    """Shared IETF body shape — same for /health and /healthz (D-06 full mirror)."""
    return {
        "status": overall,
        "version": settings.version,
        "releaseId": settings.release_id,
        "serviceId": "polyarb-l1",
        "description": "Polymarket L1 observation daemon — subset 2x/day, full 1/week",
        "checks": checks,
    }


async def health(request: Request) -> JSONResponse:
    """GET /health — IETF strict三态 health response.

    Returns 200 for pass/warn, 503 for fail. Better Stack外探针 reads this
    endpoint — 503 is the告警 signal. Fly platform probe reads /healthz
    instead (per Phase 02.1 D-05).

    Reads from app.state.sqlite_store and app.state.settings.
    All SQLite reads use mode=ro URI (P3.8: HTTP server never writes).
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store: SQLiteStore = request.app.state.sqlite_store
    settings = request.app.state.settings

    checks, overall = _build_health_checks(
        store,
        settings,
        time.time(),
        getattr(request.app.state, "quote_worker_runtime", None),
    )
    body = _build_health_body(overall, checks, settings)
    http_status = 503 if overall == "fail" else 200
    return JSONResponse(body, status_code=http_status, media_type=HEALTH_CONTENT_TYPE)


async def healthz(request: Request) -> JSONResponse:
    """GET /healthz — Fly-friendly probe. ALWAYS HTTP 200.

    Same JSON body schema as /health (D-06 full mirror). The underlying check
    status is exposed in body["status"], but the HTTP code is always 200 so
    Fly platform's [http_service.checks] never marks the machine unhealthy.

    Why this matters (BUG-6, Phase 02 Inj 2 + Plan 02.1-02 Inj 4):
    Fly proxy stops routing traffic to a machine whose service check fails.
    If we point Fly at /health (IETF strict) then a PAUSED daemon /
    stale Supabase mirror / failed R2 upload pulls the entire machine out of
    the routing pool — including /control/unpause, blocking recovery.
    /healthz keeps the proxy happy so external administrative endpoints
    stay reachable, while /health continues to deliver true alarm semantics
    to Better Stack.

    D-22 + D-06: public endpoint, no HMAC, same body schema as /health.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store: SQLiteStore = request.app.state.sqlite_store
    settings = request.app.state.settings

    checks, overall = _build_health_checks(
        store,
        settings,
        time.time(),
        getattr(request.app.state, "quote_worker_runtime", None),
    )
    body = _build_health_body(overall, checks, settings)
    # KEY: ignore overall when deciding HTTP code — always 200.
    return JSONResponse(body, status_code=200, media_type=HEALTH_CONTENT_TYPE)
