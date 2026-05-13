"""IETF draft-inadarei-api-health-check-06 compliant /health endpoint.

Phase 02 Plan 02 — D-12 / D-13 / D-16.

Three-state health: pass | warn | fail
- pass  → all checks green (HTTP 200)
- warn  → at least one check degraded, none failed (HTTP 200)
- fail  → at least one check failed (HTTP 503)

Plan 02 checks (minimal set — Plan 03 adds supabase/r2):
1. snapshot:last_success_age_seconds
   - pass  < 14h  (subset cron interval 12h + 2h buffer)
   - warn  14-25h
   - fail  > 25h  OR no snapshot at all
2. snapshot:last_status
   - maps SnapshotStatus.OK → pass, DEGRADED → warn, FAILED → fail
   - if no snapshot → omitted from checks (age check already reports fail)

Overall = worst-of all sub-checks (fail > warn > pass).
HTTP 200 for pass/warn; 503 for fail (Better Stack convention).

Security note (T-02-09): /health is intentionally PUBLIC (no HMAC).
Better Stack uptime probe needs unauthenticated access. Response exposes only
snapshot age + status enum — no DB schema, no IPs, no secrets.

Source: datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06
        RESEARCH.md §8
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

HEALTH_CONTENT_TYPE = "application/health+json"

# Age thresholds in seconds
_PASS_AGE_S = 14 * 3600     # < 14h → pass
_WARN_AGE_S = 25 * 3600     # 14-25h → warn; > 25h → fail


def _utc_now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format with Z suffix."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _severity(a: str, b: str) -> str:
    """Return worst of two health statuses (fail > warn > pass)."""
    order = {"pass": 0, "warn": 1, "fail": 2}
    return a if order.get(a, 0) >= order.get(b, 0) else b


async def health(request: Request) -> JSONResponse:
    """GET /health — IETF三态 health response.

    Reads from app.state.sqlite_store and app.state.settings.
    All SQLite reads use mode=ro URI (P3.8: HTTP server never writes).
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store: SQLiteStore = request.app.state.sqlite_store
    settings = request.app.state.settings

    checks: dict[str, list[dict[str, Any]]] = {}
    overall = "pass"
    now_s = time.time()

    # ── Check 1: snapshot age ─────────────────────────────────────────────
    last_snapshot = store.get_latest_snapshot()

    if last_snapshot is None:
        age_s = None
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
    _MIRROR_WARN_S = 25 * 3600
    _MIRROR_FAIL_S = 48 * 3600
    if settings.supabase_mirror_enabled:
        if last_snapshot is not None and last_snapshot.get("supabase_mirror_at_ms") is not None:
            mirror_age_s = now_s - last_snapshot["supabase_mirror_at_ms"] / 1000.0
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
            r2_value = True
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

    # ── Build response ─────────────────────────────────────────────────────
    body = {
        "status": overall,
        "version": settings.version,
        "releaseId": settings.release_id,
        "serviceId": "polyarb-l1",
        "description": "Polymarket L1 observation daemon — subset 2x/day, full 1/week",
        "checks": checks,
    }

    http_status = 503 if overall == "fail" else 200
    return JSONResponse(body, status_code=http_status, media_type=HEALTH_CONTENT_TYPE)
