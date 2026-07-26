"""Tests for /health IETF三态 endpoint.

Covers D-12 / D-16 — IETF draft-inadarei-api-health-check-06 compliance.
Three-state health: pass (< 14h), warn (14-25h stale), fail (> 25h stale OR no snapshot).
HTTP 200 for pass/warn, 503 for fail.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

from starlette.testclient import TestClient

# ---------------------------------------------------------------------------
# Helper: insert a snapshot row into a tmp SQLite DB
# ---------------------------------------------------------------------------


def _insert_snapshot(
    db_path: Path,
    *,
    taken_at_ms: int,
    status: str = "ok",
    market_count: int = 100,
) -> None:
    """Insert a minimal snapshots row so get_latest_snapshot has data."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(db_path)
    store.init_schema()

    # Insert a snapshot row directly
    con = sqlite3.connect(db_path)
    try:
        now_ms = int(time.time() * 1000)
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes"
            ")"
            " VALUES (?,?,?,?,?,?,?)",
            (taken_at_ms, now_ms, "subset", market_count, 1, "/tmp/dummy.parquet", status),
        )
        con.commit()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pass_when_fresh(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot taken 1h ago → status=pass, HTTP 200."""
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - int(1 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=one_hour_ago_ms)

    resp = http_test_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pass"
    ct = resp.headers.get("content-type", "")
    assert "application/health+json" in ct


def test_warn_when_stale(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot taken 15h ago → status=warn, HTTP 200."""
    now_ms = int(time.time() * 1000)
    fifteen_hours_ago_ms = now_ms - int(15 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=fifteen_hours_ago_ms)

    resp = http_test_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "warn"


def test_fail_when_very_stale(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot taken 26h ago → status=fail, HTTP 503."""
    now_ms = int(time.time() * 1000)
    twenty_six_hours_ago_ms = now_ms - int(26 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms)

    resp = http_test_client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "fail"


def test_checks_structure(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Response body has key 'checks' with expected sub-check keys per IETF schema."""
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - int(1 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=one_hour_ago_ms)

    resp = http_test_client.get("/health")
    body = resp.json()

    assert "checks" in body
    checks = body["checks"]
    assert "snapshot:last_success_age_seconds" in checks
    assert "snapshot:last_status" in checks

    # Each value is a list of dicts per IETF schema
    age_checks = checks["snapshot:last_success_age_seconds"]
    assert isinstance(age_checks, list)
    assert len(age_checks) >= 1
    assert isinstance(age_checks[0], dict)

    status_checks = checks["snapshot:last_status"]
    assert isinstance(status_checks, list)
    assert len(status_checks) >= 1
    assert isinstance(status_checks[0], dict)


def test_no_snapshot_returns_fail(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Empty DB (no snapshots) → status=fail, HTTP 503 (first deploy edge case)."""
    # db_path exists but has no snapshots — init schema only
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    resp = http_test_client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "fail"


# ---------------------------------------------------------------------------
# Plan 02.1-03 — /healthz Fly-friendly probe (D-05 / D-06)
#
# /healthz mirrors /health body schema but ALWAYS returns HTTP 200 regardless
# of underlying check status. Fly platform probe reads only HTTP code, so this
# keeps Fly proxy routing traffic even when daemon is PAUSED or
# Supabase/R2 are stale. /health (IETF strict 503) remains for Better Stack.
# ---------------------------------------------------------------------------


def test_healthz_returns_200_when_fresh(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot 1h ago (underlying pass) → /healthz HTTP 200."""
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - int(1 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=one_hour_ago_ms)

    resp = http_test_client.get("/healthz")
    assert resp.status_code == 200


def test_healthz_returns_200_when_stale(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot 15h ago (underlying warn) → /healthz HTTP 200."""
    now_ms = int(time.time() * 1000)
    fifteen_hours_ago_ms = now_ms - int(15 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=fifteen_hours_ago_ms)

    resp = http_test_client.get("/healthz")
    assert resp.status_code == 200


def test_healthz_returns_200_when_failed(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot 26h ago (underlying fail) → /healthz STILL HTTP 200 (key D-05 differentiator)."""
    now_ms = int(time.time() * 1000)
    twenty_six_hours_ago_ms = now_ms - int(26 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms)

    resp = http_test_client.get("/healthz")
    # KEY: NOT 503 — Fly probe sees 200, proxy keeps routing traffic.
    assert resp.status_code == 200


def test_healthz_body_has_status_field_and_content_type(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """GET /healthz body has status field + application/health+json content-type (D-06)."""
    now_ms = int(time.time() * 1000)
    twenty_six_hours_ago_ms = now_ms - int(26 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms)

    resp = http_test_client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert body["status"] in ("pass", "warn", "fail")
    # underlying state is fail; only HTTP wrapping differs from /health
    assert body["status"] == "fail"
    # D-06: same schema as /health
    assert "checks" in body
    # RESEARCH Pitfall 5: must use application/health+json content type
    ct = resp.headers.get("content-type", "")
    assert "health+json" in ct
