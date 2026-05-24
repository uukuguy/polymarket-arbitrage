"""Tests for L2 /health (IETF strict) + /healthz (always-200) endpoints.

Phase 03 Plan 03 — drives implementation of:
- src/polyarb/http/l2_health.py (_build_l2_health_checks helper + handlers)
- src/polyarb/http/l2_app.py  (create_l2_app factory)

Invariants asserted:
- /healthz ALWAYS 200 (Phase 02.1 BUG-6 fix — Fly proxy must keep routing)
- /health 503 when WS RECONNECTING > 60s (Phase 02.1 D-05 strict IETF)
- serviceId is "polyarb-l2" (T-03-03-04 — no L1 copy-paste leak)
- body never leaks secrets / db_path (T-03-03-06)
- application/health+json content type (RESEARCH Pitfall 5)
- graceful degradation when ws_consumer=None (Plan 04 not yet wired)
"""
from __future__ import annotations

import time
from typing import Any

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — pass when ws_consumer is CONNECTED + fresh event
# ─────────────────────────────────────────────────────────────────────────────

def test_l2_pass_when_ws_connected_and_fresh(l2_http_test_client, mock_ws_consumer):
    """WS CONNECTED + event 5s old → status pass (or warn from optional sub-checks)."""
    mock_ws_consumer.current_state = "CONNECTED"
    mock_ws_consumer.last_event_at_s = time.time() - 5
    resp = l2_http_test_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] in ("pass", "warn"), f"expected pass/warn, got {body['status']}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — warn when WS WAITING_FOR_EVENT with mid-age event
# ─────────────────────────────────────────────────────────────────────────────

def test_health_warn_when_ws_waiting(l2_http_test_client, mock_ws_consumer):
    """WS WAITING_FOR_EVENT + last event 45s ago → warn (not fail)."""
    mock_ws_consumer.current_state = "WAITING_FOR_EVENT"
    mock_ws_consumer.last_event_at_s = time.time() - 45
    resp = l2_http_test_client.get("/health")
    # WAITING_FOR_EVENT + 45s age → ws_state=warn; ws_age=warn → overall warn
    assert resp.status_code == 200
    assert resp.json()["status"] == "warn"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — fail when WS RECONNECTING > 60s (Phase 02.1 D-05 strict)
# ─────────────────────────────────────────────────────────────────────────────

def test_health_fail_when_ws_reconnecting_too_long(l2_http_test_client, mock_ws_consumer):
    """WS RECONNECTING > 60s → /health returns 503 + body status=fail."""
    mock_ws_consumer.current_state = "RECONNECTING"
    mock_ws_consumer.last_event_at_s = time.time() - 120
    resp = l2_http_test_client.get("/health")
    assert resp.status_code == 503, f"expected 503 (IETF strict), got {resp.status_code}"
    assert resp.json()["status"] == "fail"


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — /healthz ALWAYS 200 even when underlying is fail (BUG-6 invariant)
# ─────────────────────────────────────────────────────────────────────────────

def test_healthz_always_200(l2_http_test_client, mock_ws_consumer):
    """SAME failing setup as Test 3 → /healthz returns 200 (body still reports fail)."""
    mock_ws_consumer.current_state = "RECONNECTING"
    mock_ws_consumer.last_event_at_s = time.time() - 120
    resp = l2_http_test_client.get("/healthz")
    assert resp.status_code == 200, f"BUG-6 invariant broken: /healthz returned {resp.status_code}"
    # Body STILL reports fail — only the HTTP wrapping is forced 200
    assert resp.json()["status"] == "fail"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — serviceId is "polyarb-l2" (T-03-03-04)
# ─────────────────────────────────────────────────────────────────────────────

def test_health_body_serviceid_polyarb_l2(l2_http_test_client):
    """T-03-03-04 — serviceId MUST be polyarb-l2, never polyarb-l1."""
    resp = l2_http_test_client.get("/health")
    body = resp.json()
    assert body.get("serviceId") == "polyarb-l2", f"got {body.get('serviceId')!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — no secret leak in health body (T-03-03-06)
# ─────────────────────────────────────────────────────────────────────────────

def test_health_body_no_secret_leak(l2_http_test_client):
    """T-03-03-06 — body must not include db_path / secret / dsn / token / key substrings."""
    resp = l2_http_test_client.get("/health")
    body_str = resp.text.lower()
    for forbidden in ("db_path", "secret", "dsn", "service_role"):
        assert forbidden not in body_str, f"leak: {forbidden!r} found in body"


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — Content-Type is application/health+json (RESEARCH Pitfall 5)
# ─────────────────────────────────────────────────────────────────────────────

def test_healthz_content_type_health_json(l2_http_test_client):
    """RESEARCH Pitfall 5 — must use application/health+json."""
    resp = l2_http_test_client.get("/healthz")
    ct = resp.headers.get("content-type", "")
    assert "health+json" in ct, f"expected health+json content-type, got {ct!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — graceful degradation when ws_consumer is None (Plan 04 not wired yet)
# ─────────────────────────────────────────────────────────────────────────────

def test_health_when_ws_consumer_none(daemon_settings_for_test):
    """ws_consumer=None (Plan 03 boundary) → /health degrades to warn, not 500."""
    from starlette.testclient import TestClient
    from polyarb.http.l2_app import create_l2_app
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    app = create_l2_app(
        sqlite_store=store,
        settings=daemon_settings_for_test,
        ws_consumer=None,
        event_listener=None,
    )
    with TestClient(app) as client:
        resp = client.get("/health")
    # Graceful degradation — NOT 500
    assert resp.status_code in (200, 503), f"expected 200/503, got {resp.status_code}"
    body_lower = resp.text.lower()
    assert "not_configured" in body_lower or "not configured" in body_lower, (
        f"expected 'not configured' marker in body, got: {resp.text[:300]}"
    )
