"""Tests for G-03: in-band HMAC ws-test-kill chaos endpoint (Phase 04.1 Plan 03).

Drives implementation of:
- src/polyarb/daemon/ws_consumer.py — process-local _ws_test_kill_flag + set/get API
- src/polyarb/http/l2_control.py — HMAC-gated POST /control/chaos/ws-test-kill handler
- src/polyarb/http/l2_app.py — ControlAuthMiddleware wiring + new route

This is the G-03 fix: instead of setting a Fly secret (which RESTARTS the machine,
killing the 60-asset pre-storm process), the chaos primitive is now an in-band HTTP
call that flips a PROCESS-LOCAL flag on the RUNNING process.

HMAC verification is delegated to ControlAuthMiddleware (reused from control.py,
path-guarded on /control/*). The route lives under /control/ so the guard covers it.

Threat model: T-04.1-01..08 (see 04.1-03-PLAN.md threat_model section).
"""
from __future__ import annotations

import hashlib
import hmac
import json

import pytest


# ── Helper: compute valid X-Signature for a body + secret ────────────────────

def _sign(body: bytes, secret: str) -> str:
    """Compute HMAC-SHA256 signature in sha256=<hex> format (same as control.py)."""
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


# ── Fake settings for l2_app ──────────────────────────────────────────────────

class _FakeSecret:
    def get_secret_value(self) -> str:
        return "test-secret"


class _FakeSettings:
    scan_shared_secret = _FakeSecret()
    l2_mirror_enabled = False
    version = "test"
    release_id = "test"


# ── Task 1 Test 1: process-local flag + set/get API ──────────────────────────


def test_set_ws_test_kill_true_then_check_raises():
    """set_ws_test_kill(True) → _check_ws_test_kill() raises WsTestKillRequested."""
    from polyarb.daemon.ws_consumer import (
        WsTestKillRequested,
        _check_ws_test_kill,
        set_ws_test_kill,
    )

    # Ensure flag is clear first (test isolation)
    set_ws_test_kill(False)
    assert _check_ws_test_kill() is None  # sanity: clear

    set_ws_test_kill(True)
    with pytest.raises(WsTestKillRequested):
        _check_ws_test_kill()

    # Cleanup
    set_ws_test_kill(False)


def test_set_ws_test_kill_false_then_check_no_raise():
    """set_ws_test_kill(False) → _check_ws_test_kill() returns None."""
    from polyarb.daemon.ws_consumer import _check_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(True)  # set to True first
    set_ws_test_kill(False)  # then clear
    assert _check_ws_test_kill() is None


# ── Task 1 Test 2: module flag seeds from env at import (cold-start compat) ──


def test_module_flag_seeds_from_env_when_set(monkeypatch):
    """When env POLYARB_WS_TEST_KILL='1' at import, initial flag is True.

    This test validates the cold-start compatibility contract: fly secrets set
    POLYARB_WS_TEST_KILL=1 before deploy still works (for the rare case where
    someone might set it at deploy time vs. the in-band endpoint at runtime).
    """
    import importlib

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "1")
    import polyarb.daemon.ws_consumer as mod

    reloaded = importlib.reload(mod)
    assert reloaded.get_ws_test_kill() is True, (
        "Flag must seed True when POLYARB_WS_TEST_KILL='1' at import"
    )
    # Reset for other tests
    reloaded.set_ws_test_kill(False)


def test_module_flag_seeds_false_when_env_unset(monkeypatch):
    """When env is unset at import, initial flag is False."""
    import importlib

    monkeypatch.delenv("POLYARB_WS_TEST_KILL", raising=False)
    import polyarb.daemon.ws_consumer as mod

    reloaded = importlib.reload(mod)
    assert reloaded.get_ws_test_kill() is False


# ── Task 1 Test 3: POST /control/chaos/ws-test-kill with valid signature ─────


@pytest.fixture()
def l2_app():
    """Create L2 Starlette app with HMAC middleware for endpoint testing."""
    from starlette.testclient import TestClient
    from polyarb.http.l2_app import create_l2_app
    from polyarb.daemon.ws_consumer import set_ws_test_kill

    # Ensure clean flag state
    set_ws_test_kill(False)
    app = create_l2_app(
        sqlite_store=None,
        settings=_FakeSettings(),
    )
    return TestClient(app, raise_server_exceptions=True)


SECRET = "test-secret"


def test_endpoint_valid_sig_enabled_true(l2_app):
    """POST /control/chaos/ws-test-kill with valid X-Signature + enabled:true → 200, flag set."""
    from polyarb.daemon.ws_consumer import get_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(False)  # start clean
    body = b'{"enabled":true}'
    resp = l2_app.post(
        "/control/chaos/ws-test-kill",
        content=body,
        headers={
            "X-Signature": _sign(body, SECRET),
            "Content-Type": "application/json",
        },
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data.get("status") == "ok"
    assert data.get("ws_test_kill") is True
    assert get_ws_test_kill() is True

    # Cleanup
    set_ws_test_kill(False)


def test_endpoint_valid_sig_enabled_false(l2_app):
    """POST /control/chaos/ws-test-kill with valid X-Signature + enabled:false → 200, flag cleared."""
    from polyarb.daemon.ws_consumer import get_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(True)  # start with flag set
    body = b'{"enabled":false}'
    resp = l2_app.post(
        "/control/chaos/ws-test-kill",
        content=body,
        headers={
            "X-Signature": _sign(body, SECRET),
            "Content-Type": "application/json",
        },
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data.get("status") == "ok"
    assert data.get("ws_test_kill") is False
    assert get_ws_test_kill() is False


# ── Task 1 Test 4: HMAC gating — missing or wrong signature → 401 ─────────────


def test_endpoint_missing_signature_returns_401(l2_app):
    """POST /control/chaos/ws-test-kill without X-Signature → 401, flag UNCHANGED."""
    from polyarb.daemon.ws_consumer import get_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(False)
    body = b'{"enabled":true}'
    resp = l2_app.post(
        "/control/chaos/ws-test-kill",
        content=body,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 401, f"Expected 401, got {resp.status_code}"
    assert get_ws_test_kill() is False, "Flag must be UNCHANGED after 401"


def test_endpoint_wrong_signature_returns_401(l2_app):
    """POST /control/chaos/ws-test-kill with wrong X-Signature → 401, flag UNCHANGED."""
    from polyarb.daemon.ws_consumer import get_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(False)
    body = b'{"enabled":true}'
    resp = l2_app.post(
        "/control/chaos/ws-test-kill",
        content=body,
        headers={
            "X-Signature": "sha256=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "Content-Type": "application/json",
        },
    )
    assert resp.status_code == 401, f"Expected 401, got {resp.status_code}"
    assert get_ws_test_kill() is False, "Flag must be UNCHANGED after 401"


# ── Task 1 Test 5: getter get_ws_test_kill() returns current flag ─────────────


def test_get_ws_test_kill_reflects_current_flag():
    """get_ws_test_kill() returns the current process-local flag value."""
    from polyarb.daemon.ws_consumer import get_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(False)
    assert get_ws_test_kill() is False

    set_ws_test_kill(True)
    assert get_ws_test_kill() is True

    set_ws_test_kill(False)
    assert get_ws_test_kill() is False


# ── Check-truth: /health surfaces process-local flag (chain-truth) ────────────


def test_health_reflects_process_local_flag_when_set(monkeypatch):
    """/health chaos:ws_test_kill_flag reads process-local flag (not os.getenv).

    Chain-truth: set_ws_test_kill(True) → /health surfaces the flag,
    even when POLYARB_WS_TEST_KILL is NOT in env (the in-band endpoint case).
    This is the G-03 key invariant: env is cold-start only; runtime toggle is via endpoint.
    """
    import time
    from polyarb.daemon.ws_consumer import set_ws_test_kill
    from polyarb.http.l2_health import _build_l2_health_checks

    monkeypatch.delenv("POLYARB_WS_TEST_KILL", raising=False)  # ensure env NOT set

    class _S:
        l2_mirror_enabled = False
        version = "test"
        release_id = "test"

    set_ws_test_kill(True)  # flip via in-band (no env var)
    try:
        checks, _ = _build_l2_health_checks(
            store=None,
            settings=_S(),
            ws_consumer=None,
            event_listener=None,
            now_s=time.time(),
        )
        assert "chaos:ws_test_kill_flag" in checks, (
            "chain-truth: process-local flag=True must surface to /health "
            "even when env POLYARB_WS_TEST_KILL is not set"
        )
        assert checks["chaos:ws_test_kill_flag"][0]["status"] == "warn"
    finally:
        set_ws_test_kill(False)


def test_health_hides_flag_when_process_local_false(monkeypatch):
    """/health chaos:ws_test_kill_flag absent when process-local flag=False.

    Even if someone somehow set the env var without going through the endpoint,
    the /health surface now reads the process-local flag, not env.
    """
    import time
    from polyarb.daemon.ws_consumer import set_ws_test_kill
    from polyarb.http.l2_health import _build_l2_health_checks

    class _S:
        l2_mirror_enabled = False
        version = "test"
        release_id = "test"

    set_ws_test_kill(False)  # ensure False
    checks, _ = _build_l2_health_checks(
        store=None,
        settings=_S(),
        ws_consumer=None,
        event_listener=None,
        now_s=time.time(),
    )
    assert "chaos:ws_test_kill_flag" not in checks


# ── Backward compat: existing _check_ws_test_kill still works (env=1 at import) ──


def test_existing_test_still_passes_after_module_flag_refactor(monkeypatch):
    """The env-based tests from test_ws_test_kill_flag.py must continue to pass.

    With the process-local flag, setting env to '1' + reloading the module
    seeds the flag True → _check_ws_test_kill() still raises. The literal '1'
    constraint is preserved in the seed logic.
    """
    import importlib
    from polyarb.daemon.ws_consumer import set_ws_test_kill

    # Set env to '1' before reload → seeds flag True
    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "1")
    import polyarb.daemon.ws_consumer as mod

    reloaded = importlib.reload(mod)

    from polyarb.daemon.ws_consumer import WsTestKillRequested

    with pytest.raises(WsTestKillRequested):
        reloaded._check_ws_test_kill()

    reloaded.set_ws_test_kill(False)
