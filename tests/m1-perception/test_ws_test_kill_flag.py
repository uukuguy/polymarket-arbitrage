"""Tests for POLYARB_WS_TEST_KILL chaos primitive (Phase 03.1 Plan 06 Task 1).

Drives implementation of:
- src/polyarb/daemon/ws_consumer.py — _check_ws_test_kill() helper + WsTestKillRequested
- src/polyarb/http/l2_health.py — chaos:test_kill_flag sub-check (chain-truth own-dog-food)

The flag is opt-in via the literal string '1'. Any other value (including '0',
'true', 'yes', empty) is ignored. The flag MUST surface to /health when set —
per feedback_code-vs-chain-truth-2026-05: fail-soft / chaos primitives cannot
be code-level only; they must be externally observable via the chain.
"""
from __future__ import annotations

import time

import pytest


# ── Helper import — driven via TDD ───────────────────────────────────────────


def test_helper_importable():
    """WsTestKillRequested + _check_ws_test_kill exist in ws_consumer."""
    from polyarb.daemon.ws_consumer import WsTestKillRequested, _check_ws_test_kill  # noqa: F401

    assert issubclass(WsTestKillRequested, Exception)
    assert callable(_check_ws_test_kill)


def test_unset_no_raise(monkeypatch):
    """Env unset → no raise, returns None (sanity)."""
    from polyarb.daemon.ws_consumer import _check_ws_test_kill

    monkeypatch.delenv("POLYARB_WS_TEST_KILL", raising=False)
    assert _check_ws_test_kill() is None


def test_set_to_1_raises(monkeypatch):
    """Env=='1' (the literal opt-in) → WsTestKillRequested raised."""
    from polyarb.daemon.ws_consumer import WsTestKillRequested, _check_ws_test_kill

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "1")
    with pytest.raises(WsTestKillRequested):
        _check_ws_test_kill()


def test_set_to_0_no_raise(monkeypatch):
    """Env=='0' → no raise (only literal '1' triggers — explicit opt-in)."""
    from polyarb.daemon.ws_consumer import _check_ws_test_kill

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "0")
    assert _check_ws_test_kill() is None


def test_set_to_true_no_raise(monkeypatch):
    """Env=='true' → no raise (only '1' triggers — guards against accidental
    boolean-like values from misconfigured fly secret).
    """
    from polyarb.daemon.ws_consumer import _check_ws_test_kill

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "true")
    assert _check_ws_test_kill() is None


def test_set_to_empty_no_raise(monkeypatch):
    """Env=='' (empty string from `fly secrets unset` race) → no raise."""
    from polyarb.daemon.ws_consumer import _check_ws_test_kill

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "")
    assert _check_ws_test_kill() is None


def test_module_import_does_not_execute(monkeypatch):
    """Importing the module does NOT trigger the kill (env-gated at call site,
    not at import time). Guards against accidental import-time crashes when a
    test or scaffold has the flag set in its env.
    """
    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "1")
    import importlib

    import polyarb.daemon.ws_consumer as mod

    importlib.reload(mod)  # if import-time check existed, this would raise
    # No exception means import is safe regardless of env.


# ── Chain-truth own-dog-food: /health surface (W-5) ───────────────────────────


def test_health_surfaces_kill_flag_when_set(monkeypatch):
    """W-5 chain-truth: POLYARB_WS_TEST_KILL=1 MUST surface to /health checks.

    Per feedback_code-vs-chain-truth-2026-05: fail-soft envelopes must be
    externally observable. Code-level OK is not enough; the flag's presence
    must be visible to operators via curl /health.
    """
    from polyarb.http.l2_health import _build_l2_health_checks

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "1")

    # Minimal stubs — health helper accepts duck-typed args
    class _StubSettings:
        l2_mirror_enabled = False
        version = "test"
        release_id = "test"

    checks, overall = _build_l2_health_checks(
        store=None,
        settings=_StubSettings(),
        ws_consumer=None,
        event_listener=None,
        now_s=time.time(),
    )

    assert "chaos:ws_test_kill_flag" in checks, (
        "chain-truth own-dog-food: POLYARB_WS_TEST_KILL=1 must surface as a "
        "chaos:ws_test_kill_flag sub-check in /health response"
    )
    sub = checks["chaos:ws_test_kill_flag"][0]
    assert sub["status"] == "warn", "chaos flag is warn (not fail) — flag itself doesn't trip overall"
    output = (sub.get("output") or "").lower()
    assert "chaos" in output or "should never" in output or "production" in output, (
        f"chaos sub-check output should mention 'should never appear in production'; got: {sub.get('output')!r}"
    )


def test_health_omits_kill_flag_when_unset(monkeypatch):
    """When POLYARB_WS_TEST_KILL is unset, chaos:ws_test_kill_flag MUST NOT
    appear in checks (no noise in prod /health response).
    """
    from polyarb.http.l2_health import _build_l2_health_checks

    monkeypatch.delenv("POLYARB_WS_TEST_KILL", raising=False)

    class _StubSettings:
        l2_mirror_enabled = False
        version = "test"
        release_id = "test"

    checks, overall = _build_l2_health_checks(
        store=None,
        settings=_StubSettings(),
        ws_consumer=None,
        event_listener=None,
        now_s=time.time(),
    )

    assert "chaos:ws_test_kill_flag" not in checks, (
        "chaos sub-check must NOT appear when flag unset — prod /health stays clean"
    )


def test_health_omits_kill_flag_when_set_to_0(monkeypatch):
    """'0' is not the opt-in value — sub-check must be absent."""
    from polyarb.http.l2_health import _build_l2_health_checks

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "0")

    class _StubSettings:
        l2_mirror_enabled = False
        version = "test"
        release_id = "test"

    checks, _ = _build_l2_health_checks(
        store=None,
        settings=_StubSettings(),
        ws_consumer=None,
        event_listener=None,
        now_s=time.time(),
    )

    assert "chaos:ws_test_kill_flag" not in checks


# ── CI gate: prod fly.toml MUST NOT contain the flag ─────────────────────────


def test_prod_fly_toml_never_sets_test_kill_flag():
    """fly.toml + fly-l2.toml must NEVER set POLYARB_WS_TEST_KILL.

    The flag is chaos-only; setting it in prod deployment config would cause
    every WS message to drop the connection.
    """
    import os

    for path in ("fly.toml", "fly-l2.toml"):
        if not os.path.exists(path):
            continue
        with open(path) as f:
            content = f.read()
        assert "POLYARB_WS_TEST_KILL" not in content, (
            f"{path} contains POLYARB_WS_TEST_KILL — that flag is chaos-only "
            f"and MUST NOT appear in production deployment config"
        )
