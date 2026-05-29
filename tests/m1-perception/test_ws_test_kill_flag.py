"""Tests for POLYARB_WS_TEST_KILL chaos primitive (Phase 03.1 Plan 06 Task 1).

Drives implementation of:
- src/polyarb/daemon/ws_consumer.py — _check_ws_test_kill() helper + WsTestKillRequested
- src/polyarb/http/l2_health.py — chaos:test_kill_flag sub-check (chain-truth own-dog-food)

Phase 04.1 G-03 update: the flag is now a PROCESS-LOCAL bool seeded from env at import,
but controlled at runtime via set_ws_test_kill(). Tests that tested env→check_raise
behavior now use set_ws_test_kill() for isolation (env-seeding is tested in
test_l2_chaos_ws_kill_endpoint.py via module reload). The prod-safety invariant
(fly-l2.toml must not contain POLYARB_WS_TEST_KILL) is unaffected.
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


def test_unset_no_raise():
    """Flag False → no raise, returns None (sanity).

    Phase 04.1 G-03: _check_ws_test_kill reads the module-level flag, not env.
    Use set_ws_test_kill(False) for reliable isolation.
    """
    from polyarb.daemon.ws_consumer import _check_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(False)
    assert _check_ws_test_kill() is None


def test_set_to_1_raises():
    """Flag True → WsTestKillRequested raised.

    Phase 04.1 G-03: use set_ws_test_kill(True) to set the process-local flag
    (the in-band endpoint path). The env cold-start seeding is tested separately
    via module reload in test_l2_chaos_ws_kill_endpoint.py.
    """
    from polyarb.daemon.ws_consumer import WsTestKillRequested, _check_ws_test_kill, set_ws_test_kill

    set_ws_test_kill(True)
    try:
        with pytest.raises(WsTestKillRequested):
            _check_ws_test_kill()
    finally:
        set_ws_test_kill(False)


def test_set_to_0_no_raise(monkeypatch):
    """Env=='0' → no raise (only literal '1' triggers env-seeded flag)."""
    from polyarb.daemon.ws_consumer import _check_ws_test_kill, set_ws_test_kill

    # Env '0' does not set the module flag; and we ensure flag is False.
    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "0")
    set_ws_test_kill(False)
    assert _check_ws_test_kill() is None


def test_set_to_true_no_raise(monkeypatch):
    """Env=='true' → no raise (only '1' triggers — guards against accidental
    boolean-like values from misconfigured fly secret).
    """
    from polyarb.daemon.ws_consumer import _check_ws_test_kill, set_ws_test_kill

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "true")
    set_ws_test_kill(False)
    assert _check_ws_test_kill() is None


def test_set_to_empty_no_raise(monkeypatch):
    """Env=='' (empty string from `fly secrets unset` race) → no raise."""
    from polyarb.daemon.ws_consumer import _check_ws_test_kill, set_ws_test_kill

    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "")
    set_ws_test_kill(False)
    assert _check_ws_test_kill() is None


def test_module_import_does_not_execute(monkeypatch):
    """Importing the module does NOT trigger the kill at import time.
    The module-level flag is seeded from env, but _check_ws_test_kill is
    only called explicitly (call site ws_consumer.py:167).
    """
    monkeypatch.setenv("POLYARB_WS_TEST_KILL", "1")
    import importlib

    import polyarb.daemon.ws_consumer as mod

    reloaded = importlib.reload(mod)  # seeds flag=True from env but does not raise
    # No exception means import is safe regardless of env.
    reloaded.set_ws_test_kill(False)  # reset for other tests


# ── Chain-truth own-dog-food: /health surface (W-5) ───────────────────────────


def test_health_surfaces_kill_flag_when_set():
    """W-5 chain-truth: process-local flag True MUST surface to /health checks.

    Phase 04.1 G-03 update: /health reads get_ws_test_kill() (process-local),
    NOT os.getenv. Use set_ws_test_kill(True) to test the in-band endpoint path.
    Per feedback_code-vs-chain-truth-2026-05: fail-soft envelopes must be
    externally observable via the chain.
    """
    from polyarb.daemon.ws_consumer import set_ws_test_kill
    from polyarb.http.l2_health import _build_l2_health_checks

    # Minimal stubs — health helper accepts duck-typed args
    class _StubSettings:
        l2_mirror_enabled = False
        version = "test"
        release_id = "test"

    set_ws_test_kill(True)
    try:
        checks, overall = _build_l2_health_checks(
            store=None,
            settings=_StubSettings(),
            ws_consumer=None,
            event_listener=None,
            now_s=time.time(),
        )
    finally:
        set_ws_test_kill(False)

    assert "chaos:ws_test_kill_flag" in checks, (
        "chain-truth own-dog-food: process-local flag=True must surface as a "
        "chaos:ws_test_kill_flag sub-check in /health response"
    )
    sub = checks["chaos:ws_test_kill_flag"][0]
    assert sub["status"] == "warn", "chaos flag is warn (not fail) — flag itself doesn't trip overall"
    output = (sub.get("output") or "").lower()
    assert "chaos" in output or "should never" in output or "production" in output, (
        f"chaos sub-check output should mention chaos/production; got: {sub.get('output')!r}"
    )


def test_health_omits_kill_flag_when_unset():
    """When process-local flag is False, chaos:ws_test_kill_flag MUST NOT
    appear in checks (no noise in prod /health response).
    """
    from polyarb.daemon.ws_consumer import set_ws_test_kill
    from polyarb.http.l2_health import _build_l2_health_checks

    set_ws_test_kill(False)

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
        "chaos sub-check must NOT appear when flag False — prod /health stays clean"
    )


def test_health_omits_kill_flag_when_set_to_0():
    """Flag False → sub-check absent. (Env '0' is not the opt-in; process flag stays False.)"""
    from polyarb.daemon.ws_consumer import set_ws_test_kill
    from polyarb.http.l2_health import _build_l2_health_checks

    set_ws_test_kill(False)

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
