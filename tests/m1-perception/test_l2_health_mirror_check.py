"""Tests for Phase 03.1 Plan 02 — /health l2_tob_age_seconds chain-truth wiring.

Two layers exercised here:

1. **Settings layer** (tests 1-3c): l2_mirror_enabled auto-detect from supabase
   secrets, l2_tob_age_warn_s / l2_tob_age_fail_s default + env override.

2. **/health mirror sub-check layer** (tests 4-7): when l2_mirror_enabled=True
   and SqliteStore.get_l2_tob_last_mirror_at_s returns various ages, the
   mirror:l2_tob_age_seconds sub-check returns pass / warn / fail per the
   thresholds; when l2_mirror_enabled=False, the sub-check is absent.

The Settings-level tests use POLYARB_ALLOW_EMPTY_SECRET=1 to bypass the prod
secret guard (this is a test escape hatch baked into config.py).

Plan 01 contract consumed (NOT touched by this plan):
- SQLiteStore.get_l2_tob_last_mirror_at_s() -> int | None
- L2SupabaseMirror(store=...) kwarg threads the freshness anchor write-side
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Section 1: Settings auto-detect + threshold defaults / env override
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _allow_empty_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """All Settings()-level tests run without a scan_shared_secret."""
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    # Make sure prior test runs in same process didn't leak secrets through env.
    for var in (
        "POLYARB_SUPABASE_URL",
        "POLYARB_SUPABASE_SERVICE_KEY",
        "POLYARB_L2_MIRROR_ENABLED",
        "POLYARB_L2_TOB_AGE_WARN_S",
        "POLYARB_L2_TOB_AGE_FAIL_S",
    ):
        monkeypatch.delenv(var, raising=False)


def test_settings_l2_mirror_enabled_auto_detect_when_secrets_present() -> None:
    """Test 1 (RED) — Settings(supabase_url=..., supabase_service_key=...) auto-sets
    BOTH supabase_mirror_enabled AND l2_mirror_enabled to True.
    """
    from polyarb.config import Settings

    s = Settings(
        supabase_url="https://x.supabase.co",
        supabase_service_key="some-key",
    )
    assert s.supabase_mirror_enabled is True, "supabase_mirror_enabled should auto-set"
    assert s.l2_mirror_enabled is True, "l2_mirror_enabled should auto-set (Plan 03.1-02)"


def test_settings_l2_mirror_disabled_when_secrets_missing() -> None:
    """Test 2 — empty supabase_url + empty service_key → both flags False."""
    from polyarb.config import Settings

    s = Settings(supabase_url="", supabase_service_key="")
    assert s.supabase_mirror_enabled is False
    assert s.l2_mirror_enabled is False


def test_settings_l2_tob_age_defaults_match_plan() -> None:
    """Test 3b — defaults: warn=300, fail=600 (parity with the original hardcoded
    _MIRROR_PASS_S = 300 + new explicit fail threshold per Plan 02)."""
    from polyarb.config import Settings

    s = Settings()
    assert s.l2_tob_age_warn_s == 300, f"warn default should be 300, got {s.l2_tob_age_warn_s}"
    assert s.l2_tob_age_fail_s == 600, f"fail default should be 600, got {s.l2_tob_age_fail_s}"


def test_settings_l2_tob_age_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test 3c — POLYARB_L2_TOB_AGE_WARN_S=15 + POLYARB_L2_TOB_AGE_FAIL_S=30 are
    honored. This is the Plan 07 chaos knob — env override lowers thresholds
    so chaos can flip /health within 60s instead of waiting 10min."""
    from polyarb.config import Settings

    monkeypatch.setenv("POLYARB_L2_TOB_AGE_WARN_S", "15")
    monkeypatch.setenv("POLYARB_L2_TOB_AGE_FAIL_S", "30")
    s = Settings()
    assert s.l2_tob_age_warn_s == 15
    assert s.l2_tob_age_fail_s == 30


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: /health mirror:l2_tob_age_seconds sub-check (via _build_l2_health_checks)
# ─────────────────────────────────────────────────────────────────────────────


def _make_settings_with_mirror(
    monkeypatch: pytest.MonkeyPatch,
    warn_s: int = 300,
    fail_s: int = 600,
) -> Any:
    """Settings with l2_mirror_enabled=True via the auto-detect path."""
    from polyarb.config import Settings

    # Override the threshold defaults via env so chaos-style tight thresholds work
    monkeypatch.setenv("POLYARB_L2_TOB_AGE_WARN_S", str(warn_s))
    monkeypatch.setenv("POLYARB_L2_TOB_AGE_FAIL_S", str(fail_s))
    return Settings(
        supabase_url="https://x.supabase.co",
        supabase_service_key="some-key",
    )


def test_health_mirror_warn_on_cold_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test 4 (RED) — get_l2_tob_last_mirror_at_s returns None → status=warn,
    cold-start tolerated (no fail trigger on first boot)."""
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _make_settings_with_mirror(monkeypatch)
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None
    now_s = time.time()

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    assert "mirror:l2_tob_age_seconds" in checks
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "warn", f"cold start should be warn, got {entry['status']}"
    assert entry["observedValue"] is None


def test_health_mirror_pass_when_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test 5 — last mirror 90s ago, warn_s=300, fail_s=600 → status=pass."""
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _make_settings_with_mirror(monkeypatch, warn_s=300, fail_s=600)
    now_s = time.time()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = int(now_s - 90)

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "pass", f"90s age, warn>300, should pass; got {entry['status']}"


def test_health_mirror_fail_when_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test 6 — last mirror 700s ago, warn_s=300, fail_s=600 → status=fail,
    overall=fail (mirror failure propagates to HTTP 503 alarm path)."""
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _make_settings_with_mirror(monkeypatch, warn_s=300, fail_s=600)
    now_s = time.time()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = int(now_s - 700)

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "fail", f"700s age, fail>600, should fail; got {entry['status']}"
    assert overall == "fail", f"overall should propagate fail, got {overall}"


def test_health_mirror_absent_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test 7 — l2_mirror_enabled=False (no secrets) → mirror sub-check absent."""
    from polyarb.config import Settings
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = Settings(supabase_url="", supabase_service_key="")
    assert settings.l2_mirror_enabled is False
    store = MagicMock()
    now_s = time.time()

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    assert "mirror:l2_tob_age_seconds" not in checks, (
        "mirror sub-check must be absent when l2_mirror_enabled=False (backwards compat)"
    )


def test_health_mirror_warn_on_borderline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test 7b — last mirror 450s ago, warn=300, fail=600 → status=warn (in-between)."""
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _make_settings_with_mirror(monkeypatch, warn_s=300, fail_s=600)
    now_s = time.time()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = int(now_s - 450)

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "warn", f"450s age, warn>=300<fail=600 → warn; got {entry['status']}"
