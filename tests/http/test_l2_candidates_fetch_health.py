"""Tests for D-01 chain-truth — candidates:supabase_fetch_age_seconds.

Phase 04 Plan 02 Task 3. The fail-soft Supabase fetch in
`l2_candidate_refresh.on_snapshot_complete` (Task 2) updates
`_last_fetch_success_at_s` on every successful fetch. This /health sub-check
reads that field — sustained failure surfaces as `fail`, not silence
(market-observation-architecture.md §1.6).

Cases:
- cold-start: never fetched → status='warn' (boot OK, NOT fail)
- fresh:      last fetch recent → status='pass'
- stale:      last fetch beyond fail threshold → status='fail'
- not configured (Supabase URL empty): sub-check absent (case (a) parity with
  the Plan 03 mirror gate)
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _allow_empty_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    for var in ("POLYARB_SUPABASE_URL", "POLYARB_SUPABASE_SERVICE_KEY"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _reset_fetch_state():
    """Reset the module-level last-fetch timestamp between tests."""
    import polyarb.observation.l2_candidate_refresh as mod

    mod._last_fetch_success_at_s = None
    yield
    mod._last_fetch_success_at_s = None


def _settings_with_supabase():
    """Settings instance with Supabase URL + key set (case-c posture)."""
    from polyarb.config import Settings

    return Settings(
        supabase_url="https://x.supabase.co",
        supabase_service_key="test-key",
    )


def test_fetch_health_cold_start_warns():
    """Before any fetch, sub-check status='warn' with 'cold-start' output."""
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _settings_with_supabase()
    store = MagicMock()
    # Pretend mirror cache has recent timestamp so the OTHER mirror sub-check
    # doesn't pollute overall status.
    store.get_l2_tob_last_mirror_at_s.return_value = time.time()

    checks, _overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=time.time()
    )
    assert "candidates:supabase_fetch_age_seconds" in checks, (
        "sub-check must be registered when supabase_url + service_key are set"
    )
    entry = checks["candidates:supabase_fetch_age_seconds"][0]
    assert entry["status"] == "warn", f"cold-start should warn, got {entry}"
    assert "cold-start" in (entry.get("output") or "").lower()


def test_fetch_health_fresh_passes():
    """Recent fetch → status='pass'."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.http.l2_health import _build_l2_health_checks

    now = time.time()
    mod._last_fetch_success_at_s = now - 5  # 5s ago — well under warn threshold.

    settings = _settings_with_supabase()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = now

    checks, _overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now
    )
    entry = checks["candidates:supabase_fetch_age_seconds"][0]
    assert entry["status"] == "pass", f"5s-old fetch should pass, got {entry}"
    assert entry["observedValue"] is not None
    assert entry["observedValue"] >= 0


def test_fetch_health_warn_threshold():
    """Age between warn and fail thresholds → status='warn'."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.http.l2_health import _build_l2_health_checks

    now = time.time()
    # warn threshold default = 120s; fail = 600s. Pick 200s.
    mod._last_fetch_success_at_s = now - 200

    settings = _settings_with_supabase()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = now

    checks, _overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now
    )
    entry = checks["candidates:supabase_fetch_age_seconds"][0]
    assert entry["status"] == "warn"


def test_fetch_health_stale_fails():
    """Age > fail threshold → status='fail' (surfaces sustained Supabase outage)."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.http.l2_health import _build_l2_health_checks

    now = time.time()
    mod._last_fetch_success_at_s = now - 1200  # 20min old — well beyond 600s fail.

    settings = _settings_with_supabase()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = now

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now
    )
    entry = checks["candidates:supabase_fetch_age_seconds"][0]
    assert entry["status"] == "fail", f"20min-stale fetch should fail, got {entry}"
    assert overall == "fail", "overall must escalate when a sub-check fails"


def test_fetch_health_not_registered_when_supabase_unconfigured():
    """When supabase_url is empty, the sub-check is absent (case-a parity)."""
    from polyarb.config import Settings
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = Settings(supabase_url="", supabase_service_key="")
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None

    checks, _overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=time.time()
    )
    assert "candidates:supabase_fetch_age_seconds" not in checks, (
        "must NOT register when Supabase not configured"
    )
