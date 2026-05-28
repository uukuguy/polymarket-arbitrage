"""Phase 04 Plan 03 — D-08 / GAP-200 three-branch tests for /health mirror gate.

Inverse of Phase 03.1 L4 lesson (feedback_code-vs-chain-truth-2026-05):
fail-soft envelopes were perfect at the code layer but the /health sub-check
gate was silent when `l2_mirror_enabled=False`. The bug GAP-200 fixes:
operator sets `POLYARB_SUPABASE_URL` but forgets `POLYARB_SUPABASE_SERVICE_KEY`
→ daemon silently reports healthy because the sub-check is entirely absent.

D-08 fix: three-branch logic at l2_health.py Check 4 mirror gate.
- Case (a) `supabase_url=""` AND `service_key=""` → NO sub-check (Supabase not
  configured at all — correct backwards-compat; tested here as
  test_both_empty_no_subcheck).
- Case (b) `supabase_url` SET but `service_key` EMPTY → register sub-check with
  status=fail, output="mirror disabled by config (service_key empty)",
  overall=fail (config mistake observable, NOT silent). Tested as
  test_url_set_key_empty_registers_fail.
- Case (c) both SET → existing pass/warn/fail age logic unchanged. Regression
  guard lives in `tests/m1-perception/test_l2_health_mirror_check.py` (tests 4-7
  cover cold-start / fresh / stale / disabled / borderline).

Pattern reference (copied verbatim where applicable):
- Header + autouse fixture: tests/m1-perception/test_l2_health_mirror_check.py
- Mock store + _build_l2_health_checks call shape: same file (Section 2)
"""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _allow_empty_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bypass the prod scan_shared_secret guard + force a clean env so prior
    pytest runs in the same process can't leak Supabase URL / key through env.

    Copied verbatim from tests/m1-perception/test_l2_health_mirror_check.py
    so the two suites stay in sync if the env-leakage guard ever needs an
    extra var added.
    """
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    for var in (
        "POLYARB_SUPABASE_URL",
        "POLYARB_SUPABASE_SERVICE_KEY",
        "POLYARB_L2_MIRROR_ENABLED",
        "POLYARB_L2_TOB_AGE_WARN_S",
        "POLYARB_L2_TOB_AGE_FAIL_S",
    ):
        monkeypatch.delenv(var, raising=False)


def _call_health_checks(settings: Any) -> tuple[dict, str]:
    """Thin wrapper: call _build_l2_health_checks with a mock store + no
    ws_consumer / event_listener (so the mirror gate is the only check that
    can produce 'fail' in these scenarios)."""
    from polyarb.http.l2_health import _build_l2_health_checks

    store = MagicMock()
    return _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=time.time()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Case (a): both empty — NO sub-check (Supabase not configured at all)
# ─────────────────────────────────────────────────────────────────────────────


def test_both_empty_no_subcheck() -> None:
    """Case (a): supabase_url=='' AND service_key=='' → mirror sub-check is
    entirely absent from .checks (backwards-compat — operator chose not to
    configure Supabase at all, so reporting fail would be a false alarm)."""
    from polyarb.config import Settings

    settings = Settings(supabase_url="", supabase_service_key="")
    # Sanity: model_validator did NOT auto-enable the mirror flag.
    assert settings.l2_mirror_enabled is False

    checks, _overall = _call_health_checks(settings)

    assert "mirror:l2_tob_age_seconds" not in checks, (
        "case (a): both empty must NOT register mirror sub-check "
        "(operator opted out — backwards-compat)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Case (b): url set, key empty — register fail (GAP-200 core fix)
# ─────────────────────────────────────────────────────────────────────────────


def test_url_set_key_empty_registers_fail() -> None:
    """Case (b): supabase_url SET but service_key EMPTY → sub-check present
    with status=fail, output mentions "service_key empty", overall=fail.

    This is the GAP-200 chain-truth requirement: a config mistake (URL
    configured but key forgotten) must be OBSERVABLE on /health, not silent.
    Inverse of the Phase 03.1 L4 lesson — fail-soft surfaces have to reach
    /health, not just logs / breadcrumbs.
    """
    from polyarb.config import Settings

    settings = Settings(
        supabase_url="https://x.supabase.co",
        supabase_service_key="",
    )
    # Sanity: model_validator did NOT auto-enable l2_mirror_enabled because
    # the AND condition requires BOTH url AND key non-empty (config.py:238).
    assert settings.l2_mirror_enabled is False, (
        "l2_mirror_enabled must remain False in case (b) — model_validator "
        "requires both url AND key. Only /health PRESENTATION changes (D-08)."
    )

    checks, overall = _call_health_checks(settings)

    assert "mirror:l2_tob_age_seconds" in checks, (
        "case (b): config-mistake must be surfaced as a /health sub-check"
    )
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "fail", (
        f"case (b): status must be 'fail' (chain-truth — operator mistake "
        f"is visible), got {entry['status']!r}"
    )
    assert entry["observedValue"] is None, (
        "case (b): no age measurement possible when mirror is disabled"
    )
    assert "service_key empty" in entry["output"], (
        f"case (b): output must name the missing field; got {entry['output']!r}"
    )
    assert overall == "fail", (
        f"case (b): overall must propagate fail (so /health → 503 alarm), "
        f"got {overall!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Case (c) regression guard — covered fully by
# tests/m1-perception/test_l2_health_mirror_check.py (tests 4-7b). We add one
# light parity check here so this file alone can demonstrate three-branch
# coverage when run in isolation.
# ─────────────────────────────────────────────────────────────────────────────


def test_both_set_existing_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    """Case (c) parity: both url+key SET → existing age sub-check runs.

    With cold-start (get_l2_tob_last_mirror_at_s returns None), the existing
    code path produces status='warn' and observedValue=None. This proves the
    three-branch refactor preserves the case-(c) body unchanged. Full
    pass/warn/fail/borderline coverage lives in the m1-perception suite.
    """
    from polyarb.config import Settings
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = Settings(
        supabase_url="https://x.supabase.co",
        supabase_service_key="some-key",
    )
    assert settings.l2_mirror_enabled is True

    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None  # cold start
    checks, _overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=time.time()
    )

    assert "mirror:l2_tob_age_seconds" in checks
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "warn", (
        f"case (c) cold-start must be 'warn' (NOT 'fail' — first boot "
        f"tolerated by design), got {entry['status']!r}"
    )
    assert entry["observedValue"] is None
