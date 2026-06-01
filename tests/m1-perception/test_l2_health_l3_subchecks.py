"""Tests for Phase 05 Plan 04 — /health 3 new L3 sub-checks (chain-truth).

Chain-truth contract (CLAUDE.md §chain-truth + Phase 04 D-08, Inj L2-2 RCA):

  l3:active_count                  ← l3_promote.get_l3_active_count()
  l3:last_promote_at_s             ← l3_promote.get_last_promote_at_s()
  l3:last_book_levels_write_at_s   ← l3_promote.get_last_book_levels_write_at_s()

Each getter reads a module-level field that the WRITE side really mutates
(promote_run success path + L2SupabaseMirror.push_book_levels success
path). NO config-flag gating between mutation and surface.

active_count uses L3_EXPECTED_TOKEN_COUNT = 10 (revision-1 strict — D-05
N=5 markets × 2 Yes+No tokens). active_count < 10 → status=warn,
informational only (does NOT bump overall). last_promote_at_s + book_levels
DO bump overall when stale.
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


HEALTH_MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "polyarb"
    / "http"
    / "l2_health.py"
)


@pytest.fixture(autouse=True)
def _reset_l3_promote_state() -> Any:
    """Reset module-level state between tests."""
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = set()
    l3_promote._last_promote_at_s = None
    l3_promote._last_book_levels_write_at_s = None
    if hasattr(l3_promote, "_last_known_tob_rows"):
        l3_promote._last_known_tob_rows = None
    if hasattr(l3_promote, "_last_known_market_token_map"):
        l3_promote._last_known_market_token_map = None
    yield


def _make_settings() -> Any:
    """Minimal Settings for /health builder."""
    from polyarb.config import Settings

    return Settings()


def _call_build_checks(now_s: float) -> tuple[dict, str]:
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _make_settings()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None
    return _build_l2_health_checks(
        store,
        settings,
        ws_consumer=None,
        event_listener=None,
        now_s=now_s,
    )


# ─────────────────────────────────────────────────────────────────────────
# l3:active_count
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_active_count_cold_start_warn() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = set()
    checks, _overall = _call_build_checks(time.time())
    assert "l3:active_count" in checks
    entry = checks["l3:active_count"][0]
    assert entry["status"] == "warn"
    assert "0/10" in entry["output"]
    assert entry["observedValue"] == 0


def test_health_l3_active_count_full_pass_at_10_tokens() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {f"t{i}" for i in range(10)}
    checks, _ = _call_build_checks(time.time())
    entry = checks["l3:active_count"][0]
    assert entry["status"] == "pass"
    assert entry["observedValue"] == 10


def test_health_l3_active_count_under_filled_warn_at_9_tokens() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {f"t{i}" for i in range(9)}
    checks, _ = _call_build_checks(time.time())
    entry = checks["l3:active_count"][0]
    assert entry["status"] == "warn"
    assert "9/10" in entry["output"]


def test_health_l3_active_count_does_not_bump_overall_when_under_filled() -> None:
    """Informational-only — active_count < 10 must NOT bump overall to warn
    purely on its own. (Other sub-checks may still warn — we test that this
    specific sub-check is not the cause.)"""
    from polyarb.observation import l3_promote

    # Set last_promote_at + last_book_levels_at to FRESH so those sub-checks
    # are pass; if `overall` bumps, it must be l3:active_count's fault.
    now = time.time()
    l3_promote._l3_active_set = set()  # 0 tokens — under-filled
    l3_promote._last_promote_at_s = now - 5  # fresh
    l3_promote._last_book_levels_write_at_s = now - 5  # fresh

    checks, overall = _call_build_checks(now)
    assert checks["l3:active_count"][0]["status"] == "warn"
    assert checks["l3:last_promote_at_s"][0]["status"] == "pass"
    assert checks["l3:last_book_levels_write_at_s"][0]["status"] == "pass"
    # Other unrelated sub-checks (ws/event_bus = not configured = warn) may
    # still bump overall; but the l3 trio alone should NOT cause `fail`.
    assert overall != "fail", f"l3 trio must not propagate fail; got overall={overall}"


# ─────────────────────────────────────────────────────────────────────────
# l3:last_promote_at_s
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_last_promote_cold_start_warn_but_overall_not_fail() -> None:
    from polyarb.observation import l3_promote

    l3_promote._last_promote_at_s = None
    l3_promote._last_book_levels_write_at_s = time.time() - 5  # fresh
    checks, overall = _call_build_checks(time.time())
    entry = checks["l3:last_promote_at_s"][0]
    assert entry["status"] == "warn"
    assert "cold-start" in entry["output"]
    assert overall != "fail"


def test_health_l3_last_promote_fresh_pass() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_promote_at_s = now - 60
    l3_promote._last_book_levels_write_at_s = now - 5  # fresh
    checks, _ = _call_build_checks(now)
    entry = checks["l3:last_promote_at_s"][0]
    assert entry["status"] == "pass", f"got {entry['status']}: {entry['output']}"


def test_health_l3_last_promote_stale_fail() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_promote_at_s = now - 2000  # > 1800s = fail
    l3_promote._last_book_levels_write_at_s = now - 5  # fresh
    checks, overall = _call_build_checks(now)
    entry = checks["l3:last_promote_at_s"][0]
    assert entry["status"] == "fail", f"got {entry['status']}: {entry['output']}"
    assert overall == "fail", f"stale promote must propagate fail; got {overall}"


def test_health_l3_last_promote_borderline_warn() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_promote_at_s = now - 700  # > 600s warn, < 1800s fail
    l3_promote._last_book_levels_write_at_s = now - 5
    checks, _ = _call_build_checks(now)
    assert checks["l3:last_promote_at_s"][0]["status"] == "warn"


# ─────────────────────────────────────────────────────────────────────────
# l3:last_book_levels_write_at_s
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_book_levels_cold_start_warn() -> None:
    from polyarb.observation import l3_promote

    l3_promote._last_book_levels_write_at_s = None
    l3_promote._last_promote_at_s = time.time() - 5
    checks, _ = _call_build_checks(time.time())
    entry = checks["l3:last_book_levels_write_at_s"][0]
    assert entry["status"] == "warn"
    assert "cold-start" in entry["output"]


def test_health_l3_book_levels_stale_fail() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_book_levels_write_at_s = now - 1000  # > 600 fail
    l3_promote._last_promote_at_s = now - 5  # fresh so it isn't the cause
    checks, overall = _call_build_checks(now)
    entry = checks["l3:last_book_levels_write_at_s"][0]
    assert entry["status"] == "fail"
    assert overall == "fail", f"stale book_levels must propagate fail; got {overall}"


# ─────────────────────────────────────────────────────────────────────────
# Chain-truth — no config-flag gating (Inj L2-2 RCA)
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_subchecks_chain_truth_no_config_gate() -> None:
    """Lint test — within the L3 sub-check block in l2_health.py, there must
    be NO `getattr(settings, "l3_enabled"` or `settings.l3_*_enabled` gate.

    The sub-checks read getters that the WRITE side mutates — never a
    config flag (CLAUDE.md chain-truth + Phase 04 D-08 / GAP-200 RCA).
    """
    text = HEALTH_MODULE_PATH.read_text()

    # Find the L3 sub-check region (between the L3 D-08 marker comment and
    # the `return checks, overall` line). All three checks must live here.
    region_start = text.find('# ── Phase 05 Plan 04 D-08')
    assert region_start >= 0, "L3 sub-check region marker missing"
    region_end = text.find("return checks, overall", region_start)
    assert region_end >= 0
    region = text[region_start:region_end]

    # Each sub-check name present in the region.
    for name in (
        '"l3:active_count"',
        '"l3:last_promote_at_s"',
        '"l3:last_book_levels_write_at_s"',
    ):
        assert name in region, f"sub-check {name} missing from L3 region"

    # No config-flag gate in the region. Specifically reject:
    #   - getattr(settings, "l3_..._enabled"
    #   - settings.l3_..._enabled
    forbidden_patterns = [
        r'getattr\(\s*settings\s*,\s*["\']l3[_a-z]*enabled["\']',
        r'settings\.l3[_a-z]*enabled',
    ]
    for pat in forbidden_patterns:
        m = re.search(pat, region)
        assert m is None, (
            f"L3 region must not gate on a config flag; matched pattern "
            f"{pat!r}: {m.group(0) if m else None}"
        )


def test_health_l3_subchecks_expected_token_constant_is_10() -> None:
    """Revision 1 strict — L3_EXPECTED_TOKEN_COUNT must be exactly 10
    (D-05 N=5 markets × 2 tokens, not the looser '>=5' relaxation)."""
    text = HEALTH_MODULE_PATH.read_text()
    assert "L3_EXPECTED_TOKEN_COUNT = 10" in text


def test_health_l3_subchecks_use_chain_truth_getters() -> None:
    """The L3 region must call the three getters directly, not read the
    module attributes — getters are the chain-truth interface."""
    text = HEALTH_MODULE_PATH.read_text()
    region_start = text.find('# ── Phase 05 Plan 04 D-08')
    region_end = text.find("return checks, overall", region_start)
    region = text[region_start:region_end]

    for getter in (
        "get_l3_active_count",
        "get_last_promote_at_s",
        "get_last_book_levels_write_at_s",
    ):
        assert getter in region, f"chain-truth getter {getter} not called in L3 region"
