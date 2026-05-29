"""Tests for L2 /health process:rss_kb informational sub-check.

Phase 04.1 Plan 02 — TDD RED gate. G-04 fix: measures the current Python
L2 daemon process RSS via psutil.Process() (no arg), NOT PID 1.

Invariants:
- observedValue is a positive number (kB), observedUnit == "kB"
- componentId == "l2-daemon", status == "pass" when psutil available
- Uses psutil.Process() with NO pid argument (current process = daemon)
- Fail-soft: psutil failure → status "warn" + None observedValue, no /health crash
- process:rss_kb NEVER escalates overall to "fail" (informational — D-04.4)
"""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch


def _call_build_checks(**kwargs: Any):
    """Helper: call _build_l2_health_checks with safe defaults for irrelevant args."""
    from polyarb.http.l2_health import _build_l2_health_checks

    defaults: dict[str, Any] = dict(
        store=MagicMock(),
        settings=MagicMock(
            supabase_url="",
            l2_mirror_enabled=False,
        ),
        ws_consumer=None,
        event_listener=None,
        now_s=time.time(),
    )
    defaults.update(kwargs)
    return _build_l2_health_checks(**defaults)


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: process:rss_kb present with correct shape when psutil is available
# ─────────────────────────────────────────────────────────────────────────────

def test_rss_kb_present_with_correct_shape():
    """process:rss_kb sub-check has correct shape when psutil is importable."""
    checks, _overall = _call_build_checks()

    assert "process:rss_kb" in checks, (
        "Expected 'process:rss_kb' sub-check in /health output (G-04 fix)"
    )
    entry = checks["process:rss_kb"][0]

    assert entry["componentId"] == "l2-daemon"
    assert entry["observedUnit"] == "kB"
    assert entry["status"] == "pass"
    assert isinstance(entry["observedValue"], (int, float)), (
        f"observedValue must be a number (kB), got {entry['observedValue']!r}"
    )
    assert entry["observedValue"] > 0, (
        f"observedValue must be positive, got {entry['observedValue']!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: uses psutil.Process() with NO pid argument (current-process fix)
# ─────────────────────────────────────────────────────────────────────────────

def test_rss_reads_current_process_not_pid1():
    """process:rss_kb uses psutil.Process() with no arg — current process, not PID 1."""
    import psutil

    calls_recorded: list = []

    original_process_class = psutil.Process

    class _TrackingProcess:
        """Wraps psutil.Process and records how it was constructed."""

        def __init__(self, pid=None):
            calls_recorded.append(pid)
            self._real = original_process_class()

        def memory_info(self):
            return self._real.memory_info()

    with patch("psutil.Process", _TrackingProcess):
        checks, _ = _call_build_checks()

    assert "process:rss_kb" in checks
    assert len(calls_recorded) >= 1, "psutil.Process must be instantiated"
    assert calls_recorded[0] is None, (
        "psutil.Process() MUST be called with no pid arg (None = current process, "
        f"not PID 1). Got: {calls_recorded}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: fail-soft — psutil import/read failure degrades to warn + None
# ─────────────────────────────────────────────────────────────────────────────

def test_rss_fail_soft_when_psutil_raises():
    """If psutil read raises, sub-check degrades to status='warn', observedValue=None."""

    class _BrokenProcess:
        def __init__(self, pid=None):
            raise RuntimeError("psutil intentionally broken for test")

    with patch("psutil.Process", _BrokenProcess):
        checks, overall = _call_build_checks()

    assert "process:rss_kb" in checks
    entry = checks["process:rss_kb"][0]
    assert entry["status"] == "warn", (
        f"Expected 'warn' on psutil failure, got {entry['status']!r}"
    )
    assert entry["observedValue"] is None, (
        f"Expected None observedValue on failure, got {entry['observedValue']!r}"
    )
    # /health must not crash — overall is still a valid status
    assert overall in ("pass", "warn", "fail"), f"overall must be valid status, got {overall!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: process:rss_kb NEVER escalates overall to "fail" (informational)
# ─────────────────────────────────────────────────────────────────────────────

def test_rss_warn_does_not_escalate_overall_to_fail():
    """Even when rss sub-check returns warn (fail-soft path), overall must not become 'fail'."""

    class _BrokenProcess:
        def __init__(self, pid=None):
            raise RuntimeError("psutil broken")

    # Use a settings mock that disables all other sub-checks that could contribute fail.
    settings = MagicMock()
    settings.supabase_url = ""
    settings.l2_mirror_enabled = False

    with patch("psutil.Process", _BrokenProcess):
        checks, overall = _call_build_checks(
            ws_consumer=None,
            event_listener=None,
            settings=settings,
        )

    rss_status = checks.get("process:rss_kb", [{}])[0].get("status", "absent")
    assert rss_status == "warn", f"Expected warn from fail-soft, got {rss_status!r}"
    # The rss warn must NOT have driven overall to "fail"
    assert overall != "fail", (
        "process:rss_kb status 'warn' must NEVER escalate overall to 'fail' "
        f"(D-04.4 informational constraint). overall={overall!r}"
    )
