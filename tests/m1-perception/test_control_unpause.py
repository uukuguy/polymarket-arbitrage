"""Tests for POST /control/unpause HMAC endpoint + scheduler.unpause() wiring.

Phase 02.1 Plan 02 — D-03 / D-04 / D-22 / T-02.1-8-01..03 / BUG-8.

Covers:
- HMAC X-Signature enforcement (missing → 401, invalid → 401, constant-time compare)
- POST /control/unpause (HMAC valid) when scheduler.state=PAUSED → 200 + state=RUNNING + counter=0
- POST /control/unpause (HMAC valid) when scheduler.state=RUNNING → 200 + status=already_running (idempotent)
- ISSUE-04 sentinel: empty-body HMAC test exercises the EXACT bytes that
  ``make unpause-prod`` sends in production (``b""``, not ``b"{}"``). Without
  this test, fixture-based unit tests can be 100% green while ``make unpause-prod``
  returns 401 in prod due to body-shape mismatch.

Wave 0 — these tests intentionally fail (RED) until Plan 02 Task 2 (control.py)
and Task 3 (app.py route registration) land.
"""
from __future__ import annotations

import hashlib
import hmac
from typing import Any

import pytest
from starlette.testclient import TestClient

from polyarb.daemon.scheduler import SchedulerState


# The test secret is defined in conftest.py as a module-private constant.
# We re-declare it here (same value) so this test file does not need to import
# from conftest — pytest's conftest is auto-loaded but its private constants
# are not part of its public surface.
_TEST_SCAN_SECRET = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"


def _install_real_unpause(scheduler: Any) -> None:
    """Wire the MagicMock scheduler's .unpause() side-effect so it mutates state.

    The conftest http_test_client uses ``MagicMock()`` as the scheduler. Without
    a side_effect, ``scheduler.unpause()`` is a no-op — so the handler's contract
    (state→RUNNING, counter→0) would be unverifiable. We patch the mock's
    ``unpause`` attribute to a callable that mutates the mock's own attributes,
    mirroring the real ``SnapshotScheduler.unpause()`` body (scheduler.py:192).
    """
    def _side_effect() -> None:
        scheduler.state = SchedulerState.RUNNING
        scheduler._failure_counter = 0

    scheduler.unpause.side_effect = _side_effect


# ---------------------------------------------------------------------------
# Tests (5 — 4 fixture-based + 1 empty-body prod-match)
# ---------------------------------------------------------------------------


def test_unpause_when_paused_transitions_to_running(
    http_test_client: TestClient,
    make_signed_request: Any,
) -> None:
    """POST /control/unpause (HMAC valid) when PAUSED → 200 + state=RUNNING + counter=0."""
    scheduler = http_test_client.app.state.scheduler
    scheduler.state = SchedulerState.PAUSED
    scheduler._failure_counter = 3
    _install_real_unpause(scheduler)

    resp = make_signed_request(http_test_client, "/control/unpause", {})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["status"] == "ok"
    assert body["state"] == "RUNNING"
    assert body["failure_counter"] == 0
    # Side-effect: scheduler state really transitioned (proves handler called unpause())
    assert scheduler.state == SchedulerState.RUNNING


def test_unpause_when_already_running_returns_200_idempotent(
    http_test_client: TestClient,
    make_signed_request: Any,
) -> None:
    """POST /control/unpause (HMAC valid) when already RUNNING → 200 + status=already_running."""
    scheduler = http_test_client.app.state.scheduler
    scheduler.state = SchedulerState.RUNNING
    scheduler._failure_counter = 0

    resp = make_signed_request(http_test_client, "/control/unpause", {})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["status"] == "already_running"
    assert body["state"] == "RUNNING"


def test_unpause_missing_hmac_returns_401(
    http_test_client: TestClient,
) -> None:
    """POST /control/unpause without X-Signature header → 401 (T-02.1-8-02)."""
    resp = http_test_client.post("/control/unpause")
    assert resp.status_code == 401
    err = resp.json().get("error", "")
    assert "X-Signature" in err or "signature" in err.lower()


def test_unpause_invalid_hmac_returns_401(
    http_test_client: TestClient,
) -> None:
    """POST /control/unpause with invalid X-Signature → 401 (T-02.1-8-01 constant-time compare)."""
    resp = http_test_client.post(
        "/control/unpause",
        content=b"{}",
        headers={"X-Signature": "sha256=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"},
    )
    assert resp.status_code == 401


def test_unpause_with_empty_body_matches_makefile_target(
    http_test_client: TestClient,
) -> None:
    """ISSUE-04 sentinel: HMAC of EMPTY body (b"") matches ``make unpause-prod``.

    ``make_signed_request({})`` sends ``b"{}"`` (2 bytes). ``make unpause-prod``
    sends ``b""`` (0 bytes, ``printf ''``). The HMAC digests differ. Tests 1-4
    use the fixture body and exercise only HMAC enforcement semantics — they
    do not catch this byte-shape gap. This test exercises the EXACT bytes the
    prod Makefile target sends so unit tests fail if the prod path would 401.
    """
    scheduler = http_test_client.app.state.scheduler
    scheduler.state = SchedulerState.PAUSED
    scheduler._failure_counter = 3
    _install_real_unpause(scheduler)

    # Build signature over EMPTY body — same as `printf '' | openssl dgst -sha256 -hmac <secret>`
    signature = hmac.new(
        _TEST_SCAN_SECRET.encode("utf-8"),
        b"",
        hashlib.sha256,
    ).hexdigest()

    resp = http_test_client.post(
        "/control/unpause",
        content=b"",
        headers={
            "X-Signature": signature,
            "Content-Length": "0",
        },
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["status"] == "ok"
    assert body["state"] == "RUNNING"
    assert body["failure_counter"] == 0
