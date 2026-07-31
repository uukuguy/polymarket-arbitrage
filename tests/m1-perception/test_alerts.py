"""Tests for recovery alerts, heartbeat, dedup, and Telegram fallback.

Plan 02-05 — D-16 (Better Stack heartbeat) + D-17 (Telegram via Better Stack with direct fallback).

Coverage:
- send_recovering_alert posts to Better Stack /fail endpoint
- send_recovering_alert also calls sentry_sdk.capture_message(level="error")
- Better Stack 503 → direct Telegram fallback
- send_heartbeat_ok posts to Better Stack heartbeat OK endpoint
- alert dedup window suppresses repeat within window
- scheduler RECOVERING transition invokes alerts.send_recovering_alert
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

# ---------------------------------------------------------------------------
# send_recovering_alert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_recovering_alert_calls_better_stack_heartbeat_fail(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """Recovery alerts post to settings.better_stack_heartbeat_url + '/fail'."""
    from polyarb.daemon import alerts

    # Reset module-level dedup state so concurrent tests do not bleed
    alerts._LAST_ALERT_TIME_MS.clear()

    await alerts.send_recovering_alert(
        daemon_settings_with_observability,
        reason="3 consecutive FAILED snapshots",
    )

    # Better Stack /fail endpoint received a POST
    fail_calls = [c for c in mocked_better_stack.calls if c[0] == "POST" and "/fail" in c[1]]
    assert fail_calls, (
        f"recovery alert did not POST to Better Stack /fail: calls={mocked_better_stack.calls}"
    )


@pytest.mark.asyncio
async def test_send_recovering_alert_also_captures_to_sentry(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """Recovery alerts also call sentry_sdk.capture_message(level='error')."""
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    await alerts.send_recovering_alert(
        daemon_settings_with_observability,
        reason="3 consecutive FAILED snapshots",
    )

    assert mocked_sentry.capture_message.call_count >= 1
    # level must be error
    call_kwargs = mocked_sentry.capture_message.call_args.kwargs
    assert call_kwargs.get("level") == "error", (
        f"expected level=error, got {call_kwargs.get('level')!r}"
    )


@pytest.mark.asyncio
async def test_send_recovering_alert_calls_telegram_direct_when_better_stack_503(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """Better Stack returns 503 → Telegram direct still fires (now unconditional)."""
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    # Override Better Stack /fail to return 503
    mocked_better_stack.set_response("POST", "betterstack.com", 503)

    await alerts.send_recovering_alert(
        daemon_settings_with_observability,
        reason="testing with BS 503",
    )

    telegram_calls = [
        c for c in mocked_better_stack.calls if c[0] == "POST" and "api.telegram.org" in c[1]
    ]
    assert telegram_calls, (
        f"Telegram direct must fire on BS 503. observed={mocked_better_stack.calls}"
    )


@pytest.mark.asyncio
async def test_send_recovering_alert_calls_telegram_direct_when_better_stack_200(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """2026-05-19 contract change: Telegram direct fires unconditionally.

    Pre-fix: BS /fail returning 200 was treated as "user notified" → Telegram
    direct was suppressed. Chaos Inj 1 (2026-05-19) revealed BS 200 only
    means "signal accepted", not "user reached". Telegram direct is now the
    unconditional primary user-facing path.
    """
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    # Better Stack /fail returns 200 (the SAD path before this fix: TG was skipped)
    mocked_better_stack.set_response("POST", "betterstack.com", 200)

    await alerts.send_recovering_alert(
        daemon_settings_with_observability,
        reason="testing with BS 200",
    )

    telegram_calls = [
        c for c in mocked_better_stack.calls if c[0] == "POST" and "api.telegram.org" in c[1]
    ]
    assert telegram_calls, (
        f"Telegram direct MUST fire even when BS returns 200 (2026-05-19 contract). "
        f"observed={mocked_better_stack.calls}"
    )


@pytest.mark.asyncio
async def test_send_heartbeat_ok(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
) -> None:
    """send_heartbeat_ok GETs the Better Stack heartbeat URL."""
    from polyarb.daemon import alerts

    await alerts.send_heartbeat_ok(daemon_settings_with_observability)

    heartbeat_calls = [
        c
        for c in mocked_better_stack.calls
        if "uptime.betterstack.com" in c[1] and "/fail" not in c[1]
    ]
    assert heartbeat_calls, (
        f"send_heartbeat_ok did not call heartbeat URL: {mocked_better_stack.calls}"
    )


@pytest.mark.asyncio
async def test_alert_deduplication(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """Recovery alert called twice within the dedup window → second call is no-op.

    First call posts to Better Stack + Sentry. Second call within window does nothing.
    """
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    # First call: should reach both Better Stack and Sentry
    await alerts.send_recovering_alert(
        daemon_settings_with_observability,
        reason="first call",
    )
    first_call_count = mocked_sentry.capture_message.call_count
    first_bs_count = len([c for c in mocked_better_stack.calls if "/fail" in c[1]])

    # Second call within window: should be deduped
    await alerts.send_recovering_alert(
        daemon_settings_with_observability,
        reason="second call (within 5min)",
    )

    assert mocked_sentry.capture_message.call_count == first_call_count, (
        "second call within dedup window should NOT re-capture to Sentry"
    )
    bs_count_now = len([c for c in mocked_better_stack.calls if "/fail" in c[1]])
    assert bs_count_now == first_bs_count, (
        "second call within dedup window should NOT re-post to Better Stack"
    )


# ---------------------------------------------------------------------------
# Integration: scheduler._on_recovering → alerts.send_recovering_alert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_recovering_invokes_alerts(
    daemon_settings_with_observability: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify FAILURE_THRESHOLD consecutive failures call the recovery alert.

    Phase 03.1-04 D-02: threshold is 5 (was 3). Drive the loop off the class
    attribute so future tuning doesn't drift this assertion.
    """
    from polyarb.daemon import alerts
    from polyarb.daemon.scheduler import SchedulerState, SnapshotScheduler
    from polyarb.storage.sqlite_store import SQLiteStore

    alerts._LAST_ALERT_TIME_MS.clear()

    store = SQLiteStore(daemon_settings_with_observability.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_with_observability, sqlite_store=store)

    # Replace alerts.send_recovering_alert with a counting AsyncMock
    send_mock = AsyncMock()
    monkeypatch.setattr(alerts, "send_recovering_alert", send_mock)

    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    for _ in range(SnapshotScheduler.FAILURE_THRESHOLD):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.RECOVERING
    send_mock.assert_called()
    # Verify the call signature passes settings + reason kwarg
    call_kwargs = send_mock.call_args.kwargs
    assert "reason" in call_kwargs, (
        f"_on_recovering should pass reason=, got call: {send_mock.call_args}"
    )
