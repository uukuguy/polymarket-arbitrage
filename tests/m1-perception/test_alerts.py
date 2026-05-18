"""Tests for polyarb.daemon.alerts — paused alert + heartbeat + dedup + Telegram fallback.

Plan 02-05 — D-16 (Better Stack heartbeat) + D-17 (Telegram via Better Stack with direct fallback).

Coverage:
- send_paused_alert posts to Better Stack /fail endpoint
- send_paused_alert also calls sentry_sdk.capture_message(level="error")
- Better Stack 503 → direct Telegram fallback
- send_heartbeat_ok posts to Better Stack heartbeat OK endpoint
- alert dedup window suppresses repeat within window
- scheduler PAUSED transition invokes alerts.send_paused_alert
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest


# ---------------------------------------------------------------------------
# send_paused_alert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_paused_alert_calls_better_stack_heartbeat_fail(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """send_paused_alert posts to settings.better_stack_heartbeat_url + '/fail'."""
    from polyarb.daemon import alerts

    # Reset module-level dedup state so concurrent tests do not bleed
    alerts._LAST_ALERT_TIME_MS.clear()

    await alerts.send_paused_alert(
        daemon_settings_with_observability,
        reason="3 consecutive FAILED snapshots",
    )

    # Better Stack /fail endpoint received a POST
    fail_calls = [c for c in mocked_better_stack.calls if c[0] == "POST" and "/fail" in c[1]]
    assert fail_calls, (
        f"send_paused_alert did not POST to Better Stack /fail: "
        f"calls={mocked_better_stack.calls}"
    )


@pytest.mark.asyncio
async def test_send_paused_alert_also_captures_to_sentry(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """send_paused_alert also calls sentry_sdk.capture_message(level='error')."""
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    await alerts.send_paused_alert(
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
async def test_send_paused_alert_falls_back_to_telegram_direct_when_better_stack_503(
    daemon_settings_with_observability: Any,
    mocked_better_stack: Any,
    mocked_sentry: Any,
) -> None:
    """Better Stack returns 503 → alerts use telegram_bot_token + chat_id direct."""
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    # Override Better Stack /fail to return 503
    mocked_better_stack.set_response("POST", "betterstack.com", 503)

    await alerts.send_paused_alert(
        daemon_settings_with_observability,
        reason="testing fallback path",
    )

    # Find a POST to api.telegram.org/bot.../sendMessage
    telegram_calls = [
        c for c in mocked_better_stack.calls
        if c[0] == "POST" and "api.telegram.org" in c[1]
    ]
    assert telegram_calls, (
        f"Better Stack 503 should trigger direct Telegram fallback. "
        f"observed calls={mocked_better_stack.calls}"
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
        c for c in mocked_better_stack.calls
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
    """send_paused_alert called twice within the dedup window → second call is no-op.

    First call posts to Better Stack + Sentry. Second call within window does nothing.
    """
    from polyarb.daemon import alerts

    alerts._LAST_ALERT_TIME_MS.clear()

    # First call: should reach both Better Stack and Sentry
    await alerts.send_paused_alert(
        daemon_settings_with_observability,
        reason="first call",
    )
    first_call_count = mocked_sentry.capture_message.call_count
    first_bs_count = len(
        [c for c in mocked_better_stack.calls if "/fail" in c[1]]
    )

    # Second call within window: should be deduped
    await alerts.send_paused_alert(
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
# Integration: scheduler._on_paused → alerts.send_paused_alert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_paused_invokes_alerts(
    daemon_settings_with_observability: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When scheduler hits 3 consecutive failures, _on_paused → alerts.send_paused_alert."""
    from polyarb.daemon import alerts
    from polyarb.daemon.scheduler import SchedulerState, SnapshotScheduler
    from polyarb.storage.sqlite_store import SQLiteStore

    alerts._LAST_ALERT_TIME_MS.clear()

    store = SQLiteStore(daemon_settings_with_observability.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(
        settings=daemon_settings_with_observability, sqlite_store=store
    )

    # Replace alerts.send_paused_alert with a counting AsyncMock
    send_mock = AsyncMock()
    monkeypatch.setattr(alerts, "send_paused_alert", send_mock)

    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    for _ in range(3):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.PAUSED
    send_mock.assert_called()
    # Verify the call signature passes settings + reason kwarg
    call_kwargs = send_mock.call_args.kwargs
    assert "reason" in call_kwargs, (
        f"_on_paused should pass reason=, got call: {send_mock.call_args}"
    )
