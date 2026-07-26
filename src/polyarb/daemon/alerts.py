"""Alerts: scheduler-paused notifications + Better Stack heartbeat + Telegram fallback.

Plan 02-05 — D-16 (Better Stack heartbeat) + D-17 (Telegram via Better Stack with
direct fallback for outage scenarios).

Normal alert path (D-17):
    daemon error → sentry_sdk.capture_message
                 → POST {better_stack_heartbeat_url}/fail
                       → Better Stack routes to Telegram channel via its
                         native integration

Fallback path (Better Stack itself unreachable):
    Better Stack returns 5xx OR network error
    → POST api.telegram.org/bot<token>/sendMessage directly

Dedup: a paused-alert fired twice within ``alert_dedupe_window_seconds`` counts
as one. Prevents alert storm when a flaky network keeps tripping the 5-failure
threshold every few minutes.

Module-level state ``_LAST_ALERT_TIME_MS`` is intentional — alerts are
process-global, and the daemon is a single process. Tests clear it between
runs (``alerts._LAST_ALERT_TIME_MS.clear()``).
"""

from __future__ import annotations

import time

import httpx
import sentry_sdk
from loguru import logger

from polyarb.config import Settings

# ---------------------------------------------------------------------------
# Module-level dedup state
# ---------------------------------------------------------------------------

# Maps alert-key → unix-ms timestamp of the most recent emission.
# Tests reset this with ``alerts._LAST_ALERT_TIME_MS.clear()`` to avoid bleeding
# between cases. Production never resets it; the process restart implicitly
# resets it (which is the right behaviour — a restart is a real signal).
_LAST_ALERT_TIME_MS: dict[str, int] = {}


def _is_deduped(key: str, window_seconds: int) -> bool:
    """True if `key` fired within the last ``window_seconds`` (and we should suppress)."""
    now_ms = int(time.time() * 1000)
    last = _LAST_ALERT_TIME_MS.get(key)
    if last is not None and (now_ms - last) < window_seconds * 1000:
        return True
    _LAST_ALERT_TIME_MS[key] = now_ms
    return False


# ---------------------------------------------------------------------------
# Public alert entry points
# ---------------------------------------------------------------------------


async def send_paused_alert(settings: Settings, *, reason: str) -> None:
    """Fire a scheduler-paused alert across all configured channels.

    Channels (all fire unconditionally — 2026-05-19 fix after chaos Inj 1
    revealed that Better Stack `/fail` returning 200 does NOT guarantee the
    user receives a notification — it only acknowledges the signal):

      1. Sentry capture_message(level="error") — long-lived audit trail
         (whether Sentry delivers email depends on Sentry alert rule, not us).
      2. Better Stack /fail endpoint — signals heartbeat downtime to Better
         Stack (whether Better Stack routes to email/Telegram depends on its
         on-call config, not on the POST status code).
      3. Direct Telegram — unconditional primary user-facing channel. Proven
         working in chaos Inj 1; this is the path that 100% reaches the
         operator's phone regardless of Sentry/BS configuration drift.

    Deduplication: a second call within ``settings.alert_dedupe_window_seconds``
    is a no-op.
    """
    key = "scheduler-paused"
    if _is_deduped(key, settings.alert_dedupe_window_seconds):
        logger.debug(f"alert {key} within dedup window; suppressing")
        return

    text = f"scheduler paused: {reason}"
    logger.error(f"ALERT: {text}")

    # (1) Sentry — audit trail + Sentry-side Slack/email routes
    try:
        sentry_sdk.capture_message(text, level="error")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"sentry capture_message failed: {e!r}")

    # (2) Better Stack /fail — best-effort signal; 200 ≠ user notified
    await _better_stack_fail(settings, reason=reason)

    # (3) Telegram direct — unconditional primary path
    if settings.telegram_bot_token.get_secret_value():
        await _telegram_direct(settings, text=f"polyarb-l1 scheduler PAUSED: {reason}")


async def send_heartbeat_ok(settings: Settings) -> None:
    """Ping the Better Stack heartbeat URL to confirm "I am alive".

    Better Stack heartbeat monitors expect a GET to the heartbeat URL within
    their configured interval (e.g. every 5 min). Missing one triggers an
    alert on their side.

    Fail-soft: if the GET errors, log a warning but do NOT raise — heartbeat
    failure is itself an availability signal handled by Better Stack's
    "missed beat" alarm, not by the daemon.
    """
    if not settings.better_stack_heartbeat_url:
        return
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.get(settings.better_stack_heartbeat_url)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"heartbeat send failed: {e!r}")


# ---------------------------------------------------------------------------
# Channel internals
# ---------------------------------------------------------------------------


async def _better_stack_fail(settings: Settings, *, reason: str) -> bool:
    """POST to {better_stack_heartbeat_url}/fail with the reason.

    Returns True if the response code is < 500 (Better Stack accepted the
    signal), False on 5xx or network error (caller should try fallback).
    """
    if not settings.better_stack_heartbeat_url:
        return False
    fail_url = settings.better_stack_heartbeat_url.rstrip("/") + "/fail"
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.post(fail_url, json={"reason": reason})
            return resp.status_code < 500
        except Exception as e:  # noqa: BLE001
            logger.error(f"Better Stack /fail POST failed: {e!r}")
            return False


async def _telegram_direct(settings: Settings, *, text: str) -> None:
    """POST to api.telegram.org/bot<token>/sendMessage as a last-resort fallback.

    Skipped (no-op) if telegram_bot_token or telegram_chat_id are empty —
    these are optional. The fail-soft contract: if the daemon can't reach
    Telegram either, we've already done what we can; the daemon process
    crash + Fly auto-restart is the final layer.
    """
    token = settings.telegram_bot_token.get_secret_value()
    chat_id = settings.telegram_chat_id
    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(url, json=payload)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Telegram direct send failed: {e!r}")
