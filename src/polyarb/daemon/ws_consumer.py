"""WsConsumer — wires stream_market_events + WsWatchdog + on_event callback.

Plan 03-04 D-02 / D-03. Replaces Plan 03's Mock-shaped placeholder in
``l2_main.py``. The on_event callback is a placeholder until Plan 06
wires the real Supabase mirror dispatch.

Public surface (read by health endpoint via ``app.state.ws_consumer``):
- ``current_state``: "DISCONNECTED" pre-run / delegates to watchdog once running
- ``last_event_at_s``: epoch float of the last received frame
- ``subscribed_assets``: defensive copy of the configured asset_ids
- ``frame_count``: total frames received since start

Phase 02 F-04: ``run(stop_event)`` propagates ``asyncio.CancelledError``.
T-03-04-01: on_event callback failures are logged at warning but never
crash the consume loop (only the placeholder dispatches downstream).
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable

from loguru import logger

from polyarb.clients.ws_market_client import stream_market_events
from polyarb.daemon.ws_watchdog import WsWatchdog


# ── Phase 03.1-06 D-04 / Phase 04.1 G-03: POLYARB_WS_TEST_KILL chaos primitive ─
#
# Used by `make chaos-l2-inj4` to drop the WS connection mid-stream WITHOUT
# OS-level pkill (which doesn't exist in python:3.12-slim base — Phase 03 L2-1
# lesson, see feedback_container-image-aware-chaos-2026-05).
#
# Phase 04.1 G-03 REDESIGN — process-local flag (in-band endpoint):
#   The old approach used `flyctl secrets set POLYARB_WS_TEST_KILL=1` to flip the
#   flag, which RESTARTS the Fly machine — killing the 60-asset pre-storm process
#   and making Pitfall 4 (watchdog false-trip on healthy long-lived process)
#   unobservable (04-SOAK-LOG §G-03).
#
#   Fix: a PROCESS-LOCAL boolean flag (_ws_test_kill_flag) replaces the direct
#   os.getenv read. The flag is seeded from env at import (cold-start compat for
#   backward compat with the old flyctl secrets approach), but can be FLIPPED AT
#   RUNTIME without a restart via the HMAC-gated POST /control/chaos/ws-test-kill
#   endpoint (l2_control.py). This way, the 60-asset process SURVIVES the storm.
#
# PROD SAFETY CONTRACT (CI-enforced by
# tests/m1-perception/test_ws_test_kill_flag.py::test_prod_fly_toml_never_sets_test_kill_flag):
#   - fly.toml MUST NOT contain POLYARB_WS_TEST_KILL
#   - fly-l2.toml MUST NOT contain POLYARB_WS_TEST_KILL
#
# The flag surfaces to /health as chaos:ws_test_kill_flag sub-check (l2_health.py)
# reading get_ws_test_kill() — NOT os.getenv — so an in-flight toggle is visible
# without a restart (chain-truth, feedback_code-vs-chain-truth-2026-05).

# Module-level mutable flag. Seeded from env at import (cold-start compat:
# `flyctl secrets set POLYARB_WS_TEST_KILL=1` before deploy still works).
# Runtime value is controlled by set_ws_test_kill() via the HTTP endpoint.
# Opt-in seed: only the literal string "1" triggers (same semantics as before).
_ws_test_kill_flag: bool = (os.getenv("POLYARB_WS_TEST_KILL") == "1")


def set_ws_test_kill(enabled: bool) -> None:
    """Flip the process-local WS-kill chaos flag (no restart needed).

    Called by the HMAC-gated POST /control/chaos/ws-test-kill endpoint
    (l2_control.py) to enable or disable the chaos primitive on a RUNNING
    process. Also used by tests for isolation.
    """
    global _ws_test_kill_flag
    _ws_test_kill_flag = bool(enabled)


def get_ws_test_kill() -> bool:
    """Return the current process-local WS-kill flag.

    Used by /health chaos:ws_test_kill_flag sub-check (l2_health.py) to
    surface the in-flight flag value — not os.getenv — so a runtime toggle
    via the endpoint is immediately visible in /health (chain-truth).
    """
    return _ws_test_kill_flag


class WsTestKillRequested(Exception):
    """Raised when the chaos kill flag forces a synthetic WS close.

    Chaos-only. Caught by the consumer loop and re-raised so the watchdog's
    reconnect path runs as if the WS had naturally dropped. NEVER set this
    flag in production — Phase 03.1-03 chaos-toolkit + CLAUDE.md document
    the prod safety contract.
    """


def _check_ws_test_kill() -> None:
    """Check the process-local chaos kill flag — raise WsTestKillRequested when True.

    Phase 04.1 G-03: reads the MODULE-LEVEL _ws_test_kill_flag (NOT os.getenv).
    The flag is seeded from env at import (cold-start compat) but is flipped at
    runtime via set_ws_test_kill() — the endpoint sets it without restarting the
    process. Returns None when False (the no-op fast path, normal prod).
    """
    if _ws_test_kill_flag:
        raise WsTestKillRequested(
            "WS test-kill flag set — synthetic WS close (Phase 04.1 G-03 in-band endpoint)"
        )


class WsConsumer:
    """Wires the WS data plane: stream → watchdog.touch() → on_event."""

    def __init__(
        self,
        *,
        settings: Any,
        watchdog: WsWatchdog,
        on_event: Callable[[dict], None],
        initial_assets: list[str] | None = None,
    ) -> None:
        self._settings = settings
        self._watchdog = watchdog
        self._on_event = on_event
        self._subscribed_assets: list[str] = list(initial_assets or [])
        self._state: str = "DISCONNECTED"
        self._last_event_at_s: float = time.time()
        self._frame_count: int = 0
        # Phase 04 Plan 04 D-06 indicator 1 — frames RECEIVED but downstream
        # on_event dispatch raised. Distinct from frame_count: a frame may be
        # received successfully (frame_count += 1) yet fail to process
        # (dropped_frame_count += 1). Surfaces operational throughput health
        # during the prod chaos run (`make chaos-l2-inj4-throughput`).
        self._dropped_frame_count: int = 0

    # ── Properties (read by health endpoint) ───────────────────────────────

    @property
    def current_state(self) -> str:
        """Pre-run: DISCONNECTED. Once running: delegates to watchdog."""
        if self._state == "DISCONNECTED":
            return "DISCONNECTED"
        return self._watchdog.current_state

    @property
    def last_event_at_s(self) -> float:
        return self._last_event_at_s

    @property
    def subscribed_assets(self) -> list[str]:
        # Defensive copy — Plan 05 candidate refresh MUST NOT mutate via property
        return list(self._subscribed_assets)

    @property
    def frame_count(self) -> int:
        return self._frame_count

    @property
    def dropped_frame_count(self) -> int:
        """Frames RECEIVED but whose on_event callback raised.

        Phase 04 Plan 04 D-06 indicator 1. A non-zero value during prod
        throughput chaos means downstream dispatch is failing while WS
        delivery is healthy — read alongside frame_count for context.
        """
        return self._dropped_frame_count

    # ── Main loop ──────────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        """Consume stream_market_events until stop_event fires.

        Phase 02 F-04 invariant: CancelledError propagates.
        """
        try:
            # Wait until subscribed_assets is non-empty (Plan 05 may populate
            # later via candidate_refresh). Re-check periodically.
            while not self._subscribed_assets and not stop_event.is_set():
                logger.warning(
                    "ws_consumer: subscribed_assets is empty — waiting for "
                    "Plan 05 candidate_refresh to populate"
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue

            if stop_event.is_set():
                return

            self._state = "CONNECTED"
            logger.info(
                f"ws_consumer: starting consume loop with "
                f"{len(self._subscribed_assets)} subscribed assets"
            )

            async for event in stream_market_events(
                self._subscribed_assets, initial_dump=True
            ):
                if stop_event.is_set():
                    break
                # Phase 03.1-06 D-04: chaos kill check BEFORE business logic.
                # When POLYARB_WS_TEST_KILL=1, raise to trigger watchdog reconnect.
                # Lets it propagate out of the async for — stream_market_events'
                # context manager will close the WS, then run() returns and the
                # outer task supervisor (l2_main) restarts the consumer.
                _check_ws_test_kill()
                self._frame_count += 1
                self._last_event_at_s = time.time()
                self._watchdog.touch()
                # Dispatch to placeholder/mirror; isolated failure must NOT crash loop
                try:
                    self._on_event(event)
                except Exception as e:  # noqa: BLE001
                    # Phase 04 Plan 04 D-06 indicator 1: count the drop so the
                    # throughput chaos run has a numeric signal beyond log-grep.
                    self._dropped_frame_count += 1
                    logger.warning(f"ws_consumer: on_event raised: {e!r}")
        except WsTestKillRequested as e:
            # Phase 03.1-06 D-04: synthetic close via chaos flag. Log loudly
            # so chaos runs are visible in flyctl logs grep.
            logger.warning(
                f"ws_consumer: {e} — closing WS for chaos test (this MUST NOT appear in production)"
            )
            self._state = "DISCONNECTED"
            # Do NOT re-raise: returning lets the supervisor (l2_main) decide
            # whether to relaunch the consumer. Watchdog will mark RECONNECTING
            # via its stale_s timeout if no fresh touch arrives.
            return
        except asyncio.CancelledError:
            # F-04: must propagate.
            logger.info("ws_consumer: cancelled, propagating CancelledError")
            self._state = "DISCONNECTED"
            raise
