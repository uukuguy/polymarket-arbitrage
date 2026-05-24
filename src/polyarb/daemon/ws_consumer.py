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
import time
from typing import Any, Callable

from loguru import logger

from polyarb.clients.ws_market_client import stream_market_events
from polyarb.daemon.ws_watchdog import WsWatchdog


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
                self._frame_count += 1
                self._last_event_at_s = time.time()
                self._watchdog.touch()
                # Dispatch to placeholder/mirror; isolated failure must NOT crash loop
                try:
                    self._on_event(event)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"ws_consumer: on_event raised: {e!r}")
        except asyncio.CancelledError:
            # F-04: must propagate.
            logger.info("ws_consumer: cancelled, propagating CancelledError")
            self._state = "DISCONNECTED"
            raise
