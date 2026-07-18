"""WsWatchdog — 30s WS silence detector + reconnect state machine.

Plan 03-04 D-03 (stale_s=30 LOCKED) + R5 mitigation (reconnect storm cap).

State machine:
    CONNECTED ── touch() ──► WAITING_FOR_EVENT ── elapsed > 30s ──► RECONNECTING
                                  ▲                                       │
                                  └──── touch() ──────────────────────────┘
                                                                          │
                                                       >10 reconnects/hour
                                                                          ▼
                                                              DEGRADED_REST_POLLING

Hard locks (do NOT relax without a D-XX amendment):
- ``stale_s = 30.0`` — D-03 locked. Polymarket issue #292 (silent freeze)
  means TCP keepalive alone is insufficient; business-layer event-presence
  is the source of truth.
- ``_BACKOFF_S = (1, 2, 4, 8, 16, 30)`` — last value caps further attempts
  at 30s (avoids unbounded growth which would invite IP-ban per R5).
- ``_STORM_THRESHOLD = 10`` reconnects per ``_STORM_WINDOW_S = 3600`` seconds —
  R5 mitigation: switch to DEGRADED_REST_POLLING for a cool-down window so
  Polymarket doesn't IP-ban during a sustained outage.

Phase 02 F-04 invariant: ``asyncio.CancelledError`` MUST propagate from
``watch()`` (NOT swallowed). SIGTERM relies on this.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

import sentry_sdk
from loguru import logger

# D-03 LOCKED + R5: backoff sequence in seconds. Final value caps further
# attempts at 30s.
_BACKOFF_S: tuple[int, ...] = (1, 2, 4, 8, 16, 30)

# R5 storm cap: > _STORM_THRESHOLD reconnects within _STORM_WINDOW_S triggers
# DEGRADED_REST_POLLING for _DEGRADED_SLEEP_S.
_STORM_THRESHOLD: int = 10
_STORM_WINDOW_S: float = 3600.0
_DEGRADED_SLEEP_S: float = 60.0

# R5 hard cap exposed for external assertions (e.g. health-check tooling)
MAX_RECONNECTS_PER_HOUR: int = _STORM_THRESHOLD


@dataclass
class WatchdogState:
    """In-memory state — restart re-establishes WS from scratch, no SQLite needed."""

    last_event_time_s: float = field(default_factory=time.monotonic)
    reconnect_attempt: int = 0
    state: str = "CONNECTED"


class WsWatchdog:
    """30s WS silence detector with exp backoff + reconnect storm cap.

    Wire into ``WsConsumer``:
        watchdog = WsWatchdog(stale_s=30.0, on_reconnect=ws_consumer._force_reconnect)
        await watchdog.watch(stop_event)

    The consumer MUST call ``watchdog.touch()`` from inside its per-frame
    loop so the watchdog knows the data plane is healthy.
    """

    def __init__(
        self,
        stale_s: float = 30.0,  # D-03 LOCKED — DO NOT make user-configurable
        on_reconnect: Callable[[], None] | None = None,
        liveness_check: Callable[[], bool] | None = None,
    ) -> None:
        self.stale_s = stale_s
        self._state = WatchdogState()
        self._last_touch_event = asyncio.Event()
        self._on_reconnect = on_reconnect
        # GAP-401: optional liveness probe supplied by WsConsumer.
        # When set and returning True (socket OPEN + pong seen), a data-silence
        # window is treated as benign (market quiet) rather than a true stale.
        # The silence baseline resets and NO reconnect is attempted.
        # When None or returning False the existing _on_stale() reconnect path runs.
        self._liveness_check = liveness_check
        # Sliding-window deque of recent reconnect monotonic timestamps.
        # maxlen 2x threshold so window pruning has room to operate.
        self._reconnect_timestamps: deque[float] = deque(maxlen=_STORM_THRESHOLD * 2)

    # ── Properties ─────────────────────────────────────────────────────────

    @property
    def current_state(self) -> str:
        return self._state.state

    @property
    def last_event_at_s(self) -> float:
        """Epoch float? No — this returns the monotonic timestamp used for
        elapsed math. WsConsumer maintains its own ``time.time()`` epoch
        attribute for the health endpoint."""
        return self._state.last_event_time_s

    @property
    def reconnect_attempt(self) -> int:
        return self._state.reconnect_attempt

    # ── State transitions ──────────────────────────────────────────────────

    def touch(self) -> None:
        """Called from WsConsumer on EVERY incoming WS frame.

        Resets the silence timer, marks healthy, and resets the backoff
        attempt counter so the *next* failure starts from 1s.
        """
        self._state.last_event_time_s = time.monotonic()
        self._state.state = "WAITING_FOR_EVENT"
        # Wake any wait_for() blocked on _last_touch_event
        self._last_touch_event.set()
        self._last_touch_event.clear()
        self._state.reconnect_attempt = 0

    def reserve_reconnect(self) -> bool:
        """Reserve one reconnect from the shared 10/hour storm budget."""
        now = time.monotonic()
        cutoff = now - _STORM_WINDOW_S
        while self._reconnect_timestamps and self._reconnect_timestamps[0] < cutoff:
            self._reconnect_timestamps.popleft()
        if len(self._reconnect_timestamps) >= MAX_RECONNECTS_PER_HOUR:
            return False
        self._reconnect_timestamps.append(now)
        return True

    # ── Main loop ──────────────────────────────────────────────────────────

    async def watch(self, stop_event: asyncio.Event) -> None:
        """Run until stop_event is set or CancelledError propagates.

        Loop body:
        1. Compute elapsed since last touch (monotonic).
        2. If elapsed > stale_s → trigger reconnect path (storm cap or backoff).
        3. Else → wait_for(_last_touch_event.wait(), timeout=stale_s-elapsed),
           catching TimeoutError to recheck on next iteration.
        """
        try:
            while not stop_event.is_set():
                elapsed = time.monotonic() - self._state.last_event_time_s
                if elapsed > self.stale_s:
                    await self._on_stale()
                else:
                    # Cap inner wait at 0.5s so stop_event is rechecked
                    # promptly during graceful shutdown (Phase 02 F-04 spirit:
                    # cancellation/stop signals should reach watch quickly).
                    remaining = self.stale_s - elapsed
                    cap = 0.5
                    timeout = min(remaining, cap) if remaining > 0 else 0.001
                    try:
                        await asyncio.wait_for(
                            self._last_touch_event.wait(),
                            timeout=timeout,
                        )
                    except TimeoutError:
                        # Recheck on next iteration
                        pass
        except asyncio.CancelledError:
            # F-04: MUST propagate. Do NOT swallow.
            logger.info("ws_watchdog: cancelled, propagating CancelledError")
            raise

    # ── Internal: stale-handling path ──────────────────────────────────────

    async def _on_stale(self) -> None:
        """Called when elapsed since last touch exceeds stale_s.

        GAP-401 liveness gate: if liveness_check is set and returns True,
        the silence is benign (socket alive, market quiet) — reset the baseline
        and return without reconnecting. This prevents false-trips from burning
        the reconnect storm-cap during healthy-but-quiet windows.
        """
        # GAP-401: check socket liveness before treating silence as stale.
        if self._liveness_check is not None and self._liveness_check():
            # Socket is provably alive (OPEN + keepalive pong seen).
            # Reset the silence clock and stay in WAITING_FOR_EVENT.
            self._state.last_event_time_s = time.monotonic()
            self._state.state = "WAITING_FOR_EVENT"
            return

        # R5 storm cap — switch to DEGRADED_REST_POLLING (no more reconnects
        # for _DEGRADED_SLEEP_S; emit Sentry warning so the operator sees it).
        if not self.reserve_reconnect():
            recent = len(self._reconnect_timestamps)
            if self._state.state != "DEGRADED_REST_POLLING":
                self._state.state = "DEGRADED_REST_POLLING"
                logger.warning(
                    f"ws_watchdog: reconnect storm cap hit "
                    f"({recent} reconnects in last {_STORM_WINDOW_S / 60:.0f}min); "
                    f"degrading to REST polling"
                )
                sentry_sdk.add_breadcrumb(
                    category="l2-ws",
                    level="warning",
                    message=f"reconnect storm cap hit ({recent} in 1h) → DEGRADED_REST_POLLING",
                )
                sentry_sdk.capture_message(
                    "L2 WS reconnect storm cap hit — degrading to REST polling",
                    level="warning",
                )
            await asyncio.sleep(_DEGRADED_SLEEP_S)
            return

        # Normal reconnect path — exponential backoff
        attempt_idx = min(self._state.reconnect_attempt, len(_BACKOFF_S) - 1)
        wait_s = _BACKOFF_S[attempt_idx]

        self._state.state = "RECONNECTING"
        self._state.reconnect_attempt += 1

        logger.info(
            f"ws_watchdog: stale ({self.stale_s}s silence) → "
            f"RECONNECTING attempt={self._state.reconnect_attempt} backoff={wait_s}s"
        )
        sentry_sdk.add_breadcrumb(
            category="l2-ws",
            level="info",
            message=(
                f"WS silence > {self.stale_s}s — reconnect attempt "
                f"{self._state.reconnect_attempt} backoff={wait_s}s"
            ),
        )

        # Invoke caller's reconnect hook (suppress to keep loop alive)
        if self._on_reconnect is not None:
            try:
                self._on_reconnect()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"ws_watchdog: on_reconnect hook raised: {e!r}")

        await asyncio.sleep(wait_s)
        # Reset the silence clock so next iteration measures fresh elapsed
        self._state.last_event_time_s = time.monotonic()
