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
import json
import os
import time
import warnings
from typing import Any, Callable, Iterable

from loguru import logger

from polyarb.clients.ws_market_client import stream_market_events
from polyarb.daemon.ws_watchdog import WsWatchdog

# GAP-401: websockets State enum for the liveness closure.
# Import from websockets.protocol (canonical in websockets 15+;
# websockets.connection is deprecated and emits a DeprecationWarning).
from websockets.protocol import State as WsState


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
        # Phase 05 Plan 02 — D-11 refactor (Pitfall 5 fix):
        # _subscribed_assets is no longer a single list. Two disjoint roles:
        # - _candidate_set: tokens chosen by L2 candidate refresh (5-min cron-ish)
        # - _l3_active_set: tokens promoted to L3 (5-min L3 promoter task)
        # The public subscribed_assets property + the legacy _subscribed_assets
        # property+setter both expose the union via _compute_active_assets().
        self._candidate_set: set[str] = set(initial_assets or [])
        self._l3_active_set: set[str] = set()
        self._state: str = "DISCONNECTED"
        self._last_event_at_s: float = time.time()
        self._frame_count: int = 0
        # Phase 04 Plan 04 D-06 indicator 1 — frames RECEIVED but downstream
        # on_event dispatch raised. Distinct from frame_count: a frame may be
        # received successfully (frame_count += 1) yet fail to process
        # (dropped_frame_count += 1). Surfaces operational throughput health
        # during the prod chaos run (`make chaos-l2-inj4-throughput`).
        self._dropped_frame_count: int = 0
        # GAP-401: stash of the current live ws object (None until first connect).
        # Updated by _stash_ws() on each (re)connect via the on_connect hook in
        # stream_market_events. Read by _liveness_check() to tell the watchdog
        # whether the socket is provably alive (OPEN + keepalive pong received).
        self._current_ws: Any = None
        # Wire the liveness closure into the watchdog so it uses it for the gate.
        self._watchdog._liveness_check = self._liveness_check

    # ── GAP-401: liveness probe ────────────────────────────────────────────

    def _stash_ws(self, ws: Any) -> None:
        """Store the current live ws object (called by on_connect hook each connect).

        Called by stream_market_events' on_connect side-channel once per (re)connect
        so the liveness closure always reads the CURRENT connection's state/latency.
        """
        self._current_ws = ws

    def _liveness_check(self) -> bool:
        """Return True when the current WS connection is provably alive.

        Liveness = socket OPEN AND keepalive pong seen (latency > 0).
        - ws.state == WsState.OPEN: the websockets lib's protocol state machine
          is in OPEN (not CONNECTING/CLOSING/CLOSED).
        - ws.latency > 0: at least one ping/pong round-trip completed; a frozen
          socket stops exchanging pings → latency sticks at last value (which
          stays > 0) but the lib's ping_timeout fires → closes the conn itself.
          If the lib's keepalive never started (ping_interval=None), latency stays
          0 forever and we conservatively return False (rely on _on_stale path).
        When False, the watchdog falls through to its normal _on_stale reconnect.
        """
        ws = self._current_ws
        if ws is None:
            return False
        try:
            return ws.state is WsState.OPEN and ws.latency > 0
        except Exception:  # noqa: BLE001
            # Defensive: unexpected attribute error on ws object → not alive
            return False

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
        # Defensive copy of the candidate ∪ L3 union — Plan 05 candidate refresh
        # MUST NOT mutate via this property (returns a fresh list each call).
        return self._compute_active_assets()

    # ── Phase 05 Plan 02 — D-11 helpers (Pitfall 5 fix) ────────────────────

    def _compute_active_assets(self) -> list[str]:
        """Return sorted union of _candidate_set and _l3_active_set.

        Called by:
          - subscribed_assets property (public)
          - run() loop (replaces direct self._subscribed_assets read)
          - _subscribed_assets backward-compat property getter
        """
        return sorted(self._candidate_set | self._l3_active_set)

    @property
    def _subscribed_assets(self) -> list[str]:
        """Backward-compat read of the union (matches pre-Plan-02 semantics)."""
        return self._compute_active_assets()

    @_subscribed_assets.setter
    def _subscribed_assets(self, new_list: list[str]) -> None:
        """Backward-compat writer — preserves L3 set, replaces candidate set.

        Legacy callers (l2_candidate_refresh.on_snapshot_complete pre-Plan-02
        migration) used to do ``ws_consumer._subscribed_assets = list(...)``
        as a full-list overwrite. Plan 02 splits the set in two — this setter
        interprets the incoming list as the NEW candidate set ONLY, leaving
        _l3_active_set untouched. Emits DeprecationWarning so any remaining
        callers are visible in test output.
        """
        warnings.warn(
            "Direct assignment to WsConsumer._subscribed_assets is deprecated; "
            "use update_candidate_set(asset_ids) instead. The legacy setter "
            "interprets the new list as the candidate set only — _l3_active_set "
            "is preserved (Phase 05 Pitfall 5 fix).",
            DeprecationWarning,
            stacklevel=2,
        )
        # NOTE: We do NOT subtract _l3_active_set from the new candidate set.
        # Rationale: a token can legitimately be in BOTH sets simultaneously
        # (e.g. a high-liquidity candidate that ALSO got promoted to L3); the
        # union semantics in _compute_active_assets handle the overlap.
        self._candidate_set = set(new_list)

    def update_candidate_set(self, asset_ids: Iterable[str]) -> None:
        """Replace the L2-candidate portion of the subscription set.

        Phase 05 Plan 02 Task 3 — `l2_candidate_refresh.on_snapshot_complete`
        migrates from the legacy `_subscribed_assets = list(...)` overwrite to
        this dedicated helper, which leaves `_l3_active_set` untouched (Pitfall 5).
        """
        self._candidate_set = set(asset_ids)

    async def add_subscriptions(self, asset_ids: list[str]) -> bool:
        """Add asset_ids to L3 subscriptions; send subscribe payload if ws is live.

        Phase 05 Plan 02 D-11 contract (revision 1 — Warning #12 deterministic):

          1. Empty asset_ids → return True (noop).
          2. _current_ws is None → mutate _l3_active_set fallback, return False.
             The next reconnect will pick them up via _compute_active_assets().
          3. Else attempt `await self._current_ws.send(json.dumps(payload))`
             with payload = {"operation": "subscribe", "assets_ids": [...],
             "initial_dump": True}. (See ws_market_client docstring + thread
             §2.2 Q1 — `operation` key not `type` key for mid-conn payloads.)
          4. Send raises → log warning, do NOT mutate _l3_active_set, return
             False. Caller can safely retry. (Warning #12: subscribed_assets
             must not include the failed token after this path runs.)
          5. Send succeeds → mutate _l3_active_set, return True.

        Concurrent safety: websockets 15+ supports send + recv from different
        async tasks; this method does not need synchronization (the recv loop
        runs in a separate task and the library handles send/recv decoupling
        internally).
        """
        if not asset_ids:
            return True
        ws = self._current_ws
        if ws is None:
            # Fallback path — no live socket yet. Mutate _l3_active_set so the
            # next reconnect picks up the new tokens via _compute_active_assets.
            self._l3_active_set.update(asset_ids)
            return False
        payload = {
            "operation": "subscribe",
            "assets_ids": list(asset_ids),
            "initial_dump": True,
        }
        try:
            await ws.send(json.dumps(payload))
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12 envelope
            logger.warning(
                f"ws_consumer.add_subscriptions: send failed ({e!r}) — "
                f"asset_ids={list(asset_ids)[:5]}{'...' if len(asset_ids) > 5 else ''} "
                f"(Warning #12: _l3_active_set NOT polluted; caller may retry)"
            )
            return False
        # Send succeeded — commit to _l3_active_set.
        self._l3_active_set.update(asset_ids)
        return True

    async def remove_subscriptions(self, asset_ids: list[str]) -> bool:
        """Remove asset_ids from L3 subscriptions; send unsubscribe payload if live.

        Symmetric to add_subscriptions. Payload schema (thread §2.2 Q1):
          {"operation": "unsubscribe", "assets_ids": [...]}  — no initial_dump.

        On send failure: log warning, do NOT mutate _l3_active_set, return False.
        On no live ws: mutate _l3_active_set (fallback) and return False.
        On send success: mutate _l3_active_set (discard tokens) and return True.
        """
        if not asset_ids:
            return True
        ws = self._current_ws
        if ws is None:
            # Fallback path — no live socket. Discard from _l3_active_set so
            # the next reconnect omits the tokens via _compute_active_assets.
            self._l3_active_set.difference_update(asset_ids)
            return False
        payload = {
            "operation": "unsubscribe",
            "assets_ids": list(asset_ids),
        }
        try:
            await ws.send(json.dumps(payload))
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"ws_consumer.remove_subscriptions: send failed ({e!r}) — "
                f"asset_ids={list(asset_ids)[:5]}{'...' if len(asset_ids) > 5 else ''}"
            )
            return False
        self._l3_active_set.difference_update(asset_ids)
        return True

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
            # Wait until the active set (candidate ∪ L3) is non-empty.
            # Plan 05 Plan 02: read the union directly via _compute_active_assets
            # (avoids the legacy _subscribed_assets property's DeprecationWarning
            # in the hot path).
            while not self._compute_active_assets() and not stop_event.is_set():
                logger.warning(
                    "ws_consumer: active asset set is empty — waiting for "
                    "candidate_refresh / L3 promoter to populate"
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue

            if stop_event.is_set():
                return

            self._state = "CONNECTED"
            active_assets = self._compute_active_assets()
            logger.info(
                f"ws_consumer: starting consume loop with "
                f"{len(active_assets)} subscribed assets "
                f"(candidate={len(self._candidate_set)} l3={len(self._l3_active_set)})"
            )

            async for event in stream_market_events(
                active_assets,
                initial_dump=True,
                on_connect=self._stash_ws,  # GAP-401: stash ws for liveness gate
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
            self._current_ws = None  # GAP-401: clear stash on disconnect
            # Do NOT re-raise: returning lets the supervisor (l2_main) decide
            # whether to relaunch the consumer. Watchdog will mark RECONNECTING
            # via its stale_s timeout if no fresh touch arrives.
            return
        except asyncio.CancelledError:
            # F-04: must propagate.
            logger.info("ws_consumer: cancelled, propagating CancelledError")
            self._state = "DISCONNECTED"
            self._current_ws = None  # GAP-401: clear stash on disconnect
            raise
