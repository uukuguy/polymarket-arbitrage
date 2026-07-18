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
from collections.abc import Callable, Iterable
from typing import Any

from loguru import logger

# GAP-401: websockets State enum for the liveness closure.
# Import from websockets.protocol (canonical in websockets 15+;
# websockets.connection is deprecated and emits a DeprecationWarning).
from websockets.protocol import State as WsState

from polyarb.clients.ws_market_client import stream_market_events
from polyarb.daemon.ws_watchdog import WsWatchdog

_QUIET_REFRESH_SEND_TIMEOUT_S: float = 5.0
_BOOK_EVIDENCE_TIMEOUT_S: float = 5.0

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
_ws_test_kill_flag: bool = os.getenv("POLYARB_WS_TEST_KILL") == "1"


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
        self._last_quiet_refresh_attempt_at_s: float = 0.0
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
        self._subscription_control_lock = asyncio.Lock()
        self._connection_generation = 0
        self._book_evidence_waiters: dict[tuple[int, str], set[asyncio.Future[bool]]] = {}
        self._compensated_generations: set[int] = set()
        # Wire the liveness closure into the watchdog so it uses it for the gate.
        self._watchdog._liveness_check = self._liveness_check

    # ── GAP-401: liveness probe ────────────────────────────────────────────

    def _stash_ws(self, ws: Any) -> None:
        """Store the current live ws object (called by on_connect hook each connect).

        Called by stream_market_events' on_connect side-channel once per (re)connect
        so the liveness closure always reads the CURRENT connection's state/latency.
        """
        self._current_ws = ws

    async def _send_control(self, ws: Any, payload: dict[str, Any]) -> bool:
        """Bound every subscription-control write by one production timeout."""
        try:
            await asyncio.wait_for(
                ws.send(json.dumps(payload)), timeout=_QUIET_REFRESH_SEND_TIMEOUT_S
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"ws subscription control send failed: {exc!r}")
            return False

    async def _initialize_connection(self, ws: Any) -> None:
        """Fence generation, initial subscribe, and publication atomically."""
        async with self._subscription_control_lock:
            generation = self._connection_generation + 1
            self._connection_generation = generation
            active_assets = self._compute_active_assets()
            ok = await self._send_control(
                ws,
                {
                    "type": "market",
                    "assets_ids": active_assets,
                    "initial_dump": True,
                },
            )
            if ok:
                self._current_ws = ws
                return
        await self._compensate_generation(ws, generation)
        raise RuntimeError("initial WS subscription failed")

    async def _compensate_generation(self, ws: Any, generation: int) -> None:
        """Close exactly one ambiguous socket per generation, preserving cancel."""
        async with self._subscription_control_lock:
            if generation in self._compensated_generations:
                return
            self._compensated_generations.add(generation)
            if not self._watchdog.reserve_reconnect():
                logger.warning("ws compensation skipped: reconnect budget exhausted")
                return
        close_task = asyncio.create_task(
            asyncio.wait_for(ws.close(), timeout=_QUIET_REFRESH_SEND_TIMEOUT_S)
        )
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            await asyncio.shield(close_task)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"ws compensation close failed: {exc!r}")

    def record_book_evidence(
        self, *, asset_id: str, generation: int, mirror_succeeded: bool
    ) -> None:
        """Resolve only same-generation book waiters after production mirror success."""
        if not mirror_succeeded:
            return
        for future in tuple(self._book_evidence_waiters.get((generation, asset_id), set())):
            if not future.done():
                future.set_result(True)

    async def _send_single_control_transaction(
        self,
        payload: dict[str, Any],
        *,
        commit: Callable[[], None] | None = None,
        offline_commit: Callable[[], None] | None = None,
    ) -> bool:
        """Fence one control send, identity check, and optional state commit."""
        ws: Any = None
        generation = 0
        try:
            async with self._subscription_control_lock:
                ws = self._current_ws
                generation = self._connection_generation
                if ws is None:
                    if offline_commit is not None:
                        offline_commit()
                    return False
                succeeded = await self._send_control(ws, payload)
                identity_matches = (
                    self._current_ws is ws
                    and self._connection_generation == generation
                )
                if succeeded and identity_matches:
                    if commit is not None:
                        commit()
                    return True
        except asyncio.CancelledError:
            if ws is not None:
                await self._compensate_generation(ws, generation)
            raise
        if ws is not None:
            await self._compensate_generation(ws, generation)
        return False

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

    async def request_book_refresh(self) -> bool:
        """Request an initial book dump for the current active asset union.

        Sending a request is transport activity, not business-data freshness:
        this method intentionally leaves candidate/L3 state, event timestamps,
        and the watchdog untouched. Only the receive path may advance them.
        """
        waiter: asyncio.Future[bool] | None = None
        ws: Any = None
        generation = 0
        active_assets: list[str] = []
        try:
            async with self._subscription_control_lock:
                active_assets = self._compute_active_assets()
                ws = self._current_ws
                generation = self._connection_generation
                if not active_assets or ws is None:
                    return False
                waiter = asyncio.get_running_loop().create_future()
                for asset_id in active_assets:
                    self._book_evidence_waiters.setdefault((generation, asset_id), set()).add(
                        waiter
                    )
                logger.info(f"ws quiet refresh: sending assets={len(active_assets)}")
                if not await self._send_control(
                    ws,
                    {"operation": "unsubscribe", "assets_ids": active_assets},
                ):
                    raise RuntimeError("quiet unsubscribe failed")
                if self._current_ws is not ws or self._connection_generation != generation:
                    raise RuntimeError("connection identity changed during refresh")
                if not await self._send_control(
                    ws,
                    {
                        "operation": "subscribe",
                        "assets_ids": active_assets,
                        "initial_dump": True,
                    },
                ):
                    raise RuntimeError("quiet subscribe failed")
            assert waiter is not None
            await asyncio.wait_for(asyncio.shield(waiter), timeout=_BOOK_EVIDENCE_TIMEOUT_S)
            logger.info(f"ws quiet refresh: evidenced assets={len(active_assets)}")
            return True
        except asyncio.CancelledError:
            if ws is not None:
                await self._compensate_generation(ws, generation)
            raise
        except Exception as e:  # noqa: BLE001 — ambiguity requires reconnect
            logger.warning(f"ws quiet refresh failed assets={len(active_assets)} error={e!r}")
            if ws is not None:
                await self._compensate_generation(ws, generation)
            return False
        finally:
            if waiter is not None:
                for asset_id in active_assets:
                    waiters = self._book_evidence_waiters.get((generation, asset_id))
                    if waiters is not None:
                        waiters.discard(waiter)
                        if not waiters:
                            self._book_evidence_waiters.pop((generation, asset_id), None)

    async def refresh_if_quiet(
        self,
        *,
        now_s: float | None = None,
        quiet_after_s: float = 60.0,
        retry_s: float = 30.0,
    ) -> bool | None:
        """Request a book dump when business frames are quiet and retry is due."""
        now = time.time() if now_s is None else now_s
        if now - self._last_event_at_s < quiet_after_s:
            return None
        if (
            self._last_quiet_refresh_attempt_at_s != 0.0
            and now - self._last_quiet_refresh_attempt_at_s < retry_s
        ):
            return None
        # Record before awaiting so a slow or failed send cannot create a storm.
        self._last_quiet_refresh_attempt_at_s = now
        return await self.request_book_refresh()

    async def run_quiet_refresh(
        self,
        stop_event: asyncio.Event,
        *,
        quiet_after_s: float = 60.0,
        retry_s: float = 30.0,
        check_interval_s: float = 5.0,
    ) -> None:
        """Request truthful initial dumps on a bounded quiet-market cadence."""
        while not stop_event.is_set():
            await self.refresh_if_quiet(
                quiet_after_s=quiet_after_s,
                retry_s=retry_s,
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=check_interval_s)
            except TimeoutError:
                continue

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

    async def replace_candidate_set(self, asset_ids: Iterable[str]) -> bool:
        """Atomically send the candidate diff and commit desired state."""
        desired = set(asset_ids)
        ws: Any = None
        generation = 0
        failed = False
        try:
            async with self._subscription_control_lock:
                added = sorted(desired - self._candidate_set)
                removed = sorted(self._candidate_set - desired)
                ws = self._current_ws
                generation = self._connection_generation
                if ws is None:
                    # Publish desired state so a cold-start consumer can leave its
                    # empty-set wait and the next connection provider subscribes
                    # the newest candidates. False keeps the durable cursor
                    # retryable until live convergence is proven.
                    self._candidate_set = desired
                    return False
                if added and not await self._send_control(
                    ws,
                    {
                        "operation": "subscribe",
                        "assets_ids": added,
                        "initial_dump": True,
                    },
                ):
                    failed = True
                if (
                    not failed
                    and removed
                    and not await self._send_control(
                        ws, {"operation": "unsubscribe", "assets_ids": removed}
                    )
                ):
                    failed = True
                if not failed and (
                    self._current_ws is not ws
                    or self._connection_generation != generation
                ):
                    failed = True
                if not failed:
                    self._candidate_set = desired
                    return True
        except asyncio.CancelledError:
            if ws is not None:
                await self._compensate_generation(ws, generation)
            raise
        if ws is not None:
            await self._compensate_generation(ws, generation)
        return False

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
        return await self._send_single_control_transaction(
            {
                "operation": "subscribe",
                "assets_ids": list(asset_ids),
                "initial_dump": True,
            },
            commit=lambda: self._l3_active_set.update(asset_ids),
            offline_commit=lambda: self._l3_active_set.update(asset_ids),
        )

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
        return await self._send_single_control_transaction(
            {"operation": "unsubscribe", "assets_ids": list(asset_ids)},
            commit=lambda: self._l3_active_set.difference_update(asset_ids),
            offline_commit=lambda: self._l3_active_set.difference_update(asset_ids),
        )

    # ── Quick task 260602-ws-dynamic-subscribe ──────────────────────────────
    #
    # Payload-only subscribe/unsubscribe — dual of add_subscriptions /
    # remove_subscriptions for the L2 candidate-refresh flow. State mutation
    # (`_candidate_set`) is already handled by `update_candidate_set`; these
    # helpers ONLY push the mid-connection WS payload so the live socket
    # actually starts/stops receiving frames for the new asset_ids.
    #
    # Why a separate method: `add_subscriptions` mutates `_l3_active_set` —
    # using it for L2 candidates would clobber the L3 set (Pitfall 5
    # regression — verified by test_candidate_refresh_l3_protection).
    #
    # Returns True if (a) asset_ids empty (noop) or (b) WS send succeeded.
    # Returns False on no-live-ws or send error — caller logs, no state
    # mutation. The next reconnect picks up the new candidate set via
    # _compute_active_assets() in either case.

    async def subscribe_candidates_payload(self, asset_ids: list[str]) -> bool:
        """Send mid-conn `subscribe` payload for L2 candidate add diff."""
        if not asset_ids:
            return True
        return await self._send_single_control_transaction(
            {
                "operation": "subscribe",
                "assets_ids": list(asset_ids),
                "initial_dump": True,
            }
        )

    async def unsubscribe_candidates_payload(self, asset_ids: list[str]) -> bool:
        """Send mid-conn `unsubscribe` payload for L2 candidate remove diff."""
        if not asset_ids:
            return True
        return await self._send_single_control_transaction(
            {"operation": "unsubscribe", "assets_ids": list(asset_ids)}
        )

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
                except TimeoutError:
                    continue

            if stop_event.is_set():
                return

            self._state = "CONNECTED"
            logger.info(
                f"ws_consumer: starting consume loop with "
                f"{len(self._compute_active_assets())} subscribed assets "
                f"(candidate={len(self._candidate_set)} l3={len(self._l3_active_set)})"
            )

            async for event in stream_market_events(
                self._compute_active_assets,
                initial_dump=True,
                connection_initializer=self._initialize_connection,
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
                    mirror_succeeded = self._on_event(event)
                    if event.get("event_type") == "book":
                        self.record_book_evidence(
                            asset_id=str(event.get("asset_id") or ""),
                            generation=self._connection_generation,
                            mirror_succeeded=mirror_succeeded is True,
                        )
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
