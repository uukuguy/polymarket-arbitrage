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
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from loguru import logger

# GAP-401: websockets State enum for the liveness closure.
# Import from websockets.protocol (canonical in websockets 15+;
# websockets.connection is deprecated and emits a DeprecationWarning).
from websockets.protocol import State as WsState

from polyarb.clients.ws_market_client import (
    WsConnectionInitializationFailed,
    stream_market_events,
)
from polyarb.daemon.ws_watchdog import WsWatchdog
from polyarb.observation.l3_evidence import (
    FrameDispatchResult,
    RuntimeEventKind,
    RuntimeEventSeverity,
    WsMembershipSnapshot,
    build_runtime_event_detail,
    safe_runtime_error_type,
)

_QUIET_REFRESH_SEND_TIMEOUT_S: float = 5.0
# A full 10-token initial dump is bounded by one 30-second evidence slot.
# Production showed that five seconds can close a healthy generation before
# the last quiet books arrive, while the reconnect completes inside one slot.
_BOOK_EVIDENCE_TIMEOUT_S: float = 25.0
_BOOK_EVIDENCE_RETRY_AFTER_S: float = 8.0
_COMPENSATED_GENERATIONS_MAX: int = 128


@dataclass(eq=False)
class _BookEvidenceWaiter:
    """One refresh barrier isolated to a captured connection generation."""

    future: asyncio.Future[bool]
    missing: set[str] = field(default_factory=set)


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
        on_event: Callable[
            [dict],
            FrameDispatchResult | Awaitable[FrameDispatchResult],
        ],
        initial_assets: list[str] | None = None,
        membership_observer: Callable[[WsMembershipSnapshot], None] | None = None,
        event_recorder: Callable[..., None] | None = None,
    ) -> None:
        self._settings = settings
        self._watchdog = watchdog
        self._on_event = on_event
        # Phase 05 Plan 02 — D-11 refactor (Pitfall 5 fix):
        # _subscribed_assets is no longer a single list. Two disjoint roles:
        # - _candidate_set: tokens chosen by L2 candidate refresh (5-min cron-ish)
        # - _l3_desired_set: reconnect intent chosen by the L3 promoter
        # - _l3_committed_set: successful controls on the current generation
        # - _l3_business_evidence: current-generation depth-write evidence
        # The public subscribed_assets property + the legacy _subscribed_assets
        # property+setter both expose the union via _compute_active_assets().
        self._candidate_set: set[str] = set(initial_assets or [])
        self._l3_desired_set: set[str] = set()
        self._l3_committed_set: set[str] = set()
        self._l3_business_evidence: dict[str, tuple[int, datetime]] = {}
        self._membership_observer = membership_observer
        # Plan 03 wires immutable runtime events. Accept the dependency now so
        # Watchdog/Consumer construction does not need another signature break.
        self._event_recorder = event_recorder
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
        self._book_evidence_waiters: dict[int, list[_BookEvidenceWaiter]] = {}
        self._connection_initialized_at_s: float | None = None
        self._last_quiet_refresh_missing_assets: frozenset[str] = frozenset()
        self._last_quiet_refresh_missing_generation: int | None = None
        self._compensated_generations: set[int] = set()
        self._compensated_generation_order: deque[int] = deque()
        # Wire the liveness closure into the watchdog so it uses it for the gate.
        self._watchdog._liveness_check = self._liveness_check
        self._publish_l3_membership_locked()

    def _record_runtime_event(
        self,
        kind: RuntimeEventKind,
        *,
        reason_code: str,
        detail: dict[str, object],
        severity: RuntimeEventSeverity = RuntimeEventSeverity.INFO,
        generation: int | None = None,
    ) -> None:
        """Enqueue one immutable bounded record without blocking the data plane."""
        if self._event_recorder is None:
            return
        try:
            self._event_recorder(
                kind,
                occurred_at=datetime.now(UTC),
                severity=severity,
                generation=self._connection_generation if generation is None else generation,
                reason_code=reason_code,
                detail=build_runtime_event_detail(kind, detail),
            )
        except Exception as exc:  # noqa: BLE001 - queue overflow is runtime fail truth
            logger.warning(
                "ws runtime event enqueue failed kind={} error_type={}",
                kind.value,
                type(exc).__name__,
            )

    def l3_membership_snapshot(self) -> WsMembershipSnapshot:
        """Return an immutable copy of current L3 membership truth."""
        evidence_times = {
            asset_id: observed_at
            for asset_id, (generation, observed_at) in self._l3_business_evidence.items()
            if generation == self._connection_generation and asset_id in self._l3_committed_set
        }
        return WsMembershipSnapshot(
            generation=self._connection_generation,
            desired=frozenset(self._l3_desired_set),
            committed=frozenset(self._l3_committed_set),
            evidenced=frozenset(evidence_times),
            evidenced_at=evidence_times,
        )

    def _publish_l3_membership_locked(self) -> None:
        """Synchronously publish one immutable snapshot at a mutation boundary."""
        if self._membership_observer is not None:
            self._membership_observer(self.l3_membership_snapshot())

    def _clear_l3_connection_state_locked(self) -> None:
        self._l3_committed_set.clear()
        self._l3_business_evidence.clear()
        self._connection_initialized_at_s = None
        self._last_quiet_refresh_missing_assets = frozenset()
        self._last_quiet_refresh_missing_generation = None

    def _fail_book_evidence_waiters_locked(self, generation: int) -> None:
        """Fail every barrier captured from one invalidated generation."""
        for waiter in tuple(self._book_evidence_waiters.get(generation, ())):
            if not waiter.future.done():
                waiter.future.set_result(False)

    def _discard_book_evidence_waiter(
        self,
        generation: int,
        waiter: _BookEvidenceWaiter,
    ) -> None:
        """Remove one waiter without touching barriers from other generations."""
        waiters = self._book_evidence_waiters.get(generation)
        if waiters is None:
            return
        try:
            waiters.remove(waiter)
        except ValueError:
            return
        if not waiters:
            self._book_evidence_waiters.pop(generation, None)

    def set_l3_desired(self, asset_ids: Iterable[str]) -> None:
        """Replace reconnect intent without claiming current control success."""
        self._l3_desired_set = set(asset_ids)
        self._publish_l3_membership_locked()

    @property
    def _l3_active_set(self) -> set[str]:
        """Deprecated defensive read of L3 desired state.

        Production candidate refresh still reads this legacy name for bounded
        logging. Returning a copy prevents old callers from bypassing the
        synchronous membership publisher.
        """
        return set(self._l3_desired_set)

    @_l3_active_set.setter
    def _l3_active_set(self, asset_ids: Iterable[str]) -> None:
        """Route legacy assignments through the truthful desired-state API."""
        self.set_l3_desired(asset_ids)

    # ── GAP-401: liveness probe ────────────────────────────────────────────

    def _stash_ws(self, ws: Any) -> None:
        """Store the current live ws object (called by on_connect hook each connect).

        Called by stream_market_events' on_connect side-channel once per (re)connect
        so the liveness closure always reads the CURRENT connection's state/latency.
        """
        self._current_ws = ws

    async def _send_control(self, ws: Any, payload: dict[str, Any]) -> bool:
        """Bound every subscription-control write by one production timeout."""
        operation = str(payload.get("operation") or "initial_subscribe")
        try:
            await asyncio.wait_for(
                ws.send(json.dumps(payload)), timeout=_QUIET_REFRESH_SEND_TIMEOUT_S
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._record_runtime_event(
                RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                reason_code="control_send_failed",
                detail={
                    "operation": operation,
                    "error_type": safe_runtime_error_type(exc),
                },
                severity=RuntimeEventSeverity.WARNING,
            )
            logger.warning(
                "ws subscription control send failed error_type={}",
                type(exc).__name__,
            )
            return False

    async def _initialize_connection(self, ws: Any) -> None:
        """Fence generation, initial subscribe, and publication atomically."""
        generation = self._connection_generation + 1
        is_reconnect = self._connection_generation > 0
        try:
            async with self._subscription_control_lock:
                previous_generation = self._connection_generation
                generation = self._connection_generation + 1
                is_reconnect = previous_generation > 0
                if is_reconnect:
                    self._record_runtime_event(
                        RuntimeEventKind.RECONNECT_STARTED,
                        reason_code="connection_initializing",
                        detail={"source": "connection_initializer"},
                        generation=generation,
                    )
                self._fail_book_evidence_waiters_locked(self._connection_generation)
                self._connection_generation = generation
                self._clear_l3_connection_state_locked()
                self._publish_l3_membership_locked()
                self._record_runtime_event(
                    RuntimeEventKind.WS_GENERATION_CHANGED,
                    reason_code="connection_initialized",
                    detail={
                        "previous_generation": previous_generation,
                        "new_generation": generation,
                    },
                    generation=generation,
                )
                previous_ws = self._current_ws
                desired_snapshot = frozenset(self._l3_desired_set)
                active_assets = self._compute_active_assets()
                ok = await self._send_control(
                    ws,
                    {
                        "type": "market",
                        "assets_ids": active_assets,
                        "initial_dump": True,
                    },
                )
                identity_matches = (
                    self._connection_generation == generation and self._current_ws is previous_ws
                )
                snapshot_matches = (
                    frozenset(self._l3_desired_set) == desired_snapshot
                    and self._compute_active_assets() == active_assets
                )
                if ok and identity_matches and snapshot_matches:
                    self._current_ws = ws
                    self._connection_initialized_at_s = time.time()
                    # Commit exactly the desired membership represented in the
                    # payload above. A mutation while send() was suspended is
                    # unsent truth and must force compensation/reconnect.
                    self._l3_committed_set = set(desired_snapshot)
                    self._publish_l3_membership_locked()
                    if is_reconnect:
                        self._record_runtime_event(
                            RuntimeEventKind.RECONNECT_SUCCEEDED,
                            reason_code="initial_control_committed",
                            detail={"source": "connection_initializer"},
                            generation=generation,
                        )
                    return
                if ok:
                    self._record_runtime_event(
                        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                        reason_code="control_state_changed",
                        detail={
                            "operation": "initial_subscribe",
                            "error_type": "StateMismatch",
                        },
                        severity=RuntimeEventSeverity.WARNING,
                        generation=generation,
                    )
                self._publish_l3_membership_locked()
        except asyncio.CancelledError:
            await self._compensate_generation(ws, generation)
            raise
        retry_after_s = await self._compensate_generation(ws, generation)
        if is_reconnect:
            self._record_runtime_event(
                RuntimeEventKind.RECONNECT_FAILED,
                reason_code="initial_control_failed",
                detail={
                    "operation": "initial_subscribe",
                    "error_type": "ControlRejected",
                },
                severity=RuntimeEventSeverity.WARNING,
                generation=generation,
            )
        raise WsConnectionInitializationFailed(
            "initial WS subscription failed", retry_after_s=retry_after_s
        )

    async def _release_connection(self, ws: Any) -> None:
        """Clear only the disconnected generation's published socket."""
        async with self._subscription_control_lock:
            if self._current_ws is ws:
                self._fail_book_evidence_waiters_locked(self._connection_generation)
                self._current_ws = None
                self._clear_l3_connection_state_locked()
            self._publish_l3_membership_locked()

    async def _compensate_generation(self, ws: Any, generation: int) -> float:
        """Close exactly one ambiguous socket per generation, preserving cancel."""
        retry_after_s = 0.0
        async with self._subscription_control_lock:
            if generation in self._compensated_generations:
                self._publish_l3_membership_locked()
                return self._watchdog.reconnect_retry_after_s()
            if len(self._compensated_generation_order) >= _COMPENSATED_GENERATIONS_MAX:
                expired = self._compensated_generation_order.popleft()
                self._compensated_generations.discard(expired)
            self._compensated_generations.add(generation)
            self._compensated_generation_order.append(generation)
            if generation == self._connection_generation:
                self._fail_book_evidence_waiters_locked(generation)
                if self._current_ws is ws:
                    self._current_ws = None
                self._clear_l3_connection_state_locked()
            self._publish_l3_membership_locked()
            if not self._watchdog.reserve_reconnect():
                retry_after_s = self._watchdog.reconnect_retry_after_s()
                self._record_runtime_event(
                    RuntimeEventKind.RECONNECT_DEFERRED,
                    reason_code="storm_budget_exhausted",
                    detail={
                        "retry_after_ms": max(0, int(retry_after_s * 1000)),
                        "budget_count": len(self._watchdog._reconnect_timestamps),
                    },
                    severity=RuntimeEventSeverity.WARNING,
                    generation=generation,
                )
                logger.warning(
                    "ws reconnect deferred: reconnect budget exhausted "
                    f"retry_after_s={retry_after_s:.3f}"
                )
            else:
                self._record_runtime_event(
                    RuntimeEventKind.RECONNECT_RESERVED,
                    reason_code="compensation_reserved",
                    detail={
                        "reconnect_attempt": self._watchdog.reconnect_attempt + 1,
                        "budget_count": len(self._watchdog._reconnect_timestamps),
                    },
                    generation=generation,
                )
        close_succeeded = True
        close_task = asyncio.create_task(
            asyncio.wait_for(ws.close(), timeout=_QUIET_REFRESH_SEND_TIMEOUT_S)
        )
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            await asyncio.shield(close_task)
            raise
        except Exception as exc:  # noqa: BLE001
            close_succeeded = False
            logger.warning("ws compensation close failed error_type={}", type(exc).__name__)
        self._record_runtime_event(
            RuntimeEventKind.SUBSCRIPTION_COMPENSATED,
            reason_code="ambiguous_generation_closed",
            detail={
                "operation": "connection_close",
                "close_succeeded": close_succeeded,
            },
            severity=(
                RuntimeEventSeverity.INFO if close_succeeded else RuntimeEventSeverity.WARNING
            ),
            generation=generation,
        )
        return retry_after_s

    def requires_book_levels(self, asset_id: str) -> bool:
        """Return current-generation depth-write eligibility for one token.

        Durable L3 membership and temporary quiet-refresh barriers are the two
        canonical consumers of depth.  Candidate tokens become eligible only
        while they are still missing from a registered current-generation
        barrier; unrelated frames never inherit a global depth-write gate.
        """
        if asset_id in self._l3_committed_set:
            return True
        return any(
            asset_id in waiter.missing
            for waiter in self._book_evidence_waiters.get(self._connection_generation, ())
        )

    @property
    def last_quiet_refresh_missing_assets(self) -> frozenset[str]:
        """Exact last failed barrier identities, exposed without log leakage."""
        return frozenset(self._last_quiet_refresh_missing_assets)

    def record_book_evidence(
        self,
        *,
        asset_id: str,
        generation: int,
        book_levels_succeeded: bool,
        observed_at: datetime,
    ) -> None:
        """Accept only current-generation depth evidence for committed tokens."""
        if not book_levels_succeeded or generation != self._connection_generation:
            return
        accepted = True
        if asset_id in self._l3_committed_set:
            previous = self._l3_business_evidence.get(asset_id)
            if previous is not None and observed_at < previous[1]:
                accepted = False
            else:
                self._l3_business_evidence[asset_id] = (generation, observed_at)
                self._publish_l3_membership_locked()
        if not accepted:
            return
        if (
            generation == self._last_quiet_refresh_missing_generation
            and asset_id in self._last_quiet_refresh_missing_assets
        ):
            self._last_quiet_refresh_missing_assets = self._last_quiet_refresh_missing_assets - {
                asset_id
            }
            if not self._last_quiet_refresh_missing_assets:
                self._last_quiet_refresh_missing_generation = None
        for waiter in tuple(self._book_evidence_waiters.get(generation, ())):
            waiter.missing.discard(asset_id)
            if not waiter.missing and not waiter.future.done():
                waiter.future.set_result(True)

    async def _send_single_control_transaction(
        self,
        payload: dict[str, Any],
        *,
        commit: Callable[[], None] | None = None,
    ) -> bool:
        """Fence one control send, identity check, and optional state commit."""
        ws: Any = None
        generation = 0
        try:
            async with self._subscription_control_lock:
                ws = self._current_ws
                generation = self._connection_generation
                if ws is None:
                    operation = str(payload.get("operation") or "initial_subscribe")
                    self._record_runtime_event(
                        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                        reason_code="no_active_connection",
                        detail={
                            "operation": operation,
                            "error_type": "NoActiveConnection",
                        },
                        severity=RuntimeEventSeverity.WARNING,
                        generation=generation,
                    )
                    self._publish_l3_membership_locked()
                    return False
                succeeded = await self._send_control(ws, payload)
                identity_matches = (
                    self._current_ws is ws and self._connection_generation == generation
                )
                if succeeded and identity_matches:
                    if commit is not None:
                        commit()
                    self._publish_l3_membership_locked()
                    return True
                if succeeded:
                    operation = str(payload.get("operation") or "initial_subscribe")
                    self._record_runtime_event(
                        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                        reason_code="control_state_changed",
                        detail={
                            "operation": operation,
                            "error_type": "StateMismatch",
                        },
                        severity=RuntimeEventSeverity.WARNING,
                        generation=generation,
                    )
                self._publish_l3_membership_locked()
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
    def has_active_connection(self) -> bool:
        """Return whether a successfully initialized socket is published."""
        return self._current_ws is not None

    @property
    def subscribed_assets(self) -> list[str]:
        # Defensive copy of the candidate ∪ L3 union — Plan 05 candidate refresh
        # MUST NOT mutate via this property (returns a fresh list each call).
        return self._compute_active_assets()

    # ── Phase 05 Plan 02 — D-11 helpers (Pitfall 5 fix) ────────────────────

    def _compute_active_assets(self) -> list[str]:
        """Return sorted union of candidate and L3 reconnect intent.

        Called by:
          - subscribed_assets property (public)
          - run() loop (replaces direct self._subscribed_assets read)
          - _subscribed_assets backward-compat property getter
        """
        return sorted(self._candidate_set | self._l3_desired_set)

    async def request_book_refresh(
        self,
        *,
        required_asset_ids: frozenset[str] | None = None,
    ) -> bool:
        """Request an initial book dump for the required asset scope.

        Sending a request is transport activity, not business-data freshness:
        this method intentionally leaves candidate/L3 state, event timestamps,
        and the watchdog untouched. Only the receive path may advance them. A
        direct call refreshes the full active union; the quiet-market path passes
        the committed L3 set explicitly so unrelated candidates are not cycled.
        """
        waiter: _BookEvidenceWaiter | None = None
        ws: Any = None
        generation = 0
        active_assets: list[str] = []
        refresh_assets: list[str] = []
        required_assets: frozenset[str] = frozenset()
        failure_reason = "unexpected_exception"
        try:
            async with self._subscription_control_lock:
                active_assets = self._compute_active_assets()
                if required_asset_ids is not None:
                    required_assets = frozenset(required_asset_ids)
                    refresh_assets = sorted(required_assets)
                elif self._l3_desired_set:
                    required_assets = frozenset(self._l3_desired_set)
                    refresh_assets = active_assets
                else:
                    required_assets = frozenset(active_assets)
                    refresh_assets = active_assets
                ws = self._current_ws
                generation = self._connection_generation
                if not refresh_assets:
                    failure_reason = "no_active_assets"
                    raise RuntimeError("quiet refresh has no active assets")
                if not required_assets:
                    failure_reason = "no_required_assets"
                    raise RuntimeError("quiet refresh has no required assets")
                if ws is None:
                    failure_reason = "no_connection"
                    self._record_runtime_event(
                        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                        reason_code="no_active_connection",
                        detail={
                            "operation": "book_refresh",
                            "error_type": "NoActiveConnection",
                        },
                        severity=RuntimeEventSeverity.WARNING,
                        generation=generation,
                    )
                    raise RuntimeError("quiet refresh has no connection")
                waiter = _BookEvidenceWaiter(
                    future=asyncio.get_running_loop().create_future(),
                    missing=set(required_assets),
                )
                self._book_evidence_waiters.setdefault(generation, []).append(waiter)
                logger.info(f"ws quiet refresh: sending assets={len(refresh_assets)}")
                failure_reason = "unsubscribe_failed"
                if not await self._send_control(
                    ws,
                    {"operation": "unsubscribe", "assets_ids": refresh_assets},
                ):
                    raise RuntimeError("quiet unsubscribe failed")
                failure_reason = "generation_changed"
                if self._current_ws is not ws or self._connection_generation != generation:
                    raise RuntimeError("connection identity changed during refresh")
                failure_reason = "subscribe_failed"
                if not await self._send_control(
                    ws,
                    {
                        "operation": "subscribe",
                        "assets_ids": refresh_assets,
                        "initial_dump": True,
                    },
                ):
                    raise RuntimeError("quiet subscribe failed")
                failure_reason = "generation_changed"
                if self._current_ws is not ws or self._connection_generation != generation:
                    raise RuntimeError("connection identity changed during refresh")
            assert waiter is not None
            failure_reason = "evidence_timeout"
            first_wait_s = min(
                _BOOK_EVIDENCE_RETRY_AFTER_S,
                _BOOK_EVIDENCE_TIMEOUT_S / 2,
            )
            try:
                completed = await asyncio.wait_for(
                    asyncio.shield(waiter.future),
                    timeout=first_wait_s,
                )
            except TimeoutError:
                completed = False
            if not completed and not waiter.future.done():
                retry_assets = sorted(waiter.missing)
                async with self._subscription_control_lock:
                    failure_reason = "generation_changed"
                    if self._current_ws is not ws or self._connection_generation != generation:
                        raise RuntimeError("connection identity changed before refresh retry")
                    failure_reason = "unsubscribe_failed"
                    if not await self._send_control(
                        ws,
                        {"operation": "unsubscribe", "assets_ids": retry_assets},
                    ):
                        raise RuntimeError("quiet retry unsubscribe failed")
                    failure_reason = "generation_changed"
                    if self._current_ws is not ws or self._connection_generation != generation:
                        raise RuntimeError("connection identity changed during refresh retry")
                    failure_reason = "subscribe_failed"
                    if not await self._send_control(
                        ws,
                        {
                            "operation": "subscribe",
                            "assets_ids": retry_assets,
                            "initial_dump": True,
                        },
                    ):
                        raise RuntimeError("quiet retry subscribe failed")
                    failure_reason = "generation_changed"
                    if self._current_ws is not ws or self._connection_generation != generation:
                        raise RuntimeError("connection identity changed during refresh retry")
                failure_reason = "evidence_timeout"
                completed = await asyncio.wait_for(
                    asyncio.shield(waiter.future),
                    timeout=_BOOK_EVIDENCE_TIMEOUT_S - first_wait_s,
                )
            elif waiter.future.done():
                completed = waiter.future.result()
            if not completed:
                failure_reason = "generation_invalidated"
                raise RuntimeError("refresh generation invalidated")
            logger.info(f"ws quiet refresh: evidenced assets={len(required_assets)}")
            self._last_quiet_refresh_missing_assets = frozenset()
            self._last_quiet_refresh_missing_generation = None
            return True
        except asyncio.CancelledError:
            if ws is not None:
                await self._compensate_generation(ws, generation)
            raise
        except Exception as exc:  # noqa: BLE001 — ambiguity requires reconnect
            missing_assets = frozenset(waiter.missing if waiter is not None else required_assets)
            self._last_quiet_refresh_missing_assets = missing_assets
            self._last_quiet_refresh_missing_generation = generation
            logger.warning(
                "ws quiet refresh failed reason={} error_type={} generation={} "
                "total_count={} required_count={} missing_count={}",
                failure_reason,
                type(exc).__name__,
                generation,
                len(refresh_assets),
                len(required_assets),
                len(missing_assets),
            )
            if failure_reason == "evidence_timeout":
                self._record_runtime_event(
                    RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                    reason_code="evidence_timeout",
                    detail={
                        "operation": "book_refresh",
                        "error_type": "TimeoutError",
                        "required_count": len(required_assets),
                        "missing_count": len(missing_assets),
                    },
                    severity=RuntimeEventSeverity.WARNING,
                    generation=generation,
                )
            if ws is not None:
                await self._compensate_generation(ws, generation)
            return False
        finally:
            if waiter is not None:
                self._discard_book_evidence_waiter(generation, waiter)

    async def refresh_if_quiet(
        self,
        *,
        now_s: float | None = None,
        quiet_after_s: float = 60.0,
        retry_s: float = 30.0,
    ) -> bool | None:
        """Request a book dump when business frames are quiet and retry is due."""
        now = time.time() if now_s is None else now_s
        required_l3 = frozenset(self._l3_committed_set)
        if required_l3:
            evidence_times: list[float] = []
            missing_current_generation = False
            for asset_id in required_l3:
                evidence = self._l3_business_evidence.get(asset_id)
                if evidence is None or evidence[0] != self._connection_generation:
                    missing_current_generation = True
                    break
                evidence_times.append(evidence[1].timestamp())
            if missing_current_generation:
                initialized_at_s = self._connection_initialized_at_s
                if initialized_at_s is not None and now - initialized_at_s < quiet_after_s:
                    return None
            else:
                if evidence_times and now - min(evidence_times) < quiet_after_s:
                    return None
        elif now - self._last_event_at_s < quiet_after_s:
            return None
        if (
            self._last_quiet_refresh_attempt_at_s != 0.0
            and now - self._last_quiet_refresh_attempt_at_s < retry_s
        ):
            return None
        # Record before awaiting so a slow or failed send cannot create a storm.
        self._last_quiet_refresh_attempt_at_s = now
        retry_missing = frozenset()
        if self._last_quiet_refresh_missing_generation == self._connection_generation:
            retry_missing = self._last_quiet_refresh_missing_assets
        elif self._last_quiet_refresh_missing_generation is not None:
            self._last_quiet_refresh_missing_assets = frozenset()
            self._last_quiet_refresh_missing_generation = None
        return await self.request_book_refresh(
            required_asset_ids=retry_missing or required_l3 or None,
        )

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
        L3 desired state untouched. Emits DeprecationWarning so any remaining
        callers are visible in test output.
        """
        warnings.warn(
            "Direct assignment to WsConsumer._subscribed_assets is deprecated; "
            "use update_candidate_set(asset_ids) instead. The legacy setter "
            "interprets the new list as the candidate set only — L3 desired state "
            "is preserved (Phase 05 Pitfall 5 fix).",
            DeprecationWarning,
            stacklevel=2,
        )
        # NOTE: We do NOT subtract L3 desired tokens from the new candidate set.
        # Rationale: a token can legitimately be in BOTH sets simultaneously
        # (e.g. a high-liquidity candidate that ALSO got promoted to L3); the
        # union semantics in _compute_active_assets handle the overlap.
        self._candidate_set = set(new_list)

    def update_candidate_set(self, asset_ids: Iterable[str]) -> None:
        """Replace the L2-candidate portion of the subscription set.

        Phase 05 Plan 02 Task 3 — `l2_candidate_refresh.on_snapshot_complete`
        migrates from the legacy `_subscribed_assets = list(...)` overwrite to
        this dedicated helper, which leaves L3 desired state untouched (Pitfall 5).
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
                    self._record_runtime_event(
                        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
                        reason_code="no_active_connection",
                        detail={
                            "operation": "candidate_replace",
                            "error_type": "NoActiveConnection",
                        },
                        severity=RuntimeEventSeverity.WARNING,
                        generation=generation,
                    )
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
                    self._current_ws is not ws or self._connection_generation != generation
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
        """Commit L3 additions only after a current-generation control succeeds."""
        if not asset_ids:
            return True
        return await self._send_single_control_transaction(
            {
                "operation": "subscribe",
                "assets_ids": list(asset_ids),
                "initial_dump": True,
            },
            commit=lambda: self._l3_committed_set.update(asset_ids),
        )

    async def remove_subscriptions(self, asset_ids: list[str]) -> bool:
        """Commit L3 removals only after a current-generation control succeeds."""
        if not asset_ids:
            return True

        def _commit_remove() -> None:
            self._l3_committed_set.difference_update(asset_ids)
            for asset_id in asset_ids:
                self._l3_business_evidence.pop(asset_id, None)

        return await self._send_single_control_transaction(
            {"operation": "unsubscribe", "assets_ids": list(asset_ids)},
            commit=_commit_remove,
        )

    # ── Quick task 260602-ws-dynamic-subscribe ──────────────────────────────
    #
    # Payload-only subscribe/unsubscribe — dual of add_subscriptions /
    # remove_subscriptions for the L2 candidate-refresh flow. State mutation
    # (`_candidate_set`) is already handled by `update_candidate_set`; these
    # helpers ONLY push the mid-connection WS payload so the live socket
    # actually starts/stops receiving frames for the new asset_ids.
    #
    # Why a separate method: `add_subscriptions` mutates L3 committed state —
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
                f"(candidate={len(self._candidate_set)} l3={len(self._l3_desired_set)})"
            )

            async for event in stream_market_events(
                self._compute_active_assets,
                initial_dump=True,
                connection_initializer=self._initialize_connection,
                on_disconnect=self._release_connection,
                reconnect_retry_after=self._watchdog.reconnect_retry_after_s,
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
                    dispatch_result = self._on_event(event)
                    if isinstance(dispatch_result, Awaitable):
                        dispatch_result = await dispatch_result
                    if (
                        event.get("event_type") == "book"
                        and isinstance(dispatch_result, FrameDispatchResult)
                        and dispatch_result.observed_at is not None
                    ):
                        self.record_book_evidence(
                            asset_id=str(event.get("asset_id") or ""),
                            generation=self._connection_generation,
                            book_levels_succeeded=dispatch_result.book_levels_written,
                            observed_at=dispatch_result.observed_at,
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
            # Do NOT re-raise: returning lets the supervisor (l2_main) decide
            # whether to relaunch the consumer. Watchdog will mark RECONNECTING
            # via its stale_s timeout if no fresh touch arrives.
            return
        except asyncio.CancelledError:
            # F-04: must propagate.
            logger.info("ws_consumer: cancelled, propagating CancelledError")
            raise
        finally:
            # Every exit path publishes the same terminal truth. This is
            # intentionally idempotent with stream_market_events' on_disconnect
            # callback: normal exhaustion, stop/break, chaos, cancellation, and
            # unexpected exceptions must never leave a stale live socket or
            # current-generation membership behind.
            self._state = "DISCONNECTED"
            self._fail_book_evidence_waiters_locked(self._connection_generation)
            self._current_ws = None  # GAP-401: clear stash on disconnect
            self._clear_l3_connection_state_locked()
            self._publish_l3_membership_locked()
