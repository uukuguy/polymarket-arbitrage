"""Observer-only reconciliation of certified neg-risk quote projections."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loguru import logger

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.config import Settings
from polyarb.daemon.alerts import send_opportunity_alert
from polyarb.routing.focused_quote_collector import (
    BooksReader,
    FocusedObservation,
    MembershipReader,
    SqliteStructureMembershipReader,
    collect_focused_observation,
)
from polyarb.routing.neg_risk_quote_store import CompleteQuoteProjection
from polyarb.routing.opportunity_ledger import (
    FocusedObservationStaleError,
    OpportunityLedger,
    PendingNotification,
)
from polyarb.routing.opportunity_scanner import (
    assess_certified_neg_risk_quote_projection,
)

SendTelegram = Callable[[Settings, str], Awaitable[None]]
ClockMs = Callable[[], int]
WaitForStop = Callable[[asyncio.Event, float], Awaitable[bool]]

_OBSERVER_WARNING = "仅观察，未扣手续费、滑点和多腿成交风险"


@dataclass(frozen=True)
class OpportunityWatcherSnapshot:
    """Small process-local delivery state; lifecycle truth remains durable."""

    reconciliation_count: int
    last_reconciled_at_ms: int | None
    notification_delivery_count: int
    notification_failure_count: int
    last_notification_error_kind: str | None


class OpportunityWatcher:
    """Persist global observer facts and deliver their durable outbox cards."""

    def __init__(
        self,
        settings: Settings,
        *,
        ledger: OpportunityLedger | None = None,
        send_telegram: SendTelegram = send_opportunity_alert,
        clock_ms: ClockMs | None = None,
        focused_reader: BooksReader | None = None,
        membership_reader: MembershipReader | None = None,
        wait_for_stop: WaitForStop | None = None,
        focused_interval_s: float = 15.0,
    ) -> None:
        self._settings = settings
        self._ledger = ledger or OpportunityLedger(settings.db_path)
        self._send_telegram = send_telegram
        self._clock_ms = clock_ms or _wall_clock_ms
        self._focused_reader = focused_reader
        self._membership_reader = membership_reader
        self._wait_for_stop = wait_for_stop or _wait_for_stop
        self._focused_interval_s = focused_interval_s
        self._reconciliation_count = 0
        self._last_reconciled_at_ms: int | None = None
        self._notification_delivery_count = 0
        self._notification_failure_count = 0
        self._last_notification_error_kind: str | None = None

    @classmethod
    def for_test(
        cls,
        settings: Settings,
        *,
        ledger: OpportunityLedger,
        send_telegram: SendTelegram = send_opportunity_alert,
        clock_ms: ClockMs | None = None,
        focused_reader: BooksReader | None = None,
        membership_reader: MembershipReader | None = None,
        wait_for_stop: WaitForStop | None = None,
        focused_interval_s: float = 15.0,
    ) -> OpportunityWatcher:
        """Construct with explicit durable and transport seams for unit tests."""
        return cls(
            settings,
            ledger=ledger,
            send_telegram=send_telegram,
            clock_ms=clock_ms,
            focused_reader=focused_reader,
            membership_reader=membership_reader,
            wait_for_stop=wait_for_stop,
            focused_interval_s=focused_interval_s,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll only durable active masters and preserve every completed fact."""
        if self._focused_reader is None or self._membership_reader is None:
            logger.warning("focused opportunity watcher has no collection dependencies")
            return
        while not stop_event.is_set():
            masters = await asyncio.to_thread(self._ledger.active_masters)
            for master in masters:
                try:
                    observation = await collect_focused_observation(
                        master,
                        reader=self._focused_reader,
                        membership_reader=self._membership_reader,
                        now_ms=self._clock_ms,
                        min_edge_bps=self._observe_min_edge_bps(),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    logger.warning(
                        "focused opportunity quote fetch failed "
                        f"opportunity_id={master.id} kind={type(error).__name__}"
                    )
                    observation = FocusedObservation(
                        opportunity_id=master.id,
                        status="unavailable",
                        reason="clob-fetch-failed",
                        bundle_cost=None,
                        gross_edge_bps=None,
                        max_bundle_size=None,
                        legs=(),
                        structure_revision=master.structure_revision,
                        quote_run_id=master.quote_run_id,
                        observed_at_ms=self._clock_ms(),
                    )
                try:
                    await asyncio.to_thread(self._ledger.record_focused, observation)
                except asyncio.CancelledError:
                    raise
                except FocusedObservationStaleError:
                    logger.warning(
                        "focused observation dropped after global reconciliation "
                        f"opportunity_id={master.id}"
                    )
            await self.deliver_pending_notifications()
            if await self._wait_for_stop(stop_event, self._focused_interval_s):
                break

    async def reconcile_global_projection(
        self,
        projection: CompleteQuoteProjection,
    ) -> None:
        """Reconcile only an already-certified complete global Quote projection."""
        assessed = await asyncio.to_thread(
            assess_certified_neg_risk_quote_projection,
            projection,
            min_edge_bps=self._observe_min_edge_bps(),
        )
        for assessment in assessed.assessments:
            # Incomplete groups are not zero-price/no-edge evidence.  The
            # current global ledger only accepts complete observe/no-edge facts,
            # so leave unavailable groups unchanged until focused invalidation
            # handling is introduced.
            if assessment.status == "unavailable":
                continue
            await asyncio.to_thread(
                self._ledger.reconcile_global,
                assessment,
                observed_at_ms=self._clock_ms(),
            )
        self._reconciliation_count += 1
        self._last_reconciled_at_ms = self._clock_ms()
        await self.deliver_pending_notifications()

    async def deliver_pending_notifications(self) -> None:
        """Attempt every durable card without changing its market observation."""
        notifications = await asyncio.to_thread(
            self._ledger.pending_notifications,
            now_ms=self._clock_ms(),
        )
        for notification in notifications:
            try:
                await self._send_telegram(
                    self._settings,
                    _format_card(notification),
                )
            except Exception as error:  # delivery is deliberately retryable
                self._notification_failure_count += 1
                self._last_notification_error_kind = type(error).__name__
                await asyncio.to_thread(
                    self._ledger.mark_notification_failed,
                    notification.id,
                    attempted_at_ms=self._clock_ms(),
                    error_kind=type(error).__name__,
                )
                logger.warning(
                    "opportunity notification delivery failed "
                    f"id={notification.id} kind={type(error).__name__}"
                )
            else:
                self._notification_delivery_count += 1
                self._last_notification_error_kind = None
                await asyncio.to_thread(
                    self._ledger.mark_notification_delivered,
                    notification.id,
                    delivered_at_ms=self._clock_ms(),
                )

    def snapshot(self) -> OpportunityWatcherSnapshot:
        return OpportunityWatcherSnapshot(
            reconciliation_count=self._reconciliation_count,
            last_reconciled_at_ms=self._last_reconciled_at_ms,
            notification_delivery_count=self._notification_delivery_count,
            notification_failure_count=self._notification_failure_count,
            last_notification_error_kind=self._last_notification_error_kind,
        )

    def _observe_min_edge_bps(self) -> float:
        # Task 5 promotes this production default into Settings.  Retaining it
        # here keeps Task 3 compatible with the current Settings contract.
        return float(getattr(self._settings, "neg_risk_observe_min_edge_bps", 100.0))


def _format_card(notification: PendingNotification) -> str:
    payload = notification.payload
    fields = (
        ("reason", notification.reason),
        ("status", payload.get("status", "unknown")),
        ("strategy", payload.get("strategy", "neg-risk-buy-all")),
        ("event_id", payload.get("event_id", "unknown")),
        ("group_id", payload.get("group_id", "unknown")),
        ("membership_hash", payload.get("membership_hash", "unknown")),
        (
            "legs",
            json.dumps(payload.get("legs", "unknown"), separators=(",", ":"), sort_keys=True),
        ),
        ("bundle_cost", payload.get("bundle_cost", "unknown")),
        ("gross_edge_bps", payload.get("gross_edge_bps", "unknown")),
        ("max_bundle_size", payload.get("max_bundle_size", "unknown")),
        ("structure_revision", payload.get("structure_revision", "unknown")),
        ("quote_run_id", payload.get("quote_run_id", "unknown")),
        ("quoted_at_ms", payload.get("quoted_at_ms", "unknown")),
        ("transition_reason", payload.get("transition_reason", notification.reason)),
        ("execution_status", "not-verified"),
    )
    return "\n".join(
        ["Polymarket neg-risk observation"]
        + [f"{key}={value}" for key, value in fields]
        + [_OBSERVER_WARNING]
    )


def _wall_clock_ms() -> int:
    return int(time.time() * 1000)


async def _wait_for_stop(stop_event: asyncio.Event, delay_s: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
    except TimeoutError:
        return False
    return True


def build_focused_opportunity_watcher(settings: Settings) -> OpportunityWatcher:
    """Build the local observer loop with the CLOB client's existing limiter."""
    return OpportunityWatcher(
        settings,
        focused_reader=ClobReaderClient(settings),
        membership_reader=SqliteStructureMembershipReader(settings.db_path),
        focused_interval_s=settings.neg_risk_focused_interval_s,
    )
