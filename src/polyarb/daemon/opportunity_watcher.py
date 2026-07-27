"""Observer-only reconciliation of certified neg-risk quote projections."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loguru import logger

from polyarb.config import Settings
from polyarb.daemon.alerts import send_opportunity_alert
from polyarb.routing.neg_risk_quote_store import CompleteQuoteProjection
from polyarb.routing.opportunity_ledger import OpportunityLedger, PendingNotification
from polyarb.routing.opportunity_scanner import (
    assess_certified_neg_risk_quote_projection,
)

SendTelegram = Callable[[Settings, str], Awaitable[None]]
ClockMs = Callable[[], int]

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
    ) -> None:
        self._settings = settings
        self._ledger = ledger or OpportunityLedger(settings.db_path)
        self._send_telegram = send_telegram
        self._clock_ms = clock_ms or _wall_clock_ms
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
    ) -> OpportunityWatcher:
        """Construct with explicit durable and transport seams for unit tests."""
        return cls(
            settings,
            ledger=ledger,
            send_telegram=send_telegram,
            clock_ms=clock_ms,
        )

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
