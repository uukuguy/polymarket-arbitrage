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
from polyarb.perception.fault_adapters import (
    QualifiedTelegramTransportError,
    TelegramDeliveryFault,
)
from polyarb.perception.fault_control import FaultKind, FaultRecoveryWriter
from polyarb.perception.fault_runtime import (
    FaultRecoveryOutcome,
    FaultRuntimeProtocol,
    PassThroughFaultRuntime,
    cleanup_active_fault,
)
from polyarb.perception.notification_incidents import NotificationIncidents
from polyarb.perception.store import OpportunityPerceptionStore
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
_TELEGRAM_CARD_LIMIT = 4_000


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
        fault_runtime: FaultRuntimeProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._ledger = ledger or OpportunityLedger(settings.db_path)
        self._send_telegram = send_telegram
        self._clock_ms = clock_ms or _wall_clock_ms
        self._focused_reader = focused_reader
        self._membership_reader = membership_reader
        self._wait_for_stop = wait_for_stop or _wait_for_stop
        self._focused_interval_s = focused_interval_s
        self._fault_runtime = fault_runtime or PassThroughFaultRuntime()
        self._telegram_fault = TelegramDeliveryFault(runtime=self._fault_runtime)
        self._reconciliation_count = 0
        self._last_reconciled_at_ms: int | None = None
        self._notification_delivery_count = 0
        self._notification_failure_count = 0
        self._last_notification_error_kind: str | None = None
        self._notification_incidents = NotificationIncidents(
            OpportunityPerceptionStore(settings.db_path),
            clock_ms=self._clock_ms,
        )

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
        fault_runtime: FaultRuntimeProtocol | None = None,
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
            fault_runtime=fault_runtime,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll only durable active masters and preserve every completed fact."""
        try:
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
        finally:
            await cleanup_active_fault(
                self._fault_runtime,
                reason="notification-stopped",
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
        active_identity_keys = await asyncio.to_thread(
            self._ledger.active_identity_keys
        )
        for assessment in assessed.assessments:
            # Incomplete groups are not zero-price/no-edge evidence.  The
            # current global ledger only accepts complete observe/no-edge facts,
            # so leave unavailable groups unchanged until focused invalidation
            # handling is introduced.
            if assessment.status == "unavailable":
                continue
            if assessment.status == "no-edge" and (
                assessment.event_id,
                assessment.group_id,
                assessment.membership_hash,
            ) not in active_identity_keys:
                # The ledger's no-edge path is intentionally a no-op when no
                # matching master exists.  Filter those overwhelmingly common
                # groups with one bounded read instead of opening and
                # committing one SQLite transaction per group.
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
        await self._fault_runtime.sync_before_batch()
        notifications = await asyncio.to_thread(
            self._ledger.pending_notifications,
            now_ms=self._clock_ms(),
        )
        for notification in notifications:
            try:
                await self._telegram_fault.before_send(notification.id)
                await self._send_telegram(
                    self._settings,
                    _format_card(notification),
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:  # delivery is deliberately retryable
                self._notification_failure_count += 1
                error_kind = type(error).__name__
                self._last_notification_error_kind = error_kind
                try:
                    failed_attempt, cancellation = await self._settle_attempt_write(
                        lambda: self._ledger.mark_notification_failed(
                            notification.id,
                            attempted_at_ms=self._clock_ms(),
                            error_kind=error_kind,
                        )
                    )
                except asyncio.CancelledError:
                    if isinstance(error, QualifiedTelegramTransportError):
                        await self._settle_operation(
                            self._fault_runtime.cleanup(
                                error.fault_id,
                                "notification-attempt-uncommitted",
                            )
                        )
                    raise
                except Exception as store_error:
                    if isinstance(error, QualifiedTelegramTransportError):
                        await self._settle_operation(
                            self._fault_runtime.evidence_unavailable(
                                error.fault_id,
                                "notification-failed-attempt-unavailable",
                            )
                        )
                    logger.warning(
                        "opportunity notification attempt unavailable "
                        f"id={notification.id} kind={type(store_error).__name__}"
                    )
                    continue
                if failed_attempt is None:
                    if isinstance(error, QualifiedTelegramTransportError):
                        _, evidence_cancellation = await self._settle_operation(
                            self._fault_runtime.evidence_unavailable(
                                error.fault_id,
                                "notification-failed-attempt-unavailable",
                            )
                        )
                        cancellation = cancellation or evidence_cancellation
                    if cancellation is not None:
                        raise cancellation
                    continue
                _, evidence_cancellation = await self._settle_operation(
                    self._record_failed_delivery(
                        notification,
                        failed_attempt,
                        error,
                    )
                )
                cancellation = cancellation or evidence_cancellation
                logger.warning(
                    "opportunity notification delivery failed "
                    f"id={notification.id} kind={type(error).__name__}"
                )
                if cancellation is not None:
                    raise cancellation
            else:
                self._notification_delivery_count += 1
                self._last_notification_error_kind = None
                try:
                    delivered_attempt, cancellation = await self._settle_attempt_write(
                        lambda: self._ledger.mark_notification_delivered(
                            notification.id,
                            delivered_at_ms=self._clock_ms(),
                        )
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as store_error:
                    logger.warning(
                        "opportunity notification attempt unavailable "
                        f"id={notification.id} kind={type(store_error).__name__}"
                    )
                    continue
                if delivered_attempt is not None:
                    _, evidence_cancellation = await self._settle_operation(
                        self._record_delivered_delivery(
                            notification,
                            delivered_attempt,
                        )
                    )
                    cancellation = cancellation or evidence_cancellation
                if cancellation is not None:
                    raise cancellation
        try:
            await asyncio.to_thread(
                self._notification_incidents.reconcile_delivered,
            )
        except Exception as error:
            logger.warning(
                "notification incident reconciliation failed "
                f"kind={type(error).__name__}"
            )

    async def _record_failed_delivery(
        self,
        notification: PendingNotification,
        failed_attempt,
        error: BaseException,
    ) -> None:
        if isinstance(error, QualifiedTelegramTransportError):
            try:
                receipt = await asyncio.to_thread(
                    self._notification_incidents.record_qualified_failure,
                    notification_id=notification.id,
                    failed_attempt_id=failed_attempt.id,
                    error_kind=type(error).__name__,
                    fault_call_id=error.call_id,
                )
                valid = (
                    receipt is not None
                    and await asyncio.to_thread(
                        self._notification_incidents.validate_qualified_receipt,
                        receipt,
                    )
                )
            except Exception:
                await self._fault_runtime.evidence_unavailable(
                    error.fault_id,
                    "notification-incident-evidence-unavailable",
                )
                return
            if not valid:
                await self._fault_runtime.invalidate_evidence(
                    error.fault_id,
                    "notification-incident-evidence-invalid",
                )
            elif await self._fault_runtime.link_detection(
                error.fault_id,
                kind=FaultKind.TELEGRAM_FAILURE,
                detection_id=receipt.incident_id,
            ):
                await self._fault_runtime.cleanup(
                    error.fault_id,
                    "notification-delivery-failed",
                )
            else:
                await self._fault_runtime.evidence_unavailable(
                    error.fault_id,
                    "notification-detection-link-unavailable",
                )
            return
        await asyncio.to_thread(
            self._notification_incidents.record_failure,
            notification_id=notification.id,
            failed_attempt_id=failed_attempt.id,
            error_kind=type(error).__name__,
        )

    async def _record_delivered_delivery(
        self,
        notification: PendingNotification,
        delivered_attempt,
    ) -> None:
        outcome = await self._fault_runtime.record_writer_recovery_outcome(
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
            target_key=str(notification.id),
            writer_id=delivered_attempt.id,
            writer_occurred_at_ms=delivered_attempt.attempted_at_ms,
        )
        if outcome in {
            FaultRecoveryOutcome.RECORDED,
            FaultRecoveryOutcome.NOT_APPLICABLE,
        }:
            await asyncio.to_thread(
                self._notification_incidents.verify_delivery,
                notification_id=notification.id,
                delivered_attempt_id=delivered_attempt.id,
            )

    @staticmethod
    async def _settle_attempt_write(call):
        return await OpportunityWatcher._settle_operation(asyncio.to_thread(call))

    @staticmethod
    async def _settle_operation(operation):
        task = asyncio.create_task(operation)
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                result = await asyncio.shield(task)
                return result, cancellation
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
                continue
            except BaseException as error:
                if cancellation is not None:
                    raise cancellation from error
                raise

    def snapshot(self) -> OpportunityWatcherSnapshot:
        return OpportunityWatcherSnapshot(
            reconciliation_count=self._reconciliation_count,
            last_reconciled_at_ms=self._last_reconciled_at_ms,
            notification_delivery_count=self._notification_delivery_count,
            notification_failure_count=self._notification_failure_count,
            last_notification_error_kind=self._last_notification_error_kind,
        )

    def candidate_group_ids(self) -> tuple[str, ...]:
        """Seed Slice B from durable legacy masters until Discovery owns promotion."""
        masters = self._ledger.active_masters()
        return tuple(dict.fromkeys(master.group_id for master in masters))

    def _observe_min_edge_bps(self) -> float:
        # Task 5 promotes this production default into Settings.  Retaining it
        # here keeps Task 3 compatible with the current Settings contract.
        return float(getattr(self._settings, "neg_risk_observe_min_edge_bps", 100.0))


def _format_card(notification: PendingNotification) -> str:
    payload = notification.payload
    fields_without_legs = (
        ("reason", notification.reason),
        ("status", payload.get("status", "unknown")),
        ("strategy", payload.get("strategy", "neg-risk-buy-all")),
        ("event_id", payload.get("event_id", "unknown")),
        ("group_id", payload.get("group_id", "unknown")),
        ("membership_hash", payload.get("membership_hash", "unknown")),
        ("bundle_cost", payload.get("bundle_cost", "unknown")),
        ("gross_edge_bps", payload.get("gross_edge_bps", "unknown")),
        ("max_bundle_size", payload.get("max_bundle_size", "unknown")),
        ("structure_revision", payload.get("structure_revision", "unknown")),
        ("quote_run_id", payload.get("quote_run_id", "unknown")),
        ("quoted_at_ms", payload.get("quoted_at_ms", "unknown")),
        ("transition_reason", payload.get("transition_reason", notification.reason)),
        ("execution_status", "not-verified"),
    )
    raw_legs = payload.get("legs", "unknown")
    full_fields = fields_without_legs[:6] + (
        ("legs", json.dumps(raw_legs, separators=(",", ":"), sort_keys=True)),
    ) + fields_without_legs[6:]
    full_card = "\n".join(
        ["Polymarket neg-risk observation"]
        + [f"{key}={value}" for key, value in full_fields]
        + [_OBSERVER_WARNING]
    )
    if len(full_card) <= _TELEGRAM_CARD_LIMIT:
        return full_card

    legs = raw_legs if isinstance(raw_legs, list) else []
    preview: list[object] = []
    bounded_card = ""
    for leg in legs:
        candidate = preview + [leg]
        truncated = len(legs) - len(candidate)
        preview_fields = fields_without_legs[:6] + (
            ("legs_count", len(legs)),
            ("legs_preview", json.dumps(candidate, separators=(",", ":"), sort_keys=True)),
            ("legs_truncated", truncated),
        ) + fields_without_legs[6:]
        candidate_card = "\n".join(
            ["Polymarket neg-risk observation"]
            + [f"{key}={value}" for key, value in preview_fields]
            + [_OBSERVER_WARNING]
        )
        if len(candidate_card) > _TELEGRAM_CARD_LIMIT:
            break
        preview = candidate
        bounded_card = candidate_card

    if not bounded_card:
        preview_fields = fields_without_legs[:6] + (
            ("legs_count", len(legs)),
            ("legs_preview", "[]"),
            ("legs_truncated", len(legs)),
        ) + fields_without_legs[6:]
        bounded_card = "\n".join(
            ["Polymarket neg-risk observation"]
            + [f"{key}={value}" for key, value in preview_fields]
            + [_OBSERVER_WARNING]
        )
    if len(bounded_card) > _TELEGRAM_CARD_LIMIT:
        suffix = "\ncard_truncated=true\n" + _OBSERVER_WARNING
        bounded_card = bounded_card[: _TELEGRAM_CARD_LIMIT - len(suffix)] + suffix
    return bounded_card


def _wall_clock_ms() -> int:
    return int(time.time() * 1000)


async def _wait_for_stop(stop_event: asyncio.Event, delay_s: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
    except TimeoutError:
        return False
    return True


def build_focused_opportunity_watcher(
    settings: Settings,
    *,
    fault_runtime: FaultRuntimeProtocol | None = None,
) -> OpportunityWatcher:
    """Build the local observer loop with the CLOB client's existing limiter."""
    return OpportunityWatcher(
        settings,
        focused_reader=ClobReaderClient(settings),
        membership_reader=SqliteStructureMembershipReader(settings.db_path),
        focused_interval_s=settings.neg_risk_focused_interval_s,
        fault_runtime=fault_runtime,
    )
