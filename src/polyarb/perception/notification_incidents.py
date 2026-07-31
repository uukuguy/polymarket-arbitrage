"""Durable lifecycle for opportunity notification delivery failures."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass

from polyarb.perception.fault_control import normalize_fault_call_id
from polyarb.perception.incidents import Incident, IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.opportunity_ledger import NotificationAttempt


@dataclass(frozen=True, slots=True)
class QualifiedNotificationIncidentReceipt:
    incident_id: str
    detection_event_id: int
    detection_sequence: int
    scope: str
    kind: str
    fault_call_id: str


class NotificationIncidents:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._store = store
        self._manager = IncidentManager(store, clock_ms=clock_ms)

    def record_failure(
        self,
        *,
        notification_id: int,
        failed_attempt_id: int,
        error_kind: str,
    ) -> Incident:
        incident, _ = self._record_failure(
            notification_id=notification_id,
            failed_attempt_id=failed_attempt_id,
            error_kind=error_kind,
            fault_call_id=None,
        )
        return incident

    def record_qualified_failure(
        self,
        *,
        notification_id: int,
        failed_attempt_id: int,
        error_kind: str,
        fault_call_id: str,
    ) -> QualifiedNotificationIncidentReceipt | None:
        try:
            call_id = normalize_fault_call_id(fault_call_id)
        except (TypeError, ValueError):
            return None
        _, receipt = self._record_failure(
            notification_id=notification_id,
            failed_attempt_id=failed_attempt_id,
            error_kind=error_kind,
            fault_call_id=call_id,
        )
        return receipt

    def _record_failure(
        self,
        *,
        notification_id: int,
        failed_attempt_id: int,
        error_kind: str,
        fault_call_id: str | None,
    ) -> tuple[Incident, QualifiedNotificationIncidentReceipt | None]:
        scope = f"notification:{notification_id}"
        evidence: dict[str, object] = {
            "error_kind": error_kind,
            "failed_attempt_id": failed_attempt_id,
            "notification_id": notification_id,
        }
        if fault_call_id is not None:
            evidence["fault_call_id"] = fault_call_id
        incident = self._manager.detect(
            scope,
            "telegram-delivery-failed",
            evidence,
        )
        receipt: QualifiedNotificationIncidentReceipt | None = None
        if (
            fault_call_id is not None
            and incident.sequence == 1
            and incident.state == "detected"
            and incident.evidence.get("fault_call_id") == fault_call_id
        ):
            history = self._manager.incident_history(incident.id, limit=1)
            if history is not None and history.history_complete:
                detected = history.items[0]
                receipt = QualifiedNotificationIncidentReceipt(
                    incident_id=incident.id,
                    detection_event_id=detected.event_id,
                    detection_sequence=incident.sequence,
                    scope=scope,
                    kind=incident.kind,
                    fault_call_id=fault_call_id,
                )
        if incident.state == "detected":
            incident = self._manager.transition(
                incident.id,
                "classified",
                {"class": "telegram-transport"},
            )
        if incident.state == "classified":
            incident = self._manager.transition(
                incident.id,
                "contained",
                {"policy": "retain-durable-outbox"},
            )
        if incident.state in {"contained", "escalated"}:
            incident = self._manager.transition(
                incident.id,
                "recovering",
                {
                    "failed_attempt_id": failed_attempt_id,
                    "notification_id": notification_id,
                },
            )
        return incident, receipt

    def validate_qualified_receipt(
        self,
        receipt: QualifiedNotificationIncidentReceipt,
    ) -> bool:
        if (
            not isinstance(receipt, QualifiedNotificationIncidentReceipt)
            or receipt.detection_sequence != 1
            or receipt.kind != "telegram-delivery-failed"
            or not receipt.scope.startswith("notification:")
        ):
            return False
        history = self._manager.incident_history(receipt.incident_id, limit=100)
        if history is None or not history.history_complete:
            return False
        matches = tuple(
            item
            for item in history.items
            if (
                item.event_id == receipt.detection_event_id
                and item.incident.sequence == receipt.detection_sequence
                and item.incident.scope == receipt.scope
                and item.incident.kind == receipt.kind
                and item.incident.state == "detected"
                and item.incident.evidence.get("fault_call_id")
                == receipt.fault_call_id
            )
        )
        return len(matches) == 1

    def verify_delivery(
        self,
        *,
        notification_id: int,
        delivered_attempt_id: int,
    ) -> NotificationAttempt | None:
        scope = f"notification:{notification_id}"
        attempt = self._notification_attempt(
            notification_id=notification_id,
            attempt_id=delivered_attempt_id,
        )
        if attempt is None or attempt.outcome != "delivered" or attempt.error_kind is not None:
            return None
        for incident in self._manager.open_incidents():
            if (
                incident.scope == scope
                and incident.kind == "telegram-delivery-failed"
                and incident.state == "recovering"
            ):
                self._manager.transition(
                    incident.id,
                    "verified",
                    {
                        "delivered_attempt_id": delivered_attempt_id,
                        "notification_id": notification_id,
                    },
                )
                return attempt
        return None

    def reconcile_delivered(self) -> None:
        for incident in self._manager.open_incidents():
            if (
                not incident.scope.startswith("notification:")
                or incident.kind != "telegram-delivery-failed"
                or incident.state != "recovering"
            ):
                continue
            try:
                notification_id = int(incident.scope.split(":", 1)[1])
            except ValueError:
                continue
            with sqlite3.connect(
                f"file:{self._store.db_path}?mode=ro",
                uri=True,
                timeout=5,
            ) as con:
                con.row_factory = sqlite3.Row
                row = con.execute(
                    "SELECT id FROM neg_risk_opportunity_notification_attempts "
                    "WHERE notification_id=? AND outcome='delivered' "
                    "ORDER BY id DESC LIMIT 1",
                    (notification_id,),
                ).fetchone()
            if row is not None:
                self.verify_delivery(
                    notification_id=notification_id,
                    delivered_attempt_id=int(row["id"]),
                )

    def _notification_attempt(
        self,
        *,
        notification_id: int,
        attempt_id: int,
    ) -> NotificationAttempt | None:
        with sqlite3.connect(
            f"file:{self._store.db_path}?mode=ro",
            uri=True,
            timeout=5,
        ) as con:
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT id,notification_id,attempted_at_ms,outcome,error_kind "
                "FROM neg_risk_opportunity_notification_attempts "
                "WHERE id=? AND notification_id=?",
                (attempt_id, notification_id),
            ).fetchone()
        if row is None:
            return None
        return NotificationAttempt(
            id=int(row["id"]),
            notification_id=int(row["notification_id"]),
            attempted_at_ms=int(row["attempted_at_ms"]),
            outcome=str(row["outcome"]),
            error_kind=(
                None if row["error_kind"] is None else str(row["error_kind"])
            ),
        )


__all__ = ["NotificationIncidents", "QualifiedNotificationIncidentReceipt"]
