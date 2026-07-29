"""Durable lifecycle for opportunity notification delivery failures."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore


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
    ) -> None:
        scope = f"notification:{notification_id}"
        incident = self._manager.detect(
            scope,
            "telegram-delivery-failed",
            {
                "error_kind": error_kind,
                "failed_attempt_id": failed_attempt_id,
                "notification_id": notification_id,
            },
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
            self._manager.transition(
                incident.id,
                "recovering",
                {
                    "failed_attempt_id": failed_attempt_id,
                    "notification_id": notification_id,
                },
            )

    def verify_delivery(
        self,
        *,
        notification_id: int,
        delivered_attempt_id: int,
    ) -> None:
        scope = f"notification:{notification_id}"
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


__all__ = ["NotificationIncidents"]
