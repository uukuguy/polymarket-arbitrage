from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from polyarb.control_plane.alert_delivery import TransactionalAlertDeliveryWorker
from polyarb.control_plane.models import AlertDeliveryLease

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class _ControlPlane:
    def __init__(self) -> None:
        self.finished: dict[str, object] | None = None

    def claim_alert_delivery(self, **kwargs: object) -> AlertDeliveryLease:
        return AlertDeliveryLease(
            outbox_id="outbox-a",
            incident_event_id="event-a",
            channel="dashboard",
            payload={"incident_key": "incident-a", "kind": "attempt-failed"},
            lease_owner="alert-a",
            lease_epoch=1,
            lease_expires_at=NOW,
            attempt_number=1,
        )

    def finish_alert_delivery(self, lease: AlertDeliveryLease, **kwargs: object) -> None:
        assert lease.outbox_id == "outbox-a"
        self.finished = kwargs


def test_dashboard_delivery_records_a_visible_receipt() -> None:
    control_plane = _ControlPlane()
    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane, worker_id="alert-a", now=lambda: NOW
    )

    assert asyncio.run(worker.run_once()).outcome == "delivered"
    assert control_plane.finished == {
        "state": "delivered",
        "provider_receipt": "dashboard-visible",
        "now": NOW,
    }
