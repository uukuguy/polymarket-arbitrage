"""One fenced turn for durable M1 notification-outbox delivery."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from .postgres import PostgresControlPlane


@dataclass(frozen=True, slots=True)
class AlertDeliveryResult:
    outbox_id: str | None
    outcome: str


class TransactionalAlertDeliveryWorker:
    """Deliver one alert intent without coupling notification availability to collection."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        worker_id: str,
        now: Callable[[], datetime],
        lease_seconds: int = 30,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not worker_id or lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("alert delivery bounds and worker_id must be positive")
        self._control_plane = control_plane
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    async def run_once(self) -> AlertDeliveryResult:
        lease = self._control_plane.claim_alert_delivery(
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return AlertDeliveryResult(outbox_id=None, outcome="idle")
        if lease.channel == "dashboard":
            self._control_plane.finish_alert_delivery(
                lease,
                state="delivered",
                provider_receipt="dashboard-visible",
                now=self._now(),
            )
            return AlertDeliveryResult(outbox_id=lease.outbox_id, outcome="delivered")
        self._control_plane.finish_alert_delivery(
            lease,
            state="retryable",
            error_class="UnsupportedAlertChannel",
            error_detail={"channel": lease.channel},
            next_attempt_at=self._now() + self._retry_delay,
            now=self._now(),
        )
        return AlertDeliveryResult(outbox_id=lease.outbox_id, outcome="retryable")
