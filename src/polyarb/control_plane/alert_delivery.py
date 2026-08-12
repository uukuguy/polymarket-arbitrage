"""One fenced turn for durable M1 notification-outbox delivery."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

import httpx

from polyarb.config import Settings

from .models import AlertDeliveryLease
from .postgres import PostgresControlPlane


@dataclass(frozen=True, slots=True)
class AlertDeliveryResult:
    outbox_id: str | None
    outcome: str


class _TelegramClient(Protocol):
    async def post(self, url: str, *, json: dict[str, object]) -> httpx.Response: ...


def incident_alert_channels(settings: Settings) -> tuple[str, ...]:
    """Select durable outbox channels from non-secret worker policy."""
    channels = tuple(
        channel.strip() for channel in settings.alert_channels.split(",") if channel.strip()
    )
    if not channels or len(set(channels)) != len(channels):
        raise ValueError("alert_channels must contain unique non-empty channels")
    if set(channels) - {"dashboard", "telegram"}:
        raise ValueError("alert_channels supports only dashboard and telegram")
    if "dashboard" not in channels:
        raise ValueError("alert_channels must include dashboard")
    return channels


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
        settings: Settings | None = None,
        telegram_client: _TelegramClient | None = None,
    ) -> None:
        if not worker_id or lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("alert delivery bounds and worker_id must be positive")
        self._control_plane = control_plane
        self._worker_id = worker_id
        self._now = now
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay
        self._settings = settings or Settings()
        self._telegram_client = telegram_client

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
        if lease.channel == "telegram":
            return await self._deliver_telegram(lease)
        self._control_plane.finish_alert_delivery(
            lease,
            state="retryable",
            error_class="UnsupportedAlertChannel",
            error_detail={"channel": lease.channel},
            next_attempt_at=self._now() + self._retry_delay,
            now=self._now(),
        )
        return AlertDeliveryResult(outbox_id=lease.outbox_id, outcome="retryable")

    async def _deliver_telegram(self, lease: AlertDeliveryLease) -> AlertDeliveryResult:
        token = self._settings.telegram_bot_token.get_secret_value()
        chat_id = self._settings.telegram_chat_id
        if not token or not chat_id:
            return self._retry(lease, "TelegramUnavailableError")
        try:
            response = await self._telegram_post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                {
                    "chat_id": chat_id,
                    "text": f"[{lease.payload['kind']}] {lease.payload['incident_key']}",
                },
            )
            response.raise_for_status()
            response_payload = response.json()
            message_id = response_payload["result"]["message_id"]
            if not response_payload.get("ok") or not isinstance(message_id, int):
                raise ValueError("Telegram response lacks a numeric message_id")
        except Exception as error:  # noqa: BLE001 - sender boundary must retain intent
            return self._retry(lease, type(error).__name__)
        self._control_plane.finish_alert_delivery(
            lease,
            state="delivered",
            provider_receipt=f"telegram:{message_id}",
            now=self._now(),
        )
        return AlertDeliveryResult(outbox_id=lease.outbox_id, outcome="delivered")

    async def _telegram_post(self, url: str, payload: dict[str, object]) -> httpx.Response:
        if self._telegram_client is not None:
            return await self._telegram_client.post(url, json=payload)
        async with httpx.AsyncClient(timeout=5.0) as client:
            return await client.post(url, json=payload)

    def _retry(self, lease: AlertDeliveryLease, error_class: str) -> AlertDeliveryResult:
        self._control_plane.finish_alert_delivery(
            lease,
            state="retryable",
            error_class=error_class,
            error_detail={"channel": "telegram"},
            next_attempt_at=self._now() + self._retry_delay,
            now=self._now(),
        )
        return AlertDeliveryResult(outbox_id=lease.outbox_id, outcome="retryable")
