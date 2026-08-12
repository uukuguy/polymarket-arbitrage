from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx

from polyarb.config import Settings
from polyarb.control_plane.alert_delivery import (
    TransactionalAlertDeliveryWorker,
    incident_alert_channels,
)
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
        assert lease.outbox_id in {"outbox-a", "outbox-tg"}
        self.finished = kwargs


class _TelegramClient:
    def __init__(self, response: httpx.Response | Exception) -> None:
        self._response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def post(self, url: str, *, json: dict[str, object]) -> httpx.Response:
        self.calls.append((url, json))
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


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


def test_telegram_delivery_persists_provider_message_id() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg", incident_event_id="event-tg", channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a", lease_epoch=1, lease_expires_at=NOW, attempt_number=1,
    )
    client = _TelegramClient(
        httpx.Response(
            200,
            json={"ok": True, "result": {"message_id": 42}},
            request=httpx.Request("POST", "https://api.telegram.org"),
        )
    )
    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane,
        worker_id="alert-a",
        now=lambda: NOW,
        settings=Settings(telegram_bot_token="token", telegram_chat_id="chat"),
        telegram_client=client,
    )

    assert asyncio.run(worker.run_once()).outcome == "delivered"
    assert client.calls[0][1]["text"] == "[attempt-failed] incident-a"
    assert control_plane.finished == {
        "state": "delivered", "provider_receipt": "telegram:42", "now": NOW
    }


def test_telegram_http_failure_is_a_retryable_delivery_receipt() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg", incident_event_id="event-tg", channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a", lease_epoch=1, lease_expires_at=NOW, attempt_number=1,
    )
    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane,
        worker_id="alert-a",
        now=lambda: NOW,
        settings=Settings(telegram_bot_token="token", telegram_chat_id="chat"),
        telegram_client=_TelegramClient(
            httpx.Response(503, request=httpx.Request("POST", "https://api.telegram.org"))
        ),
    )

    assert asyncio.run(worker.run_once()).outcome == "retryable"
    assert control_plane.finished == {
        "state": "retryable",
        "error_class": "HTTPStatusError",
        "error_detail": {"channel": "telegram"},
        "next_attempt_at": NOW + timedelta(seconds=15),
        "now": NOW,
    }


def test_unconfigured_telegram_preserves_outbox_for_retry() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg", incident_event_id="event-tg", channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a", lease_epoch=1, lease_expires_at=NOW, attempt_number=1,
    )
    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane,
        worker_id="alert-a",
        now=lambda: NOW,
        settings=Settings(),
    )

    assert asyncio.run(worker.run_once()).outcome == "retryable"
    assert control_plane.finished == {
        "state": "retryable",
        "error_class": "TelegramUnavailableError",
        "error_detail": {"channel": "telegram"},
        "next_attempt_at": NOW + timedelta(seconds=15),
        "now": NOW,
    }


def test_incident_alert_channels_are_non_secret_policy_not_delivery_credentials() -> None:
    assert incident_alert_channels(Settings()) == ("dashboard",)
    assert incident_alert_channels(
        Settings(alert_channels="dashboard,telegram")
    ) == ("dashboard", "telegram")
    assert incident_alert_channels(
        Settings(
            alert_channels="dashboard",
            telegram_bot_token="token",
            telegram_chat_id="chat",
        )
    ) == ("dashboard",)
