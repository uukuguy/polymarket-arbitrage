from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from polyarb.config import Settings
from polyarb.control_plane.alert_delivery import (
    AlertDeliveryStopRequested,
    TransactionalAlertDeliveryWorker,
    incident_alert_channels,
    render_runtime_incident_message,
)
from polyarb.control_plane.models import AlertDeliveryLease

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class _ControlPlane:
    def __init__(self) -> None:
        self.finished: dict[str, object] | None = None
        self.claim_kwargs: dict[str, object] | None = None

    def claim_alert_delivery(self, **kwargs: object) -> AlertDeliveryLease:
        self.claim_kwargs = kwargs
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


def test_alert_worker_passes_acceptance_scope_to_claim() -> None:
    control_plane = _ControlPlane()
    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane,
        worker_id="alert-a",
        now=lambda: NOW,
        acceptance_run_id="run-a",
    )

    assert asyncio.run(worker.run_once()).outcome == "delivered"
    assert control_plane.claim_kwargs == {
        "worker_id": "alert-a",
        "lease_seconds": 30,
        "now": NOW,
        "acceptance_run_id": "run-a",
    }


def test_telegram_delivery_persists_provider_message_id() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg",
        incident_event_id="event-tg",
        channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a",
        lease_epoch=1,
        lease_expires_at=NOW,
        attempt_number=1,
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
        settings=Settings(telegram_bot_token=SecretStr("token"), telegram_chat_id="chat"),
        telegram_client=client,
    )

    assert asyncio.run(worker.run_once()).outcome == "delivered"
    assert client.calls[0][1]["text"] == "[attempt-failed] incident-a"
    assert control_plane.finished == {
        "state": "delivered",
        "provider_receipt": "telegram:42",
        "now": NOW,
    }


def test_telegram_http_failure_is_a_retryable_delivery_receipt() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg",
        incident_event_id="event-tg",
        channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a",
        lease_epoch=1,
        lease_expires_at=NOW,
        attempt_number=1,
    )
    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane,
        worker_id="alert-a",
        now=lambda: NOW,
        settings=Settings(telegram_bot_token=SecretStr("token"), telegram_chat_id="chat"),
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


def test_alert_stop_after_provider_response_forbids_starting_finish_sql() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg",
        incident_event_id="event-tg",
        channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a",
        lease_epoch=1,
        lease_expires_at=NOW,
        attempt_number=1,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    class Client:
        async def post(self, _url: str, *, json: dict[str, object]) -> httpx.Response:
            assert json["chat_id"] == "chat"
            started.set()
            await release.wait()
            return httpx.Response(
                200,
                json={"ok": True, "result": {"message_id": 42}},
                request=httpx.Request("POST", "https://api.telegram.org"),
            )

    worker = TransactionalAlertDeliveryWorker(
        control_plane=control_plane,
        worker_id="alert-a",
        now=lambda: NOW,
        settings=Settings(telegram_bot_token=SecretStr("token"), telegram_chat_id="chat"),
        telegram_client=Client(),
    )

    async def run() -> None:
        task = asyncio.create_task(worker.run_once())
        await started.wait()
        worker.request_stop()
        release.set()
        with pytest.raises(AlertDeliveryStopRequested):
            await task

    asyncio.run(run())
    assert control_plane.finished is None


def test_unconfigured_telegram_preserves_outbox_for_retry() -> None:
    control_plane = _ControlPlane()
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg",
        incident_event_id="event-tg",
        channel="telegram",
        payload={"incident_key": "incident-a", "kind": "attempt-failed"},
        lease_owner="alert-a",
        lease_epoch=1,
        lease_expires_at=NOW,
        attempt_number=1,
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
    assert incident_alert_channels(Settings(alert_channels="dashboard,telegram")) == (
        "dashboard",
        "telegram",
    )
    assert incident_alert_channels(
        Settings(
            alert_channels="dashboard",
            telegram_bot_token=SecretStr("token"),
            telegram_chat_id="chat",
        )
    ) == ("dashboard",)


def _runtime_transition_payload(
    transition: str,
    *,
    reason: str = "control-api:TimeoutError",
    action: str = "restart-machine",
    qualification_impact: str = "invalidated",
) -> dict[str, object]:
    return {
        "schema_version": "m1-runtime-incident-transition-v1",
        "transition": transition,
        "incident_id": "runtime-watchdog-incident-a",
        "incident_key": "runtime-watchdog:independent-runtime-watchdog",
        "component": "runtime-watchdog",
        "source": "independent-runtime-watchdog",
        "job_key": "quote:batch:42",
        "stage": "quote-fetch",
        "reason": reason,
        "action": action,
        "qualification_impact": qualification_impact,
        "dashboard_url": "https://dashboard.example/control-plane",
        "occurred_at": "2030-01-01T00:00:00+00:00",
        "detail": "POLYARB_TELEGRAM_BOT_TOKEN=secret should never be forwarded",
    }


def test_runtime_transition_renderer_formats_all_operator_states_without_secret_detail() -> None:
    labels = {
        "detected": "DETECTED",
        "recovery-started": "RECOVERY STARTED",
        "recovered": "RECOVERED",
        "escalated": "ESCALATED",
    }

    for transition, label in labels.items():
        body = render_runtime_incident_message(_runtime_transition_payload(transition))

        assert label in body
        assert "runtime-watchdog-incident-a" in body
        assert "runtime-watchdog" in body
        assert "quote:batch:42" in body
        assert "quote-fetch" in body
        assert "control-api:TimeoutError" in body
        assert "restart-machine" in body
        assert "invalidated" in body
        assert "https://dashboard.example/control-plane" in body
        assert "secret" not in body.lower()
        assert "POLYARB_TELEGRAM_BOT_TOKEN" not in body
        assert len(body) <= 3900


def test_runtime_transition_renderer_rejects_unbounded_or_unknown_fields() -> None:
    payload = _runtime_transition_payload(
        "detected",
        reason="control-api:" + ("x" * 300),
    )

    try:
        render_runtime_incident_message(payload)
    except ValueError as error:
        assert "runtime transition payload" in str(error)
    else:
        raise AssertionError("unbounded runtime transition payload was rendered")


def test_runtime_transition_renderer_rejects_unknown_or_secret_legacy_payloads() -> None:
    for payload in (
        {"incident_key": "incident-a", "kind": "unknown"},
        {"incident_key": "incident-token", "kind": "attempt-failed"},
    ):
        try:
            render_runtime_incident_message(payload)
        except ValueError as error:
            assert "runtime transition payload" in str(error)
        else:
            raise AssertionError("unsafe legacy alert payload was rendered")


def test_runtime_transition_telegram_delivery_uses_normalized_payload() -> None:
    control_plane = _ControlPlane()
    payload = _runtime_transition_payload("detected")
    control_plane.claim_alert_delivery = lambda **_kwargs: AlertDeliveryLease(
        outbox_id="outbox-tg",
        incident_event_id="event-tg",
        channel="telegram",
        payload=payload,
        lease_owner="alert-a",
        lease_epoch=1,
        lease_expires_at=NOW,
        attempt_number=1,
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
        settings=Settings(telegram_bot_token=SecretStr("token"), telegram_chat_id="chat"),
        telegram_client=client,
    )

    assert asyncio.run(worker.run_once()).outcome == "delivered"

    sent = str(client.calls[0][1]["text"])
    assert "DETECTED" in sent
    assert "runtime-watchdog-incident-a" in sent
    assert "quote-fetch" in sent
