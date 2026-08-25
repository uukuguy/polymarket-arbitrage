"""One fenced turn for durable M1 notification-outbox delivery."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

import httpx

from polyarb.config import Settings

from .models import AlertDeliveryLease

_RUNTIME_TRANSITION_SCHEMA = "m1-runtime-incident-transition-v1"
DEFAULT_RUNTIME_DASHBOARD_URL = (
    "https://polymarket-arbitrage-jiangwen-su-s-projects.vercel.app/control-plane"
)
_RUNTIME_TRANSITION_LABELS = {
    "detected": "DETECTED",
    "recovery-started": "RECOVERY STARTED",
    "recovered": "RECOVERED",
    "escalated": "ESCALATED",
}
_LEGACY_ALERT_KINDS = {
    "attempt-failed",
    "circuit-opened",
    "circuit-probe-failed",
    "detected",
    "recovered",
    "recovery-started",
}
_QUALIFICATION_IMPACTS = {
    "none",
    "unknown",
    "delayed",
    "invalidated",
    "recovering",
    "qualified",
    "breaking",
}
_RUNTIME_ACTIONS = {
    "none",
    "heartbeat-job",
    "cancel-job",
    "retry-job",
    "reclaim-job",
    "probe-circuit",
    "restart-worker-process",
    "restart-machine",
}
_BOUNDED_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:/._ @#=+-]{0,255}$")
_BOUNDED_URL = re.compile(r"^https?://[^\s]{1,511}$")
_SECRET_WORDS = ("secret", "token", "password", "api_key", "apikey", "authorization")


@dataclass(frozen=True, slots=True)
class AlertDeliveryResult:
    outbox_id: str | None
    outcome: str


class _TelegramClient(Protocol):
    async def post(self, url: str, *, json: dict[str, object]) -> httpx.Response: ...


class _AlertControlPlane(Protocol):
    def claim_alert_delivery(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        now: datetime,
        acceptance_run_id: str | None = None,
    ) -> AlertDeliveryLease | None: ...

    def finish_alert_delivery(
        self,
        lease: AlertDeliveryLease,
        *,
        state: str,
        now: datetime,
        provider_receipt: str | None = None,
        error_class: str | None = None,
        error_detail: dict[str, object] | None = None,
        next_attempt_at: datetime | None = None,
    ) -> None: ...


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


def render_runtime_incident_message(payload: Mapping[str, object]) -> str:
    """Render one bounded runtime-transition payload for Telegram."""
    if payload.get("schema_version") != _RUNTIME_TRANSITION_SCHEMA:
        kind = _payload_choice(payload, "kind", _LEGACY_ALERT_KINDS)
        incident_key = _payload_text(payload, "incident_key", max_len=256)
        return f"[{kind}] {incident_key}"

    transition = _payload_choice(payload, "transition", set(_RUNTIME_TRANSITION_LABELS))
    qualification_impact = _payload_choice(
        payload, "qualification_impact", _QUALIFICATION_IMPACTS
    )
    incident_id = _payload_text(payload, "incident_id", max_len=256)
    incident_key = _payload_text(payload, "incident_key", max_len=256)
    component = _payload_text(payload, "component", max_len=128)
    source = _payload_text(payload, "source", max_len=128)
    job_key = _payload_text(payload, "job_key", max_len=256, required=False)
    stage = _payload_text(payload, "stage", max_len=128, required=False)
    reason = _payload_text(payload, "reason", max_len=256)
    action = _payload_choice(payload, "action", _RUNTIME_ACTIONS)
    dashboard_url = _payload_text(payload, "dashboard_url", max_len=512, pattern=_BOUNDED_URL)
    occurred_at = _payload_text(payload, "occurred_at", max_len=64)

    job_stage = f"{job_key or '-'} / {stage or '-'}"
    lines = [
        f"M1 runtime {_RUNTIME_TRANSITION_LABELS[transition]}",
        f"Incident: {incident_id}",
        f"Key: {incident_key}",
        f"Component: {component}",
        f"Source: {source}",
        f"Job/stage: {job_stage}",
        f"Reason: {reason}",
        f"Action: {action}",
        f"Qualification: {qualification_impact}",
        f"Occurred: {occurred_at}",
        f"Dashboard: {dashboard_url}",
    ]
    body = "\n".join(lines)
    if len(body) > 3900:
        raise ValueError("runtime transition payload exceeds Telegram safety margin")
    return body


def runtime_incident_transition_payload(
    *,
    transition: str,
    incident_id: str,
    incident_key: str,
    component: str,
    source: str,
    job_key: str | None,
    stage: str | None,
    reason: str,
    action: str,
    qualification_impact: str,
    dashboard_url: str,
    occurred_at: datetime,
    acceptance_run_id: str | None = None,
) -> dict[str, object]:
    """Build and validate the closed payload shared by writer, outbox, and Telegram."""
    payload: dict[str, object] = {
        "schema_version": _RUNTIME_TRANSITION_SCHEMA,
        "transition": transition,
        "incident_id": incident_id,
        "incident_key": incident_key,
        "component": component,
        "source": source,
        "job_key": job_key,
        "stage": stage,
        "reason": reason,
        "action": action,
        "qualification_impact": qualification_impact,
        "dashboard_url": dashboard_url,
        "occurred_at": occurred_at.isoformat(),
    }
    render_runtime_incident_message(payload)
    if acceptance_run_id is not None:
        if (
            not _BOUNDED_TEXT.fullmatch(acceptance_run_id)
            or any(word in acceptance_run_id.lower() for word in _SECRET_WORDS)
        ):
            raise ValueError("runtime transition payload invalid field acceptance_run_id")
        payload["acceptance_run_id"] = acceptance_run_id
    return payload


def _payload_choice(
    payload: Mapping[str, object], field: str, allowed: set[str]
) -> str:
    value = _payload_text(payload, field, max_len=128)
    if value not in allowed:
        raise ValueError(f"runtime transition payload invalid field {field}")
    return value


def _payload_text(
    payload: Mapping[str, object],
    field: str,
    *,
    max_len: int,
    required: bool = True,
    pattern: re.Pattern[str] = _BOUNDED_TEXT,
) -> str | None:
    value = payload.get(field)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value or len(value) > max_len:
        raise ValueError(f"runtime transition payload invalid field {field}")
    if not pattern.fullmatch(value):
        raise ValueError(f"runtime transition payload invalid field {field}")
    lower = value.lower()
    if any(word in lower for word in _SECRET_WORDS):
        raise ValueError(f"runtime transition payload invalid field {field}")
    return value


class TransactionalAlertDeliveryWorker:
    """Deliver one alert intent without coupling notification availability to collection."""

    def __init__(
        self,
        *,
        control_plane: _AlertControlPlane,
        worker_id: str,
        now: Callable[[], datetime],
        lease_seconds: int = 30,
        retry_delay: timedelta = timedelta(seconds=15),
        settings: Settings | None = None,
        telegram_client: _TelegramClient | None = None,
        acceptance_run_id: str | None = None,
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
        if acceptance_run_id is not None and not acceptance_run_id:
            raise ValueError("acceptance_run_id must be non-empty when provided")
        self._acceptance_run_id = acceptance_run_id

    async def run_once(self) -> AlertDeliveryResult:
        lease = self._control_plane.claim_alert_delivery(
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
            now=self._now(),
            acceptance_run_id=self._acceptance_run_id,
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
                    "text": render_runtime_incident_message(lease.payload),
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
