"""Capability-limited Fly Machines recovery adapter."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol
from urllib.parse import quote

from .recovery_models import RecoveryActionType
from .recovery_records import RecoveryActionRecord, RuntimeControllerLease

FlyRecoveryCode = Literal[
    "restarted",
    "stale-noop",
    "not-confirmed",
    "budget-exhausted",
    "provider-unavailable",
]
FlyRecoveryPreflightCode = Literal[
    "confirmed",
    "stale",
    "active-competing-action",
    "budget-exhausted",
    "provider-unavailable",
]
_CLOSED_CODES: frozenset[str] = frozenset(
    {
        "restarted",
        "stale-noop",
        "not-confirmed",
        "budget-exhausted",
        "provider-unavailable",
    }
)
_PREFLIGHT_CODES: frozenset[str] = frozenset(
    {
        "confirmed",
        "stale",
        "active-competing-action",
        "budget-exhausted",
        "provider-unavailable",
    }
)
_DEFAULT_BASE_URL = "https://api.machines.dev/v1"


class FlyRecoveryHttpClient(Protocol):
    """Bounded HTTP surface used by the adapter."""

    def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> object: ...


IndependentHealthCallback = Callable[..., bool]
FlyRecoveryPreflight = Callable[..., str]
FlyTokenProvider = Callable[[], str | None]


@dataclass(frozen=True, slots=True)
class FlyRecoveryResult:
    """Bounded Fly recovery result.

    ``reason`` is intentionally closed/bounded provider metadata only.  It never
    includes provider response bodies, tokens, or raw exception text.
    """

    code: FlyRecoveryCode
    reason: str = ""
    provider_status: int | None = None

    def __post_init__(self) -> None:
        if self.code not in _CLOSED_CODES:
            raise ValueError("Fly recovery result code is not in the closed contract")
        if len(self.reason) > 128:
            raise ValueError("Fly recovery reason must be bounded")


class FlyRecoveryAdapter:
    """Restart one exact allowlisted Fly Machine under dual confirmation."""

    def __init__(
        self,
        *,
        allowed_targets: frozenset[tuple[str, str]],
        token_provider: FlyTokenProvider,
        http_client: FlyRecoveryHttpClient,
        independent_health: IndependentHealthCallback,
        preflight: FlyRecoveryPreflight,
        enabled: bool = False,
        enabled_action_types: frozenset[RecoveryActionType] = frozenset(),
        timeout_seconds: float = 2.0,
        base_url: str = _DEFAULT_BASE_URL,
    ) -> None:
        if type(allowed_targets) is not frozenset:
            raise TypeError("allowed_targets must be a frozenset")
        if type(enabled_action_types) is not frozenset:
            raise TypeError("enabled_action_types must be a frozenset")
        for app, machine_id in allowed_targets:
            _require_nonempty(app=app, machine_id=machine_id)
        for action_type in enabled_action_types:
            if action_type not in {
                RecoveryActionType.RESTART_WORKER_PROCESS,
                RecoveryActionType.RESTART_MACHINE,
            }:
                raise ValueError("enabled_action_types must contain recovery action types")
        if not callable(token_provider):
            raise TypeError("token_provider must be callable")
        if not hasattr(http_client, "post"):
            raise TypeError("http_client must expose post")
        if not callable(independent_health):
            raise TypeError("independent_health must be callable")
        if not callable(preflight):
            raise TypeError("preflight must be callable")
        if timeout_seconds <= 0 or timeout_seconds > 10:
            raise ValueError("timeout_seconds must be bounded")
        _require_nonempty(base_url=base_url)
        if base_url.rstrip("/") != _DEFAULT_BASE_URL:
            raise ValueError("base_url must be Fly Machines API")

        self._allowed_targets = allowed_targets
        self._enabled_action_types = enabled_action_types
        self._token_provider = token_provider
        self._http_client = http_client
        self._independent_health = independent_health
        self._preflight = preflight
        self._enabled = enabled
        self._timeout_seconds = float(timeout_seconds)
        self._base_url = base_url.rstrip("/")

    def restart_exact_machine(
        self,
        *,
        app: str,
        machine_id: str,
        action: RecoveryActionRecord,
        controller: RuntimeControllerLease,
        now: datetime,
    ) -> FlyRecoveryResult:
        """POST a restart only after exact authority and health rechecks."""
        _require_nonempty(app=app, machine_id=machine_id)
        if not self._enabled:
            return FlyRecoveryResult("provider-unavailable", "disabled")
        try:
            action_type = RecoveryActionType(action.action_type)
        except ValueError:
            return FlyRecoveryResult("provider-unavailable", "action-disabled")
        if action_type not in self._enabled_action_types:
            return FlyRecoveryResult("provider-unavailable", "action-disabled")
        if action_type not in {
            RecoveryActionType.RESTART_WORKER_PROCESS,
            RecoveryActionType.RESTART_MACHINE,
        }:
            return FlyRecoveryResult("provider-unavailable", "action-disabled")
        if action.controller_id != controller.controller_id:
            return FlyRecoveryResult("stale-noop", "controller-id-mismatch")
        if action.controller_owner_id != controller.owner_id:
            return FlyRecoveryResult("stale-noop", "controller-owner-mismatch")
        if action.expected_controller_epoch != controller.lease_epoch:
            return FlyRecoveryResult("stale-noop", "controller-epoch-mismatch")
        if controller.lease_expires_at <= now:
            return FlyRecoveryResult("stale-noop", "controller-lease-expired")
        if (
            action.state != "running"
            or action.worker_id is None
            or action.worker_epoch <= 0
            or action.worker_lease_expires_at is None
            or action.worker_lease_expires_at <= now
        ):
            return FlyRecoveryResult("stale-noop", "action-worker-lease-expired")
        if (app, machine_id) not in self._allowed_targets:
            return FlyRecoveryResult("stale-noop", "target-not-allowlisted")
        if not _action_matches(action=action, app=app, machine_id=machine_id):
            return FlyRecoveryResult("stale-noop", "action-target-mismatch")

        preflight = self._run_preflight(
            app=app,
            machine_id=machine_id,
            action=action,
            controller=controller,
            now=now,
        )
        if preflight != "confirmed":
            if preflight == "budget-exhausted":
                return FlyRecoveryResult("budget-exhausted", "budget-exhausted")
            if preflight == "provider-unavailable":
                return FlyRecoveryResult("provider-unavailable", "preflight-unavailable")
            return FlyRecoveryResult("stale-noop", preflight)

        health_before = self._read_health(app=app, machine_id=machine_id, now=now)
        if health_before is None:
            return FlyRecoveryResult("not-confirmed", "health-unavailable")
        if health_before:
            return FlyRecoveryResult("stale-noop", "already-healthy")

        try:
            token = self._token_provider()
        except Exception:
            return FlyRecoveryResult("provider-unavailable", "token-unavailable")
        if not isinstance(token, str) or not token.strip():
            return FlyRecoveryResult("provider-unavailable", "missing-token")

        status_code = self._post_restart(app=app, machine_id=machine_id, token=token.strip())
        if status_code is None:
            return FlyRecoveryResult("provider-unavailable", "provider-timeout")
        if status_code == -1:
            return FlyRecoveryResult("provider-unavailable", "provider-unavailable")
        if status_code != 200:
            return FlyRecoveryResult(
                "provider-unavailable",
                f"provider-http-status-{status_code}",
                status_code,
            )

        health_after = self._read_health(app=app, machine_id=machine_id, now=now)
        if health_after:
            return FlyRecoveryResult("restarted", "restart-confirmed", status_code)
        return FlyRecoveryResult("not-confirmed", "health-not-confirmed", status_code)

    def _run_preflight(
        self,
        *,
        app: str,
        machine_id: str,
        action: RecoveryActionRecord,
        controller: RuntimeControllerLease,
        now: datetime,
    ) -> FlyRecoveryPreflightCode:
        try:
            result = self._preflight(
                app=app,
                machine_id=machine_id,
                action=action,
                controller=controller,
                now=now,
            )
        except Exception:
            return "provider-unavailable"
        if result not in _PREFLIGHT_CODES:
            return "provider-unavailable"
        return result  # type: ignore[return-value]

    def _read_health(self, *, app: str, machine_id: str, now: datetime) -> bool | None:
        try:
            result = self._independent_health(app=app, machine_id=machine_id, now=now)
        except Exception:
            return None
        return result if type(result) is bool else None

    def _post_restart(self, *, app: str, machine_id: str, token: str) -> int | None:
        url = (
            f"{self._base_url}/apps/{quote(app, safe='')}"
            f"/machines/{quote(machine_id, safe='')}/restart"
        )
        try:
            response = self._http_client.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout_seconds=self._timeout_seconds,
            )
        except TimeoutError:
            return None
        except Exception:
            return -1
        status_code = getattr(response, "status_code", None)
        if type(status_code) is not int or status_code < 100 or status_code > 599:
            return -1
        return status_code


def _require_nonempty(**values: str) -> None:
    for field_name, value in values.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must be non-empty")


def _action_matches(
    *,
    action: RecoveryActionRecord,
    app: str,
    machine_id: str,
) -> bool:
    return action.detail.get("fly_app") == app and action.detail.get("fly_machine_id") == machine_id


__all__ = ["FlyRecoveryAdapter", "FlyRecoveryCode", "FlyRecoveryResult"]
