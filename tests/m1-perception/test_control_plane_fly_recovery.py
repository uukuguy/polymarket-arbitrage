"""TDD contracts for exact Fly Machine recovery."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn, cast

import pytest

from polyarb.control_plane.fly_recovery import FlyRecoveryAdapter
from polyarb.control_plane.recovery_models import RecoveryActionType
from polyarb.control_plane.recovery_records import RecoveryActionRecord, RuntimeControllerLease

NOW = datetime(2030, 1, 1, 12, tzinfo=UTC)
APP = "polyarb-controller"
MACHINE_ID = "48ed199ba9e148"


def _controller(*, epoch: int = 7) -> RuntimeControllerLease:
    return RuntimeControllerLease(
        controller_id="runtime-controller",
        owner_id="controller-owner",
        lease_epoch=epoch,
        lease_expires_at=NOW + timedelta(minutes=5),
    )


def _action(
    *,
    action_type: RecoveryActionType = RecoveryActionType.RESTART_MACHINE,
    state: str = "running",
    result_code: str | None = None,
    worker_id: str | None = "executor-a",
    worker_epoch: int = 3,
    app: str = APP,
    machine_id: str = MACHINE_ID,
) -> RecoveryActionRecord:
    return RecoveryActionRecord(
        action_id="action:restart-machine",
        controller_id="runtime-controller",
        controller_owner_id="controller-owner",
        incident_key="incident:machine",
        target_type="machine",
        target_id=f"{app}/{machine_id}",
        action_type=action_type.value,
        expected_controller_epoch=7,
        expected_attempt_id="attempt-1",
        expected_lease_epoch=11,
        requested_at=NOW - timedelta(seconds=1),
        started_at=NOW,
        finished_at=None,
        state=state,
        result_code=result_code,
        next_allowed_at=NOW,
        worker_id=worker_id,
        worker_epoch=worker_epoch,
        worker_lease_expires_at=NOW + timedelta(seconds=30),
        detail={
            "component": "runtime-watchdog",
            "fly_app": app,
            "fly_machine_id": machine_id,
            "reason_code": "job.heartbeat-missing",
        },
        idempotency_key="idempotency:restart-machine",
    )


class BodyTrapResponse:
    def __init__(self, status_code: int, *, secret_body: str = "fly-token-secret") -> None:
        self.status_code = status_code
        self._secret_body = secret_body

    @property
    def text(self) -> NoReturn:
        raise AssertionError("adapter must not read provider response text")

    @property
    def content(self) -> NoReturn:
        raise AssertionError("adapter must not read provider response content")

    def json(self) -> NoReturn:
        raise AssertionError("adapter must not read provider response json")

    def __repr__(self) -> str:
        return f"BodyTrapResponse(status_code={self.status_code}, body={self._secret_body})"


class FakeHttpClient:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.calls: list[dict[str, object]] = []
        self.raise_timeout = False
        self.raise_transport = False

    def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> BodyTrapResponse:
        self.calls.append(
            {"url": url, "headers": dict(headers), "timeout_seconds": timeout_seconds}
        )
        if self.raise_timeout:
            raise TimeoutError("timeout included fly-token-secret")
        if self.raise_transport:
            raise RuntimeError("transport failed fly-token-secret")
        return BodyTrapResponse(self.status_code)


def _adapter(
    http: FakeHttpClient,
    *,
    enabled: bool = True,
    enabled_action_types: frozenset[RecoveryActionType] = frozenset(
        {RecoveryActionType.RESTART_MACHINE}
    ),
    health_results: tuple[bool, ...] = (False, True),
    preflight: str = "confirmed",
) -> FlyRecoveryAdapter:
    health_iter = iter(health_results)

    def independent_health(**_: object) -> bool:
        return next(health_iter)

    def token_provider() -> str:
        return "fly-token-secret"

    return FlyRecoveryAdapter(
        enabled=enabled,
        enabled_action_types=enabled_action_types,
        allowed_targets=frozenset({(APP, MACHINE_ID)}),
        token_provider=token_provider,
        http_client=http,
        independent_health=independent_health,
        preflight=lambda **_: preflight,
        timeout_seconds=0.25,
    )


def test_restart_exact_machine_is_disabled_by_default() -> None:
    http = FakeHttpClient()
    adapter = FlyRecoveryAdapter(
        allowed_targets=frozenset({(APP, MACHINE_ID)}),
        token_provider=lambda: "fly-token-secret",
        http_client=http,
        independent_health=lambda **_: False,
        preflight=lambda **_: "confirmed",
    )

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "provider-unavailable"
    assert http.calls == []


def test_restart_exact_machine_rejects_mutable_allowlist() -> None:
    with pytest.raises(TypeError, match="allowed_targets must be a frozenset"):
        FlyRecoveryAdapter(
            enabled=True,
            enabled_action_types=frozenset({RecoveryActionType.RESTART_MACHINE}),
            allowed_targets=cast(Any, {(APP, MACHINE_ID)}),
            token_provider=lambda: "fly-token-secret",
            http_client=FakeHttpClient(),
            independent_health=lambda **_: False,
            preflight=lambda **_: "confirmed",
        )


def test_restart_exact_machine_rejects_mutable_enabled_action_types() -> None:
    with pytest.raises(TypeError, match="enabled_action_types must be a frozenset"):
        FlyRecoveryAdapter(
            enabled=True,
            enabled_action_types=cast(Any, {RecoveryActionType.RESTART_MACHINE}),
            allowed_targets=frozenset({(APP, MACHINE_ID)}),
            token_provider=lambda: "fly-token-secret",
            http_client=FakeHttpClient(),
            independent_health=lambda **_: False,
            preflight=lambda **_: "confirmed",
        )


def test_restart_exact_machine_rejects_noncanonical_base_url() -> None:
    with pytest.raises(ValueError, match="base_url must be Fly Machines API"):
        FlyRecoveryAdapter(
            enabled=True,
            enabled_action_types=frozenset({RecoveryActionType.RESTART_MACHINE}),
            allowed_targets=frozenset({(APP, MACHINE_ID)}),
            token_provider=lambda: "fly-token-secret",
            http_client=FakeHttpClient(),
            independent_health=lambda **_: False,
            preflight=lambda **_: "confirmed",
            base_url="https://attacker.example/v1",
        )


@pytest.mark.parametrize(
    ("enabled_action_types", "action_type", "expected_code", "expected_posts"),
    (
        (
            frozenset({RecoveryActionType.RESTART_WORKER_PROCESS}),
            RecoveryActionType.RESTART_WORKER_PROCESS,
            "restarted",
            1,
        ),
        (
            frozenset({RecoveryActionType.RESTART_WORKER_PROCESS}),
            RecoveryActionType.RESTART_MACHINE,
            "provider-unavailable",
            0,
        ),
        (
            frozenset({RecoveryActionType.RESTART_MACHINE}),
            RecoveryActionType.RESTART_MACHINE,
            "restarted",
            1,
        ),
        (
            frozenset({RecoveryActionType.RESTART_MACHINE}),
            RecoveryActionType.RESTART_WORKER_PROCESS,
            "provider-unavailable",
            0,
        ),
    ),
)
def test_restart_exact_machine_gates_process_and_machine_actions_independently(
    enabled_action_types: frozenset[RecoveryActionType],
    action_type: RecoveryActionType,
    expected_code: str,
    expected_posts: int,
) -> None:
    http = FakeHttpClient()
    adapter = _adapter(http, enabled_action_types=enabled_action_types)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(action_type=action_type),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == expected_code
    assert len(http.calls) == expected_posts
    if expected_posts == 0:
        assert result.reason == "action-disabled"


def test_restart_exact_machine_fails_closed_on_controller_epoch_mismatch() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(epoch=8),
        now=NOW,
    )

    assert result.code == "stale-noop"
    assert result.reason == "controller-epoch-mismatch"
    assert http.calls == []


def test_restart_exact_machine_fails_closed_on_expired_controller_lease() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=replace(_controller(), lease_expires_at=NOW - timedelta(seconds=1)),
        now=NOW,
    )

    assert result.code == "stale-noop"
    assert result.reason == "controller-lease-expired"
    assert http.calls == []


def test_restart_exact_machine_fails_closed_on_stale_action_worker_lease() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=replace(
            _action(),
            worker_lease_expires_at=NOW - timedelta(seconds=1),
        ),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "stale-noop"
    assert result.reason == "action-worker-lease-expired"
    assert http.calls == []


@pytest.mark.parametrize(
    ("preflight", "expected"),
    (
        ("stale", "stale-noop"),
        ("active-competing-action", "stale-noop"),
        ("budget-exhausted", "budget-exhausted"),
    ),
)
def test_restart_exact_machine_preflight_blocks_post(
    preflight: str,
    expected: str,
) -> None:
    http = FakeHttpClient()
    adapter = _adapter(http, preflight=preflight)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == expected
    assert http.calls == []


def test_restart_exact_machine_requires_exact_allowed_target() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app="polyarb-controller-copy",
        machine_id=MACHINE_ID,
        action=_action(app="polyarb-controller-copy"),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "stale-noop"
    assert http.calls == []


def test_restart_exact_machine_requires_matching_action_target() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=replace(_action(), detail={"fly_app": APP, "fly_machine_id": "other"}),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "stale-noop"
    assert http.calls == []


def test_restart_exact_machine_noops_when_independent_health_is_already_healthy() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http, health_results=(True,))

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "stale-noop"
    assert http.calls == []


def test_restart_exact_machine_requires_health_callback_to_return_bool() -> None:
    http = FakeHttpClient()
    adapter = FlyRecoveryAdapter(
        enabled=True,
        enabled_action_types=frozenset({RecoveryActionType.RESTART_MACHINE}),
        allowed_targets=frozenset({(APP, MACHINE_ID)}),
        token_provider=lambda: "fly-token-secret",
        http_client=http,
        independent_health=lambda **_: cast(Any, "false"),
        preflight=lambda **_: "confirmed",
    )

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "not-confirmed"
    assert result.reason == "health-unavailable"
    assert http.calls == []


def test_restart_exact_machine_returns_not_confirmed_without_post_health() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http, health_results=(False, False))

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "not-confirmed"
    assert len(http.calls) == 1


@pytest.mark.parametrize("status_code", (201, 400, 404, 500))
def test_restart_exact_machine_provider_errors_are_bounded_and_secret_free(
    status_code: int,
) -> None:
    http = FakeHttpClient(status_code=status_code)
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "provider-unavailable"
    assert str(status_code) in result.reason
    assert "fly-token-secret" not in str(result)


def test_restart_exact_machine_token_provider_errors_are_bounded_and_secret_free() -> None:
    http = FakeHttpClient()

    def token_provider() -> str:
        raise RuntimeError("token exploded fly-token-secret")

    adapter = FlyRecoveryAdapter(
        enabled=True,
        enabled_action_types=frozenset({RecoveryActionType.RESTART_MACHINE}),
        allowed_targets=frozenset({(APP, MACHINE_ID)}),
        token_provider=token_provider,
        http_client=http,
        independent_health=lambda **_: False,
        preflight=lambda **_: "confirmed",
    )

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "provider-unavailable"
    assert result.reason == "token-unavailable"
    assert "fly-token-secret" not in str(result)
    assert http.calls == []


def test_restart_exact_machine_rejects_non_string_token_without_post() -> None:
    http = FakeHttpClient()
    adapter = FlyRecoveryAdapter(
        enabled=True,
        enabled_action_types=frozenset({RecoveryActionType.RESTART_MACHINE}),
        allowed_targets=frozenset({(APP, MACHINE_ID)}),
        token_provider=lambda: cast(Any, b"fly-token-secret"),
        http_client=http,
        independent_health=lambda **_: False,
        preflight=lambda **_: "confirmed",
    )

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "provider-unavailable"
    assert result.reason == "missing-token"
    assert http.calls == []


def test_restart_exact_machine_timeout_is_bounded_and_secret_free() -> None:
    http = FakeHttpClient()
    http.raise_timeout = True
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "provider-unavailable"
    assert result.reason == "provider-timeout"
    assert "fly-token-secret" not in str(result)


def test_restart_exact_machine_transport_error_is_bounded_and_secret_free() -> None:
    http = FakeHttpClient()
    http.raise_transport = True
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "provider-unavailable"
    assert result.reason == "provider-unavailable"
    assert "fly-token-secret" not in str(result)


def test_restart_exact_machine_posts_restart_with_bounded_authorization_header() -> None:
    http = FakeHttpClient()
    adapter = _adapter(http)

    result = adapter.restart_exact_machine(
        app=APP,
        machine_id=MACHINE_ID,
        action=_action(),
        controller=_controller(),
        now=NOW,
    )

    assert result.code == "restarted"
    assert http.calls == [
        {
            "url": f"https://api.machines.dev/v1/apps/{APP}/machines/{MACHINE_ID}/restart",
            "headers": {
                "Authorization": "Bearer fly-token-secret",
                "Accept": "application/json",
            },
            "timeout_seconds": 0.25,
        }
    ]
