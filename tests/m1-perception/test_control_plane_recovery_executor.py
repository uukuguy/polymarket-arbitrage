"""TDD contracts for the fenced job-level recovery executor."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from polyarb.control_plane.fly_recovery import FlyRecoveryCode, FlyRecoveryResult
from polyarb.control_plane.postgres import StaleLeaseError
from polyarb.control_plane.recovery_executor import RecoveryExecutor
from polyarb.control_plane.recovery_models import RecoveryActionType
from polyarb.control_plane.recovery_records import (
    RecoveryActionRecord,
    RuntimeControllerLease,
)

NOW = datetime(2030, 1, 1, 12, tzinfo=UTC)


def _controller() -> RuntimeControllerLease:
    return RuntimeControllerLease(
        controller_id="runtime-controller",
        owner_id="controller-owner",
        lease_epoch=1,
        lease_expires_at=NOW + timedelta(minutes=5),
    )


def _action(
    action_type: RecoveryActionType,
    *,
    state: str = "pending",
    worker_id: str | None = None,
    worker_epoch: int = 0,
    worker_lease_expires_at: datetime | None = None,
    action_id: str | None = None,
) -> RecoveryActionRecord:
    return RecoveryActionRecord(
        action_id=action_id or f"action:{action_type.value}",
        controller_id="runtime-controller",
        controller_owner_id="controller-owner",
        incident_key="incident:job-1",
        target_type="job",
        target_id="job-1",
        action_type=action_type.value,
        expected_controller_epoch=1,
        expected_attempt_id="attempt-1",
        expected_lease_epoch=3,
        requested_at=NOW,
        started_at=None,
        finished_at=None,
        state=state,
        result_code=None,
        next_allowed_at=NOW,
        worker_id=worker_id,
        worker_epoch=worker_epoch,
        worker_lease_expires_at=worker_lease_expires_at,
        detail={
            "component": "structure-normalize",
            "reason_code": "job.progress-stalled",
            "channels": '["dashboard"]',
        },
        idempotency_key=f"idempotency:{action_id or action_type.value}",
    )


class FakeStore:
    def __init__(self, actions: list[RecoveryActionRecord]) -> None:
        self.actions = actions
        self.claimed: list[RecoveryActionRecord] = []
        self.finished: list[tuple[RecoveryActionRecord, str, dict[str, object]]] = []
        self.executed: list[str] = []

    def claim_action(self, **kwargs: Any) -> RecoveryActionRecord | None:
        now = kwargs["now"]
        expected_action_id = kwargs.get("expected_action_id")
        for index, action in enumerate(self.actions):
            if expected_action_id is not None and action.action_id != expected_action_id:
                continue
            if action.state == "completed":
                continue
            if action.state == "running":
                expiry = action.worker_lease_expires_at
                if expiry is None or expiry > now:
                    continue
            claimed = replace(
                action,
                state="running",
                started_at=now,
                worker_id=kwargs["worker_id"],
                worker_epoch=action.worker_epoch + 1,
                worker_lease_expires_at=now + timedelta(seconds=kwargs["lease_seconds"]),
            )
            self.actions[index] = claimed
            self.claimed.append(claimed)
            return claimed
        return None

    def finish_action(self, **kwargs: Any) -> RecoveryActionRecord:
        for index, action in enumerate(self.actions):
            if action.action_id != kwargs["action_id"]:
                continue
            if action.state == "completed":
                if action.result_code != kwargs["result_code"]:
                    raise AssertionError("conflicting finish replay")
                return action
            if (
                action.worker_id != kwargs["worker_id"]
                or action.worker_epoch != kwargs["worker_epoch"]
                or action.worker_lease_expires_at is None
                or action.worker_lease_expires_at <= kwargs["now"]
            ):
                return action
            finished = replace(
                action,
                state="completed",
                result_code=kwargs["result_code"],
                finished_at=kwargs["now"],
                detail=dict(kwargs.get("detail") or {}),
            )
            self.actions[index] = finished
            self.finished.append(
                (finished, kwargs["result_code"], dict(kwargs.get("detail") or {}))
            )
            return finished
        raise AssertionError("action missing")

    def execute_claimed_action(self, **kwargs: Any) -> RecoveryActionRecord:
        action = next(action for action in self.actions if action.action_id == kwargs["action_id"])
        if (
            action.state != "running"
            or action.worker_id != kwargs["worker_id"]
            or action.worker_epoch != kwargs["worker_epoch"]
            or action.worker_lease_expires_at is None
            or action.worker_lease_expires_at <= kwargs["now"]
        ):
            return action
        self.executed.append(action.action_id)
        result_code = kwargs["callback"](None, action)
        if not isinstance(result_code, str) or result_code not in {
            "succeeded",
            "failed",
            "stale-noop",
            "disabled-action",
        }:
            raise ValueError("invalid fake terminal result")
        return self.finish_action(
            action_id=action.action_id,
            worker_id=action.worker_id,
            worker_epoch=action.worker_epoch,
            result_code=result_code,
            now=kwargs["now"],
            detail={"postcondition": result_code},
        )


class FakeControlPlane:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.heartbeat_lease_seconds: list[int] = []
        self.crash_once = False

    def _run(self, name: str, action: RecoveryActionRecord, *, now: datetime) -> str:
        self.calls.append((name, action.target_id))
        if self.crash_once:
            self.crash_once = False
            raise RuntimeError("simulated executor crash")
        return "succeeded"

    def _execute_recovery_action_cursor(
        self,
        cursor: Any,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        heartbeat_lease_seconds: int,
    ) -> object:
        del cursor
        if action.action_type == RecoveryActionType.HEARTBEAT_JOB.value:
            return self.heartbeat_recovering_job(
                action,
                now=now,
                lease_seconds=heartbeat_lease_seconds,
            )
        if action.action_type == RecoveryActionType.CANCEL_JOB.value:
            return self.cancel_stalled_job(action, now=now)
        if action.action_type == RecoveryActionType.RETRY_JOB.value:
            return self.release_retryable_job(action, now=now)
        if action.action_type == RecoveryActionType.RECLAIM_JOB.value:
            return self.reclaim_expired_job(action, now=now)
        if action.action_type == RecoveryActionType.PROBE_CIRCUIT.value:
            return self.release_one_circuit_probe(action, now=now)
        raise AssertionError(f"unexpected fake action {action.action_type}")

    def heartbeat_recovering_job(
        self,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> str:
        self.heartbeat_lease_seconds.append(lease_seconds)
        return self._run("heartbeat", action, now=now)

    def cancel_stalled_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("cancel", action, now=now)

    def release_retryable_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("retry", action, now=now)

    def reclaim_expired_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("reclaim", action, now=now)

    def release_one_circuit_probe(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("probe", action, now=now)


class FakeMachineRecoveryAdapter:
    def __init__(self, code: FlyRecoveryCode = "restarted") -> None:
        self.code: FlyRecoveryCode = code
        self.calls: list[dict[str, object]] = []

    def restart_exact_machine(
        self,
        *,
        app: str,
        machine_id: str,
        action: RecoveryActionRecord,
        controller: RuntimeControllerLease,
        now: datetime,
    ) -> FlyRecoveryResult:
        self.calls.append(
            {
                "app": app,
                "machine_id": machine_id,
                "action_id": action.action_id,
                "controller_epoch": controller.lease_epoch,
                "now": now,
            }
        )
        return FlyRecoveryResult(code=self.code, reason="fake")


def _executor(store: FakeStore, control_plane: FakeControlPlane) -> RecoveryExecutor:
    return RecoveryExecutor(
        store=store,
        control_plane=control_plane,
        controller=_controller(),
        worker_id="recovery-worker",
        action_lease_seconds=5,
    )


@pytest.mark.parametrize(
    ("action_type", "method"),
    (
        (RecoveryActionType.HEARTBEAT_JOB, "heartbeat"),
        (RecoveryActionType.CANCEL_JOB, "cancel"),
        (RecoveryActionType.RETRY_JOB, "retry"),
        (RecoveryActionType.RECLAIM_JOB, "reclaim"),
        (RecoveryActionType.PROBE_CIRCUIT, "probe"),
    ),
)
def test_only_allowlisted_job_actions_dispatch_to_control_plane(
    action_type: RecoveryActionType, method: str
) -> None:
    store = FakeStore([_action(action_type)])
    control_plane = FakeControlPlane()

    result = _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert result.outcome == "succeeded"
    assert control_plane.calls == [(method, "job-1")]
    assert all(
        name not in {"publish_receipt", "publish_pointer"} for name, _ in control_plane.calls
    )


def test_exact_action_claim_does_not_execute_an_older_pending_action() -> None:
    older = _action(RecoveryActionType.CANCEL_JOB, action_id="action-older")
    selected = _action(RecoveryActionType.PROBE_CIRCUIT, action_id="action-selected")
    store = FakeStore([older, selected])
    control_plane = FakeControlPlane()

    result = _executor(store, control_plane).run_once(
        now=NOW + timedelta(seconds=1),
        expected_action_id=selected.action_id,
    )

    assert result is not None
    assert result.action_id == selected.action_id
    assert [action.action_id for action in store.claimed] == [selected.action_id]
    assert store.actions[0] == older
    assert control_plane.calls == [("probe", "job-1")]


def test_heartbeat_lease_seconds_are_forwarded_to_the_typed_adapter() -> None:
    store = FakeStore([_action(RecoveryActionType.HEARTBEAT_JOB)])
    control_plane = FakeControlPlane()

    result = RecoveryExecutor(
        store=store,
        control_plane=control_plane,
        controller=_controller(),
        worker_id="recovery-worker",
        action_lease_seconds=5,
        heartbeat_lease_seconds=77,
    ).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None and result.outcome == "succeeded"
    assert control_plane.heartbeat_lease_seconds == [77]
    assert store.executed == ["action:heartbeat-job"]


@pytest.mark.parametrize(
    "action_type", (RecoveryActionType.RESTART_WORKER_PROCESS, RecoveryActionType.RESTART_MACHINE)
)
def test_process_and_machine_actions_are_durable_disabled_noops(
    action_type: RecoveryActionType,
) -> None:
    action = replace(_action(action_type), target_type="worker-process")
    store = FakeStore([action])
    control_plane = FakeControlPlane()

    result = _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert result.outcome == "disabled-action"
    assert control_plane.calls == []
    assert store.finished[0][1] == "disabled-action"


@pytest.mark.parametrize(
    "action_type", (RecoveryActionType.RESTART_WORKER_PROCESS, RecoveryActionType.RESTART_MACHINE)
)
def test_process_and_machine_actions_dispatch_to_explicit_machine_adapter(
    action_type: RecoveryActionType,
) -> None:
    action = replace(
        _action(action_type),
        target_type="machine",
        target_id="polyarb-controller/48ed199ba9e148",
        detail={
            "component": "runtime-watchdog",
            "fly_app": "polyarb-controller",
            "fly_machine_id": "48ed199ba9e148",
            "reason_code": "job.heartbeat-missing",
        },
    )
    store = FakeStore([action])
    control_plane = FakeControlPlane()
    machine_adapter = FakeMachineRecoveryAdapter()

    result = RecoveryExecutor(
        store=store,
        control_plane=control_plane,
        controller=_controller(),
        worker_id="recovery-worker",
        action_lease_seconds=5,
        machine_recovery_adapter=machine_adapter,
    ).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert result.outcome == "restarted"
    assert machine_adapter.calls == [
        {
            "app": "polyarb-controller",
            "machine_id": "48ed199ba9e148",
            "action_id": action.action_id,
            "controller_epoch": 1,
            "now": NOW + timedelta(seconds=1),
        }
    ]
    assert control_plane.calls == []


def test_process_action_preserves_adapter_disabled_result_without_machine_upgrade() -> None:
    action = replace(
        _action(RecoveryActionType.RESTART_WORKER_PROCESS),
        target_type="machine",
        target_id="polyarb-controller/48ed199ba9e148",
        detail={
            "component": "runtime-watchdog",
            "fly_app": "polyarb-controller",
            "fly_machine_id": "48ed199ba9e148",
            "reason_code": "job.heartbeat-missing",
        },
    )
    store = FakeStore([action])
    machine_adapter = FakeMachineRecoveryAdapter("provider-unavailable")

    result = RecoveryExecutor(
        store=store,
        control_plane=FakeControlPlane(),
        controller=_controller(),
        worker_id="recovery-worker",
        action_lease_seconds=5,
        machine_recovery_adapter=machine_adapter,
    ).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert result.outcome == "provider-unavailable"
    assert machine_adapter.calls == [
        {
            "app": "polyarb-controller",
            "machine_id": "48ed199ba9e148",
            "action_id": action.action_id,
            "controller_epoch": 1,
            "now": NOW + timedelta(seconds=1),
        }
    ]
    assert store.finished[0][1] == "failed"


@pytest.mark.parametrize(
    ("adapter_code", "ledger_result"),
    (
        ("restarted", "succeeded"),
        ("stale-noop", "stale-noop"),
        ("not-confirmed", "failed"),
        ("budget-exhausted", "disabled-action"),
        ("provider-unavailable", "failed"),
    ),
)
def test_machine_adapter_closed_results_are_reported_without_expanding_store_contract(
    adapter_code: FlyRecoveryCode,
    ledger_result: str,
) -> None:
    store = FakeStore(
        [
            replace(
                _action(RecoveryActionType.RESTART_MACHINE),
                target_type="machine",
                target_id="polyarb-controller/48ed199ba9e148",
                detail={
                    "fly_app": "polyarb-controller",
                    "fly_machine_id": "48ed199ba9e148",
                },
            )
        ]
    )
    machine_adapter = FakeMachineRecoveryAdapter(adapter_code)

    result = RecoveryExecutor(
        store=store,
        control_plane=FakeControlPlane(),
        controller=_controller(),
        worker_id="recovery-worker",
        action_lease_seconds=5,
        machine_recovery_adapter=machine_adapter,
    ).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert result.outcome == adapter_code
    assert store.finished[0][1] == ledger_result


def test_duplicate_command_is_completed_once_and_second_turn_is_idle() -> None:
    store = FakeStore([_action(RecoveryActionType.RECLAIM_JOB)])
    control_plane = FakeControlPlane()
    executor = _executor(store, control_plane)

    first = executor.run_once(now=NOW + timedelta(seconds=1))
    second = executor.run_once(now=NOW + timedelta(seconds=2))

    assert first is not None and first.outcome == "succeeded"
    assert second is None
    assert control_plane.calls == [("reclaim", "job-1")]
    assert len(store.finished) == 1


def test_stale_attempt_finishes_as_stale_noop_without_business_publication() -> None:
    class StaleControlPlane(FakeControlPlane):
        def reclaim_expired_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
            raise StaleLeaseError("attempt was replaced")

    store = FakeStore([_action(RecoveryActionType.RECLAIM_JOB)])
    control_plane = StaleControlPlane()

    result = _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None and result.outcome == "stale-noop"
    assert store.finished[0][1] == "stale-noop"
    assert control_plane.calls == []


def test_exhausted_budget_is_not_claimable_and_does_not_mutate_any_business_fact() -> None:
    exhausted = replace(
        _action(RecoveryActionType.CANCEL_JOB),
        state="completed",
        result_code="disabled-action",
    )
    store = FakeStore([exhausted])
    control_plane = FakeControlPlane()

    result = _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert result is None
    assert control_plane.calls == []
    assert store.finished == []


def test_none_adapter_result_is_contract_error_and_not_succeeded() -> None:
    class NoneControlPlane(FakeControlPlane):
        def cancel_stalled_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
            return cast(str, None)

    store = FakeStore([_action(RecoveryActionType.CANCEL_JOB)])
    control_plane = NoneControlPlane()

    with pytest.raises(ValueError, match="invalid result code"):
        _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert store.actions[0].state == "running"
    assert store.finished == []


def test_job_control_plane_cannot_return_machine_recovery_result_code() -> None:
    class MachineCodeControlPlane(FakeControlPlane):
        def cancel_stalled_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
            return cast(str, "restarted")

    store = FakeStore([_action(RecoveryActionType.CANCEL_JOB)])

    with pytest.raises(ValueError, match="invalid result code"):
        _executor(store, MachineCodeControlPlane()).run_once(now=NOW + timedelta(seconds=1))

    assert store.actions[0].state == "running"
    assert store.finished == []


def test_executor_crash_leaves_running_action_for_expiry_reclaim() -> None:
    store = FakeStore([_action(RecoveryActionType.CANCEL_JOB)])
    control_plane = FakeControlPlane()
    control_plane.crash_once = True
    executor = _executor(store, control_plane)

    with pytest.raises(RuntimeError, match="simulated executor crash"):
        executor.run_once(now=NOW + timedelta(seconds=1))

    assert store.actions[0].state == "running"
    assert store.finished == []

    recovered = executor.run_once(now=NOW + timedelta(seconds=7))
    assert recovered is not None and recovered.outcome == "succeeded"
    assert store.actions[0].state == "completed"
    assert store.actions[0].worker_epoch == 2
    assert control_plane.calls == [("cancel", "job-1"), ("cancel", "job-1")]


def test_expired_old_action_worker_cannot_execute_after_reclaim_epoch_bump() -> None:
    store = FakeStore([_action(RecoveryActionType.RECLAIM_JOB)])
    control_plane = FakeControlPlane()
    first = store.claim_action(
        worker_id="old-worker",
        controller=_controller(),
        lease_seconds=1,
        now=NOW + timedelta(seconds=1),
    )
    assert first is not None
    replacement = store.claim_action(
        worker_id="new-worker",
        controller=_controller(),
        lease_seconds=30,
        now=NOW + timedelta(seconds=3),
    )
    assert replacement is not None
    assert replacement.worker_epoch == first.worker_epoch + 1

    old_result = store.execute_claimed_action(
        action_id=first.action_id,
        worker_id="old-worker",
        worker_epoch=first.worker_epoch,
        controller=_controller(),
        now=NOW + timedelta(seconds=4),
        callback=lambda _cursor, _action: control_plane.reclaim_expired_job(
            _action, now=NOW + timedelta(seconds=4)
        ),
    )

    assert old_result.state == "running"
    assert old_result.worker_id == "new-worker"
    assert control_plane.calls == []

    new_result = store.execute_claimed_action(
        action_id=replacement.action_id,
        worker_id="new-worker",
        worker_epoch=replacement.worker_epoch,
        controller=_controller(),
        now=NOW + timedelta(seconds=4),
        callback=lambda _cursor, action: control_plane.reclaim_expired_job(
            action, now=NOW + timedelta(seconds=4)
        ),
    )
    assert new_result.state == "completed"
    assert new_result.result_code == "succeeded"
    assert control_plane.calls == [("reclaim", "job-1")]


def test_recovery_action_result_never_exposes_receipt_or_pointer_postconditions() -> None:
    store = FakeStore([_action(RecoveryActionType.HEARTBEAT_JOB)])
    control_plane = FakeControlPlane()

    result = _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert "receipt" not in str(result.detail).lower()
    assert "pointer" not in str(result.detail).lower()
