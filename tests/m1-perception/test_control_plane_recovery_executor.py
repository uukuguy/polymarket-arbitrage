"""TDD contracts for the fenced job-level recovery executor."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

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

    def claim_action(self, **kwargs: Any) -> RecoveryActionRecord | None:
        now = kwargs["now"]
        for index, action in enumerate(self.actions):
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


class FakeControlPlane:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.crash_once = False

    def _run(self, name: str, action: RecoveryActionRecord, *, now: datetime) -> str:
        self.calls.append((name, action.target_id))
        if self.crash_once:
            self.crash_once = False
            raise RuntimeError("simulated executor crash")
        return "succeeded"

    def heartbeat_recovering_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("heartbeat", action, now=now)

    def cancel_stalled_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("cancel", action, now=now)

    def release_retryable_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("retry", action, now=now)

    def reclaim_expired_job(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("reclaim", action, now=now)

    def release_one_circuit_probe(self, action: RecoveryActionRecord, *, now: datetime) -> str:
        return self._run("probe", action, now=now)


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
        name not in {"publish_receipt", "publish_pointer"}
        for name, _ in control_plane.calls
    )


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


def test_duplicate_command_is_completed_once_and_second_turn_is_idle() -> None:
    store = FakeStore([_action(RecoveryActionType.RECLAIM_JOB)])
    control_plane = FakeControlPlane()
    executor = _executor(store, control_plane)

    first = executor.run_once(now=NOW + timedelta(seconds=1))
    second = executor.run_once(now=NOW + timedelta(seconds=2))

    assert first is not None and first.outcome == "succeeded"
    assert second is not None and second.outcome == "idle"
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

    assert result is not None and result.outcome == "idle"
    assert control_plane.calls == []
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


def test_recovery_action_result_never_exposes_receipt_or_pointer_postconditions() -> None:
    store = FakeStore([_action(RecoveryActionType.HEARTBEAT_JOB)])
    control_plane = FakeControlPlane()

    result = _executor(store, control_plane).run_once(now=NOW + timedelta(seconds=1))

    assert result is not None
    assert "receipt" not in str(result.detail).lower()
    assert "pointer" not in str(result.detail).lower()
