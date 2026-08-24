from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from polyarb.control_plane.reconciler import RuntimeReconciler
from polyarb.control_plane.recovery_models import (
    RecoveryActionType,
    RecoveryBudget,
    RecoveryDecision,
    RecoveryFailureClass,
    RecoveryRuntimeState,
)
from polyarb.control_plane.runtime_models import RuntimeDeadlineProfile

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
PROFILE = RuntimeDeadlineProfile(
    policy_version="runtime-v1",
    lease_seconds=120,
    heartbeat_seconds=30,
    progress_seconds=90,
    attempt_seconds=300,
)


def state(
    *,
    heartbeat_age: int = 5,
    progress_age: int = 5,
    attempt_age: int = 30,
    lease_expired: bool = False,
    lease_remaining_seconds: int = 60,
    owner_is_current: bool = True,
    recovery_budget_remaining: int = 3,
    retry_count: int = 0,
    failure_class: RecoveryFailureClass | None = None,
    open_circuit: bool = False,
    circuit_open_age: int = 120,
    circuit_cooldown_seconds: int = 60,
) -> RecoveryRuntimeState:
    lease_expires_at = (
        NOW - timedelta(seconds=1)
        if lease_expired
        else NOW + timedelta(seconds=lease_remaining_seconds)
    )
    circuit_opened_at = (
        NOW - timedelta(seconds=circuit_open_age) if open_circuit else None
    )
    return RecoveryRuntimeState(
        job_key="quote:test:batch:1",
        attempt_id="attempt-1",
        lease_epoch=1,
        owner_is_current=owner_is_current,
        profile=PROFILE,
        attempt_started_at=NOW - timedelta(seconds=attempt_age),
        last_heartbeat_at=NOW - timedelta(seconds=heartbeat_age),
        last_progress_at=NOW - timedelta(seconds=progress_age),
        lease_expires_at=lease_expires_at,
        retry_count=retry_count,
        recovery_budget=RecoveryBudget(remaining_actions=recovery_budget_remaining),
        failure_class=failure_class,
        open_circuit=open_circuit,
        circuit_opened_at=circuit_opened_at,
        circuit_cooldown_seconds=circuit_cooldown_seconds,
    )


@pytest.mark.parametrize(
    ("runtime_state", "expected_action", "expected_reason"),
    [
        (state(), None, "job.healthy"),
        (
            state(heartbeat_age=31, lease_remaining_seconds=20),
            RecoveryActionType.HEARTBEAT_JOB,
            "job.lease-at-risk",
        ),
        (
            state(heartbeat_age=5, progress_age=91),
            RecoveryActionType.CANCEL_JOB,
            "job.progress-stalled",
        ),
        (state(heartbeat_age=91, lease_expired=False), None, "job.heartbeat-missing-fence"),
        (
            state(heartbeat_age=121, lease_expired=True),
            RecoveryActionType.RECLAIM_JOB,
            "job.heartbeat-missing",
        ),
        (state(attempt_age=301), RecoveryActionType.CANCEL_JOB, "job.attempt-deadline"),
        (
            state(open_circuit=True, circuit_open_age=61),
            RecoveryActionType.PROBE_CIRCUIT,
            "circuit.probe-due",
        ),
        (state(open_circuit=True, circuit_open_age=59), None, "circuit.cooldown"),
        (
            state(heartbeat_age=121, lease_expired=True, recovery_budget_remaining=0),
            None,
            "recovery.budget-exhausted",
        ),
    ],
)
def test_reconciler_classifies_bounded_runtime_recovery_table(
    runtime_state: RecoveryRuntimeState,
    expected_action: RecoveryActionType | None,
    expected_reason: str,
) -> None:
    decision = RuntimeReconciler().evaluate(runtime_state, now=NOW)

    assert decision.action is expected_action
    assert decision.reason_code == expected_reason
    assert decision.next_check_at.tzinfo is not None
    assert decision.next_check_at >= NOW


def test_missing_heartbeat_waits_for_fence_before_reclaim() -> None:
    reconciler = RuntimeReconciler()

    assert reconciler.evaluate(state(heartbeat_age=91, lease_expired=False), now=NOW).action is None
    assert (
        reconciler.evaluate(state(heartbeat_age=121, lease_expired=True), now=NOW).action
        is RecoveryActionType.RECLAIM_JOB
    )


@pytest.mark.parametrize(
    "failure_class",
    [
        RecoveryFailureClass.INTEGRITY,
        RecoveryFailureClass.AUTHENTICATION,
        RecoveryFailureClass.SCHEMA,
        RecoveryFailureClass.CREDENTIAL,
        RecoveryFailureClass.CAPACITY,
    ],
)
def test_integrity_auth_schema_credential_and_capacity_are_human_only(
    failure_class: RecoveryFailureClass,
) -> None:
    decision = RuntimeReconciler().evaluate(
        state(failure_class=failure_class, progress_age=91, lease_expired=True),
        now=NOW,
    )

    assert decision.action is None
    assert decision.reason_code == f"failure.{failure_class.value}"
    assert decision.incident_severity == "critical"
    assert decision.qualification_breaking is True


def test_precedence_fencing_budget_and_safety_outrank_retry_convenience() -> None:
    reconciler = RuntimeReconciler()

    stale = reconciler.evaluate(
        state(owner_is_current=False, progress_age=91, heartbeat_age=121, lease_expired=True),
        now=NOW,
    )
    assert stale.action is None
    assert stale.reason_code == "recovery.stale-fence"

    human_only = reconciler.evaluate(
        state(failure_class=RecoveryFailureClass.SCHEMA, progress_age=91),
        now=NOW,
    )
    assert human_only.action is None
    assert human_only.reason_code == "failure.schema"

    budget_exhausted = reconciler.evaluate(
        state(progress_age=91, recovery_budget_remaining=0),
        now=NOW,
    )
    assert budget_exhausted.action is None
    assert budget_exhausted.reason_code == "recovery.budget-exhausted"


def test_process_and_machine_actions_exist_but_are_not_chosen_automatically() -> None:
    decision = RuntimeReconciler().evaluate(
        state(heartbeat_age=300, progress_age=300, lease_expired=True),
        now=NOW,
    )

    assert RecoveryActionType.RESTART_WORKER_PROCESS.value == "restart-worker-process"
    assert RecoveryActionType.RESTART_MACHINE.value == "restart-machine"
    assert decision.action is RecoveryActionType.RECLAIM_JOB
    assert decision.action not in {
        RecoveryActionType.RESTART_WORKER_PROCESS,
        RecoveryActionType.RESTART_MACHINE,
    }


def test_next_check_at_is_deterministic_from_inputs() -> None:
    runtime_state = state(heartbeat_age=10, progress_age=20, attempt_age=30)
    reconciler = RuntimeReconciler()

    first = reconciler.evaluate(runtime_state, now=NOW)
    second = reconciler.evaluate(runtime_state, now=NOW)

    assert first == second
    assert first.next_check_at == NOW + timedelta(seconds=20)


def test_recovery_types_reject_naive_times_invalid_types_and_negative_counts() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        RuntimeReconciler().evaluate(state(), now=datetime(2026, 8, 24, 12, 0))

    with pytest.raises(ValueError, match="timezone-aware"):
        replace(state(), last_heartbeat_at=datetime(2026, 8, 24, 12, 0))

    with pytest.raises(TypeError, match="lease_epoch must be an exact int"):
        replace(state(), lease_epoch=True)

    with pytest.raises(ValueError, match="non-negative"):
        RecoveryBudget(remaining_actions=-1)

    with pytest.raises(TypeError, match="remaining_actions must be an exact int"):
        RecoveryBudget(remaining_actions=True)

    with pytest.raises(TypeError, match="failure_class"):
        replace(state(), failure_class="schema")

    with pytest.raises(ValueError, match="open circuit"):
        replace(state(open_circuit=True), circuit_opened_at=None)


def test_recovery_decision_enforces_closed_reason_codes_and_invariants() -> None:
    with pytest.raises(ValueError, match="reason_code"):
        RecoveryDecision(
            action=None,
            reason_code="unbounded.freeform",
            incident_severity="warning",
            qualification_breaking=False,
            next_check_at=NOW,
        )

    with pytest.raises(ValueError, match="automatic action"):
        RecoveryDecision(
            action=RecoveryActionType.CANCEL_JOB,
            reason_code="recovery.budget-exhausted",
            incident_severity="critical",
            qualification_breaking=True,
            next_check_at=NOW,
        )

    with pytest.raises(ValueError, match="qualification-breaking"):
        RecoveryDecision(
            action=None,
            reason_code="failure.schema",
            incident_severity="warning",
            qualification_breaking=False,
            next_check_at=NOW,
        )
