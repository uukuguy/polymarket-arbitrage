"""Execute one database-fenced, job-level runtime recovery command.

The executor deliberately owns only the action-worker boundary.  Scheduling,
controller fencing, budgets, and action leases remain in :mod:`recovery_store`;
business mutations remain in :class:`PostgresControlPlane`.  In particular,
this module has no receipt or publication APIs and cannot be used to publish a
data product as part of recovery.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from . import recovery_store as recovery_store_module
from .fly_recovery import FlyRecoveryResult
from .postgres import StaleLeaseError
from .recovery_models import RecoveryActionType
from .recovery_records import RecoveryActionRecord, RuntimeControllerLease

_STORE_CLOSED_RESULT_CODES = frozenset({"succeeded", "failed", "stale-noop", "disabled-action"})
_MACHINE_STORE_RESULT_BY_OUTCOME = {
    "restarted": "succeeded",
    "stale-noop": "stale-noop",
    "not-confirmed": "failed",
    "budget-exhausted": "disabled-action",
    "provider-unavailable": "failed",
    "disabled-action": "disabled-action",
}
_MACHINE_OUTCOMES = frozenset(_MACHINE_STORE_RESULT_BY_OUTCOME)


class _RecoveryControlPlane(Protocol):
    """Minimal atomic adapter surface required by the executor."""

    def _execute_recovery_action_cursor(
        self,
        cursor: Any,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        heartbeat_lease_seconds: int,
    ) -> object: ...


class _MachineRecoveryAdapter(Protocol):
    def restart_exact_machine(
        self,
        *,
        app: str,
        machine_id: str,
        action: RecoveryActionRecord,
        controller: RuntimeControllerLease,
        now: datetime,
    ) -> FlyRecoveryResult: ...


# This is intentionally the complete job-level authority for Plan 03.  Do not
# add process or Machine methods here: those actions are handled as durable
# disabled no-ops until the independent topology gate exists.
_JOB_ACTIONS: dict[RecoveryActionType, str] = {
    RecoveryActionType.HEARTBEAT_JOB: "heartbeat_recovering_job",
    RecoveryActionType.CANCEL_JOB: "cancel_stalled_job",
    RecoveryActionType.RETRY_JOB: "release_retryable_job",
    RecoveryActionType.RECLAIM_JOB: "reclaim_expired_job",
    RecoveryActionType.PROBE_CIRCUIT: "release_one_circuit_probe",
}
_DISABLED_ACTIONS = frozenset(
    {
        RecoveryActionType.RESTART_WORKER_PROCESS,
        RecoveryActionType.RESTART_MACHINE,
    }
)


@dataclass(frozen=True, slots=True)
class RecoveryActionResult:
    """Observable result of one executor turn.

    ``detail`` is bounded action metadata only.  It intentionally contains no
    receipt, artifact, or publication-pointer value.
    """

    action_id: str | None
    outcome: str
    target_id: str | None = None
    action_type: str | None = None
    detail: Mapping[str, object] = field(default_factory=dict)

    @property
    def job_key(self) -> str | None:
        """Compatibility alias for worker-style result consumers."""
        return self.target_id


class _RecoveryStore(Protocol):
    def claim_action(
        self,
        *,
        worker_id: str,
        controller: RuntimeControllerLease,
        lease_seconds: int,
        now: datetime,
        expected_action_id: str | None = None,
    ) -> RecoveryActionRecord | None: ...

    def execute_claimed_action(
        self,
        *,
        action_id: str,
        worker_id: str,
        worker_epoch: int,
        controller: RuntimeControllerLease,
        now: datetime,
        callback: Callable[[Any, RecoveryActionRecord], object],
    ) -> RecoveryActionRecord: ...


class _ModuleRecoveryStore:
    """Small object facade over Task 2's typed module-level store API."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def claim_action(self, **kwargs: Any) -> RecoveryActionRecord | None:
        return recovery_store_module.claim_action(self._connection_factory, **kwargs)

    def execute_claimed_action(self, **kwargs: Any) -> RecoveryActionRecord:
        return recovery_store_module.execute_claimed_action(self._connection_factory, **kwargs)


class RecoveryExecutor:
    """Claim, execute, and exactly finish one fenced recovery action."""

    def __init__(
        self,
        *,
        control_plane: _RecoveryControlPlane,
        controller: RuntimeControllerLease,
        worker_id: str,
        store: _RecoveryStore | Callable[[], Any] | None = None,
        connection_factory: Callable[[], Any] | None = None,
        machine_recovery_adapter: _MachineRecoveryAdapter | None = None,
        action_lease_seconds: int = 30,
        heartbeat_lease_seconds: int = 30,
    ) -> None:
        if type(controller) is not RuntimeControllerLease:
            raise TypeError("controller must be RuntimeControllerLease")
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if action_lease_seconds <= 0 or heartbeat_lease_seconds <= 0:
            raise ValueError("recovery lease bounds must be positive")
        if store is not None and connection_factory is not None:
            raise ValueError("provide store or connection_factory, not both")
        if store is None:
            if connection_factory is None:
                raise ValueError("store or connection_factory is required")
            selected_store: _RecoveryStore = _ModuleRecoveryStore(connection_factory)
        elif callable(store) and not hasattr(store, "claim_action"):
            selected_store = _ModuleRecoveryStore(store)
        else:
            selected_store = store  # type: ignore[assignment]

        self._control_plane = control_plane
        self._controller = controller
        self._worker_id = worker_id
        self._store = selected_store
        self._machine_recovery_adapter = machine_recovery_adapter
        self._action_lease_seconds = action_lease_seconds
        self._heartbeat_lease_seconds = heartbeat_lease_seconds

    def run_once(
        self,
        *,
        now: datetime,
        expected_action_id: str | None = None,
    ) -> RecoveryActionResult | None:
        """Execute at most one claimable action.

        A control-plane exception is intentionally allowed to escape.  The
        action remains ``running`` until its worker lease expires, so a later
        executor turn can reclaim it.  Only a stale job fence is converted to a
        durable ``stale-noop`` completion.  Operator-selected turns may bind
        the claim to the exact action they just scheduled; ordinary service
        turns retain oldest-first queue behavior.
        """

        action = self._store.claim_action(
            worker_id=self._worker_id,
            controller=self._controller,
            lease_seconds=self._action_lease_seconds,
            now=now,
            expected_action_id=expected_action_id,
        )
        if action is None:
            return None
        if action.state == "completed":
            return self._result_from_record(action)

        machine_outcomes: list[str] = []
        finished = self._store.execute_claimed_action(
            action_id=action.action_id,
            worker_id=action.worker_id or self._worker_id,
            worker_epoch=action.worker_epoch,
            controller=self._controller,
            now=now,
            callback=lambda cursor, claimed: self._execute_claimed(
                cursor,
                claimed,
                now=now,
                machine_outcomes=machine_outcomes,
            ),
        )
        result = self._result_from_record(finished)
        if machine_outcomes and finished.state == "completed":
            return RecoveryActionResult(
                action_id=result.action_id,
                outcome=machine_outcomes[0],
                target_id=result.target_id,
                action_type=result.action_type,
                detail=result.detail,
            )
        return result

    def _execute_claimed(
        self,
        cursor: Any,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        machine_outcomes: list[str],
    ) -> str:
        """Dispatch inside the store-owned transaction boundary."""
        try:
            action_type = RecoveryActionType(action.action_type)
        except ValueError:
            return "disabled-action"
        if action_type in _DISABLED_ACTIONS:
            outcome = self._execute_machine_recovery(action, now=now)
            machine_outcomes.append(outcome)
            return _MACHINE_STORE_RESULT_BY_OUTCOME[outcome]
        if action_type not in _JOB_ACTIONS:
            return "disabled-action"

        try:
            result = self._control_plane._execute_recovery_action_cursor(
                cursor,
                action,
                now=now,
                heartbeat_lease_seconds=self._heartbeat_lease_seconds,
            )
        except StaleLeaseError:
            return "stale-noop"

        if not isinstance(result, str) or result not in _STORE_CLOSED_RESULT_CODES:
            raise ValueError("recovery control-plane method returned an invalid result code")
        return result

    def _execute_machine_recovery(
        self,
        action: RecoveryActionRecord,
        *,
        now: datetime,
    ) -> str:
        if self._machine_recovery_adapter is None:
            return "disabled-action"
        app = action.detail.get("fly_app")
        machine_id = action.detail.get("fly_machine_id")
        if not isinstance(app, str) or not app.strip():
            return "stale-noop"
        if not isinstance(machine_id, str) or not machine_id.strip():
            return "stale-noop"
        result = self._machine_recovery_adapter.restart_exact_machine(
            app=app,
            machine_id=machine_id,
            action=action,
            controller=self._controller,
            now=now,
        )
        if result.code not in _MACHINE_OUTCOMES:
            raise ValueError("machine recovery adapter returned an invalid result code")
        return result.code

    @staticmethod
    def _result_from_record(action: RecoveryActionRecord) -> RecoveryActionResult:
        return RecoveryActionResult(
            action_id=action.action_id,
            outcome=action.result_code or "running",
            target_id=action.target_id,
            action_type=action.action_type,
            detail=dict(action.detail),
        )


__all__ = ["RecoveryActionResult", "RecoveryExecutor"]
