"""Pure M1 runtime deadline reconciliation."""

from __future__ import annotations

from datetime import datetime, timedelta

from .recovery_models import (
    RecoveryActionType,
    RecoveryDecision,
    RecoveryRuntimeState,
    require_timezone_aware,
)


class RuntimeReconciler:
    """Classify persisted runtime facts into one bounded recovery decision."""

    def evaluate(self, state: RecoveryRuntimeState, *, now: datetime) -> RecoveryDecision:
        if type(state) is not RecoveryRuntimeState:
            raise TypeError("state must be RecoveryRuntimeState")
        require_timezone_aware(now, field_name="now")

        if not state.owner_is_current:
            return self._decision(
                None,
                "recovery.stale-fence",
                now=now,
                next_check_at=now,
                critical=True,
                breaking=True,
            )

        if state.failure_class is not None:
            return self._decision(
                None,
                f"failure.{state.failure_class.value}",
                now=now,
                next_check_at=now,
                critical=True,
                breaking=True,
            )

        if state.recovery_budget.remaining_actions == 0 and self._has_recoverable_condition(
            state,
            now=now,
        ):
            return self._decision(
                None,
                "recovery.budget-exhausted",
                now=now,
                next_check_at=now,
                critical=True,
                breaking=True,
            )

        heartbeat_missing = (
            now
            >= state.last_heartbeat_at
            + timedelta(seconds=state.profile.missed_heartbeat_incident_seconds)
        )
        if heartbeat_missing:
            if now >= state.lease_expires_at:
                return self._decision(
                    RecoveryActionType.RECLAIM_JOB,
                    "job.heartbeat-missing",
                    now=now,
                    next_check_at=now,
                    critical=True,
                    breaking=True,
                )
            return self._decision(
                None,
                "job.heartbeat-missing-fence",
                now=now,
                next_check_at=state.lease_expires_at,
                critical=True,
                breaking=True,
            )

        if now >= state.lease_expires_at:
            return self._decision(
                RecoveryActionType.RECLAIM_JOB,
                "job.lease-expired",
                now=now,
                next_check_at=now,
                critical=True,
                breaking=True,
            )

        if state.open_circuit:
            assert state.circuit_opened_at is not None
            probe_at = state.circuit_opened_at + timedelta(
                seconds=state.circuit_cooldown_seconds
            )
            if now >= probe_at:
                return self._decision(
                    RecoveryActionType.PROBE_CIRCUIT,
                    "circuit.probe-due",
                    now=now,
                    next_check_at=now,
                )
            return self._decision(
                None,
                "circuit.cooldown",
                now=now,
                next_check_at=probe_at,
            )

        if now >= state.attempt_started_at + timedelta(seconds=state.profile.attempt_seconds):
            return self._decision(
                RecoveryActionType.CANCEL_JOB,
                "job.attempt-deadline",
                now=now,
                next_check_at=now,
                critical=True,
                breaking=True,
            )

        if now >= state.last_progress_at + timedelta(seconds=state.profile.progress_seconds):
            return self._decision(
                RecoveryActionType.CANCEL_JOB,
                "job.progress-stalled",
                now=now,
                next_check_at=now,
            )

        heartbeat_due_at = state.last_heartbeat_at + timedelta(
            seconds=state.profile.heartbeat_seconds
        )
        lease_at_risk_at = state.lease_expires_at - timedelta(
            seconds=state.profile.heartbeat_seconds
        )
        if now >= heartbeat_due_at or now >= lease_at_risk_at:
            return self._decision(
                RecoveryActionType.HEARTBEAT_JOB,
                "job.lease-at-risk",
                now=now,
                next_check_at=now,
            )

        return self._decision(
            None,
            "job.healthy",
            now=now,
            next_check_at=min(
                heartbeat_due_at,
                lease_at_risk_at,
                state.last_progress_at + timedelta(seconds=state.profile.progress_seconds),
                state.attempt_started_at + timedelta(seconds=state.profile.attempt_seconds),
                state.lease_expires_at,
            ),
        )

    def _has_recoverable_condition(self, state: RecoveryRuntimeState, *, now: datetime) -> bool:
        return (
            state.open_circuit
            or now >= state.attempt_started_at + timedelta(seconds=state.profile.attempt_seconds)
            or now >= state.last_progress_at + timedelta(seconds=state.profile.progress_seconds)
            or now
            >= state.last_heartbeat_at
            + timedelta(seconds=state.profile.missed_heartbeat_incident_seconds)
            or now
            >= state.last_heartbeat_at + timedelta(seconds=state.profile.heartbeat_seconds)
            or now >= state.lease_expires_at
            or now >= state.lease_expires_at - timedelta(seconds=state.profile.heartbeat_seconds)
        )

    def _decision(
        self,
        action: RecoveryActionType | None,
        reason_code: str,
        *,
        now: datetime,
        next_check_at: datetime,
        critical: bool = False,
        breaking: bool = False,
    ) -> RecoveryDecision:
        return RecoveryDecision(
            action=action,
            reason_code=reason_code,
            incident_severity="critical" if critical else "warning",
            qualification_breaking=breaking,
            next_check_at=max(now, next_check_at),
        )


__all__ = ["RuntimeReconciler"]
