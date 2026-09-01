"""Observe-only runtime reconciliation decision ledger."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Literal, cast

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .reconciler import RuntimeReconciler
from .recovery_models import (
    RECOVERY_REASON_CODES,
    RecoveryActionType,
    RecoveryBudget,
    RecoveryDecision,
    RecoveryFailureClass,
    RecoveryRuntimeState,
    require_timezone_aware,
)
from .recovery_store import (
    _RUNTIME_COMPONENTS,
    ConnectionFactory,
    RuntimeReconcileCandidate,
    _runtime_deadline_profile,
    _runtime_failure_class,
    _safe_text,
    _set_recovery_timeouts,
)
from .runtime_models import RuntimeDeadlineProfile

_MAX_CANONICAL_PAYLOAD_BYTES = 8192
class RuntimeObserveError(ValueError):
    """Base class for observe-only runtime evidence errors."""


class RuntimeObserveVerificationError(RuntimeObserveError):
    """Observe-only evidence is missing, stale, inconsistent, or mutating."""


@dataclass(frozen=True, slots=True)
class RuntimeObserveDecisionRecord:
    decision_id: str
    idempotency_key: str
    controller_id: str
    controller_owner_id: str
    controller_epoch: int
    observed_at: datetime
    decision_kind: Literal["decision", "idle"]
    target_type: Literal["job", "circuit"] | None
    target_id: str | None
    action_type: str | None
    reason_code: str
    incident_severity: Literal["warning", "critical"]
    qualification_breaking: bool
    next_check_at: datetime
    runtime_state_digest: str | None
    decision_digest: str
    payload: Mapping[str, object]
    payload_sha256: str

    def __post_init__(self) -> None:
        _require_nonempty("decision_id", self.decision_id)
        _require_nonempty("idempotency_key", self.idempotency_key)
        _require_nonempty("controller_id", self.controller_id)
        _require_nonempty("controller_owner_id", self.controller_owner_id)
        _require_positive_int("controller_epoch", self.controller_epoch)
        require_timezone_aware(self.observed_at, field_name="observed_at")
        require_timezone_aware(self.next_check_at, field_name="next_check_at")
        if self.decision_kind not in {"decision", "idle"}:
            raise ValueError("decision_kind must be decision or idle")
        if self.target_type is not None and self.target_type not in {"job", "circuit"}:
            raise ValueError("target_type must be job, circuit, or None")
        if self.decision_kind == "idle":
            if (
                self.target_type is not None
                or self.target_id is not None
                or self.action_type is not None
                or self.runtime_state_digest is not None
            ):
                raise ValueError("idle records cannot carry target/action/runtime state")
        else:
            if self.target_type is None or not self.target_id or self.runtime_state_digest is None:
                raise ValueError("decision records require target and runtime state")
        if self.action_type is not None and self.action_type not in {
            item.value for item in RecoveryActionType
        }:
            raise ValueError("action_type must use the bounded recovery action vocabulary")
        if self.reason_code not in RECOVERY_REASON_CODES:
            raise ValueError("reason_code must use the bounded recovery reason vocabulary")
        if self.incident_severity not in {"warning", "critical"}:
            raise ValueError("incident_severity must be warning or critical")
        if type(self.qualification_breaking) is not bool:
            raise TypeError("qualification_breaking must be bool")
        for name, value in (
            ("decision_digest", self.decision_digest),
            ("payload_sha256", self.payload_sha256),
        ):
            _require_sha256(name, value)
        if self.runtime_state_digest is not None:
            _require_sha256("runtime_state_digest", self.runtime_state_digest)
        canonical = canonical_observe_record_bytes(self.payload)
        if len(canonical) > _MAX_CANONICAL_PAYLOAD_BYTES:
            raise ValueError("observe payload exceeds bounded canonical size")
        if sha256(canonical).hexdigest() != self.payload_sha256:
            raise ValueError("payload_sha256 must match canonical payload")
        if self.decision_digest != self.payload_sha256:
            raise ValueError("decision_digest must match payload_sha256")


@dataclass(frozen=True, slots=True)
class RuntimeObserveVerification:
    status: Literal["pass"]
    controller_id: str
    controller_owner_id: str
    controller_epoch: int
    started_at: datetime
    latest_observed_at: datetime
    duration_seconds: int
    decision_count: int
    idle_count: int
    recovery_action_count: int
    current_candidate_count: int
    max_gap_seconds: int
    latest_decision_digest: str


def canonical_observe_record_bytes(payload: Mapping[str, object]) -> bytes:
    _assert_secret_free(payload)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def build_runtime_observe_decision_record(
    *,
    controller_id: str,
    controller_owner_id: str,
    controller_epoch: int,
    observed_at: datetime,
    candidate: RuntimeReconcileCandidate,
    decision: RecoveryDecision,
    observed_by: str,
) -> RuntimeObserveDecisionRecord:
    _require_nonempty("controller_id", controller_id)
    _require_nonempty("controller_owner_id", controller_owner_id)
    _require_nonempty("observed_by", observed_by)
    _require_positive_int("controller_epoch", controller_epoch)
    require_timezone_aware(observed_at, field_name="observed_at")
    if type(candidate) is not RuntimeReconcileCandidate:
        raise TypeError("candidate must be RuntimeReconcileCandidate")
    if type(decision) is not RecoveryDecision:
        raise TypeError("decision must be RecoveryDecision")
    replay = RuntimeReconciler().evaluate(candidate.runtime_state, now=observed_at)
    _assert_same_decision(decision, replay, context="RuntimeReconciler replay")
    runtime_state_payload = _runtime_state_payload(candidate.runtime_state)
    runtime_state_digest = _digest(runtime_state_payload)
    decision_payload = _decision_payload(decision)
    payload: dict[str, object] = {
        "schema": "m1-runtime-observe-decision-v1",
        "controller_id": controller_id,
        "controller_owner_id": controller_owner_id,
        "controller_epoch": controller_epoch,
        "observed_at": _dt(observed_at),
        "observed_by": observed_by,
        "decision_kind": "decision",
        "target": {
            "target_type": candidate.target_type,
            "target_id": candidate.target_id,
            "component": candidate.component,
            "job_type": candidate.job_type,
            "job_state": candidate.job_state,
            "worker_id": candidate.worker_id,
            "incident_key": candidate.incident_key,
            "channels": list(candidate.channels),
            "cooldown_seconds": candidate.cooldown_seconds,
        },
        "runtime_state": runtime_state_payload,
        "runtime_state_digest": runtime_state_digest,
        "decision": decision_payload,
    }
    digest = _digest(payload)
    return RuntimeObserveDecisionRecord(
        decision_id=f"runtime-observe:{digest}",
        idempotency_key=f"runtime-observe-idempotency:{digest}",
        controller_id=controller_id,
        controller_owner_id=controller_owner_id,
        controller_epoch=controller_epoch,
        observed_at=observed_at,
        decision_kind="decision",
        target_type=cast(Literal["job", "circuit"], candidate.target_type),
        target_id=candidate.target_id,
        action_type=None if decision.action is None else decision.action.value,
        reason_code=decision.reason_code,
        incident_severity=decision.incident_severity,
        qualification_breaking=decision.qualification_breaking,
        next_check_at=decision.next_check_at,
        runtime_state_digest=runtime_state_digest,
        decision_digest=digest,
        payload=payload,
        payload_sha256=digest,
    )


def build_runtime_observe_idle_record(
    *,
    controller_id: str,
    controller_owner_id: str,
    controller_epoch: int,
    observed_at: datetime,
    next_check_at: datetime,
    observed_by: str,
) -> RuntimeObserveDecisionRecord:
    _require_nonempty("controller_id", controller_id)
    _require_nonempty("controller_owner_id", controller_owner_id)
    _require_nonempty("observed_by", observed_by)
    _require_positive_int("controller_epoch", controller_epoch)
    require_timezone_aware(observed_at, field_name="observed_at")
    require_timezone_aware(next_check_at, field_name="next_check_at")
    payload: dict[str, object] = {
        "schema": "m1-runtime-observe-decision-v1",
        "controller_id": controller_id,
        "controller_owner_id": controller_owner_id,
        "controller_epoch": controller_epoch,
        "observed_at": _dt(observed_at),
        "observed_by": observed_by,
        "decision_kind": "idle",
        "target": None,
        "runtime_state": None,
        "runtime_state_digest": None,
        "decision": {
            "action_type": None,
            "reason_code": "job.healthy",
            "incident_severity": "warning",
            "qualification_breaking": False,
            "next_check_at": _dt(next_check_at),
        },
    }
    digest = _digest(payload)
    return RuntimeObserveDecisionRecord(
        decision_id=f"runtime-observe:{digest}",
        idempotency_key=f"runtime-observe-idempotency:{digest}",
        controller_id=controller_id,
        controller_owner_id=controller_owner_id,
        controller_epoch=controller_epoch,
        observed_at=observed_at,
        decision_kind="idle",
        target_type=None,
        target_id=None,
        action_type=None,
        reason_code="job.healthy",
        incident_severity="warning",
        qualification_breaking=False,
        next_check_at=next_check_at,
        runtime_state_digest=None,
        decision_digest=digest,
        payload=payload,
        payload_sha256=digest,
    )


def _semantic_digest(record: RuntimeObserveDecisionRecord) -> str:
    """Identify an operational state without volatile observation timestamps."""
    payload = {
        "target_type": record.target_type,
        "target_id": record.target_id,
        "action_type": record.action_type,
        "reason_code": record.reason_code,
        "incident_severity": record.incident_severity,
        "qualification_breaking": record.qualification_breaking,
        "decision_kind": record.decision_kind,
    }
    return sha256(canonical_observe_record_bytes(payload)).hexdigest()


def _turn_from_records(
    records: Sequence[RuntimeObserveDecisionRecord],
    *,
    coverage_truncated: bool = False,
) -> dict[str, object]:
    """Translate one same-clock legacy observation batch into the bounded RPC wire form."""
    batch = tuple(records)
    if not batch:
        raise ValueError("runtime observe turn must not be empty")
    first = batch[0]
    identity = (first.controller_id, first.controller_owner_id, first.controller_epoch)
    if any(
        (record.controller_id, record.controller_owner_id, record.controller_epoch) != identity
        or record.observed_at != first.observed_at
        for record in batch[1:]
    ):
        raise ValueError("runtime observe turn must share identity and observation clock")
    candidates: list[dict[str, object]] = []
    for record in batch:
        if record.decision_kind == "idle":
            continue
        assert record.target_type is not None
        assert record.target_id is not None
        payload = {
            "schema": "m1-runtime-observe-current-v1",
            "target": {
                "target_type": record.target_type,
                "target_id": record.target_id,
            },
            "decision": {
                "action_type": record.action_type,
                "reason_code": record.reason_code,
                "incident_severity": record.incident_severity,
                "qualification_breaking": record.qualification_breaking,
            },
            "runtime_state_digest": record.runtime_state_digest,
        }
        if len(canonical_observe_record_bytes(payload)) > 2_048:
            raise RuntimeObserveError("bounded runtime observe candidate payload is too large")
        candidates.append(
            {
                "target_type": record.target_type,
                "target_id": record.target_id,
                "semantic_digest": _semantic_digest(record),
                "action_type": record.action_type,
                "reason_code": record.reason_code,
                "severity": record.incident_severity,
                "qualification_breaking": record.qualification_breaking,
                "payload": payload,
            }
        )
    return {
        "controller_id": first.controller_id,
        "controller_owner_id": first.controller_owner_id,
        "controller_epoch": first.controller_epoch,
        "observed_at": first.observed_at.isoformat(),
        "coverage_truncated": coverage_truncated,
        "candidates": candidates,
    }


def apply_runtime_observe_turn(
    connection_factory: ConnectionFactory,
    records: Sequence[RuntimeObserveDecisionRecord],
    *,
    coverage_truncated: bool = False,
    stop_requested: Callable[[], bool] | None = None,
) -> dict[str, object]:
    """Apply one complete bounded semantic turn through the lease-fenced RPC."""
    turn = _turn_from_records(records, coverage_truncated=coverage_truncated)
    _raise_if_observe_stopped(stop_requested)
    with (
        connection_factory() as connection,
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        cursor.execute("SET TRANSACTION READ WRITE")
        _set_recovery_timeouts(cursor)
        _raise_if_observe_stopped(stop_requested)
        try:
            cursor.execute(
                "SELECT public.m1_runtime_observe_apply_turn(%s) AS result",
                (Jsonb(turn),),
            )
        except Exception as error:
            raise RuntimeObserveError("bounded runtime observe turn was rejected") from error
        row = cursor.fetchone()
        if row is None or not isinstance(row["result"], Mapping):
            raise RuntimeObserveError("bounded runtime observe turn returned no result")
        _raise_if_observe_stopped(stop_requested)
        connection.commit()
        return dict(cast(Mapping[str, object], row["result"]))


def insert_runtime_observe_decision(
    connection_factory: ConnectionFactory,
    record: RuntimeObserveDecisionRecord,
) -> RuntimeObserveDecisionRecord:
    if type(record) is not RuntimeObserveDecisionRecord:
        raise TypeError("record must be RuntimeObserveDecisionRecord")
    apply_runtime_observe_turn(connection_factory, (record,))
    return record


def insert_runtime_observe_decisions(
    connection_factory: ConnectionFactory,
    records: Sequence[RuntimeObserveDecisionRecord],
    *,
    coverage_truncated: bool = False,
    stop_requested: Callable[[], bool] | None = None,
) -> tuple[RuntimeObserveDecisionRecord, ...]:
    """Persist one bounded observation turn in fixed database round trips."""
    batch = tuple(records)
    if not batch:
        raise ValueError("runtime observe batch must not be empty")
    if any(type(record) is not RuntimeObserveDecisionRecord for record in batch):
        raise TypeError("records must contain RuntimeObserveDecisionRecord values")
    _raise_if_observe_stopped(stop_requested)
    identity = (
        batch[0].controller_id,
        batch[0].controller_owner_id,
        batch[0].controller_epoch,
    )
    if any(
        (
            record.controller_id,
            record.controller_owner_id,
            record.controller_epoch,
        )
        != identity
        for record in batch[1:]
    ):
        raise ValueError("runtime observe batch must share one controller turn identity")
    grouped: dict[datetime, list[RuntimeObserveDecisionRecord]] = {}
    for record in batch:
        grouped.setdefault(record.observed_at, []).append(record)
    for same_clock in grouped.values():
        apply_runtime_observe_turn(
            connection_factory,
            same_clock,
            coverage_truncated=coverage_truncated,
            stop_requested=stop_requested,
        )
    return batch


def _raise_if_observe_stopped(stop_requested: Callable[[], bool] | None) -> None:
    if stop_requested is not None and stop_requested():
        raise RuntimeObserveError("runtime observe stop requested")


def verify_runtime_observe_window(
    connection_factory: ConnectionFactory,
    *,
    controller_id: str,
    controller_owner_id: str,
    controller_epoch: int,
    now: datetime,
    minimum_seconds: int,
    max_freshness_seconds: int,
    max_gap_seconds: int,
    sample_limit: int = 500,
) -> RuntimeObserveVerification:
    _require_nonempty("controller_id", controller_id)
    _require_nonempty("controller_owner_id", controller_owner_id)
    _require_positive_int("controller_epoch", controller_epoch)
    require_timezone_aware(now, field_name="now")
    for name, value in (
        ("minimum_seconds", minimum_seconds),
        ("max_freshness_seconds", max_freshness_seconds),
        ("max_gap_seconds", max_gap_seconds),
        ("sample_limit", sample_limit),
    ):
        _require_positive_int(name, value)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        _set_recovery_timeouts(cursor)
        try:
            _require_current_controller_lease(
                cursor,
                controller_id=controller_id,
                controller_owner_id=controller_owner_id,
                controller_epoch=controller_epoch,
                observed_at=now,
                lock=False,
            )
        except RuntimeObserveError as exc:
            raise RuntimeObserveVerificationError(str(exc)) from exc
        cursor.execute(
            """
            SELECT controller_owner_id, controller_epoch, continuous_since,
                   last_completed_at, max_gap_seconds, candidate_count,
                   coverage_truncated, storage_limited
            FROM public.m1_runtime_observe_status
            WHERE controller_id = %s
            """,
            (controller_id,),
        )
        status_row = cursor.fetchone()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM public.m1_recovery_actions
            WHERE controller_id = %s
              AND (
                  requested_at BETWEEN %s AND %s
                  OR started_at BETWEEN %s AND %s
                  OR finished_at BETWEEN %s AND %s
                  OR (
                      requested_at < %s
                      AND (finished_at IS NULL OR finished_at >= %s)
                  )
              )
            """,
            (
                controller_id,
                now - timedelta(seconds=minimum_seconds),
                now,
                now - timedelta(seconds=minimum_seconds),
                now,
                now - timedelta(seconds=minimum_seconds),
                now,
                now - timedelta(seconds=minimum_seconds),
                now - timedelta(seconds=minimum_seconds),
            ),
        )
        action_row = cursor.fetchone()
        current_candidates = _read_runtime_reconcile_states_in_snapshot(
            cursor,
            controller_id=controller_id,
            now=now,
            sample_limit=min(500, sample_limit),
        )
    recovery_action_count = _count_from_row(action_row)
    if recovery_action_count != 0:
        raise RuntimeObserveVerificationError("observe-only window contains recovery actions")
    if status_row is None:
        raise RuntimeObserveVerificationError("observe-only status is unavailable")
    status = _mapping(status_row, "runtime observe status")
    if (
        str(status["controller_owner_id"]) != controller_owner_id
        or _object_to_int(status["controller_epoch"]) != controller_epoch
    ):
        raise RuntimeObserveVerificationError("observe-only status mixes controller identity")
    continuous_since = _aware(status["continuous_since"], "continuous_since")
    latest_observed_at = _aware(status["last_completed_at"], "last_completed_at")
    duration = int((latest_observed_at - continuous_since).total_seconds())
    if duration < minimum_seconds:
        raise RuntimeObserveVerificationError(
            "observe-only window is shorter than minimum duration "
            f"(available_seconds={duration}, required_seconds={minimum_seconds})"
        )
    freshness = int((now - latest_observed_at).total_seconds())
    if freshness > max_freshness_seconds:
        raise RuntimeObserveVerificationError("observe-only window latest completion is stale")
    largest_gap = _object_to_int(status["max_gap_seconds"])
    if largest_gap > max_gap_seconds:
        raise RuntimeObserveVerificationError("observe-only completion gap exceeded maximum")
    if bool(status["coverage_truncated"]):
        raise RuntimeObserveVerificationError("observe-only candidate coverage is truncated")
    if bool(status["storage_limited"]):
        raise RuntimeObserveVerificationError("observe-only storage limit is active")
    if _object_to_int(status["candidate_count"]) != len(current_candidates):
        raise RuntimeObserveVerificationError("observe-only current candidate parity mismatch")
    return RuntimeObserveVerification(
        status="pass",
        controller_id=controller_id,
        controller_owner_id=controller_owner_id,
        controller_epoch=controller_epoch,
        started_at=continuous_since,
        latest_observed_at=latest_observed_at,
        duration_seconds=duration,
        decision_count=_object_to_int(status["candidate_count"]),
        idle_count=int(_object_to_int(status["candidate_count"]) == 0),
        recovery_action_count=recovery_action_count,
        current_candidate_count=len(current_candidates),
        max_gap_seconds=largest_gap,
        latest_decision_digest="bounded-runtime-observe-status",
    )


def _require_current_controller_lease(
    cursor: Any,
    *,
    controller_id: str,
    controller_owner_id: str,
    controller_epoch: int,
    observed_at: datetime,
    lock: bool,
) -> None:
    cursor.execute(
        f"""
        SELECT owner_id, lease_epoch, lease_expires_at
        FROM public.m1_runtime_controller_leases
        WHERE controller_id = %s
        {"FOR SHARE" if lock else ""}
        """,
        (controller_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeObserveError("runtime observe controller lease is missing")
    if isinstance(row, Mapping):
        owner_id = row["owner_id"]
        lease_epoch = row["lease_epoch"]
        lease_expires_at = row["lease_expires_at"]
    else:
        owner_id, lease_epoch, lease_expires_at = row
    if str(owner_id) != controller_owner_id or _object_to_int(lease_epoch) != controller_epoch:
        raise RuntimeObserveError("runtime observe controller lease identity is stale")
    if _aware(lease_expires_at, "lease_expires_at") < observed_at:
        raise RuntimeObserveError("runtime observe controller lease is expired")


def _read_runtime_reconcile_states_in_snapshot(
    cursor: Any,
    *,
    controller_id: str,
    now: datetime,
    sample_limit: int,
) -> tuple[RuntimeReconcileCandidate, ...]:
    _require_nonempty("controller_id", controller_id)
    require_timezone_aware(now, field_name="now")
    if not 1 <= sample_limit <= 500:
        raise ValueError("sample_limit must be in 1..500")
    cursor.execute(
        """
        SELECT j.job_key, j.job_type, j.state AS job_state, j.attempt_count,
               j.last_error_class, r.attempt_id, r.lease_epoch, r.worker_id,
               r.stage, r.started_at, r.last_heartbeat_at, r.last_progress_at,
               r.progress_sequence, r.progress_current, r.progress_total,
               r.lease_deadline_at, r.heartbeat_deadline_at,
               r.progress_deadline_at, r.attempt_deadline_at, r.recovery_state,
               r.policy_version, r.profile_lease_seconds,
               r.profile_heartbeat_seconds, r.profile_progress_seconds,
               r.profile_attempt_seconds,
               c.state AS circuit_state, c.opened_at AS circuit_opened_at,
               c.next_probe_at AS circuit_next_probe_at,
               c.failure_fingerprint AS circuit_failure_fingerprint,
               a.error_class AS attempt_error_class,
               b.remaining_actions
        FROM public.m1_job_runtime_state AS r
        JOIN public.m1_jobs AS j ON j.job_key = r.job_key
        LEFT JOIN public.m1_job_circuits AS c ON c.job_key = j.job_key
        LEFT JOIN public.m1_job_attempts AS a ON a.attempt_id = r.attempt_id
        LEFT JOIN public.m1_recovery_target_budgets AS b
          ON b.controller_id = %s
         AND b.target_type = CASE WHEN c.state = 'open' THEN 'circuit' ELSE 'job' END
         AND b.target_id = j.job_key
         AND b.episode_key = CASE
             WHEN c.state = 'open' THEN COALESCE(c.failure_fingerprint, 'legacy')
             ELSE r.attempt_id
         END
        WHERE j.state NOT IN ('succeeded', 'quarantined')
          AND r.recovery_state <> 'terminal'
        ORDER BY r.updated_at ASC, j.job_key ASC
        LIMIT %s
        """,
        (controller_id, sample_limit),
    )
    rows = cursor.fetchall()
    candidates: list[RuntimeReconcileCandidate] = []
    for row in rows:
        job_type = _safe_text(row["job_type"], limit=64)
        if job_type not in _RUNTIME_COMPONENTS:
            continue
        job_state = _safe_text(row["job_state"], limit=32)
        circuit_open = row["circuit_state"] == "open"
        owner_is_current = job_state == "leased" or circuit_open
        remaining = 3 if row["remaining_actions"] is None else int(row["remaining_actions"])
        circuit_opened_at = row["circuit_opened_at"]
        circuit_next_probe_at = row["circuit_next_probe_at"]
        cooldown_seconds = 0 if circuit_open else 60
        target_type = "circuit" if circuit_open else "job"
        target_id = str(row["job_key"])
        recovery_episode_key = (
            str(row["circuit_failure_fingerprint"] or "legacy")
            if circuit_open
            else str(row["attempt_id"])
        )
        candidates.append(
            RuntimeReconcileCandidate(
                runtime_state=RecoveryRuntimeState(
                    job_key=target_id,
                    attempt_id=str(row["attempt_id"]),
                    lease_epoch=int(row["lease_epoch"]),
                    owner_is_current=owner_is_current,
                    profile=_runtime_deadline_profile(row),
                    attempt_started_at=_aware(row["started_at"], "started_at"),
                    last_heartbeat_at=_aware(row["last_heartbeat_at"], "last_heartbeat_at"),
                    last_progress_at=_aware(row["last_progress_at"], "last_progress_at"),
                    lease_expires_at=_aware(row["lease_deadline_at"], "lease_deadline_at"),
                    retry_count=max(0, int(row["attempt_count"])),
                    recovery_budget=RecoveryBudget(max(0, remaining)),
                    recovery_episode_key=recovery_episode_key,
                    failure_class=_runtime_failure_class(
                        row["attempt_error_class"] or row["last_error_class"]
                    ),
                    open_circuit=circuit_open,
                    circuit_opened_at=(
                        None
                        if not circuit_open or circuit_opened_at is None
                        else _aware(circuit_opened_at, "circuit_opened_at")
                    ),
                    circuit_next_probe_at=(
                        None
                        if not circuit_open or circuit_next_probe_at is None
                        else _aware(circuit_next_probe_at, "circuit_next_probe_at")
                    ),
                ),
                job_type=job_type,
                job_state=job_state,
                worker_id=_safe_text(row["worker_id"], limit=128),
                target_type=target_type,
                target_id=target_id,
                component=job_type,
                incident_key=f"recovery:{target_type}:{target_id}",
                channels=("dashboard", "telegram"),
                cooldown_seconds=cooldown_seconds,
            )
        )
    return tuple(candidates)


def _runtime_state_payload(state: RecoveryRuntimeState) -> dict[str, object]:
    return {
        "job_key": state.job_key,
        "attempt_id": state.attempt_id,
        "lease_epoch": state.lease_epoch,
        "owner_is_current": state.owner_is_current,
        "profile": {
            "policy_version": state.profile.policy_version,
            "lease_seconds": state.profile.lease_seconds,
            "heartbeat_seconds": state.profile.heartbeat_seconds,
            "progress_seconds": state.profile.progress_seconds,
            "attempt_seconds": state.profile.attempt_seconds,
        },
        "attempt_started_at": _dt(state.attempt_started_at),
        "last_heartbeat_at": _dt(state.last_heartbeat_at),
        "last_progress_at": _dt(state.last_progress_at),
        "lease_expires_at": _dt(state.lease_expires_at),
        "retry_count": state.retry_count,
        "recovery_budget": {
            "remaining_actions": state.recovery_budget.remaining_actions,
        },
        "recovery_episode_key": state.recovery_episode_key,
        "failure_class": None if state.failure_class is None else state.failure_class.value,
        "open_circuit": state.open_circuit,
        "circuit_opened_at": None
        if state.circuit_opened_at is None
        else _dt(state.circuit_opened_at),
        "circuit_next_probe_at": None
        if state.circuit_next_probe_at is None
        else _dt(state.circuit_next_probe_at),
    }


def _runtime_state_from_payload(payload: Mapping[str, object]) -> RecoveryRuntimeState:
    profile = _mapping(payload.get("profile"), "profile")
    budget = _mapping(payload.get("recovery_budget"), "recovery_budget")
    failure_class = payload.get("failure_class")
    circuit_opened_at = (
        None
        if payload.get("circuit_opened_at") is None
        else _parse_dt(str(payload["circuit_opened_at"]))
    )
    circuit_next_probe_at = (
        None
        if payload.get("circuit_next_probe_at") is None
        else _parse_dt(str(payload["circuit_next_probe_at"]))
    )
    # Historical observe records used opened_at plus a relative duration.
    # Convert them once during replay; live state never rebuilds the clock.
    if (
        circuit_next_probe_at is None
        and circuit_opened_at is not None
        and payload.get("circuit_cooldown_seconds") is not None
    ):
        circuit_next_probe_at = circuit_opened_at + timedelta(
            seconds=_object_to_int(payload["circuit_cooldown_seconds"])
        )
    attempt_id = str(payload["attempt_id"])
    open_circuit = bool(payload["open_circuit"])
    recovery_episode_key = payload.get("recovery_episode_key")
    if recovery_episode_key is None:
        # Historical observe payloads predate episode-scoped budgets. Their
        # circuit budget identity can only be represented as legacy; job
        # attempts already carried their exact episode identity.
        recovery_episode_key = "legacy" if open_circuit else attempt_id
    return RecoveryRuntimeState(
        job_key=str(payload["job_key"]),
        attempt_id=attempt_id,
        lease_epoch=_object_to_int(payload["lease_epoch"]),
        owner_is_current=bool(payload["owner_is_current"]),
        profile=RuntimeDeadlineProfile(
            policy_version=str(profile["policy_version"]),
            lease_seconds=_object_to_int(profile["lease_seconds"]),
            heartbeat_seconds=_object_to_int(profile["heartbeat_seconds"]),
            progress_seconds=_object_to_int(profile["progress_seconds"]),
            attempt_seconds=_object_to_int(profile["attempt_seconds"]),
        ),
        attempt_started_at=_parse_dt(str(payload["attempt_started_at"])),
        last_heartbeat_at=_parse_dt(str(payload["last_heartbeat_at"])),
        last_progress_at=_parse_dt(str(payload["last_progress_at"])),
        lease_expires_at=_parse_dt(str(payload["lease_expires_at"])),
        retry_count=_object_to_int(payload["retry_count"]),
        recovery_budget=RecoveryBudget(
            remaining_actions=_object_to_int(budget["remaining_actions"])
        ),
        recovery_episode_key=str(recovery_episode_key),
        failure_class=None if failure_class is None else RecoveryFailureClass(str(failure_class)),
        open_circuit=open_circuit,
        circuit_opened_at=circuit_opened_at,
        circuit_next_probe_at=circuit_next_probe_at,
    )


def _decision_payload(decision: RecoveryDecision) -> dict[str, object]:
    return {
        "action_type": None if decision.action is None else decision.action.value,
        "reason_code": decision.reason_code,
        "incident_severity": decision.incident_severity,
        "qualification_breaking": decision.qualification_breaking,
        "next_check_at": _dt(decision.next_check_at),
    }


def _assert_same_decision(
    left: RecoveryDecision,
    right: RecoveryDecision,
    *,
    context: str,
) -> None:
    if left != right:
        raise RuntimeObserveError(f"{context} mismatch")


def _digest(payload: Mapping[str, object]) -> str:
    return sha256(canonical_observe_record_bytes(payload)).hexdigest()


def _dt(value: datetime) -> str:
    require_timezone_aware(value, field_name="datetime")
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    require_timezone_aware(parsed, field_name="datetime")
    return parsed


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RuntimeObserveVerificationError(f"{field_name} must be an object")
    return cast(Mapping[str, object], value)


def _count_from_row(row: object) -> int:
    if row is None:
        return 0
    if isinstance(row, Mapping):
        return _object_to_int(next(iter(row.values())))
    if isinstance(row, Sequence):
        return _object_to_int(row[0])
    return _object_to_int(row)


def _aware(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise RuntimeObserveVerificationError(f"{field_name} must be a datetime")
    require_timezone_aware(value, field_name=field_name)
    return value


def _object_to_int(value: object) -> int:
    return int(cast(Any, value))


def _require_nonempty(name: str, value: str) -> None:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_positive_int(name: str, value: int) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive int")


def _require_sha256(name: str, value: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"{name} must be a sha256 digest")


def _assert_secret_free(value: object) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            _assert_secret_free(key)
            _assert_secret_free(nested)
        return
    if isinstance(value, (list, tuple)):
        for nested in value:
            _assert_secret_free(nested)
        return
    if isinstance(value, str):
        lowered = value.lower()
        forbidden = (
            "bearer ",
            "postgres://",
            "postgresql://",
            "password",
            "secret",
            "token=",
            "apikey",
            "polyarb_supabase_db_dsn",
        )
        if any(marker in lowered for marker in forbidden):
            raise ValueError("observe payload must be secret-free")


__all__ = [
    "RuntimeObserveDecisionRecord",
    "RuntimeObserveError",
    "RuntimeObserveVerification",
    "RuntimeObserveVerificationError",
    "build_runtime_observe_decision_record",
    "build_runtime_observe_idle_record",
    "canonical_observe_record_bytes",
    "insert_runtime_observe_decision",
    "insert_runtime_observe_decisions",
    "verify_runtime_observe_window",
]
