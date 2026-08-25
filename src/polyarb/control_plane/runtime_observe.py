"""Observe-only runtime reconciliation decision ledger."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
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
_EXISTING_ROW_COLUMNS = (
    "decision_id",
    "controller_id",
    "controller_owner_id",
    "controller_epoch",
    "payload",
    "payload_sha256",
    "decision_digest",
)


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


def insert_runtime_observe_decision(
    connection_factory: ConnectionFactory,
    record: RuntimeObserveDecisionRecord,
) -> RuntimeObserveDecisionRecord:
    if type(record) is not RuntimeObserveDecisionRecord:
        raise TypeError("record must be RuntimeObserveDecisionRecord")
    with connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ WRITE")
        if _compare_existing_observe_row(cursor, record):
            connection.commit()
            return record
        _require_current_controller_lease(
            cursor,
            controller_id=record.controller_id,
            controller_owner_id=record.controller_owner_id,
            controller_epoch=record.controller_epoch,
            observed_at=record.observed_at,
            lock=True,
        )
        cursor.execute(
            """
            INSERT INTO m1_runtime_observe_decisions (
                decision_id, idempotency_key, controller_id, controller_owner_id,
                controller_epoch, observed_at, decision_kind, target_type, target_id,
                action_type, reason_code, incident_severity, qualification_breaking,
                next_check_at, runtime_state_digest, decision_digest, payload,
                payload_sha256
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING decision_id, controller_id, controller_owner_id, controller_epoch,
                      payload, payload_sha256, decision_digest
            """,
            (
                record.decision_id,
                record.idempotency_key,
                record.controller_id,
                record.controller_owner_id,
                record.controller_epoch,
                record.observed_at,
                record.decision_kind,
                record.target_type,
                record.target_id,
                record.action_type,
                record.reason_code,
                record.incident_severity,
                record.qualification_breaking,
                record.next_check_at,
                record.runtime_state_digest,
                record.decision_digest,
                Jsonb(record.payload),
                record.payload_sha256,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is None and not _compare_existing_observe_row(cursor, record):
            raise RuntimeObserveError("runtime observe idempotency raced without a row")
        if inserted is not None:
            _compare_existing_row(record, _existing_row_mapping(inserted))
        connection.commit()
    return record


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
    start_at = now - timedelta(seconds=minimum_seconds)
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
            SELECT decision_id, idempotency_key, controller_id, controller_owner_id,
                   controller_epoch, observed_at, decision_kind, target_type, target_id,
                   action_type, reason_code, incident_severity, qualification_breaking,
                   next_check_at, runtime_state_digest, decision_digest, payload,
                   payload_sha256
            FROM (
                SELECT decision_id, idempotency_key, controller_id, controller_owner_id,
                       controller_epoch, observed_at, decision_kind, target_type, target_id,
                       action_type, reason_code, incident_severity,
                       qualification_breaking, next_check_at, runtime_state_digest,
                       decision_digest, payload, payload_sha256
                FROM m1_runtime_observe_decisions
                WHERE controller_id = %s
                  AND controller_owner_id = %s
                  AND controller_epoch = %s
                  AND observed_at <= %s
                ORDER BY observed_at DESC, decision_id DESC
                LIMIT %s
            ) AS bounded_observe_decisions
            ORDER BY observed_at ASC, decision_id ASC
            """,
            (controller_id, controller_owner_id, controller_epoch, now, sample_limit),
        )
        rows = cursor.fetchall()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM m1_recovery_actions
            WHERE controller_id = %s
              AND requested_at >= %s
              AND requested_at <= %s
            """,
            (controller_id, start_at, now),
        )
        action_row = cursor.fetchone()
        current_candidates = _read_runtime_reconcile_states_in_snapshot(
            cursor,
            controller_id=controller_id,
            now=now,
            sample_limit=min(100, sample_limit),
        )
    recovery_action_count = _count_from_row(action_row)
    if recovery_action_count != 0:
        raise RuntimeObserveVerificationError("observe-only window contains recovery actions")
    records = tuple(_record_from_row(row) for row in rows)
    if not records:
        raise RuntimeObserveVerificationError("observe-only window has no decisions")
    for record in records:
        if (
            record.controller_id != controller_id
            or record.controller_owner_id != controller_owner_id
            or record.controller_epoch != controller_epoch
        ):
            raise RuntimeObserveVerificationError("observe-only evidence mixes controller identity")
    latest = records[-1]
    earliest = records[0]
    if earliest.observed_at > start_at:
        raise RuntimeObserveVerificationError("observe-only window lacks boundary anchor")
    duration = int((latest.observed_at - earliest.observed_at).total_seconds())
    if duration < minimum_seconds:
        raise RuntimeObserveVerificationError(
            "observe-only window is shorter than minimum duration"
        )
    freshness = int((now - latest.observed_at).total_seconds())
    if freshness > max_freshness_seconds:
        raise RuntimeObserveVerificationError("observe-only window latest decision is stale")
    observed_gaps = [
        int((right.observed_at - left.observed_at).total_seconds())
        for left, right in zip(records, records[1:])
    ]
    largest_gap = max(observed_gaps, default=0)
    if largest_gap > max_gap_seconds:
        raise RuntimeObserveVerificationError("observe-only decision gap exceeded maximum")
    _verify_historical_replay(records)
    _verify_current_candidate_parity(
        records,
        current_candidates,
        now=now,
        max_age_seconds=max_freshness_seconds,
    )
    return RuntimeObserveVerification(
        status="pass",
        controller_id=controller_id,
        controller_owner_id=controller_owner_id,
        controller_epoch=controller_epoch,
        started_at=earliest.observed_at,
        latest_observed_at=latest.observed_at,
        duration_seconds=duration,
        decision_count=len(records),
        idle_count=sum(1 for record in records if record.decision_kind == "idle"),
        recovery_action_count=recovery_action_count,
        current_candidate_count=len(current_candidates),
        max_gap_seconds=largest_gap,
        latest_decision_digest=latest.decision_digest,
    )


def _compare_existing_observe_row(cursor: Any, record: RuntimeObserveDecisionRecord) -> bool:
    cursor.execute(
        """
        SELECT decision_id, controller_id, controller_owner_id, controller_epoch,
               payload, payload_sha256, decision_digest
        FROM m1_runtime_observe_decisions
        WHERE idempotency_key = %s
        """,
        (record.idempotency_key,),
    )
    row = cursor.fetchone()
    if row is None:
        return False
    _compare_existing_row(record, _existing_row_mapping(row))
    return True


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
        FROM m1_runtime_controller_leases
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
    if (
        str(owner_id) != controller_owner_id
        or _object_to_int(lease_epoch) != controller_epoch
    ):
        raise RuntimeObserveError("runtime observe controller lease identity is stale")
    if _aware(lease_expires_at, "lease_expires_at") < observed_at:
        raise RuntimeObserveError("runtime observe controller lease is expired")


def _compare_existing_row(
    record: RuntimeObserveDecisionRecord,
    existing: Mapping[str, object],
) -> None:
    existing_payload = _mapping(existing["payload"], "payload")
    existing_canonical = canonical_observe_record_bytes(existing_payload)
    if (
        existing["decision_id"] != record.decision_id
        or existing["controller_id"] != record.controller_id
        or existing["controller_owner_id"] != record.controller_owner_id
        or _object_to_int(existing["controller_epoch"]) != record.controller_epoch
        or existing["payload_sha256"] != record.payload_sha256
        or existing["decision_digest"] != record.decision_digest
        or sha256(existing_canonical).hexdigest() != record.payload_sha256
        or existing_canonical != canonical_observe_record_bytes(record.payload)
    ):
        raise RuntimeObserveError("runtime observe idempotency key conflicts")


def _existing_row_mapping(row: object) -> Mapping[str, object]:
    if isinstance(row, Mapping):
        return cast(Mapping[str, object], row)
    if isinstance(row, Sequence):
        return dict(zip(_EXISTING_ROW_COLUMNS, row, strict=True))
    raise RuntimeObserveError("existing observe row has unsupported shape")


def _verify_historical_replay(records: tuple[RuntimeObserveDecisionRecord, ...]) -> None:
    for record in records:
        _verify_record_columns_match_payload(record)
        if record.decision_kind == "idle":
            continue
        state_payload = _mapping(record.payload.get("runtime_state"), "runtime_state")
        replay = RuntimeReconciler().evaluate(
            _runtime_state_from_payload(state_payload),
            now=record.observed_at,
        )
        if (
            (None if replay.action is None else replay.action.value) != record.action_type
            or replay.reason_code != record.reason_code
            or replay.incident_severity != record.incident_severity
            or replay.qualification_breaking != record.qualification_breaking
            or replay.next_check_at != record.next_check_at
        ):
            raise RuntimeObserveVerificationError("RuntimeReconciler replay mismatch")


def _verify_current_candidate_parity(
    records: tuple[RuntimeObserveDecisionRecord, ...],
    candidates: tuple[RuntimeReconcileCandidate, ...],
    *,
    now: datetime,
    max_age_seconds: int,
) -> None:
    latest_by_target: dict[tuple[str, str], RuntimeObserveDecisionRecord] = {}
    for record in records:
        if record.target_type is not None and record.target_id is not None:
            latest_by_target[(record.target_type, record.target_id)] = record
    if not candidates:
        if records[-1].decision_kind != "idle":
            raise RuntimeObserveVerificationError("current candidate parity requires idle record")
        return
    for candidate in candidates:
        key = (candidate.target_type, candidate.target_id)
        observed = latest_by_target.get(key)
        if observed is None:
            raise RuntimeObserveVerificationError(
                "current candidate parity missing observe decision"
            )
        age_seconds = int((now - observed.observed_at).total_seconds())
        if age_seconds > max_age_seconds:
            raise RuntimeObserveVerificationError("current candidate observe decision is stale")
        replay = RuntimeReconciler().evaluate(candidate.runtime_state, now=now)
        if (
            (None if replay.action is None else replay.action.value) != observed.action_type
            or replay.reason_code != observed.reason_code
            or replay.incident_severity != observed.incident_severity
            or replay.qualification_breaking != observed.qualification_breaking
        ):
            raise RuntimeObserveVerificationError("current candidate parity mismatch")


def _read_runtime_reconcile_states_in_snapshot(
    cursor: Any,
    *,
    controller_id: str,
    now: datetime,
    sample_limit: int,
) -> tuple[RuntimeReconcileCandidate, ...]:
    _require_nonempty("controller_id", controller_id)
    require_timezone_aware(now, field_name="now")
    if not 1 <= sample_limit <= 100:
        raise ValueError("sample_limit must be in 1..100")
    cursor.execute(
        """
        SELECT j.job_key, j.job_type, j.state AS job_state, j.attempt_count,
               j.last_error_class, r.attempt_id, r.lease_epoch, r.worker_id,
               r.stage, r.started_at, r.last_heartbeat_at, r.last_progress_at,
               r.progress_sequence, r.progress_current, r.progress_total,
               r.lease_deadline_at, r.heartbeat_deadline_at,
               r.progress_deadline_at, r.attempt_deadline_at, r.recovery_state,
               c.state AS circuit_state, c.opened_at AS circuit_opened_at,
               c.next_probe_at AS circuit_next_probe_at,
               a.error_class AS attempt_error_class,
               b.remaining_actions
        FROM m1_job_runtime_state AS r
        JOIN m1_jobs AS j ON j.job_key = r.job_key
        LEFT JOIN m1_job_circuits AS c ON c.job_key = j.job_key
        LEFT JOIN m1_job_attempts AS a ON a.attempt_id = r.attempt_id
        LEFT JOIN m1_recovery_target_budgets AS b
          ON b.controller_id = %s
         AND b.target_type = CASE WHEN c.state = 'open' THEN 'circuit' ELSE 'job' END
         AND b.target_id = j.job_key
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
        cooldown_seconds = 60
        if (
            circuit_open
            and circuit_opened_at is not None
            and row["circuit_next_probe_at"] is not None
        ):
            cooldown_seconds = max(
                0,
                int(
                    (
                        _aware(row["circuit_next_probe_at"], "circuit_next_probe_at")
                        - _aware(circuit_opened_at, "circuit_opened_at")
                    ).total_seconds()
                ),
            )
        target_type = "circuit" if circuit_open else "job"
        target_id = str(row["job_key"])
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
                    failure_class=_runtime_failure_class(
                        row["attempt_error_class"] or row["last_error_class"]
                    ),
                    open_circuit=circuit_open,
                    circuit_opened_at=(
                        None
                        if circuit_opened_at is None
                        else _aware(circuit_opened_at, "circuit_opened_at")
                    ),
                    circuit_cooldown_seconds=cooldown_seconds,
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


def _record_from_row(row: Mapping[str, object]) -> RuntimeObserveDecisionRecord:
    return RuntimeObserveDecisionRecord(
        decision_id=str(row["decision_id"]),
        idempotency_key=str(row["idempotency_key"]),
        controller_id=str(row["controller_id"]),
        controller_owner_id=str(row["controller_owner_id"]),
        controller_epoch=_object_to_int(row["controller_epoch"]),
        observed_at=cast(datetime, row["observed_at"]),
        decision_kind=cast(Literal["decision", "idle"], row["decision_kind"]),
        target_type=cast(Literal["job", "circuit"] | None, row["target_type"]),
        target_id=cast(str | None, row["target_id"]),
        action_type=cast(str | None, row["action_type"]),
        reason_code=str(row["reason_code"]),
        incident_severity=cast(Literal["warning", "critical"], row["incident_severity"]),
        qualification_breaking=bool(row["qualification_breaking"]),
        next_check_at=cast(datetime, row["next_check_at"]),
        runtime_state_digest=cast(str | None, row["runtime_state_digest"]),
        decision_digest=str(row["decision_digest"]),
        payload=cast(Mapping[str, object], row["payload"]),
        payload_sha256=str(row["payload_sha256"]),
    )


def _verify_record_columns_match_payload(record: RuntimeObserveDecisionRecord) -> None:
    decision = _mapping(record.payload.get("decision"), "decision")
    target = record.payload.get("target")
    if record.payload.get("controller_id") != record.controller_id:
        raise RuntimeObserveVerificationError("controller_id column does not match payload")
    if record.payload.get("controller_owner_id") != record.controller_owner_id:
        raise RuntimeObserveVerificationError("controller_owner_id column does not match payload")
    if _object_to_int(record.payload.get("controller_epoch", 0)) != record.controller_epoch:
        raise RuntimeObserveVerificationError("controller_epoch column does not match payload")
    if record.payload.get("decision_kind") != record.decision_kind:
        raise RuntimeObserveVerificationError("decision kind column does not match payload")
    if _parse_dt(str(record.payload.get("observed_at"))) != record.observed_at:
        raise RuntimeObserveVerificationError("observed_at column does not match payload")
    action = decision.get("action_type")
    if action != record.action_type:
        raise RuntimeObserveVerificationError("decision action column does not match payload")
    if decision.get("reason_code") != record.reason_code:
        raise RuntimeObserveVerificationError("decision reason column does not match payload")
    if decision.get("incident_severity") != record.incident_severity:
        raise RuntimeObserveVerificationError("decision severity column does not match payload")
    if decision.get("qualification_breaking") != record.qualification_breaking:
        raise RuntimeObserveVerificationError(
            "decision qualification column does not match payload"
        )
    if _parse_dt(str(decision.get("next_check_at"))) != record.next_check_at:
        raise RuntimeObserveVerificationError(
            "decision next_check_at column does not match payload"
        )
    if record.decision_kind == "idle":
        if target is not None:
            raise RuntimeObserveVerificationError("idle record target payload must be null")
        return
    target_payload = _mapping(target, "target")
    if (
        target_payload.get("target_type") != record.target_type
        or target_payload.get("target_id") != record.target_id
    ):
        raise RuntimeObserveVerificationError("target columns do not match payload")
    state_payload = _mapping(record.payload.get("runtime_state"), "runtime_state")
    if _digest(state_payload) != record.runtime_state_digest:
        raise RuntimeObserveVerificationError("runtime state digest mismatch")


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
        "failure_class": None if state.failure_class is None else state.failure_class.value,
        "open_circuit": state.open_circuit,
        "circuit_opened_at": None
        if state.circuit_opened_at is None
        else _dt(state.circuit_opened_at),
        "circuit_cooldown_seconds": state.circuit_cooldown_seconds,
    }


def _runtime_state_from_payload(payload: Mapping[str, object]) -> RecoveryRuntimeState:
    profile = _mapping(payload.get("profile"), "profile")
    budget = _mapping(payload.get("recovery_budget"), "recovery_budget")
    failure_class = payload.get("failure_class")
    return RecoveryRuntimeState(
        job_key=str(payload["job_key"]),
        attempt_id=str(payload["attempt_id"]),
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
        failure_class=None if failure_class is None else RecoveryFailureClass(str(failure_class)),
        open_circuit=bool(payload["open_circuit"]),
        circuit_opened_at=None
        if payload.get("circuit_opened_at") is None
        else _parse_dt(str(payload["circuit_opened_at"])),
        circuit_cooldown_seconds=_object_to_int(payload["circuit_cooldown_seconds"]),
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
    "verify_runtime_observe_window",
]
