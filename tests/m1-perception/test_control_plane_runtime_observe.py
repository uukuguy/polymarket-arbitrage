"""Runtime observe-only decision ledger and verifier contracts."""

from __future__ import annotations

import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, cast

import psycopg
import pytest

from polyarb.control_plane.reconciler import RuntimeReconciler
from polyarb.control_plane.recovery_models import (
    RecoveryBudget,
    RecoveryRuntimeState,
)
from polyarb.control_plane.recovery_store import ConnectionFactory, RuntimeReconcileCandidate
from polyarb.control_plane.runtime_models import RuntimeDeadlineProfile

CONTROLLER_ID = "m1-runtime-reconciler"
CONTROLLER_OWNER_ID = "runtime-controller-observe"
CONTROLLER_EPOCH = 3


def _now() -> datetime:
    return datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


def _state(base: datetime, *, stalled: bool = False) -> RecoveryRuntimeState:
    return RecoveryRuntimeState(
        job_key="quote-batch:active",
        attempt_id="attempt-1",
        lease_epoch=7,
        owner_is_current=True,
        profile=RuntimeDeadlineProfile(
            policy_version="runtime-deadline-v1",
            lease_seconds=180,
            heartbeat_seconds=30,
            progress_seconds=60,
            attempt_seconds=300,
        ),
        attempt_started_at=base - timedelta(seconds=90),
        last_heartbeat_at=base - timedelta(seconds=10),
        last_progress_at=base - timedelta(seconds=90 if stalled else 10),
        lease_expires_at=base + timedelta(seconds=90),
        retry_count=0,
        recovery_budget=RecoveryBudget(remaining_actions=3),
    )


def _candidate(base: datetime, *, stalled: bool = False) -> RuntimeReconcileCandidate:
    return RuntimeReconcileCandidate(
        runtime_state=_state(base, stalled=stalled),
        job_type="quote-batch",
        job_state="leased",
        worker_id="worker-1",
        target_type="job",
        target_id="quote-batch:active",
        component="quote-batch",
        incident_key="recovery:job:quote-batch:active",
        channels=("dashboard", "telegram"),
        cooldown_seconds=60,
    )


def test_decision_and_idle_records_are_typed_canonical_and_secret_free() -> None:
    from polyarb.control_plane.runtime_observe import (
        build_runtime_observe_decision_record,
        build_runtime_observe_idle_record,
        canonical_observe_record_bytes,
    )

    now = _now()
    candidate = _candidate(now, stalled=True)
    decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=now)
    record = build_runtime_observe_decision_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now,
        candidate=candidate,
        decision=decision,
        observed_by=CONTROLLER_OWNER_ID,
    )

    assert record.controller_owner_id == CONTROLLER_OWNER_ID
    assert record.controller_epoch == CONTROLLER_EPOCH
    assert record.payload["controller_owner_id"] == CONTROLLER_OWNER_ID
    assert record.payload["controller_epoch"] == CONTROLLER_EPOCH
    assert record.decision_kind == "decision"
    assert record.action_type == "cancel-job"
    assert record.reason_code == "job.progress-stalled"
    assert record.target_type == "job"
    assert record.target_id == "quote-batch:active"
    assert record.decision_id.startswith("runtime-observe:")
    assert len(record.decision_digest) == 64
    assert record.payload_sha256 == record.decision_digest
    assert canonical_observe_record_bytes(record.payload) == canonical_observe_record_bytes(
        dict(reversed(tuple(record.payload.items())))
    )

    idle = build_runtime_observe_idle_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now,
        next_check_at=now + timedelta(seconds=30),
        observed_by=CONTROLLER_OWNER_ID,
    )
    assert idle.decision_kind == "idle"
    assert idle.action_type is None
    assert idle.target_type is None
    assert idle.reason_code == "job.healthy"

    with pytest.raises(ValueError, match="secret"):
        build_runtime_observe_idle_record(
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            observed_at=now,
            next_check_at=now + timedelta(seconds=30),
            observed_by="Bearer leaked-token",
        )


def test_insert_decision_is_idempotent_and_never_writes_recovery_actions() -> None:
    from polyarb.control_plane.runtime_observe import (
        build_runtime_observe_decision_record,
        insert_runtime_observe_decision,
    )

    now = _now()
    candidate = _candidate(now, stalled=True)
    decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=now)
    record = build_runtime_observe_decision_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now,
        candidate=candidate,
        decision=decision,
        observed_by=CONTROLLER_OWNER_ID,
    )
    connection = FakeConnection(rows=[], lease_expires_at=now + timedelta(seconds=60))

    inserted = insert_runtime_observe_decision(_fake_factory(connection), record)

    assert inserted == record
    sql = "\n".join(connection.sql)
    assert "SELECT owner_id, lease_epoch, lease_expires_at" in sql
    assert "FOR SHARE" in sql
    assert "INSERT INTO public.m1_runtime_observe_decisions" in sql
    assert "ON CONFLICT (idempotency_key) DO NOTHING" in sql
    assert "m1_recovery_actions" not in sql
    assert any(query == "SET TRANSACTION READ WRITE" for query in connection.sql)
    assert connection.committed

    replay = insert_runtime_observe_decision(_fake_factory(connection), record)
    assert replay == record


def test_insert_rejects_stale_controller_identity_or_conflicting_idempotency() -> None:
    from polyarb.control_plane.runtime_observe import (
        RuntimeObserveError,
        build_runtime_observe_idle_record,
        canonical_observe_record_bytes,
        insert_runtime_observe_decision,
    )

    now = _now()
    record = build_runtime_observe_idle_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now,
        next_check_at=now + timedelta(seconds=30),
        observed_by=CONTROLLER_OWNER_ID,
    )
    stale_connection = FakeConnection(
        rows=[],
        lease_owner_id="other-owner",
        lease_epoch=CONTROLLER_EPOCH + 1,
        lease_expires_at=now + timedelta(seconds=60),
    )
    with pytest.raises(RuntimeObserveError, match="lease identity"):
        insert_runtime_observe_decision(_fake_factory(stale_connection), record)

    payload = dict(record.payload)
    payload["observed_by"] = "runtime-controller-observe-conflict"
    digest = sha256(canonical_observe_record_bytes(payload)).hexdigest()
    conflicting = replace(
        record,
        decision_id=f"runtime-observe:{digest}",
        payload=payload,
        payload_sha256=digest,
        decision_digest=digest,
    )
    conflict_connection = FakeConnection(
        rows=[],
        existing_row=_existing_tuple(record),
        lease_expires_at=now + timedelta(seconds=60),
    )
    with pytest.raises(RuntimeObserveError, match="idempotency"):
        insert_runtime_observe_decision(_fake_factory(conflict_connection), conflicting)


def test_verifier_requires_read_only_window_zero_actions_and_candidate_parity() -> None:
    from polyarb.control_plane.runtime_observe import (
        build_runtime_observe_decision_record,
        verify_runtime_observe_window,
    )

    now = _now()
    anchor_at = now - timedelta(seconds=150)
    records = []
    for observed_at in (anchor_at, now - timedelta(seconds=60), now):
        candidate = _candidate(observed_at, stalled=True)
        records.append(
            build_runtime_observe_decision_record(
                controller_id=CONTROLLER_ID,
                controller_owner_id=CONTROLLER_OWNER_ID,
                controller_epoch=CONTROLLER_EPOCH,
                observed_at=observed_at,
                candidate=candidate,
                decision=RuntimeReconciler().evaluate(candidate.runtime_state, now=observed_at),
                observed_by=CONTROLLER_OWNER_ID,
            )
        )
    connection = FakeConnection(
        rows=[_row(record) for record in records],
        recovery_action_count=0,
        current_candidate_rows=[_candidate_row(now, stalled=True)],
    )

    result = verify_runtime_observe_window(
        _fake_factory(connection),
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        now=now,
        minimum_seconds=120,
        max_freshness_seconds=30,
        max_gap_seconds=90,
    )

    assert result.status == "pass"
    assert result.controller_owner_id == CONTROLLER_OWNER_ID
    assert result.controller_epoch == CONTROLLER_EPOCH
    assert result.started_at == anchor_at
    assert result.decision_count == 3
    assert result.recovery_action_count == 0
    assert result.current_candidate_count == 1
    assert result.latest_decision_digest == records[-1].decision_digest
    sql = "\n".join(connection.sql)
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in sql
    assert "controller_owner_id = %s" in sql
    assert "m1_job_runtime_state" in sql
    assert "COUNT(*)" in sql and "m1_recovery_actions" in sql
    action_queries = [
        query for query in connection.sql if "FROM public.m1_recovery_actions" in query
    ]
    assert len(action_queries) == 1
    assert "controller_id = %s" in action_queries[0]
    assert "requested_at BETWEEN %s AND %s" in action_queries[0]
    assert "started_at BETWEEN %s AND %s" in action_queries[0]
    assert "finished_at BETWEEN %s AND %s" in action_queries[0]
    assert "requested_at < %s" in action_queries[0]
    assert "finished_at IS NULL OR finished_at >= %s" in action_queries[0]
    assert "controller_owner_id" not in action_queries[0]
    assert "expected_controller_epoch" not in action_queries[0]
    assert "INSERT" not in sql
    assert not connection.committed


def test_verifier_fails_on_gap_recovery_mutation_mixed_identity_or_replay_mismatch(
    monkeypatch,
) -> None:
    from polyarb.control_plane import runtime_observe
    from polyarb.control_plane.runtime_observe import (
        RuntimeObserveVerificationError,
        build_runtime_observe_decision_record,
        verify_runtime_observe_window,
    )

    now = _now()
    candidate = _candidate(now, stalled=True)
    record = build_runtime_observe_decision_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now,
        candidate=candidate,
        decision=RuntimeReconciler().evaluate(candidate.runtime_state, now=now),
        observed_by=CONTROLLER_OWNER_ID,
    )
    monkeypatch.setattr(
        runtime_observe,
        "_read_runtime_reconcile_states_in_snapshot",
        lambda *_args, **_kwargs: (candidate,),
    )

    with pytest.raises(RuntimeObserveVerificationError, match="recovery actions"):
        verify_runtime_observe_window(
            _fake_factory(FakeConnection(rows=[_row(record)], recovery_action_count=1)),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=now,
            minimum_seconds=1,
            max_freshness_seconds=30,
            max_gap_seconds=30,
        )

    with pytest.raises(RuntimeObserveVerificationError, match="controller lease identity"):
        verify_runtime_observe_window(
            _fake_factory(
                FakeConnection(
                    rows=[_row(record)],
                    recovery_action_count=0,
                    lease_owner_id="handover-owner",
                    lease_epoch=CONTROLLER_EPOCH + 1,
                )
            ),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=now,
            minimum_seconds=1,
            max_freshness_seconds=30,
            max_gap_seconds=30,
        )

    mixed = replace(record, controller_owner_id="other-owner")
    with pytest.raises(RuntimeObserveVerificationError, match="controller identity"):
        verify_runtime_observe_window(
            _fake_factory(FakeConnection(rows=[_row(mixed)], recovery_action_count=0)),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=now,
            minimum_seconds=1,
            max_freshness_seconds=30,
            max_gap_seconds=30,
        )

    with pytest.raises(RuntimeObserveVerificationError, match="boundary anchor"):
        verify_runtime_observe_window(
            _fake_factory(FakeConnection(rows=[_row(record)], recovery_action_count=0)),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=now,
            minimum_seconds=1800,
            max_freshness_seconds=30,
            max_gap_seconds=30,
        )

    stale_target_record = build_runtime_observe_decision_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now - timedelta(seconds=90),
        candidate=_candidate(now - timedelta(seconds=90), stalled=True),
        decision=RuntimeReconciler().evaluate(
            _candidate(now - timedelta(seconds=90), stalled=True).runtime_state,
            now=now - timedelta(seconds=90),
        ),
        observed_by=CONTROLLER_OWNER_ID,
    )
    fresh_idle = runtime_observe.build_runtime_observe_idle_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now,
        next_check_at=now + timedelta(seconds=30),
        observed_by=CONTROLLER_OWNER_ID,
    )
    with pytest.raises(RuntimeObserveVerificationError, match="observe decision is stale"):
        verify_runtime_observe_window(
            _fake_factory(
                FakeConnection(
                    rows=[_row(stale_target_record), _row(fresh_idle)],
                    recovery_action_count=0,
                )
            ),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=now,
            minimum_seconds=90,
            max_freshness_seconds=30,
            max_gap_seconds=90,
        )

    boundary_candidate = _candidate(now - timedelta(seconds=1), stalled=True)
    boundary_record = build_runtime_observe_decision_record(
        controller_id=CONTROLLER_ID,
        controller_owner_id=CONTROLLER_OWNER_ID,
        controller_epoch=CONTROLLER_EPOCH,
        observed_at=now - timedelta(seconds=1),
        candidate=boundary_candidate,
        decision=RuntimeReconciler().evaluate(
            boundary_candidate.runtime_state,
            now=now - timedelta(seconds=1),
        ),
        observed_by=CONTROLLER_OWNER_ID,
    )
    stale_payload = dict(boundary_record.payload)
    stale_payload["decision"] = {
        **cast(dict[str, object], stale_payload["decision"]),
        "action_type": None,
        "reason_code": "job.healthy",
    }
    stale_digest = sha256(runtime_observe.canonical_observe_record_bytes(stale_payload)).hexdigest()
    stale_decision = replace(
        boundary_record,
        decision_id=f"runtime-observe:{stale_digest}",
        action_type=None,
        reason_code="job.healthy",
        payload=stale_payload,
        payload_sha256=stale_digest,
        decision_digest=stale_digest,
    )
    with pytest.raises(RuntimeObserveVerificationError, match="replay"):
        verify_runtime_observe_window(
            _fake_factory(
                FakeConnection(
                    rows=[_row(stale_decision), _row(record)],
                    recovery_action_count=0,
                )
            ),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=now,
            minimum_seconds=1,
            max_freshness_seconds=30,
            max_gap_seconds=30,
        )


def test_real_postgres_records_idempotent_idle_window_and_verifies_read_only() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real runtime observe chain-truth")

    from testcontainers.postgres import PostgresContainer

    from polyarb.control_plane.runtime_observe import (
        RuntimeObserveError,
        RuntimeObserveVerificationError,
        build_runtime_observe_idle_record,
        canonical_observe_record_bytes,
        insert_runtime_observe_decision,
        verify_runtime_observe_window,
    )

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        _create_supabase_roles(dsn)
        _run_alembic(dsn, "upgrade", "head")
        _insert_controller_lease(
            dsn,
            controller_id=CONTROLLER_ID,
            owner_id=CONTROLLER_OWNER_ID,
            lease_epoch=CONTROLLER_EPOCH,
            lease_expires_at=_now() + timedelta(minutes=5),
        )
        records = [
            build_runtime_observe_idle_record(
                controller_id=CONTROLLER_ID,
                controller_owner_id=CONTROLLER_OWNER_ID,
                controller_epoch=CONTROLLER_EPOCH,
                observed_at=_now() - timedelta(seconds=150),
                next_check_at=_now() - timedelta(seconds=120),
                observed_by=CONTROLLER_OWNER_ID,
            ),
            build_runtime_observe_idle_record(
                controller_id=CONTROLLER_ID,
                controller_owner_id=CONTROLLER_OWNER_ID,
                controller_epoch=CONTROLLER_EPOCH,
                observed_at=_now(),
                next_check_at=_now() + timedelta(seconds=30),
                observed_by=CONTROLLER_OWNER_ID,
            ),
        ]
        for record in records:
            insert_runtime_observe_decision(lambda: psycopg.connect(dsn), record)
            insert_runtime_observe_decision(lambda: psycopg.connect(dsn), record)

        payload = dict(records[-1].payload)
        payload["observed_by"] = "runtime-controller-observe-conflict"
        digest = sha256(canonical_observe_record_bytes(payload)).hexdigest()
        conflicting = replace(
            records[-1],
            decision_id=f"runtime-observe:{digest}",
            payload=payload,
            payload_sha256=digest,
            decision_digest=digest,
        )
        with pytest.raises(RuntimeObserveError, match="idempotency"):
            insert_runtime_observe_decision(lambda: psycopg.connect(dsn), conflicting)

        result = verify_runtime_observe_window(
            lambda: psycopg.connect(dsn),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=_now(),
            minimum_seconds=120,
            max_freshness_seconds=30,
            max_gap_seconds=180,
        )

        assert result.status == "pass"
        assert result.recovery_action_count == 0
        assert _observe_row_count(dsn) == 2
        assert _recovery_action_count(dsn, controller_id=CONTROLLER_ID) == 0

        _insert_job_attempt(dsn, attempt_id="attempt-before")
        _insert_recovery_action(
            dsn,
            action_id="action-before-window",
            attempt_id="attempt-before",
            requested_at=_now() - timedelta(seconds=240),
            started_at=_now() - timedelta(seconds=230),
            finished_at=_now() - timedelta(seconds=200),
        )
        result = verify_runtime_observe_window(
            lambda: psycopg.connect(dsn),
            controller_id=CONTROLLER_ID,
            controller_owner_id=CONTROLLER_OWNER_ID,
            controller_epoch=CONTROLLER_EPOCH,
            now=_now(),
            minimum_seconds=120,
            max_freshness_seconds=30,
            max_gap_seconds=180,
        )
        assert result.recovery_action_count == 0

        _insert_recovery_action(
            dsn,
            action_id="action-overlaps-window",
            attempt_id="attempt-before",
            requested_at=_now() - timedelta(seconds=180),
            started_at=_now() - timedelta(seconds=90),
            finished_at=_now() - timedelta(seconds=30),
        )
        with pytest.raises(RuntimeObserveVerificationError, match="recovery actions"):
            verify_runtime_observe_window(
                lambda: psycopg.connect(dsn),
                controller_id=CONTROLLER_ID,
                controller_owner_id=CONTROLLER_OWNER_ID,
                controller_epoch=CONTROLLER_EPOCH,
                now=_now(),
                minimum_seconds=120,
                max_freshness_seconds=30,
                max_gap_seconds=180,
            )

        _advance_controller_lease(
            dsn,
            controller_id=CONTROLLER_ID,
            owner_id="runtime-controller-next",
            lease_epoch=CONTROLLER_EPOCH + 1,
        )
        assert _observe_row_count(dsn) == 2
        with pytest.raises(RuntimeObserveError, match="lease identity"):
            insert_runtime_observe_decision(
                lambda: psycopg.connect(dsn),
                build_runtime_observe_idle_record(
                    controller_id=CONTROLLER_ID,
                    controller_owner_id=CONTROLLER_OWNER_ID,
                    controller_epoch=CONTROLLER_EPOCH,
                    observed_at=_now() + timedelta(seconds=1),
                    next_check_at=_now() + timedelta(seconds=31),
                    observed_by=CONTROLLER_OWNER_ID,
                ),
            )


def _row(record: Any) -> dict[str, Any]:
    return {
        "decision_id": record.decision_id,
        "idempotency_key": record.idempotency_key,
        "controller_id": record.controller_id,
        "controller_owner_id": record.controller_owner_id,
        "controller_epoch": record.controller_epoch,
        "observed_at": record.observed_at,
        "decision_kind": record.decision_kind,
        "target_type": record.target_type,
        "target_id": record.target_id,
        "action_type": record.action_type,
        "reason_code": record.reason_code,
        "incident_severity": record.incident_severity,
        "qualification_breaking": record.qualification_breaking,
        "next_check_at": record.next_check_at,
        "runtime_state_digest": record.runtime_state_digest,
        "decision_digest": record.decision_digest,
        "payload": record.payload,
        "payload_sha256": record.payload_sha256,
    }


def _existing_tuple(record: Any) -> tuple[Any, ...]:
    return (
        record.decision_id,
        record.controller_id,
        record.controller_owner_id,
        record.controller_epoch,
        record.payload,
        record.payload_sha256,
        record.decision_digest,
    )


def _candidate_row(base: datetime, *, stalled: bool = False) -> dict[str, Any]:
    return {
        "job_key": "quote-batch:active",
        "job_type": "quote-batch",
        "job_state": "leased",
        "attempt_count": 0,
        "last_error_class": None,
        "attempt_id": "attempt-1",
        "lease_epoch": 7,
        "worker_id": "worker-1",
        "stage": "running",
        "started_at": base - timedelta(seconds=90),
        "last_heartbeat_at": base - timedelta(seconds=10),
        "last_progress_at": base - timedelta(seconds=90 if stalled else 10),
        "progress_sequence": 1,
        "progress_current": 1,
        "progress_total": 10,
        "lease_deadline_at": base + timedelta(seconds=90),
        "heartbeat_deadline_at": base + timedelta(seconds=20),
        "progress_deadline_at": base - timedelta(seconds=30 if stalled else -50),
        "attempt_deadline_at": base + timedelta(seconds=210),
        "recovery_state": "active",
        "circuit_state": None,
        "circuit_opened_at": None,
        "circuit_next_probe_at": None,
        "attempt_error_class": None,
        "remaining_actions": 3,
    }


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: str, params: object | None = None) -> None:
        normalized = " ".join(str(query).split())
        self.connection.sql.append(normalized)
        self.connection.params.append(params)
        if normalized.startswith("INSERT INTO public.m1_runtime_observe_decisions"):
            inserted = params
            assert isinstance(inserted, tuple)
            self.connection.pending_return = (
                inserted[0],
                inserted[2],
                inserted[3],
                inserted[4],
                getattr(inserted[16], "obj", inserted[16]),
                inserted[17],
                inserted[15],
            )

    def fetchall(self) -> list[dict[str, Any]]:
        if self.connection.sql[-1].find("FROM public.m1_job_runtime_state") >= 0:
            return self.connection.current_candidate_rows
        return self.connection.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        last_sql = self.connection.sql[-1]
        if "FROM public.m1_runtime_observe_decisions" in last_sql:
            if self.connection.pending_return is not None:
                row = self.connection.pending_return
                self.connection.existing_row = row
                self.connection.pending_return = None
                return row
            return self.connection.existing_row
        if "FROM public.m1_runtime_controller_leases" in last_sql:
            if self.connection.lease_owner_id is None:
                return None
            return (
                self.connection.lease_owner_id,
                self.connection.lease_epoch,
                self.connection.lease_expires_at,
            )
        if "FROM public.m1_recovery_actions" in last_sql:
            return (self.connection.recovery_action_count,)
        return None


class FakeConnection:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]],
        recovery_action_count: int = 0,
        existing_row: tuple[Any, ...] | None = None,
        lease_owner_id: str | None = CONTROLLER_OWNER_ID,
        lease_epoch: int = CONTROLLER_EPOCH,
        lease_expires_at: datetime | None = None,
        current_candidate_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.rows = rows
        self.recovery_action_count = recovery_action_count
        self.existing_row = existing_row
        self.pending_return: tuple[Any, ...] | None = None
        self.lease_owner_id = lease_owner_id
        self.lease_epoch = lease_epoch
        self.lease_expires_at = lease_expires_at or (_now() + timedelta(seconds=60))
        self.current_candidate_rows = current_candidate_rows or []
        self.sql: list[str] = []
        self.params: list[object | None] = []
        self.committed = False

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self, *args: object, **kwargs: object) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.committed = True


def _fake_factory(connection: FakeConnection) -> ConnectionFactory:
    return cast(ConnectionFactory, lambda: connection)


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
            == 0
        )
    except OSError:
        return False


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def _create_supabase_roles(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        for role in ("anon", "authenticated", "service_role"):
            connection.execute(f"CREATE ROLE {role} NOLOGIN")


def _insert_controller_lease(
    dsn: str,
    *,
    controller_id: str,
    owner_id: str,
    lease_epoch: int,
    lease_expires_at: datetime,
) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            """
            INSERT INTO m1_runtime_controller_leases (
                controller_id, owner_id, lease_epoch, lease_expires_at, claimed_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (controller_id, owner_id, lease_epoch, lease_expires_at, _now(), _now()),
        )
        connection.commit()


def _insert_job_attempt(dsn: str, *, attempt_id: str) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            """
            INSERT INTO m1_jobs (
                job_key, job_type, input_identity, state, lease_epoch,
                attempt_count, created_at, updated_at
            ) VALUES (
                %s, 'quote-batch', %s, 'leased', 7, 1, %s, %s
            ) ON CONFLICT (job_key) DO NOTHING
            """,
            ("quote-batch:active", "quote-batch-input", _now(), _now()),
        )
        connection.execute(
            """
            INSERT INTO m1_job_attempts (
                attempt_id, job_key, lease_epoch, worker_id, state,
                started_at, finished_at, error_class, error_detail, recorded_at
            ) VALUES (
                %s, 'quote-batch:active', 7, 'worker-1', 'running',
                %s, NULL, NULL, NULL, %s
            )
            """,
            (attempt_id, _now() - timedelta(seconds=240), _now()),
        )
        connection.commit()


def _insert_recovery_action(
    dsn: str,
    *,
    action_id: str,
    attempt_id: str,
    requested_at: datetime,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            """
            INSERT INTO m1_recovery_actions (
                action_id, controller_id, controller_owner_id, incident_key,
                target_type, target_id, action_type, expected_controller_epoch,
                expected_attempt_id, expected_lease_epoch, requested_at, started_at,
                finished_at, state, result_code, next_allowed_at, detail,
                idempotency_key
            ) VALUES (
                %s, %s, %s, NULL, 'job', 'quote-batch:active', 'cancel-job',
                %s, %s, 7, %s, %s, %s, 'completed', 'succeeded', %s, '{}'::jsonb, %s
            )
            """,
            (
                action_id,
                CONTROLLER_ID,
                CONTROLLER_OWNER_ID,
                CONTROLLER_EPOCH,
                attempt_id,
                requested_at,
                started_at,
                finished_at,
                finished_at + timedelta(seconds=60),
                f"idempotency:{action_id}",
            ),
        )
        connection.commit()


def _advance_controller_lease(
    dsn: str,
    *,
    controller_id: str,
    owner_id: str,
    lease_epoch: int,
) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            """
            UPDATE m1_runtime_controller_leases
            SET owner_id = %s,
                lease_epoch = %s,
                lease_expires_at = %s,
                updated_at = %s
            WHERE controller_id = %s
            """,
            (
                owner_id,
                lease_epoch,
                _now() + timedelta(minutes=5),
                _now() + timedelta(seconds=1),
                controller_id,
            ),
        )
        connection.commit()


def _observe_row_count(dsn: str) -> int:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM m1_runtime_observe_decisions")
        row = cursor.fetchone()
        assert row is not None
        return int(row[0])


def _recovery_action_count(dsn: str, *, controller_id: str) -> int:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM m1_recovery_actions WHERE controller_id = %s",
            (controller_id,),
        )
        row = cursor.fetchone()
        assert row is not None
        return int(row[0])
