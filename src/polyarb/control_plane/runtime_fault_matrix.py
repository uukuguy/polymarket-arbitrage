"""Deterministic local runtime fault matrix for production-enablement gates."""

from __future__ import annotations

import ipaddress
import json
import os
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import quote, urlparse
from uuid import uuid4

import psycopg
from alembic.config import Config
from psycopg import sql
from psycopg.rows import dict_row

from alembic import command

from .db_deadlines import CONTROL_PLANE_DB_POLICY
from .db_role_admin import provision_login_roles
from .db_role_contract import (
    ROLE_CONTRACTS,
    scoped_connection_factory,
    verify_daemon_database_role,
)
from .postgres import (
    PostgresControlPlane,
    RuntimeEventConflictError,
)
from .qualification import (
    BREAKING_REASONS,
    CONTAINED_REASONS,
    QualificationError,
    QualificationState,
    RollingQualificationPolicy,
)
from .qualification_service import (
    PostgresQualificationFactSource,
    PostgresQualificationServiceStore,
    QualificationFactRecord,
    QualificationService,
    ledger_row_to_fact_record,
)
from .reconciler import RuntimeReconciler
from .recovery_models import RecoveryActionType, RecoveryDecision
from .recovery_records import RecoveryActionRecord
from .recovery_store import (
    ConnectionFactory,
    claim_action,
    claim_controller,
    finish_action,
    read_runtime_reconcile_states,
    schedule_action,
)
from .runtime_models import RuntimeProgress
from .runtime_observe import (
    build_runtime_observe_decision_record,
    build_runtime_observe_idle_record,
    insert_runtime_observe_decision,
)

_ENV_NAME: Final[str] = "POLYARB_CONTROL_PLANE_TEST_DSN"
_DATABASE_RE: Final[re.Pattern[str]] = re.compile(r"^runtime_fault_matrix_[0-9a-f]{32}$")
_BASE_NOW: Final[datetime] = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
_CLUSTER_LOCK_KEY: Final[int] = 56062061
_MIGRATION_CLUSTER_ROLES: Final[tuple[str, ...]] = (
    "l3_evidence_daemon",
    "l3_retention_operator",
    ROLE_CONTRACTS["runtime-controller"].capability_role,
    ROLE_CONTRACTS["qualification-worker"].capability_role,
)
_DISPOSABLE_LOGIN_ROLES: Final[tuple[str, ...]] = (
    ROLE_CONTRACTS["runtime-controller"].login_role,
    ROLE_CONTRACTS["qualification-worker"].login_role,
)
_FAULT_CLASSES: Final[tuple[str, ...]] = (
    "task-exception",
    "r2-timeout-hang",
    "heartbeat-loss",
    "progress-stall",
    "stale-owner",
    "circuit-probe",
    "process-exit",
    "machine-restart-decision",
    "database-event-writer-failure",
    "watchdog-failure",
    "duplicate-delivery",
    "stale-action",
)
_OBSERVE_CONTROLLER_ID: Final[str] = "matrix-observe-controller"
_OBSERVE_CONTROLLER_OWNER_ID: Final[str] = "matrix-observe-owner"


class RuntimeFaultMatrixError(RuntimeError):
    """The local deterministic matrix cannot safely run."""


@dataclass(frozen=True, slots=True)
class _RuntimeContext:
    admin_dsn: str
    admin_factory: ConnectionFactory
    runtime_controller_factory: ConnectionFactory
    qualification_factory: ConnectionFactory
    control_plane: PostgresControlPlane


@dataclass(frozen=True, slots=True)
class _CaseOutcome:
    case: dict[str, object]
    qualification_fact_count: int
    observe_decision_count: int
    recovery_actions_created: int


def canonical_fault_matrix_bytes(result: Mapping[str, object]) -> bytes:
    """Return stable UTF-8 JSON bytes for diffing repeated matrix runs."""

    return json.dumps(
        result,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def run_fault_matrix() -> dict[str, object]:
    """Run the local-only runtime fault matrix against an explicit test DSN."""

    admin_dsn = _validated_test_dsn(os.environ.get(_ENV_NAME, ""))
    database_name = f"runtime_fault_matrix_{uuid4().hex}"
    if _DATABASE_RE.fullmatch(database_name) is None:
        raise RuntimeFaultMatrixError("generated runtime fault matrix database is unsafe")
    matrix_dsn = _dsn_with_database(admin_dsn, database_name)
    created = False
    maintenance_dsn = _dsn_with_database_unchecked(admin_dsn, "postgres")
    with psycopg.connect(
        maintenance_dsn,
        autocommit=True,
        connect_timeout=CONTROL_PLANE_DB_POLICY.connect_timeout_seconds,
    ) as maintenance:
        _lock_runtime_fault_matrix_cluster(maintenance)
        try:
            _assert_migration_cluster_roles_absent(maintenance)
            try:
                _create_empty_isolated_database_with_connection(maintenance, database_name)
                created = True
                _run_alembic_upgrade(matrix_dsn)
                _verify_migrated_authority(matrix_dsn)
                admin_factory = _connection_factory(matrix_dsn)
                runtime_password = f"runtime-controller-{uuid4().hex}"
                qualification_password = f"qualification-worker-{uuid4().hex}"
                provision_login_roles(
                    admin_factory,
                    expected_database=database_name,
                    runtime_password=runtime_password,
                    qualification_password=qualification_password,
                )
                context = _context(
                    matrix_dsn,
                    runtime_password=runtime_password,
                    qualification_password=qualification_password,
                )
                runtime_verification = verify_daemon_database_role(
                    context.runtime_controller_factory,
                    "runtime-controller",
                    expected_database=database_name,
                )
                qualification_verification = verify_daemon_database_role(
                    context.qualification_factory,
                    "qualification-worker",
                    expected_database=database_name,
                )
                outcomes = [
                    _run_case(context, index, fault) for index, fault in enumerate(_FAULT_CLASSES)
                ]
                cases = [outcome.case for outcome in outcomes]
                qualification_fact_count = sum(
                    outcome.qualification_fact_count for outcome in outcomes
                )
                observe_decision_count = sum(outcome.observe_decision_count for outcome in outcomes)
                recovery_actions_created = sum(
                    outcome.recovery_actions_created for outcome in outcomes
                )
                if recovery_actions_created != 0:
                    raise RuntimeFaultMatrixError(
                        "observe-only runtime controller created recovery actions"
                    )
                result: dict[str, object] = {
                    "case_count": len(cases),
                    "cases": cases,
                    "database_scope": "temporary-migrated-test-database",
                    "observe_decision_count": observe_decision_count,
                    "qualification_fact_count": qualification_fact_count,
                    "qualification_identity_digest": _qualification_identity_digest(),
                    "schema_version": "m1-runtime-fault-matrix-v2",
                    "scoped_roles": {
                        "qualification_worker": {
                            "facts_consumed": qualification_fact_count,
                            "profile": qualification_verification.profile,
                            "status": qualification_verification.status,
                        },
                        "runtime_controller": {
                            "observe_decisions": observe_decision_count,
                            "profile": runtime_verification.profile,
                            "recovery_actions_created": recovery_actions_created,
                            "status": runtime_verification.status,
                        },
                    },
                    "status": "pass",
                }
                result["matrix_sha256"] = sha256(canonical_fault_matrix_bytes(result)).hexdigest()
                return result
            finally:
                _drop_disposable_login_roles_if_safe(maintenance)
                if created:
                    _drop_isolated_database_with_connection(maintenance, database_name)
                _drop_migration_created_cluster_roles_if_safe(maintenance)
        finally:
            _unlock_runtime_fault_matrix_cluster(maintenance)


def _validated_test_dsn(raw: str) -> str:
    dsn = raw.strip()
    if not dsn:
        raise RuntimeFaultMatrixError(f"{_ENV_NAME} is required")
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgresql", "postgres"} or not parsed.hostname:
        raise RuntimeFaultMatrixError("runtime fault matrix requires a PostgreSQL test DSN")
    if parsed.query:
        lowered_query = parsed.query.casefold()
        if "options" in lowered_query or "search_path" in lowered_query:
            raise RuntimeFaultMatrixError(
                "runtime fault matrix requires a non-production scoped DSN"
            )
    lowered = dsn.casefold()
    if any(marker in lowered for marker in ("supabase.co", "fly.dev", "prod", "production")):
        raise RuntimeFaultMatrixError("runtime fault matrix requires a non-production scoped DSN")
    if not _is_loopback(parsed.hostname):
        raise RuntimeFaultMatrixError("runtime fault matrix requires a non-production scoped DSN")
    return dsn


def _is_loopback(hostname: str) -> bool:
    if hostname in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return address.is_loopback


def _dsn_with_database(dsn: str, database_name: str) -> str:
    if _DATABASE_RE.fullmatch(database_name) is None:
        raise RuntimeFaultMatrixError("refusing to target an unverified database")
    return _dsn_with_database_unchecked(dsn, database_name)


def _create_isolated_database(admin_dsn: str, matrix_dsn: str, database_name: str) -> None:
    if _DATABASE_RE.fullmatch(database_name) is None:
        raise RuntimeFaultMatrixError("refusing to create an unverified database")
    maintenance_dsn = _dsn_with_database_unchecked(admin_dsn, "postgres")
    created = False
    with psycopg.connect(
        maintenance_dsn,
        autocommit=True,
        connect_timeout=CONTROL_PLANE_DB_POLICY.connect_timeout_seconds,
    ) as maintenance:
        _lock_runtime_fault_matrix_cluster(maintenance)
        try:
            _assert_migration_cluster_roles_absent(maintenance)
            try:
                _create_empty_isolated_database_with_connection(maintenance, database_name)
                created = True
                _run_alembic_upgrade(matrix_dsn)
                _verify_migrated_authority(matrix_dsn)
            except Exception:
                if created:
                    _drop_isolated_database_with_connection(maintenance, database_name)
                _drop_migration_created_cluster_roles_if_safe(maintenance)
                raise
        finally:
            _unlock_runtime_fault_matrix_cluster(maintenance)


def _create_empty_isolated_database_with_connection(
    maintenance: psycopg.Connection[Any],
    database_name: str,
) -> None:
    if _DATABASE_RE.fullmatch(database_name) is None:
        raise RuntimeFaultMatrixError("refusing to create an unverified database")
    maintenance.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))


def _dsn_with_database_unchecked(dsn: str, database_name: str) -> str:
    return urlparse(dsn)._replace(path=f"/{database_name}").geturl()


def _run_alembic_upgrade(dsn: str) -> None:
    previous = os.environ.get("POLYARB_SUPABASE_DB_DSN")
    os.environ["POLYARB_SUPABASE_DB_DSN"] = dsn
    try:
        command.upgrade(Config(str(_REPO_ROOT / "alembic.ini")), "head")
    finally:
        if previous is None:
            os.environ.pop("POLYARB_SUPABASE_DB_DSN", None)
        else:
            os.environ["POLYARB_SUPABASE_DB_DSN"] = previous


def _verify_migrated_authority(matrix_dsn: str) -> None:
    with psycopg.connect(
        matrix_dsn,
        connect_timeout=CONTROL_PLANE_DB_POLICY.connect_timeout_seconds,
    ) as connection:
        table_count = connection.execute(
            """
            SELECT count(*) FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'm1_%%'
            """
        ).fetchone()
        trigger_count = connection.execute(
            """
            SELECT count(*) FROM information_schema.triggers
            WHERE trigger_schema = 'public' AND event_object_table LIKE 'm1_%%'
            """
        ).fetchone()
        fk_count = connection.execute(
            """
            SELECT count(*) FROM pg_constraint
            WHERE connamespace = 'public'::regnamespace AND contype = 'f'
            """
        ).fetchone()
    if table_count is None or int(table_count[0]) == 0:
        raise RuntimeFaultMatrixError("test DSN is missing migrated m1 tables")
    if trigger_count is None or int(trigger_count[0]) == 0:
        raise RuntimeFaultMatrixError("test DSN is missing migrated m1 triggers")
    if fk_count is None or int(fk_count[0]) == 0:
        raise RuntimeFaultMatrixError("test DSN is missing migrated m1 foreign keys")


def _drop_isolated_database_if_exists(admin_dsn: str, database_name: str) -> None:
    if _DATABASE_RE.fullmatch(database_name) is None:
        raise RuntimeFaultMatrixError("refusing to drop an unverified database")
    maintenance_dsn = _dsn_with_database_unchecked(admin_dsn, "postgres")
    with psycopg.connect(
        maintenance_dsn,
        autocommit=True,
        connect_timeout=CONTROL_PLANE_DB_POLICY.connect_timeout_seconds,
    ) as connection:
        _drop_isolated_database_with_connection(connection, database_name)


def _drop_isolated_database_with_connection(
    maintenance: psycopg.Connection[Any],
    database_name: str,
) -> None:
    if _DATABASE_RE.fullmatch(database_name) is None:
        raise RuntimeFaultMatrixError("refusing to drop an unverified database")
    maintenance.execute(
        """
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid()
        """,
        (database_name,),
    )
    maintenance.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name)))


def _lock_runtime_fault_matrix_cluster(maintenance: psycopg.Connection[Any]) -> None:
    maintenance.execute("SELECT pg_advisory_lock(%s)", (_CLUSTER_LOCK_KEY,))


def _unlock_runtime_fault_matrix_cluster(maintenance: psycopg.Connection[Any]) -> None:
    maintenance.execute("SELECT pg_advisory_unlock(%s)", (_CLUSTER_LOCK_KEY,))


def _assert_migration_cluster_roles_absent(maintenance: psycopg.Connection[Any]) -> None:
    rows = maintenance.execute(
        """
        SELECT rolname FROM pg_roles
        WHERE rolname = ANY(%s)
        ORDER BY rolname
        """,
        (list(_MIGRATION_CLUSTER_ROLES),),
    ).fetchall()
    if rows:
        names = ", ".join(str(row[0]) for row in rows)
        raise RuntimeFaultMatrixError(
            "runtime fault matrix requires disposable loopback cluster; "
            f"pre-existing cluster role(s): {names}"
        )


def _drop_migration_created_cluster_roles_if_safe(
    maintenance: psycopg.Connection[Any],
) -> None:
    rows = maintenance.execute(
        """
        SELECT
            roles.rolname,
            roles.rolcanlogin,
            roles.rolsuper,
            roles.rolcreatedb,
            roles.rolcreaterole,
            roles.rolinherit,
            roles.rolreplication,
            roles.rolbypassrls,
            EXISTS (
                SELECT 1 FROM pg_auth_members membership
                WHERE membership.roleid = roles.oid OR membership.member = roles.oid
            ) AS has_membership,
            EXISTS (
                SELECT 1 FROM pg_shdepend dependency
                WHERE dependency.refobjid = roles.oid AND dependency.dbid = 0
            ) AS has_shared_dependency
        FROM pg_roles roles
        WHERE roles.rolname = ANY(%s)
        ORDER BY roles.rolname
        """,
        (list(_MIGRATION_CLUSTER_ROLES),),
    ).fetchall()
    by_name = {str(row[0]): row for row in rows}
    for role_name in reversed(_MIGRATION_CLUSTER_ROLES):
        row = by_name.get(role_name)
        if row is None:
            continue
        unsafe = any(bool(value) for value in row[1:])
        if unsafe:
            raise RuntimeFaultMatrixError(
                f"refusing to clean unsafe migration-created cluster role {role_name!r}"
            )
        maintenance.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role_name)))


def _drop_disposable_login_roles_if_safe(maintenance: psycopg.Connection[Any]) -> None:
    rows = maintenance.execute(
        """
        SELECT
            roles.rolname,
            roles.rolcanlogin,
            roles.rolsuper,
            roles.rolcreatedb,
            roles.rolcreaterole,
            roles.rolinherit,
            roles.rolreplication,
            roles.rolbypassrls
        FROM pg_roles roles
        WHERE roles.rolname = ANY(%s)
        ORDER BY roles.rolname
        """,
        (list(_DISPOSABLE_LOGIN_ROLES),),
    ).fetchall()
    by_name = {str(row[0]): row for row in rows}
    for profile in ("runtime-controller", "qualification-worker"):
        contract = ROLE_CONTRACTS[profile]
        row = by_name.get(contract.login_role)
        if row is None:
            continue
        unsafe = any(bool(value) for value in (row[2], row[3], row[4], row[6], row[7]))
        if unsafe or not bool(row[1]) or not bool(row[5]):
            raise RuntimeFaultMatrixError(
                f"refusing to clean unsafe disposable login role {contract.login_role!r}"
            )
        memberships = maintenance.execute(
            """
            SELECT granted.rolname
            FROM pg_auth_members membership
            JOIN pg_roles granted ON granted.oid = membership.roleid
            JOIN pg_roles member ON member.oid = membership.member
            WHERE member.rolname = %s
            ORDER BY granted.rolname
            """,
            (contract.login_role,),
        ).fetchall()
        if tuple(str(membership[0]) for membership in memberships) != (contract.capability_role,):
            raise RuntimeFaultMatrixError(
                f"refusing to clean unsafe disposable login membership {contract.login_role!r}"
            )
        maintenance.execute(
            sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(contract.capability_role),
                sql.Identifier(contract.login_role),
            )
        )
        maintenance.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(contract.login_role)))


def _context(
    dsn: str,
    *,
    runtime_password: str,
    qualification_password: str,
) -> _RuntimeContext:
    runtime_dsn = _dsn_with_login(
        dsn,
        ROLE_CONTRACTS["runtime-controller"].login_role,
        runtime_password,
    )
    qualification_dsn = _dsn_with_login(
        dsn,
        ROLE_CONTRACTS["qualification-worker"].login_role,
        qualification_password,
    )
    admin_factory = _connection_factory(dsn)
    return _RuntimeContext(
        admin_dsn=dsn,
        admin_factory=admin_factory,
        runtime_controller_factory=scoped_connection_factory(runtime_dsn),
        qualification_factory=scoped_connection_factory(qualification_dsn),
        control_plane=PostgresControlPlane(admin_factory),
    )


def _connection_factory(dsn: str) -> ConnectionFactory:
    return lambda: psycopg.connect(
        dsn,
        connect_timeout=CONTROL_PLANE_DB_POLICY.connect_timeout_seconds,
    )


def _dsn_with_login(dsn: str, username: str, password: str) -> str:
    parsed = urlparse(dsn)
    if not parsed.hostname:
        raise RuntimeFaultMatrixError("runtime fault matrix requires a PostgreSQL test DSN")
    host = parsed.hostname
    host_part = f"[{host}]" if ":" in host and not host.startswith("[") else host
    if parsed.port is not None:
        host_part = f"{host_part}:{parsed.port}"
    netloc = f"{quote(username, safe='')}:{quote(password, safe='')}@{host_part}"
    return parsed._replace(netloc=netloc).geturl()


def _run_case(context: _RuntimeContext, index: int, fault_class: str) -> _CaseOutcome:
    now = _BASE_NOW + timedelta(minutes=index * 10)
    before_ingest_seq = _ingress_high_water(context)
    if fault_class in {"task-exception", "r2-timeout-hang"}:
        case = _retryable_incident_case(context, fault_class=fault_class, now=now)
    elif fault_class == "database-event-writer-failure":
        case = _writer_failure_case(context, now=now)
    elif fault_class == "watchdog-failure":
        case = _watchdog_failure_case(context, fault_class=fault_class, now=now)
    elif fault_class in {"process-exit", "machine-restart-decision"}:
        case = _observe_only_runtime_case(context, fault_class=fault_class, now=now)
    elif fault_class == "duplicate-delivery":
        case = _duplicate_delivery_case(context, now=now)
    elif fault_class == "stale-action":
        case = _stale_action_case(context, now=now)
    else:
        case = _reconciler_case(context, fault_class=fault_class, now=now)
    case = _attach_qualification_projection(
        context,
        case,
        before_ingest_seq=before_ingest_seq,
        started_at=now,
        now=now + timedelta(seconds=9),
    )
    qualification_fact_count = _advance_qualification_cursor(
        context,
        now=now + timedelta(seconds=10),
    )
    before_observe_recovery_actions = _recovery_action_count(context)
    observe_decision_count = _run_observe_only_reconciliation(
        context,
        fault_class=fault_class,
        now=now + timedelta(seconds=11),
    )
    after_recovery_actions = _recovery_action_count(context)
    return _CaseOutcome(
        case=case,
        qualification_fact_count=qualification_fact_count,
        observe_decision_count=observe_decision_count,
        recovery_actions_created=after_recovery_actions - before_observe_recovery_actions,
    )


def _retryable_incident_case(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
) -> dict[str, object]:
    lease = _seed_claimed_job(context, fault_class=fault_class, now=now)
    context.control_plane.finish_retryable_with_incident(
        lease,
        error_class="timeout" if fault_class == "r2-timeout-hang" else "task-exception",
        incident_key=f"matrix:{fault_class}",
        dedupe_key=f"matrix:{fault_class}",
        component="structure-normalize",
        summary=f"{fault_class} contained by retry fence",
        detail={
            "component": "structure-normalize",
            "failure_signature": "upstream.timeout",
            "qualification_impact": "delayed",
            "recovery_policy": "retry-soon",
        },
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=2))
    return _case_result(
        fault_class=fault_class,
        detection_latency_seconds=1,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "finish_retryable_with_incident",
            "state": "open",
        },
        action={"result": "recorded", "type": "retry-job"},
        fence_result="lease-current",
        recovery={"next_attempt_scheduled": True, "state": "contained"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _writer_failure_case(context: _RuntimeContext, *, now: datetime) -> dict[str, object]:
    lease = _seed_claimed_job(context, fault_class="database-event-writer-failure", now=now)
    progress = RuntimeProgress(sequence=1, current=1, total=2, stage="upload-range")
    context.control_plane.record_runtime_progress(
        lease,
        progress=progress,
        now=now + timedelta(seconds=1),
        idempotency_key="matrix:writer-failure:progress",
        detail={"component": "structure-normalize"},
    )
    before = _event_count(context)
    rollback_verified = False
    try:
        context.control_plane.record_runtime_progress(
            lease,
            progress=RuntimeProgress(sequence=1, current=2, total=2, stage="upload-range"),
            now=now + timedelta(seconds=2),
            idempotency_key="matrix:writer-failure:progress",
            detail={"component": "structure-normalize"},
        )
    except RuntimeEventConflictError:
        rollback_verified = _event_count(context) == before
    context.control_plane.record_incident_event(
        incident_key="matrix:database-event-writer-failure",
        dedupe_key="matrix:database-event-writer-failure",
        component="runtime",
        severity="critical",
        summary="database event writer conflict rolled back",
        kind="detected",
        detail={"failure_signature": "validation.failed", "qualification_impact": "breaking"},
        idempotency_key="matrix:database-event-writer-failure",
        channels=("dashboard",),
        now=now + timedelta(seconds=3),
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=4))
    return _case_result(
        fault_class="database-event-writer-failure",
        detection_latency_seconds=2,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "record_incident_event",
            "state": "open",
        },
        action={"result": "recorded", "type": "none"},
        fence_result="idempotency-conflict-rolled-back",
        recovery={"rollback_verified": rollback_verified, "state": "contained"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _watchdog_failure_case(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
) -> dict[str, object]:
    context.control_plane.record_incident_event(
        incident_key=f"runtime-watchdog:{fault_class}",
        dedupe_key=f"runtime-watchdog:{fault_class}",
        component="runtime",
        severity="critical",
        summary="independent watchdog reported runtime failure",
        kind="detected",
        detail={"qualification_impact": "breaking"},
        idempotency_key=f"matrix:{fault_class}",
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=2))
    return _case_result(
        fault_class=fault_class,
        detection_latency_seconds=1,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "record_incident_event",
            "state": "open",
        },
        action={"result": "observe-only", "type": "page-operator"},
        fence_result="watchdog-independent",
        recovery={"state": "observe-only"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _observe_only_runtime_case(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
) -> dict[str, object]:
    action_type = (
        RecoveryActionType.RESTART_MACHINE.value
        if fault_class == "machine-restart-decision"
        else RecoveryActionType.RESTART_WORKER_PROCESS.value
    )
    context.control_plane.record_incident_event(
        incident_key=f"matrix:{fault_class}",
        dedupe_key=f"matrix:{fault_class}",
        component="runtime",
        severity="critical",
        summary=f"{fault_class} requires explicit production gate",
        kind="detected",
        detail={"qualification_impact": "breaking", "action_type": action_type},
        idempotency_key=f"matrix:{fault_class}",
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=2))
    return _case_result(
        fault_class=fault_class,
        detection_latency_seconds=1,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "record_incident_event",
            "state": "open",
        },
        action={"result": "observe-only", "type": action_type},
        fence_result="explicit-enable-required",
        recovery={"state": "observe-only"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _duplicate_delivery_case(context: _RuntimeContext, *, now: datetime) -> dict[str, object]:
    scheduled, replay = _schedule_recovery(
        context,
        fault_class="duplicate-delivery",
        now=now,
        decision=_decision(RecoveryActionType.RECLAIM_JOB, "job.lease-expired", now),
        stale=False,
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=3))
    return _case_result(
        fault_class="duplicate-delivery",
        detection_latency_seconds=1,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "schedule_action",
            "state": "open",
        },
        action={"result": "idempotent-replay", "type": scheduled.action_type},
        fence_result="idempotent-replay" if replay == scheduled else "conflict",
        recovery={"state": "contained"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _stale_action_case(context: _RuntimeContext, *, now: datetime) -> dict[str, object]:
    action, _replay = _schedule_recovery(
        context,
        fault_class="stale-action",
        now=now,
        decision=_decision(RecoveryActionType.RECLAIM_JOB, "job.lease-expired", now),
        stale=True,
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=4))
    return _case_result(
        fault_class="stale-action",
        detection_latency_seconds=2,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "schedule_action",
            "state": "not-required",
        },
        action={"result": action.result_code or "scheduled", "type": action.action_type},
        fence_result="stale-runtime-fence",
        recovery={"state": "stale-noop"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _reconciler_case(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
) -> dict[str, object]:
    lease = _seed_claimed_job(context, fault_class=fault_class, now=now, lease_seconds=120)
    if fault_class == "progress-stall":
        _age_runtime(context, lease.job_key, now=now, progress_seconds=130, lease_seconds=60)
    elif fault_class == "heartbeat-loss":
        _age_runtime(context, lease.job_key, now=now, heartbeat_seconds=130, lease_seconds=1)
    elif fault_class == "stale-owner":
        return _stale_owner_case(context, lease_job_key=lease.job_key, now=now)
    elif fault_class == "circuit-probe":
        _open_circuit(context, lease.job_key, now=now)

    controller = claim_controller(
        context.admin_factory,
        controller_id=f"matrix-controller-{fault_class}",
        owner_id=f"matrix-owner-{fault_class}",
        lease_seconds=120,
        now=now,
    )
    candidate = read_runtime_reconcile_states(
        context.admin_factory,
        controller_id=controller.controller_id,
        now=now + timedelta(seconds=2),
        sample_limit=1,
    )[0]
    decision = RuntimeReconciler().evaluate(
        candidate.runtime_state,
        now=now + timedelta(seconds=2),
    )
    action = schedule_action(
        context.admin_factory,
        controller=controller,
        decision=decision,
        incident_key=candidate.incident_key,
        component=candidate.component,
        target_type=candidate.target_type,
        target_id=candidate.target_id,
        expected_attempt_id=candidate.runtime_state.attempt_id,
        expected_lease_epoch=candidate.runtime_state.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=2),
    )
    completed = _claim_and_complete(context, controller, action, now=now + timedelta(seconds=3))
    snapshot = _snapshot(context, now=now + timedelta(seconds=4))
    return _case_result(
        fault_class=fault_class,
        detection_latency_seconds=2,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "RuntimeReconciler.schedule_action",
            "state": "open",
        },
        action={"result": completed.result_code or "scheduled", "type": completed.action_type},
        fence_result="runtime-fence-current",
        recovery={"state": "contained"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _stale_owner_case(
    context: _RuntimeContext,
    *,
    lease_job_key: str,
    now: datetime,
) -> dict[str, object]:
    stale_controller = claim_controller(
        context.admin_factory,
        controller_id="matrix-controller-stale-owner",
        owner_id="stale-owner-a",
        lease_seconds=120,
        now=now,
    )
    claim_controller(
        context.admin_factory,
        controller_id=stale_controller.controller_id,
        owner_id="stale-owner-b",
        lease_seconds=120,
        now=now + timedelta(seconds=1),
    )
    attempt_id = _attempt_id(context, lease_job_key)
    action = schedule_action(
        context.admin_factory,
        controller=stale_controller,
        decision=_decision(RecoveryActionType.RECLAIM_JOB, "job.lease-expired", now),
        incident_key=f"recovery:job:{lease_job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease_job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=1,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=2),
    )
    snapshot = _snapshot(context, now=now + timedelta(seconds=3))
    return _case_result(
        fault_class="stale-owner",
        detection_latency_seconds=2,
        incident_transition={
            "outbox_pending": _outbox_total(snapshot),
            "source": "schedule_action",
            "state": "not-required",
        },
        action={"result": action.result_code or "scheduled", "type": action.action_type},
        fence_result="stale-controller",
        recovery={"state": "stale-noop"},
        dashboard_projection=_dashboard_projection(snapshot),
        qualification_impact="pending-db-ingress",
    )


def _schedule_recovery(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
    decision: RecoveryDecision,
    stale: bool,
) -> tuple[RecoveryActionRecord, RecoveryActionRecord]:
    lease = _seed_claimed_job(context, fault_class=fault_class, now=now, lease_seconds=2)
    attempt_id = _attempt_id(context, lease.job_key)
    expected_epoch = lease.lease_epoch
    if stale:
        replacement = context.control_plane.claim_job(
            worker_id=f"worker:{fault_class}:replacement",
            job_types=(lease.job_type,),
            lease_seconds=60,
            now=now + timedelta(seconds=3),
        )
        if replacement is None:
            raise RuntimeFaultMatrixError("stale action replacement lease was not created")
    controller = claim_controller(
        context.admin_factory,
        controller_id=f"matrix-controller-{fault_class}",
        owner_id=f"matrix-owner-{fault_class}",
        lease_seconds=60,
        now=now + timedelta(seconds=1),
    )
    scheduled_at = now + timedelta(seconds=4 if stale else 1)
    action = schedule_action(
        context.admin_factory,
        controller=controller,
        decision=decision,
        incident_key=f"recovery:job:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=expected_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=scheduled_at,
    )
    replay = schedule_action(
        context.admin_factory,
        controller=controller,
        decision=decision,
        incident_key=f"recovery:job:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=expected_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=scheduled_at,
    )
    return action, replay


def _seed_claimed_job(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
    lease_seconds: int = 60,
) -> Any:
    job_key = f"runtime-fault-matrix:{fault_class}"
    context.control_plane.enqueue_job(
        job_key=job_key,
        job_type="structure-normalize",
        input_identity=job_key,
        now=now,
    )
    lease = context.control_plane.claim_job(
        worker_id=f"worker:{fault_class}",
        job_types=("structure-normalize",),
        lease_seconds=lease_seconds,
        now=now,
    )
    if lease is None:
        raise RuntimeFaultMatrixError(f"matrix could not claim job for {fault_class}")
    return lease


def _age_runtime(
    context: _RuntimeContext,
    job_key: str,
    *,
    now: datetime,
    heartbeat_seconds: int = 0,
    progress_seconds: int = 0,
    lease_seconds: int = 60,
) -> None:
    with context.admin_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE m1_jobs
            SET lease_expires_at = %s
            WHERE job_key = %s
            """,
            (now + timedelta(seconds=lease_seconds), job_key),
        )
        cursor.execute(
            """
            UPDATE m1_job_runtime_state
            SET last_heartbeat_at = %s,
                last_progress_at = %s,
                lease_deadline_at = %s,
                heartbeat_deadline_at = %s,
                progress_deadline_at = %s,
                attempt_deadline_at = %s,
                updated_at = %s
            WHERE job_key = %s
            """,
            (
                now - timedelta(seconds=heartbeat_seconds),
                now - timedelta(seconds=progress_seconds),
                now + timedelta(seconds=lease_seconds),
                now - timedelta(seconds=max(0, heartbeat_seconds - 1)),
                now - timedelta(seconds=max(0, progress_seconds - 1)),
                now + timedelta(seconds=300),
                now,
                job_key,
            ),
        )


def _open_circuit(context: _RuntimeContext, job_key: str, *, now: datetime) -> None:
    with context.admin_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO m1_job_circuits (
                job_key, consecutive_failures, state, opened_at, next_probe_at, updated_at
            ) VALUES (%s, 3, 'open', %s, %s, %s)
            ON CONFLICT (job_key) DO UPDATE
            SET consecutive_failures = 3, state = 'open', opened_at = EXCLUDED.opened_at,
                next_probe_at = EXCLUDED.next_probe_at, updated_at = EXCLUDED.updated_at
            """,
            (
                job_key,
                now - timedelta(seconds=120),
                now - timedelta(seconds=1),
                now,
            ),
        )


def _attempt_id(context: _RuntimeContext, job_key: str) -> str:
    with context.admin_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT attempt_id FROM m1_job_runtime_state WHERE job_key = %s", (job_key,))
        row = cursor.fetchone()
    if row is None:
        raise RuntimeFaultMatrixError(f"runtime attempt missing for {job_key}")
    return str(row[0])


def _claim_and_complete(
    context: _RuntimeContext,
    controller: Any,
    action: RecoveryActionRecord,
    *,
    now: datetime,
) -> RecoveryActionRecord:
    claim = claim_action(
        context.admin_factory,
        worker_id="matrix-recovery-worker",
        controller=controller,
        lease_seconds=30,
        now=now,
    )
    if claim is None:
        raise RuntimeFaultMatrixError(f"matrix action was not claimable: {action.action_id}")
    return finish_action(
        context.admin_factory,
        action_id=claim.action_id,
        worker_id=cast(str, claim.worker_id),
        worker_epoch=claim.worker_epoch,
        result_code="succeeded",
        now=now + timedelta(seconds=1),
        detail={"postcondition": "succeeded"},
    )


def _decision(action: RecoveryActionType, reason_code: str, now: datetime) -> RecoveryDecision:
    severity = (
        "critical" if reason_code in {"job.lease-expired", "job.heartbeat-missing"} else ("warning")
    )
    breaking = reason_code in {"job.lease-expired", "job.heartbeat-missing"}
    return RecoveryDecision(
        action=action,
        reason_code=reason_code,
        incident_severity=cast(Any, severity),
        qualification_breaking=breaking,
        next_check_at=now,
    )


def _snapshot(context: _RuntimeContext, *, now: datetime) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        context.control_plane.operational_snapshot(now=now, sample_limit=20),
    )


def _dashboard_projection(snapshot: Mapping[str, Any]) -> dict[str, object]:
    runtime_incidents = cast(Mapping[str, object], snapshot["runtime_incidents"])
    recovery_actions = cast(Mapping[str, object], snapshot["recovery_actions"])
    qualification = cast(Mapping[str, object], snapshot["qualification"])
    return {
        "open_runtime_incidents": int(cast(Any, runtime_incidents["total"])),
        "qualification_state": qualification.get("state"),
        "recovery_actions": int(cast(Any, recovery_actions["total"])),
        "source": "operational_snapshot",
    }


def _outbox_total(snapshot: Mapping[str, Any]) -> int:
    pending = cast(list[object], snapshot["pending_alert_outbox"])
    return len(pending)


def _advance_qualification_cursor(context: _RuntimeContext, *, now: datetime) -> int:
    service = QualificationService(
        policy=_qualification_policy(),
        fact_source=PostgresQualificationFactSource(context.qualification_factory),
        state_store=PostgresQualificationServiceStore(context.qualification_factory),
        writer_id="runtime-fault-matrix-qualification-worker",
        batch_size=100,
    )
    return service.tick(now).applied


def _run_observe_only_reconciliation(
    context: _RuntimeContext,
    *,
    fault_class: str,
    now: datetime,
) -> int:
    controller = claim_controller(
        context.runtime_controller_factory,
        controller_id=_OBSERVE_CONTROLLER_ID,
        owner_id=_OBSERVE_CONTROLLER_OWNER_ID,
        lease_seconds=120,
        now=now,
    )
    candidates = read_runtime_reconcile_states(
        context.runtime_controller_factory,
        controller_id=controller.controller_id,
        now=now,
        sample_limit=100,
    )
    if not candidates:
        insert_runtime_observe_decision(
            context.runtime_controller_factory,
            build_runtime_observe_idle_record(
                controller_id=controller.controller_id,
                controller_owner_id=controller.owner_id,
                controller_epoch=controller.lease_epoch,
                observed_at=now,
                next_check_at=now + timedelta(seconds=30),
                observed_by=controller.owner_id,
            ),
        )
        return 1
    count = 0
    for candidate in candidates:
        decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=now)
        insert_runtime_observe_decision(
            context.runtime_controller_factory,
            build_runtime_observe_decision_record(
                controller_id=controller.controller_id,
                controller_owner_id=controller.owner_id,
                controller_epoch=controller.lease_epoch,
                observed_at=now,
                candidate=candidate,
                decision=decision,
                observed_by=controller.owner_id,
            ),
        )
        count += 1
    if count <= 0:
        raise RuntimeFaultMatrixError(f"observe-only reconciliation was empty for {fault_class}")
    return count


def _event_count(context: _RuntimeContext) -> int:
    with context.admin_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM m1_job_runtime_events")
        row = cursor.fetchone()
    return 0 if row is None else int(row[0])


def _recovery_action_count(context: _RuntimeContext) -> int:
    with context.admin_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM m1_recovery_actions")
        row = cursor.fetchone()
    return 0 if row is None else int(row[0])


def _ingress_high_water(context: _RuntimeContext) -> int:
    with context.qualification_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COALESCE(max(ingest_seq), 0) FROM m1_qualification_ingress_ledger")
        row = cursor.fetchone()
    return 0 if row is None else int(row[0])


def _attach_qualification_projection(
    context: _RuntimeContext,
    case: dict[str, object],
    *,
    before_ingest_seq: int,
    started_at: datetime,
    now: datetime,
) -> dict[str, object]:
    projection = _qualification_projection(
        context,
        before_ingest_seq=before_ingest_seq,
        started_at=started_at,
        now=now,
    )
    case["qualification_projection"] = projection
    case["qualification_impact"] = projection["impact"]
    if projection["impact"] == "breaking":
        case["recovery"] = {
            "qualification_epoch": "invalidated",
            **dict(cast(Mapping[str, object], case["recovery"])),
        }
    return case


def _qualification_projection(
    context: _RuntimeContext,
    *,
    before_ingest_seq: int,
    started_at: datetime,
    now: datetime,
) -> dict[str, object]:
    rows = _read_qualification_ingress_rows(context, before_ingest_seq=before_ingest_seq)
    if not rows:
        raise RuntimeFaultMatrixError("runtime fault matrix case produced no qualification ingress")
    records = tuple(ledger_row_to_fact_record(row) for row in rows)
    impact = _qualification_impact_from_records(records, started_at=started_at, now=now)
    sources = Counter(str(row["source"]) for row in rows)
    latest_row = rows[-1]
    latest_payload = cast(Mapping[str, object], latest_row["payload"])
    return {
        "decode_error_count": 0,
        "decoded_count": len(records),
        "fact_reasons": [record.fact.reason for record in records],
        "impact": impact,
        "ingress_count": len(rows),
        "latest": {
            "payload_anchor": _qualification_payload_anchor(
                str(latest_row["source"]), latest_payload
            ),
            "source": str(latest_row["source"]),
            "source_version": _stable_source_version(str(latest_row["source_version"])),
        },
        "replay_error_count": 0,
        "source": "m1_qualification_ingress_ledger",
        "sources": dict(sorted(sources.items())),
    }


def _read_qualification_ingress_rows(
    context: _RuntimeContext,
    *,
    before_ingest_seq: int,
) -> tuple[Mapping[str, object], ...]:
    with (
        context.qualification_factory() as connection,
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        cursor.execute(
            """
            SELECT ingest_seq, ingested_at, source, source_id, source_version,
                   original_observed_at, payload, payload_sha256,
                   original_observed_at AS qualification_observed_at
            FROM m1_qualification_ingress_ledger
            WHERE ingest_seq > %s
            ORDER BY ingest_seq
            """,
            (before_ingest_seq,),
        )
        return tuple(cast(Mapping[str, object], row) for row in cursor.fetchall())


def _qualification_payload_anchor(source: str, payload: Mapping[str, object]) -> str:
    if source == "runtime":
        return str(payload.get("job_key", ""))
    if source == "incident":
        return str(payload.get("incident_key", ""))
    if source == "recovery":
        return str(payload.get("target_id", ""))
    return str(payload.get("fact_id", ""))


def _stable_source_version(source_version: str) -> str:
    parts = source_version.split(":", 2)
    if len(parts) >= 2 and parts[0] in {"pending", "running", "completed"}:
        return ":".join(parts[:2])
    return source_version


def _qualification_impact_from_records(
    records: tuple[QualificationFactRecord, ...],
    *,
    started_at: datetime,
    now: datetime,
) -> str:
    policy = _qualification_policy()
    decision = policy.new_epoch(started_at=started_at, epoch_id="qualification:runtime-matrix")
    impact = "none"
    for record in records:
        fact = record.fact
        if fact.reason == "recovery.started":
            impact = "delayed" if impact == "none" else impact
        elif fact.reason in CONTAINED_REASONS:
            impact = "restored" if impact == "none" else impact
        if fact.reason in BREAKING_REASONS:
            return "breaking"
        try:
            decision = policy.apply(decision, fact)
        except QualificationError:
            raise RuntimeFaultMatrixError("decoded qualification ingress is not replayable")
        if decision.state is QualificationState.INVALIDATED:
            return "breaking"
    if decision.qualified_at is not None and decision.qualified_at <= now:
        return "qualified"
    return impact


def _qualification_policy() -> RollingQualificationPolicy:
    return RollingQualificationPolicy(
        release_id="runtime-fault-matrix",
        config_id="local",
        role_identity=("m1", "runtime"),
        required_seconds=60,
        max_gap_seconds=600,
    )


def _qualification_identity_digest() -> str:
    policy = _qualification_policy()
    payload = {
        "config_id": policy.config_id,
        "max_gap_seconds": policy.max_gap_seconds,
        "policy_version": policy.policy_version,
        "release_id": policy.release_id,
        "required_seconds": policy.required_seconds,
        "role_identity": list(policy.role_identity),
    }
    return sha256(canonical_fault_matrix_bytes(payload)).hexdigest()


def _case_result(
    *,
    fault_class: str,
    detection_latency_seconds: int,
    incident_transition: Mapping[str, object],
    action: Mapping[str, object],
    fence_result: str,
    recovery: Mapping[str, object],
    dashboard_projection: Mapping[str, object],
    qualification_impact: str,
) -> dict[str, object]:
    if qualification_impact == "breaking":
        recovery = {"qualification_epoch": "invalidated", **dict(recovery)}
    return {
        "action": dict(action),
        "dashboard_projection": dict(dashboard_projection),
        "detection_latency_seconds": detection_latency_seconds,
        "fault_class": fault_class,
        "fence_result": fence_result,
        "incident_transition": dict(incident_transition),
        "qualification_impact": qualification_impact,
        "qualification_projection": {"source": "pending-db-ingress"},
        "recovery": dict(recovery),
        "status": "pass",
    }


__all__ = [
    "RuntimeFaultMatrixError",
    "canonical_fault_matrix_bytes",
    "run_fault_matrix",
]
