"""Read-only fail-closed database identity contract for daemon roles."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg

ConnectionFactory = Callable[[], psycopg.Connection[Any]]
TABLE_PRIVILEGES = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "REFERENCES",
    "TRIGGER",
)
SEQUENCE_PRIVILEGES = ("USAGE", "SELECT", "UPDATE")

RUNTIME_ALLOWED = {
    "m1_runtime_controller_leases": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_runtime_observe_decisions": frozenset({"SELECT", "INSERT"}),
    "m1_job_runtime_state": frozenset({"SELECT"}),
    "m1_jobs": frozenset({"SELECT"}),
    "m1_job_circuits": frozenset({"SELECT"}),
    "m1_job_attempts": frozenset({"SELECT"}),
    "m1_recovery_target_budgets": frozenset({"SELECT"}),
    "m1_recovery_actions": frozenset({"SELECT"}),
    "m1_job_runtime_events": frozenset(),
    "m1_incidents": frozenset(),
    "m1_incident_events": frozenset(),
    "m1_alert_outbox": frozenset(),
}
QUALIFICATION_ALLOWED = {
    "m1_qualification_ingress_ledger": frozenset({"SELECT"}),
    "m1_qualification_source_cursors": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_qualification_epochs": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_qualification_recovery_observations": frozenset({"SELECT", "INSERT"}),
    "m1_qualification_certificates": frozenset({"SELECT"}),
    "m1_publication_pointers": frozenset({"SELECT"}),
    "m1_generation_manifests": frozenset({"SELECT"}),
    "m1_opportunity_publication_pointers": frozenset({"SELECT"}),
    "m1_opportunity_projections": frozenset({"SELECT"}),
    "m1_job_runtime_events": frozenset(),
    "m1_incidents": frozenset(),
    "m1_incident_events": frozenset(),
    "m1_recovery_actions": frozenset(),
    "m1_alert_outbox": frozenset(),
}
RUNTIME_SEQUENCE_COLUMNS = (
    ("m1_runtime_controller_leases", "controller_id"),
    ("m1_runtime_observe_decisions", "decision_id"),
    ("m1_job_runtime_state", "job_key"),
    ("m1_jobs", "job_key"),
    ("m1_job_circuits", "job_key"),
    ("m1_job_attempts", "attempt_id"),
    ("m1_recovery_target_budgets", "controller_id"),
    ("m1_recovery_actions", "action_id"),
    ("m1_job_runtime_events", "event_id"),
    ("m1_incidents", "incident_key"),
    ("m1_incident_events", "incident_event_id"),
    ("m1_alert_outbox", "outbox_id"),
)
QUALIFICATION_SEQUENCE_COLUMNS = (("m1_qualification_ingress_ledger", "ingest_seq"),)

FRESHNESS_FUNCTION = (
    "public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)"
)
CERTIFICATE_FUNCTION = (
    "public.m1_insert_qualification_certificate("
    "text,text,text,text,jsonb,timestamptz,timestamptz,jsonb,text,text,text,text)"
)
GENERAL_INGRESS_FUNCTION = (
    "public.m1_record_qualification_ingress(text,text,text,timestamptz,jsonb)"
)
HARDENED_TRIGGER_FUNCTIONS = (
    "public.m1_project_runtime_qualification_ingress()",
    "public.m1_project_incident_qualification_ingress()",
    "public.m1_project_recovery_qualification_ingress()",
    "public.m1_verify_qualification_certificate_insert()",
)


@dataclass(frozen=True, slots=True)
class DatabaseRoleContract:
    profile: str
    login_role: str
    capability_role: str
    required_table_privileges: tuple[tuple[str, str], ...]
    forbidden_table_privileges: tuple[tuple[str, str], ...]
    required_function_privileges: tuple[str, ...] = ()
    forbidden_function_privileges: tuple[str, ...] = ()
    forbidden_sequence_columns: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class DatabaseRoleVerification:
    profile: str
    session_user: str
    capability_role: str
    database_name: str
    status: str = "pass"


@dataclass(frozen=True, slots=True)
class _RoleAttributes:
    role_name: str
    can_login: bool
    superuser: bool
    create_db: bool
    create_role: bool
    inherits: bool
    replication: bool
    bypass_rls: bool


class DatabaseRoleContractError(RuntimeError):
    """Closed role-contract refusal with no database exception detail."""

    def __init__(self, reason_code: str, object_identifier: str) -> None:
        self.reason_code = reason_code
        self.object_identifier = object_identifier
        super().__init__(f"{reason_code}: {object_identifier}")


def _table_contract(
    allowed: Mapping[str, frozenset[str]],
) -> tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]:
    required: list[tuple[str, str]] = []
    forbidden: list[tuple[str, str]] = []
    for table, privileges in allowed.items():
        for privilege in TABLE_PRIVILEGES:
            target = required if privilege in privileges else forbidden
            target.append((table, privilege))
    return tuple(required), tuple(forbidden)


_RUNTIME_REQUIRED, _RUNTIME_FORBIDDEN = _table_contract(RUNTIME_ALLOWED)
_QUALIFICATION_REQUIRED, _QUALIFICATION_FORBIDDEN = _table_contract(QUALIFICATION_ALLOWED)
ROLE_CONTRACTS = {
    "runtime-controller": DatabaseRoleContract(
        profile="runtime-controller",
        login_role="m1_runtime_controller_login",
        capability_role="m1_runtime_controller_capability",
        required_table_privileges=_RUNTIME_REQUIRED,
        forbidden_table_privileges=_RUNTIME_FORBIDDEN,
        forbidden_function_privileges=(
            GENERAL_INGRESS_FUNCTION,
            FRESHNESS_FUNCTION,
            CERTIFICATE_FUNCTION,
            *HARDENED_TRIGGER_FUNCTIONS,
        ),
        forbidden_sequence_columns=RUNTIME_SEQUENCE_COLUMNS,
    ),
    "qualification-worker": DatabaseRoleContract(
        profile="qualification-worker",
        login_role="m1_qualification_worker_login",
        capability_role="m1_qualification_worker_capability",
        required_table_privileges=_QUALIFICATION_REQUIRED,
        forbidden_table_privileges=_QUALIFICATION_FORBIDDEN,
        required_function_privileges=(FRESHNESS_FUNCTION, CERTIFICATE_FUNCTION),
        forbidden_function_privileges=(
            GENERAL_INGRESS_FUNCTION,
            *HARDENED_TRIGGER_FUNCTIONS,
        ),
        forbidden_sequence_columns=QUALIFICATION_SEQUENCE_COLUMNS,
    ),
}
_CAPABILITY_ROLES = tuple(contract.capability_role for contract in ROLE_CONTRACTS.values())


def verify_daemon_database_role(
    connection_factory: ConnectionFactory,
    profile: str,
    *,
    expected_database: str,
) -> DatabaseRoleVerification:
    """Verify the active daemon DB identity before any daemon mutation."""

    contract = ROLE_CONTRACTS.get(profile)
    if contract is None:
        raise DatabaseRoleContractError("database-role.unknown-profile", profile)
    normalized_database = expected_database.strip()
    if not normalized_database:
        raise DatabaseRoleContractError(
            "database-role.expected-database-missing",
            "POLYARB_DB_EXPECTED_DATABASE",
        )
    try:
        with connection_factory() as connection, connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                (
                    database_name,
                    session_user,
                    current_user,
                    rolsuper,
                    rolcreatedb,
                    rolcreaterole,
                    rolreplication,
                    rolbypassrls,
                ) = _identity_row(cursor)
                if database_name != normalized_database:
                    _fail("database-role.login-mismatch", f"database:{database_name}")
                if session_user != contract.login_role:
                    _fail("database-role.login-mismatch", f"session_user:{session_user}")
                if current_user not in {contract.login_role, contract.capability_role}:
                    _fail("database-role.login-mismatch", f"current_user:{current_user}")
                if any((rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls)):
                    _fail("database-role.unsafe-attribute", contract.login_role)
                _verify_role_attributes(cursor, contract)
                _require_privilege(
                    cursor,
                    "has_database_privilege",
                    "database-role.required-privilege-missing",
                    (session_user, normalized_database, "CONNECT"),
                    f"database:{normalized_database}:CONNECT",
                )
                _require_privilege(
                    cursor,
                    "has_schema_privilege",
                    "database-role.required-privilege-missing",
                    (session_user, "public", "USAGE"),
                    "schema:public:USAGE",
                )
                _require_role(cursor, session_user, contract.capability_role)
                for capability_role in _CAPABILITY_ROLES:
                    if capability_role != contract.capability_role:
                        _reject_role(cursor, session_user, capability_role)
                _reject_unapproved_inherited_role(cursor, contract, session_user)
                for table, privilege in contract.required_table_privileges:
                    _require_table_privilege(cursor, session_user, table, privilege)
                for table, privilege in contract.forbidden_table_privileges:
                    _reject_table_privilege(cursor, session_user, table, privilege)
                for function_signature in contract.required_function_privileges:
                    _require_function_privilege(cursor, session_user, function_signature)
                for function_signature in contract.forbidden_function_privileges:
                    _reject_function_privilege(cursor, session_user, function_signature)
                for table, column in contract.forbidden_sequence_columns:
                    _reject_sequence_privileges(cursor, session_user, table, column)
    except DatabaseRoleContractError:
        raise
    except Exception as exc:
        raise DatabaseRoleContractError(
            "database-role.unavailable",
            contract.profile,
        ) from exc
    return DatabaseRoleVerification(
        profile=contract.profile,
        session_user=contract.login_role,
        capability_role=contract.capability_role,
        database_name=normalized_database,
    )


def _identity_row(cursor: Any) -> tuple[str, str, str, bool, bool, bool, bool, bool]:
    cursor.execute(
        """
        SELECT current_database(), session_user, current_user,
               role.rolsuper, role.rolcreatedb, role.rolcreaterole,
               role.rolreplication, role.rolbypassrls
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = session_user
        """
    )
    row = cursor.fetchone()
    if row is None:
        _fail("database-role.login-mismatch", "session_user")
    values = tuple(_row_value(row, index, name) for index, name in enumerate(_IDENTITY_COLUMNS))
    return (
        str(values[0]),
        str(values[1]),
        str(values[2]),
        bool(values[3]),
        bool(values[4]),
        bool(values[5]),
        bool(values[6]),
        bool(values[7]),
    )


_IDENTITY_COLUMNS = (
    "current_database",
    "session_user",
    "current_user",
    "rolsuper",
    "rolcreatedb",
    "rolcreaterole",
    "rolreplication",
    "rolbypassrls",
)


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, Mapping):
        return row[name]
    return row[index]  # type: ignore[index]


def _scalar_bool(row: object) -> bool:
    if row is None:
        return False
    if isinstance(row, Mapping):
        return bool(next(iter(row.values())))
    return bool(row[0])  # type: ignore[index]


def _role_attributes(cursor: Any, role_name: str, *, missing_reason_code: str) -> _RoleAttributes:
    cursor.execute(
        """
        SELECT role.rolname, role.rolcanlogin, role.rolsuper,
               role.rolcreatedb, role.rolcreaterole, role.rolinherit,
               role.rolreplication, role.rolbypassrls
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = %s
        """,
        (role_name,),
    )
    row = cursor.fetchone()
    if row is None:
        _fail(missing_reason_code, role_name)
    values = tuple(_row_value(row, index, name) for index, name in enumerate(_ROLE_COLUMNS))
    return _RoleAttributes(
        role_name=str(values[0]),
        can_login=bool(values[1]),
        superuser=bool(values[2]),
        create_db=bool(values[3]),
        create_role=bool(values[4]),
        inherits=bool(values[5]),
        replication=bool(values[6]),
        bypass_rls=bool(values[7]),
    )


_ROLE_COLUMNS = (
    "rolname",
    "rolcanlogin",
    "rolsuper",
    "rolcreatedb",
    "rolcreaterole",
    "rolinherit",
    "rolreplication",
    "rolbypassrls",
)


def _verify_role_attributes(cursor: Any, contract: DatabaseRoleContract) -> None:
    login = _role_attributes(
        cursor,
        contract.login_role,
        missing_reason_code="database-role.login-attribute",
    )
    capability = _role_attributes(
        cursor,
        contract.capability_role,
        missing_reason_code="database-role.capability-attribute",
    )
    if login.role_name != contract.login_role:
        _fail("database-role.login-attribute", login.role_name)
    if not login.can_login or not login.inherits:
        _fail("database-role.login-attribute", contract.login_role)
    if any(
        (
            login.superuser,
            login.create_db,
            login.create_role,
            login.replication,
            login.bypass_rls,
        )
    ):
        _fail("database-role.login-attribute", contract.login_role)
    if capability.role_name != contract.capability_role:
        _fail("database-role.capability-attribute", capability.role_name)
    if capability.can_login or capability.inherits:
        _fail("database-role.capability-attribute", contract.capability_role)
    if any(
        (
            capability.superuser,
            capability.create_db,
            capability.create_role,
            capability.replication,
            capability.bypass_rls,
        )
    ):
        _fail("database-role.capability-attribute", contract.capability_role)


def _check_privilege(cursor: Any, function_name: str, params: tuple[str, ...]) -> bool:
    cursor.execute(f"SELECT {function_name}(%s, %s, %s)", params)
    return _scalar_bool(cursor.fetchone())


def _require_privilege(
    cursor: Any,
    function_name: str,
    reason_code: str,
    params: tuple[str, str, str],
    object_identifier: str,
) -> None:
    if not _check_privilege(cursor, function_name, params):
        _fail(reason_code, object_identifier)


def _require_role(cursor: Any, session_user: str, role: str) -> None:
    cursor.execute("SELECT pg_has_role(%s, %s, 'USAGE')", (session_user, role))
    if not _scalar_bool(cursor.fetchone()):
        _fail("database-role.capability-missing", role)


def _reject_role(cursor: Any, session_user: str, role: str) -> None:
    cursor.execute(
        "SELECT pg_has_role(%s, %s, 'USAGE') OR pg_has_role(%s, %s, 'MEMBER')",
        (session_user, role, session_user, role),
    )
    if _scalar_bool(cursor.fetchone()):
        _fail("database-role.cross-capability", role)


def _reject_unapproved_inherited_role(
    cursor: Any,
    contract: DatabaseRoleContract,
    session_user: str,
) -> None:
    cursor.execute(
        """
        SELECT inherited_role.rolname
        FROM pg_catalog.pg_roles AS inherited_role
        WHERE inherited_role.rolname NOT IN (%s, %s)
          AND (
              pg_has_role(%s, inherited_role.rolname, 'USAGE')
              OR pg_has_role(%s, inherited_role.rolname, 'MEMBER')
          )
        ORDER BY inherited_role.rolname
        LIMIT 1
        """,
        (contract.login_role, contract.capability_role, session_user, session_user),
    )
    row = cursor.fetchone()
    if row is not None:
        _fail("database-role.cross-capability", str(_row_value(row, 0, "rolname")))


def _require_table_privilege(
    cursor: Any,
    session_user: str,
    table: str,
    privilege: str,
) -> None:
    object_identifier = f"public.{table}:{privilege}"
    if not _check_privilege(
        cursor,
        "has_table_privilege",
        (session_user, f"public.{table}", privilege),
    ):
        _fail("database-role.required-privilege-missing", object_identifier)


def _reject_table_privilege(
    cursor: Any,
    session_user: str,
    table: str,
    privilege: str,
) -> None:
    object_identifier = f"public.{table}:{privilege}"
    if _check_privilege(
        cursor,
        "has_table_privilege",
        (session_user, f"public.{table}", privilege),
    ):
        _fail("database-role.forbidden-privilege-present", object_identifier)


def _require_function_privilege(cursor: Any, session_user: str, function_signature: str) -> None:
    if not _check_privilege(
        cursor,
        "has_function_privilege",
        (session_user, function_signature, "EXECUTE"),
    ):
        _fail("database-role.required-privilege-missing", f"{function_signature}:EXECUTE")


def _reject_function_privilege(cursor: Any, session_user: str, function_signature: str) -> None:
    if _check_privilege(
        cursor,
        "has_function_privilege",
        (session_user, function_signature, "EXECUTE"),
    ):
        _fail("database-role.forbidden-privilege-present", f"{function_signature}:EXECUTE")


def _reject_sequence_privileges(
    cursor: Any,
    session_user: str,
    table: str,
    column: str,
) -> None:
    cursor.execute(
        "SELECT pg_get_serial_sequence(%s, %s)",
        (f"public.{table}", column),
    )
    row = cursor.fetchone()
    if row is None:
        return
    sequence = _row_value(row, 0, "pg_get_serial_sequence")
    if sequence is None or str(sequence) == "":
        return
    sequence_name = str(sequence)
    for privilege in SEQUENCE_PRIVILEGES:
        if _check_privilege(
            cursor,
            "has_sequence_privilege",
            (session_user, sequence_name, privilege),
        ):
            _fail("database-role.forbidden-privilege-present", f"{sequence_name}:{privilege}")


def _fail(reason_code: str, object_identifier: str) -> None:
    raise DatabaseRoleContractError(reason_code, object_identifier)


__all__ = [
    "ConnectionFactory",
    "DatabaseRoleContract",
    "DatabaseRoleContractError",
    "DatabaseRoleVerification",
    "SEQUENCE_PRIVILEGES",
    "TABLE_PRIVILEGES",
    "verify_daemon_database_role",
]
