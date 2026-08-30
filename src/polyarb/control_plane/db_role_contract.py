"""Read-only fail-closed database identity contract for daemon roles."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from threading import Event, Timer
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import ConnectionPool

from .db_deadlines import (
    CONTROL_PLANE_DB_POLICY,
    CONTROL_PLANE_DB_POOL_DEFAULT_MAX_SIZE,
    CONTROL_PLANE_DB_POOL_MAX_IDLE_SECONDS,
    CONTROL_PLANE_DB_POOL_MAX_SIZE,
    CONTROL_PLANE_DB_POOL_MAX_WAITING,
    DatabaseDeadlinePolicy,
)

ConnectionFactory = Callable[[], AbstractContextManager[psycopg.Connection[Any]]]
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
CONTROLLED_SEARCH_PATH = ("pg_catalog", "public")
CONTROLLED_CONNECTION_OPTIONS = (
    "-csearch_path=pg_catalog,public " + CONTROL_PLANE_DB_POLICY.connection_options
)
_BOOTSTRAP_TIMEOUT_SECONDS = CONTROL_PLANE_DB_POLICY.statement_timeout_ms / 1_000


def _canonical_timeout_setting(milliseconds: int) -> str:
    return f"{milliseconds // 1_000}s" if milliseconds % 1_000 == 0 else f"{milliseconds}ms"


# TEMPORARY is intentionally allowed. PostgreSQL grants it to PUBLIC by default;
# revoking it globally would change the original four applications. Namespace
# safety instead comes from qualified daemon SQL plus this controlled path.
TEMPORARY_POSTURE = "allowed"
SUPABASE_CREATOR_MEMBERSHIP = ("postgres", True, False, False)

RUNTIME_ALLOWED = {
    "m1_runtime_controller_leases": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_runtime_observe_decisions": frozenset({"SELECT", "INSERT"}),
    "m1_job_runtime_state": frozenset({"SELECT", "UPDATE"}),
    "m1_jobs": frozenset({"SELECT", "UPDATE"}),
    "m1_job_circuits": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_job_attempts": frozenset({"SELECT", "UPDATE"}),
    "m1_recovery_target_budgets": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_recovery_actions": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_job_runtime_events": frozenset({"SELECT", "INSERT"}),
    "m1_incidents": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_incident_events": frozenset({"SELECT", "INSERT"}),
    # INSERT .. ON CONFLICT uses the unique incident/channel key and therefore
    # also requires SELECT even though recovery never reads alert payload rows.
    "m1_alert_outbox": frozenset({"SELECT", "INSERT"}),
}
QUALIFICATION_ALLOWED = {
    "m1_qualification_ingress_ledger": frozenset({"SELECT"}),
    "m1_qualification_source_cursors": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_qualification_epochs": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_qualification_epoch_facts": frozenset({"SELECT", "INSERT"}),
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
QUALIFICATION_SEQUENCE_COLUMNS = (("m1_qualification_ingress_ledger", "ingest_seq"),)

FRESHNESS_FUNCTION = "public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)"
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
    forbidden_sequence_schemas: tuple[str, ...] = ()


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
    settings: tuple[str, ...]


class DatabaseRoleContractError(RuntimeError):
    """Closed role-contract refusal with no database exception detail."""

    def __init__(self, reason_code: str, object_identifier: str) -> None:
        self.reason_code = reason_code
        self.object_identifier = object_identifier
        super().__init__(f"{reason_code}: {object_identifier}")


class ScopedConnectionFactory:
    """Own a process-local pool and preserve the existing callable contract."""

    def __init__(self, pool: ConnectionPool[Any]) -> None:
        self._pool = pool

    def __call__(self) -> AbstractContextManager[psycopg.Connection[Any]]:
        return self._pool.connection()

    def close(self) -> None:
        self._pool.close()

    def pool_stats(self) -> dict[str, int]:
        return self._pool.get_stats()

    def __del__(self) -> None:
        # Short-lived CLI/test owners must not strand idle PostgreSQL sessions.
        # Production services close at process teardown; this is the last-resort
        # ownership boundary if their normal shutdown path is interrupted.
        try:
            self.close()
        except Exception:
            pass


def scoped_connection_factory(
    dsn: str,
    *,
    deadline_policy: DatabaseDeadlinePolicy = CONTROL_PLANE_DB_POLICY,
    pool_max_size: int | None = None,
) -> ScopedConnectionFactory:
    """Return one lazy bounded pool with a non-overridable application namespace."""

    _reject_dsn_namespace_override(dsn)
    if pool_max_size is None:
        raw_pool_max_size = os.environ.get(
            "POLYARB_DB_POOL_MAX_SIZE", str(CONTROL_PLANE_DB_POOL_DEFAULT_MAX_SIZE)
        )
        try:
            pool_max_size = int(raw_pool_max_size)
        except ValueError as error:
            raise ValueError("POLYARB_DB_POOL_MAX_SIZE must be an integer") from error
    if not 1 <= pool_max_size <= CONTROL_PLANE_DB_POOL_MAX_SIZE:
        raise ValueError(
            f"pool_max_size must be between 1 and {CONTROL_PLANE_DB_POOL_MAX_SIZE}"
        )
    connection_options = "-csearch_path=pg_catalog,public " + deadline_policy.connection_options
    pool: ConnectionPool[Any] = ConnectionPool(
        dsn,
        kwargs={
            "connect_timeout": deadline_policy.connect_timeout_seconds,
            "options": connection_options,
        },
        min_size=0,
        max_size=pool_max_size,
        open=True,
        configure=lambda connection: _bootstrap_scoped_session(
            connection,
            deadline_policy=deadline_policy,
        ),
        timeout=deadline_policy.connect_timeout_seconds,
        max_waiting=CONTROL_PLANE_DB_POOL_MAX_WAITING,
        max_idle=CONTROL_PLANE_DB_POOL_MAX_IDLE_SECONDS,
        reconnect_timeout=deadline_policy.connect_timeout_seconds,
    )
    return ScopedConnectionFactory(pool)


def _bootstrap_scoped_session(
    connection: psycopg.Connection[Any],
    *,
    deadline_policy: DatabaseDeadlinePolicy = CONTROL_PLANE_DB_POLICY,
) -> None:
    """Reassert and verify policy when a session pooler drops startup options."""
    completed = Event()
    timed_out = Event()
    session_settings = (
        ",".join(CONTROLLED_SEARCH_PATH),
        deadline_policy.statement_setting,
        deadline_policy.lock_setting,
    )
    expected_session_settings = (
        session_settings[0],
        _canonical_timeout_setting(deadline_policy.statement_timeout_ms),
        _canonical_timeout_setting(deadline_policy.lock_timeout_ms),
    )
    bootstrap_timeout_seconds = (
        _BOOTSTRAP_TIMEOUT_SECONDS
        if deadline_policy is CONTROL_PLANE_DB_POLICY
        else deadline_policy.statement_timeout_ms / 1_000
    )

    def cancel_bootstrap() -> None:
        if completed.is_set():
            return
        timed_out.set()
        try:
            connection.cancel_safe(timeout=deadline_policy.connect_timeout_seconds)
        except Exception:
            # The caller still closes the connection and fails closed. No
            # provider exception detail may replace the stable contract error.
            pass

    timer = Timer(bootstrap_timeout_seconds, cancel_bootstrap)
    timer.daemon = True
    original_autocommit = connection.autocommit
    connection.autocommit = True
    timer.start()
    try:
        result = connection.execute(
            """
            WITH configured AS MATERIALIZED (
                SELECT pg_catalog.set_config('search_path', %s, false) AS search_path,
                       pg_catalog.set_config('statement_timeout', %s, false)
                           AS statement_timeout,
                       pg_catalog.set_config('lock_timeout', %s, false) AS lock_timeout
            )
            SELECT search_path, statement_timeout, lock_timeout,
                   pg_catalog.current_schemas(false)
            FROM configured
            """,
            session_settings,
        )
        row = result.fetchone()
    except Exception as error:
        if timed_out.is_set():
            raise DatabaseRoleContractError("database-role.bootstrap-timeout", "session") from error
        raise
    finally:
        completed.set()
        timer.cancel()
    if timed_out.is_set():
        raise DatabaseRoleContractError("database-role.bootstrap-timeout", "session")
    if (
        row is None
        or tuple(row[:3]) != expected_session_settings
        or tuple(row[3]) != CONTROLLED_SEARCH_PATH
    ):
        raise DatabaseRoleContractError("database-role.bootstrap-unsafe", "session")
    connection.autocommit = original_autocommit


def _reject_dsn_namespace_override(dsn: str) -> None:
    normalized = dsn.strip()
    if not normalized:
        raise DatabaseRoleContractError("database-role.dsn-invalid", "connection")
    override = False
    try:
        split = urlsplit(normalized)
        if split.scheme:
            override = any(
                key.lower() in {"options", "search_path"}
                for key, _value in parse_qsl(split.query, keep_blank_values=True)
            )
        else:
            override = bool(re.search(r"(?:^|\s)(?:options|search_path)\s*=", normalized, re.I))
        parsed = conninfo_to_dict(normalized)
    except Exception as exc:
        if re.search(r"(?:[?&]|^|\s)(?:options|search_path)(?:=|%3d)", normalized, re.I):
            override = True
        if not override:
            raise DatabaseRoleContractError(
                "database-role.dsn-invalid",
                "connection",
            ) from exc
        parsed = {}
    if override or any(key.lower() in {"options", "search_path"} for key in parsed):
        raise DatabaseRoleContractError("database-role.dsn-override", "namespace")


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
        forbidden_sequence_schemas=("public",),
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
                _verify_namespace_settings(cursor, contract, normalized_database)
                _require_role(cursor, session_user, contract.capability_role)
                for capability_role in _CAPABILITY_ROLES:
                    if capability_role != contract.capability_role:
                        _reject_role(cursor, session_user, capability_role)
                _reject_unapproved_inherited_role(cursor, contract, session_user)
                _verify_exact_capability_membership(cursor, contract)
                verify_effective_database_role_authority(
                    cursor,
                    contract,
                    subject_role=session_user,
                    expected_database=normalized_database,
                )
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
               role.rolreplication, role.rolbypassrls, role.rolconfig
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
        settings=_settings_tuple(values[8]),
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
    "rolconfig",
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
    if _has_search_path_setting(login.settings):
        _fail("database-role.namespace-unsafe", contract.login_role)
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
    if _has_search_path_setting(capability.settings):
        _fail("database-role.namespace-unsafe", contract.capability_role)


def _has_search_path_setting(settings: tuple[str, ...]) -> bool:
    return any(setting.partition("=")[0].strip().lower() == "search_path" for setting in settings)


def _settings_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(setting) for setting in value)


def _verify_namespace_settings(
    cursor: Any,
    contract: DatabaseRoleContract,
    expected_database: str,
) -> None:
    cursor.execute("SELECT current_setting('search_path'), current_schemas(false)")
    row = cursor.fetchone()
    if row is None:
        _fail("database-role.namespace-unsafe", "active-search-path")
    active_setting = str(_row_value(row, 0, "current_setting"))
    active_schemas_value = _row_value(row, 1, "current_schemas")
    if isinstance(active_schemas_value, str):
        active_schemas = tuple(
            part.strip().strip('"')
            for part in active_schemas_value.strip("{}").split(",")
            if part.strip()
        )
    else:
        active_schemas = tuple(str(value) for value in active_schemas_value)  # type: ignore[union-attr]
    configured = tuple(
        part.strip().strip('"') for part in active_setting.split(",") if part.strip()
    )
    if configured != CONTROLLED_SEARCH_PATH or active_schemas != CONTROLLED_SEARCH_PATH:
        _fail("database-role.namespace-unsafe", "active-search-path")

    cursor.execute(
        """
        SELECT setting.setconfig
        FROM pg_catalog.pg_db_role_setting AS setting
        LEFT JOIN pg_catalog.pg_roles AS role ON role.oid = setting.setrole
        LEFT JOIN pg_catalog.pg_database AS database ON database.oid = setting.setdatabase
        WHERE (setting.setrole = 0 OR role.rolname IN (%s, %s))
          AND (setting.setdatabase = 0 OR database.datname = %s)
        ORDER BY setting.setdatabase, setting.setrole
        """,
        (contract.login_role, contract.capability_role, expected_database),
    )
    for setting_row in cursor.fetchall():
        values = _settings_tuple(_row_value(setting_row, 0, "setconfig"))
        if _has_search_path_setting(values):
            _fail("database-role.namespace-unsafe", "configured-search-path")


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


def _verify_exact_capability_membership(
    cursor: Any,
    contract: DatabaseRoleContract,
) -> None:
    cursor.execute(
        """
        SELECT member.rolname, membership.admin_option,
               membership.inherit_option, membership.set_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        WHERE granted.rolname = %s
        ORDER BY member.rolname
        """,
        (contract.capability_role,),
    )
    incoming = frozenset(
        (
            str(_row_value(row, 0, "rolname")),
            bool(_row_value(row, 1, "admin_option")),
            bool(_row_value(row, 2, "inherit_option")),
            bool(_row_value(row, 3, "set_option")),
        )
        for row in cursor.fetchall()
    )
    required_incoming = frozenset({(contract.login_role, False, True, True)})
    allowed_incoming = (
        required_incoming,
        required_incoming | frozenset({SUPABASE_CREATOR_MEMBERSHIP}),
    )
    if incoming not in allowed_incoming:
        _fail("database-role.forbidden-privilege-present", "capability-membership")
    cursor.execute(
        """
        SELECT granted.rolname, membership.admin_option,
               membership.inherit_option, membership.set_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        WHERE member.rolname = %s
        ORDER BY granted.rolname
        """,
        (contract.capability_role,),
    )
    if cursor.fetchone() is not None:
        _fail("database-role.forbidden-privilege-present", "capability-membership")


def verify_effective_database_role_authority(
    cursor: Any,
    contract: DatabaseRoleContract,
    *,
    subject_role: str,
    expected_database: str,
) -> None:
    """Compare all effective application authority to a closed allowlist.

    TEMPORARY remains explicitly allowed for compatibility with the original
    applications; CREATE and every non-system namespace capability are closed.
    """

    _require_privilege(
        cursor,
        "has_database_privilege",
        "database-role.required-privilege-missing",
        (subject_role, expected_database, "CONNECT"),
        f"database:{expected_database}:CONNECT",
    )
    if _check_privilege(
        cursor,
        "has_database_privilege",
        (subject_role, expected_database, "CREATE"),
    ):
        _fail(
            "database-role.forbidden-privilege-present",
            f"database:{expected_database}:CREATE",
        )
    # TEMPORARY is an explicit compatibility allowance, not a requirement.
    _check_privilege(
        cursor,
        "has_database_privilege",
        (subject_role, expected_database, "TEMPORARY"),
    )
    _verify_application_schema_privileges(cursor, subject_role)
    _verify_public_relation_privileges(cursor, contract, subject_role)
    _verify_public_sequence_privileges(cursor, subject_role)
    _verify_security_definer_execute(cursor, contract, subject_role)
    _reject_public_object_ownership(cursor, subject_role)


def _verify_application_schema_privileges(cursor: Any, subject_role: str) -> None:
    cursor.execute(
        """
        SELECT namespace.nspname,
               pg_catalog.has_schema_privilege(%s, namespace.oid, 'USAGE') AS can_usage,
               pg_catalog.has_schema_privilege(%s, namespace.oid, 'CREATE') AS can_create
        FROM pg_catalog.pg_namespace AS namespace
        WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
          AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
        ORDER BY namespace.nspname
        """,
        (subject_role, subject_role),
    )
    seen_public = False
    for row in cursor.fetchall():
        schema_name = str(_row_value(row, 0, "nspname"))
        if schema_name == "public":
            seen_public = True
        for privilege, effective in (
            ("USAGE", bool(_row_value(row, 1, "can_usage"))),
            ("CREATE", bool(_row_value(row, 2, "can_create"))),
        ):
            expected = schema_name == "public" and privilege == "USAGE"
            if effective and not expected:
                _fail("database-role.forbidden-privilege-present", "application-schema")
            if not effective and expected:
                _fail(
                    "database-role.required-privilege-missing",
                    "schema:public:USAGE",
                )
    if not seen_public:
        _fail("database-role.required-privilege-missing", "schema:public")


def _allowed_tables(contract: DatabaseRoleContract) -> Mapping[str, frozenset[str]]:
    if contract.profile == "runtime-controller":
        return RUNTIME_ALLOWED
    return QUALIFICATION_ALLOWED


def _verify_public_relation_privileges(
    cursor: Any,
    contract: DatabaseRoleContract,
    subject_role: str,
) -> None:
    cursor.execute(
        """
        SELECT pg_catalog.format('%%I.%%I', namespace.nspname, relation.relname)
                   AS relation_name,
               relation.relname,
               namespace.nspname,
               pg_catalog.has_table_privilege(%s, relation.oid, 'SELECT') AS can_select,
               pg_catalog.has_table_privilege(%s, relation.oid, 'INSERT') AS can_insert,
               pg_catalog.has_table_privilege(%s, relation.oid, 'UPDATE') AS can_update,
               pg_catalog.has_table_privilege(%s, relation.oid, 'DELETE') AS can_delete,
               pg_catalog.has_table_privilege(%s, relation.oid, 'TRUNCATE') AS can_truncate,
               pg_catalog.has_table_privilege(%s, relation.oid, 'REFERENCES') AS can_references,
               pg_catalog.has_table_privilege(%s, relation.oid, 'TRIGGER') AS can_trigger
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
          AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
          AND pg_catalog.has_schema_privilege(%s, namespace.oid, 'USAGE')
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
        ORDER BY namespace.nspname, relation.relname
        """,
        (subject_role,) * 8,
    )
    allowed_tables = _allowed_tables(contract)
    seen: set[str] = set()
    for row in cursor.fetchall():
        table_name = str(_row_value(row, 1, "relname"))
        schema_name = str(_row_value(row, 2, "nspname"))
        if schema_name == "public":
            seen.add(table_name)
        expected = (
            allowed_tables.get(table_name, frozenset()) if schema_name == "public" else frozenset()
        )
        for index, privilege in enumerate(TABLE_PRIVILEGES, start=3):
            effective = bool(_row_value(row, index, f"can_{privilege.lower()}"))
            if effective and privilege not in expected:
                _fail("database-role.forbidden-privilege-present", "application-relation")
            if not effective and privilege in expected:
                _fail(
                    "database-role.required-privilege-missing",
                    f"public.{table_name}:{privilege}",
                )
    for table_name, expected in allowed_tables.items():
        if expected and table_name not in seen:
            _fail("database-role.required-privilege-missing", f"public.{table_name}")


def _verify_public_sequence_privileges(cursor: Any, subject_role: str) -> None:
    cursor.execute(
        """
        SELECT pg_catalog.format('%%I.%%I', namespace.nspname, sequence.relname)
                   AS sequence_name,
               pg_catalog.has_sequence_privilege(%s, sequence.oid, 'USAGE') AS can_usage,
               pg_catalog.has_sequence_privilege(%s, sequence.oid, 'SELECT') AS can_select,
               pg_catalog.has_sequence_privilege(%s, sequence.oid, 'UPDATE') AS can_update
        FROM pg_catalog.pg_class AS sequence
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = sequence.relnamespace
        WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
          AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
          AND pg_catalog.has_schema_privilege(%s, namespace.oid, 'USAGE')
          AND sequence.relkind = 'S'
        ORDER BY namespace.nspname, sequence.relname
        """,
        (subject_role,) * 4,
    )
    for row in cursor.fetchall():
        for index, privilege in enumerate(SEQUENCE_PRIVILEGES, start=1):
            if bool(_row_value(row, index, f"can_{privilege.lower()}")):
                _fail("database-role.forbidden-privilege-present", "application-sequence")


def _verify_security_definer_execute(
    cursor: Any,
    contract: DatabaseRoleContract,
    subject_role: str,
) -> None:
    cursor.execute(
        """
        SELECT pg_catalog.format(
                   '%%I.%%I(%%s)', namespace.nspname, routine.proname,
                   pg_catalog.oidvectortypes(routine.proargtypes)
               ) AS function_signature,
               pg_catalog.has_function_privilege(%s, routine.oid, 'EXECUTE') AS can_execute
        FROM pg_catalog.pg_proc AS routine
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = routine.pronamespace
        WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
          AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
          AND pg_catalog.has_schema_privilege(%s, namespace.oid, 'USAGE')
          AND routine.prosecdef
        ORDER BY function_signature
        """,
        (subject_role, subject_role),
    )
    expected = {
        _normalize_function_signature(signature)
        for signature in contract.required_function_privileges
    }
    seen: set[str] = set()
    for row in cursor.fetchall():
        function_signature = str(_row_value(row, 0, "function_signature"))
        normalized = _normalize_function_signature(function_signature)
        seen.add(normalized)
        effective = bool(_row_value(row, 1, "can_execute"))
        if effective and normalized not in expected:
            _fail(
                "database-role.forbidden-privilege-present",
                "security-definer-routine",
            )
        if not effective and normalized in expected:
            _fail(
                "database-role.required-privilege-missing",
                "security-definer-routine",
            )
    if expected - seen:
        _fail("database-role.required-privilege-missing", "security-definer-routine")


def _normalize_function_signature(signature: str) -> str:
    canonical = signature.replace("timestamp with time zone", "timestamptz")
    canonical = canonical.replace("timestamp without time zone", "timestamp")
    return "".join(canonical.split())


def _reject_public_object_ownership(cursor: Any, subject_role: str) -> None:
    cursor.execute(
        """
        SELECT owned_object
        FROM (
            SELECT pg_catalog.format('schema:%%I', namespace.nspname) AS owned_object
            FROM pg_catalog.pg_namespace AS namespace
            JOIN pg_catalog.pg_roles AS owner ON owner.oid = namespace.nspowner
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
              AND owner.rolname = %s
            UNION ALL
            SELECT pg_catalog.format('relation:%%I.%%I', namespace.nspname, relation.relname)
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            JOIN pg_catalog.pg_roles AS owner ON owner.oid = relation.relowner
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
              AND owner.rolname = %s
            UNION ALL
            SELECT pg_catalog.format('routine:%%I.%%I', namespace.nspname, routine.proname)
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
            JOIN pg_catalog.pg_roles AS owner ON owner.oid = routine.proowner
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
              AND owner.rolname = %s
        ) AS owned
        ORDER BY owned_object
        LIMIT 1
        """,
        (subject_role, subject_role, subject_role),
    )
    if cursor.fetchone() is not None:
        _fail("database-role.forbidden-privilege-present", "application-object-owner")


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


def _reject_schema_sequence_privileges(cursor: Any, session_user: str, schema: str) -> None:
    cursor.execute(
        """
        SELECT pg_catalog.format('%%I.%%I', namespace.nspname, sequence.relname)
               AS sequence_name
        FROM pg_catalog.pg_class AS sequence
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = sequence.relnamespace
        WHERE sequence.relkind = 'S'
          AND namespace.nspname = %s
        ORDER BY namespace.nspname, sequence.relname
        """,
        (schema,),
    )
    for row in cursor.fetchall():
        sequence_name = str(_row_value(row, 0, "sequence_name"))
        for privilege in SEQUENCE_PRIVILEGES:
            if _check_privilege(
                cursor,
                "has_sequence_privilege",
                (session_user, sequence_name, privilege),
            ):
                _fail(
                    "database-role.forbidden-privilege-present",
                    f"{sequence_name}:{privilege}",
                )


def _fail(reason_code: str, object_identifier: str) -> None:
    raise DatabaseRoleContractError(reason_code, object_identifier)


__all__ = [
    "CONTROLLED_CONNECTION_OPTIONS",
    "CONTROLLED_SEARCH_PATH",
    "ConnectionFactory",
    "DatabaseRoleContract",
    "DatabaseRoleContractError",
    "DatabaseRoleVerification",
    "SEQUENCE_PRIVILEGES",
    "TABLE_PRIVILEGES",
    "TEMPORARY_POSTURE",
    "scoped_connection_factory",
    "verify_effective_database_role_authority",
    "verify_daemon_database_role",
]
