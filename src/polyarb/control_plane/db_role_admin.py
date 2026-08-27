"""Safe operator tooling for scoped control-plane database login roles."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, NoReturn

from psycopg import sql

from polyarb.control_plane.db_role_contract import (
    ROLE_CONTRACTS,
    ConnectionFactory,
    DatabaseRoleContractError,
    DatabaseRoleVerification,
    scoped_connection_factory,
    verify_daemon_database_role,
    verify_effective_database_role_authority,
)

RUNTIME_PROFILE = "runtime-controller"
QUALIFICATION_PROFILE = "qualification-worker"
EXPECTED_REVISION = "026"
ADMIN_DSN_ENV = "POLYARB_CONTROL_PLANE_DB_ADMIN_DSN"
RUNTIME_PASSWORD_ENV = "POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD"
QUALIFICATION_PASSWORD_ENV = "POLYARB_QUALIFICATION_WORKER_DB_PASSWORD"
RUNTIME_DSN_ENV = "POLYARB_SUPABASE_DB_DSN"
QUALIFICATION_DSN_ENV = "POLYARB_QUALIFICATION_DB_DSN"
PROFILE_DSN_ENV = {
    RUNTIME_PROFILE: RUNTIME_DSN_ENV,
    QUALIFICATION_PROFILE: QUALIFICATION_DSN_ENV,
}
LOGIN_ROLE_CAPABILITY = {
    ROLE_CONTRACTS[RUNTIME_PROFILE].login_role: ROLE_CONTRACTS[RUNTIME_PROFILE].capability_role,
    ROLE_CONTRACTS[QUALIFICATION_PROFILE].login_role: ROLE_CONTRACTS[
        QUALIFICATION_PROFILE
    ].capability_role,
}
LOGIN_ROLES = tuple(LOGIN_ROLE_CAPABILITY)
CAPABILITY_ROLES = tuple(LOGIN_ROLE_CAPABILITY.values())
ALL_ROLES = (*CAPABILITY_ROLES, *LOGIN_ROLES)
SUPABASE_CREATOR_MEMBERSHIP = ("postgres", True, False, False)


@dataclass(frozen=True, slots=True)
class RoleAttributes:
    role_name: str
    can_login: bool
    superuser: bool
    create_db: bool
    create_role: bool
    inherits: bool
    replication: bool
    bypass_rls: bool
    settings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AdminRoleSnapshot:
    database_name: str
    revision: str
    roles: Mapping[str, RoleAttributes]
    memberships: Mapping[str, frozenset[str]]
    incoming_members: Mapping[str, frozenset[str]] = field(default_factory=dict)
    owned_objects: Mapping[str, frozenset[str]] = field(default_factory=dict)
    direct_privileges: Mapping[str, frozenset[str]] = field(default_factory=dict)
    membership_options: Mapping[str, frozenset[tuple[str, bool, bool, bool]]] = field(
        default_factory=dict
    )
    incoming_membership_options: Mapping[str, frozenset[tuple[str, bool, bool, bool]]] = field(
        default_factory=dict
    )
    configured_search_paths: frozenset[str] = frozenset()


class DatabaseRoleAdminError(RuntimeError):
    """Fail-closed admin refusal with no SQL, DSN, or password detail."""

    def __init__(self, reason_code: str, object_identifier: str) -> None:
        self.reason_code = reason_code
        self.object_identifier = object_identifier
        super().__init__(f"{reason_code}: {object_identifier}")


def preflight_capability_roles(
    connection_factory: ConnectionFactory,
    *,
    expected_database: str,
) -> Mapping[str, object]:
    with connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        snapshot = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(snapshot, expected_database, EXPECTED_REVISION)
        _require_capability_roles_safe(snapshot)
        _require_login_roles_safe(snapshot)
        _require_effective_capability_authority(cursor, snapshot)
    return {"database": expected_database, "status": "ready"}


def provision_login_roles(
    connection_factory: ConnectionFactory,
    *,
    expected_database: str,
    runtime_password: str,
    qualification_password: str,
) -> Mapping[str, object]:
    _require_independent_passwords(runtime_password, qualification_password)
    with connection_factory() as connection, connection.cursor() as cursor:
        snapshot = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(snapshot, expected_database, EXPECTED_REVISION)
        _require_capability_roles_safe(snapshot)
        _require_login_roles_safe(snapshot)
        _require_effective_capability_authority(cursor, snapshot)
        _create_or_rotate_login(
            cursor,
            login_role=ROLE_CONTRACTS[RUNTIME_PROFILE].login_role,
            capability_role=ROLE_CONTRACTS[RUNTIME_PROFILE].capability_role,
            password=runtime_password,
        )
        _create_or_rotate_login(
            cursor,
            login_role=ROLE_CONTRACTS[QUALIFICATION_PROFILE].login_role,
            capability_role=ROLE_CONTRACTS[QUALIFICATION_PROFILE].capability_role,
            password=qualification_password,
        )
        provisioned = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(provisioned, expected_database, EXPECTED_REVISION)
        _require_capability_roles_safe(provisioned, require_provisioned=True)
        _require_login_roles_safe(provisioned)
        _require_effective_capability_authority(cursor, provisioned)
        connection.commit()
    return {"database": expected_database, "status": "provisioned"}


def verify_login_role(
    connection_factory: ConnectionFactory,
    profile: str,
    *,
    expected_database: str,
) -> Mapping[str, object]:
    verification = verify_daemon_database_role(
        connection_factory,
        profile,
        expected_database=expected_database,
    )
    return _verification_mapping(verification)


def disable_login_roles(
    connection_factory: ConnectionFactory,
    *,
    expected_database: str,
) -> Mapping[str, object]:
    with connection_factory() as connection, connection.cursor() as cursor:
        snapshot = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(snapshot, expected_database, EXPECTED_REVISION)
        _require_capability_roles_safe(snapshot)
        _require_login_roles_safe(snapshot)
        _require_login_roles_present(snapshot)
        _require_effective_capability_authority(cursor, snapshot)
        for role_name in LOGIN_ROLES:
            cursor.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(role_name)))
        connection.commit()
    return {"database": expected_database, "status": "disabled"}


def _read_admin_role_snapshot(cursor: Any) -> AdminRoleSnapshot:
    cursor.execute("SELECT current_database()")
    database_row = cursor.fetchone()
    if database_row is None:
        _fail("database-role-admin.database-unavailable", "current_database")

    cursor.execute("SELECT version_num FROM public.alembic_version")
    revision_row = cursor.fetchone()
    if revision_row is None:
        _fail("database-role-admin.revision-missing", "alembic_version")

    cursor.execute(
        """
        SELECT role.rolname, role.rolcanlogin, role.rolsuper,
               role.rolcreatedb, role.rolcreaterole, role.rolinherit,
               role.rolreplication, role.rolbypassrls, role.rolconfig
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = ANY(%s)
        ORDER BY role.rolname
        """,
        (list(ALL_ROLES),),
    )
    roles = {
        str(_row_value(row, 0, "rolname")): _role_attributes_from_row(row)
        for row in cursor.fetchall()
    }

    cursor.execute(
        """
        SELECT member.rolname AS member_role, granted.rolname AS granted_role,
               membership.admin_option, membership.inherit_option,
               membership.set_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s)
        ORDER BY member.rolname, granted.rolname
        """,
        (list(ALL_ROLES), list(ALL_ROLES)),
    )
    mutable_memberships: dict[str, set[str]] = {role: set() for role in ALL_ROLES}
    mutable_incoming: dict[str, set[str]] = {role: set() for role in ALL_ROLES}
    mutable_membership_options: dict[str, set[tuple[str, bool, bool, bool]]] = {
        role: set() for role in ALL_ROLES
    }
    mutable_incoming_options: dict[str, set[tuple[str, bool, bool, bool]]] = {
        role: set() for role in ALL_ROLES
    }
    for row in cursor.fetchall():
        member_role = str(_row_value(row, 0, "member_role"))
        granted_role = str(_row_value(row, 1, "granted_role"))
        mutable_memberships.setdefault(member_role, set()).add(granted_role)
        mutable_incoming.setdefault(granted_role, set()).add(member_role)
        admin_option = bool(_row_value(row, 2, "admin_option"))
        inherit_option = bool(_row_value(row, 3, "inherit_option"))
        set_option = bool(_row_value(row, 4, "set_option"))
        mutable_membership_options.setdefault(member_role, set()).add(
            (granted_role, admin_option, inherit_option, set_option)
        )
        mutable_incoming_options.setdefault(granted_role, set()).add(
            (member_role, admin_option, inherit_option, set_option)
        )
    memberships = {role: frozenset(grants) for role, grants in mutable_memberships.items()}
    cursor.execute(
        """
        SELECT owner.rolname AS owner_role, owned_object
        FROM (
            SELECT namespace.nspowner AS owner_oid,
                   pg_catalog.format('schema:%%I', namespace.nspname) AS owned_object
            FROM pg_catalog.pg_namespace AS namespace
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
            UNION ALL
            SELECT relation.relowner,
                   pg_catalog.format(
                       '%%s:%%I.%%I',
                       CASE WHEN relation.relkind = 'S' THEN 'sequence'
                            ELSE 'relation' END,
                       namespace.nspname,
                       relation.relname
                   )
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
            UNION ALL
            SELECT routine.proowner,
                   pg_catalog.format(
                       'routine:%%I.%%I(%%s)', namespace.nspname, routine.proname,
                       pg_catalog.oidvectortypes(routine.proargtypes)
                   )
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
        ) AS owned
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = owned.owner_oid
        WHERE owner.rolname = ANY(%s)
        ORDER BY owner.rolname, owned_object
        """,
        (list(ALL_ROLES),),
    )
    mutable_owned: dict[str, set[str]] = {role: set() for role in ALL_ROLES}
    for row in cursor.fetchall():
        mutable_owned.setdefault(str(_row_value(row, 0, "owner_role")), set()).add(
            _normalize_object_identifier(str(_row_value(row, 1, "owned_object")))
        )

    cursor.execute(
        """
        WITH target_roles AS (
            SELECT oid, rolname FROM pg_catalog.pg_roles WHERE rolname = ANY(%s)
        )
        SELECT target.rolname AS grantee_role, object_identifier, privilege_type
        FROM (
            SELECT acl.grantee,
                   pg_catalog.format('database:%%I', database.datname) AS object_identifier,
                   acl.privilege_type
            FROM pg_catalog.pg_database AS database
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
            ) AS acl
            WHERE database.datname = pg_catalog.current_database()
            UNION ALL
            SELECT acl.grantee,
                   pg_catalog.format('schema:%%I', namespace.nspname),
                   acl.privilege_type
            FROM pg_catalog.pg_namespace AS namespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(namespace.nspacl, pg_catalog.acldefault('n', namespace.nspowner))
            ) AS acl
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
            UNION ALL
            SELECT acl.grantee,
                   pg_catalog.format(
                       '%%s:%%I.%%I',
                       CASE WHEN relation.relkind = 'S' THEN 'sequence'
                            ELSE 'relation' END,
                       namespace.nspname,
                       relation.relname
                   ),
                   acl.privilege_type
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    relation.relacl,
                    pg_catalog.acldefault(
                        CASE WHEN relation.relkind = 'S' THEN 's'::"char" ELSE 'r'::"char" END,
                        relation.relowner
                    )
                )
            ) AS acl
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
            UNION ALL
            SELECT acl.grantee,
                   pg_catalog.format(
                       'routine:%%I.%%I(%%s)', namespace.nspname, routine.proname,
                       pg_catalog.oidvectortypes(routine.proargtypes)
                   ),
                   acl.privilege_type
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
            ) AS acl
            WHERE namespace.nspname NOT IN ('pg_catalog', 'information_schema')
              AND namespace.nspname !~ '^pg_(toast|temp)(_|$)'
            UNION ALL
            SELECT acl.grantee,
                   pg_catalog.format(
                       'default-acl:%%s:%%s:%%s', defaults.defaclrole,
                       COALESCE(defaults.defaclnamespace::text, 'global'),
                       defaults.defaclobjtype
                   ),
                   acl.privilege_type
            FROM pg_catalog.pg_default_acl AS defaults
            CROSS JOIN LATERAL pg_catalog.aclexplode(defaults.defaclacl) AS acl
        ) AS direct_acl
        JOIN target_roles AS target ON target.oid = direct_acl.grantee
        ORDER BY target.rolname, object_identifier, privilege_type
        """,
        (list(ALL_ROLES),),
    )
    mutable_direct: dict[str, set[str]] = {role: set() for role in ALL_ROLES}
    for row in cursor.fetchall():
        role_name = str(_row_value(row, 0, "grantee_role"))
        object_identifier = _normalize_object_identifier(
            str(_row_value(row, 1, "object_identifier"))
        )
        privilege = str(_row_value(row, 2, "privilege_type")).upper()
        mutable_direct.setdefault(role_name, set()).add(f"{object_identifier}:{privilege}")
    cursor.execute(
        """
        SELECT COALESCE(role.rolname, 'DATABASE') AS setting_role,
               setting.setconfig
        FROM pg_catalog.pg_db_role_setting AS setting
        LEFT JOIN pg_catalog.pg_roles AS role ON role.oid = setting.setrole
        LEFT JOIN pg_catalog.pg_database AS database ON database.oid = setting.setdatabase
        WHERE (setting.setrole = 0 OR role.rolname = ANY(%s))
          AND (setting.setdatabase = 0 OR database.datname = pg_catalog.current_database())
        ORDER BY setting.setdatabase, setting.setrole
        """,
        (list(ALL_ROLES),),
    )
    configured_search_paths: set[str] = set()
    for row in cursor.fetchall():
        setting_role = str(_row_value(row, 0, "setting_role"))
        values = _settings_tuple(_row_value(row, 1, "setconfig"))
        for value in values:
            setting = str(value)
            if setting.partition("=")[0].strip().lower() == "search_path":
                configured_search_paths.add(setting_role)
    return AdminRoleSnapshot(
        database_name=str(_row_value(database_row, 0, "current_database")),
        revision=str(_row_value(revision_row, 0, "version_num")),
        roles=roles,
        memberships=memberships,
        incoming_members={role: frozenset(members) for role, members in mutable_incoming.items()},
        owned_objects={role: frozenset(items) for role, items in mutable_owned.items()},
        direct_privileges={role: frozenset(items) for role, items in mutable_direct.items()},
        membership_options={
            role: frozenset(items) for role, items in mutable_membership_options.items()
        },
        incoming_membership_options={
            role: frozenset(items) for role, items in mutable_incoming_options.items()
        },
        configured_search_paths=frozenset(configured_search_paths),
    )


def _require_database_and_revision(
    snapshot: AdminRoleSnapshot,
    expected_database: str,
    expected_revision: str,
) -> None:
    normalized_database = expected_database.strip()
    if not normalized_database:
        _fail("database-role-admin.expected-database-missing", "expected_database")
    if snapshot.database_name != normalized_database:
        _fail("database-role-admin.database-mismatch", f"database:{snapshot.database_name}")
    if snapshot.revision != expected_revision:
        _fail("database-role-admin.revision-mismatch", f"revision:{snapshot.revision}")


def _require_capability_roles_safe(
    snapshot: AdminRoleSnapshot,
    *,
    require_provisioned: bool = False,
) -> None:
    for login_role, role_name in LOGIN_ROLE_CAPABILITY.items():
        attributes = snapshot.roles.get(role_name)
        if attributes is None:
            _fail("database-role-admin.capability-missing", role_name)
        if attributes.can_login or attributes.inherits or _has_elevated_attribute(attributes):
            _fail("database-role-admin.capability-unsafe", role_name)
        if _has_search_path_setting(attributes.settings):
            _fail("database-role-admin.namespace-unsafe", role_name)
        if snapshot.memberships.get(role_name, frozenset()):
            _fail("database-role-admin.membership-unsafe", role_name)
        login_incoming_options = (
            frozenset({(login_role, False, True, True)})
            if login_role in snapshot.roles or require_provisioned
            else frozenset()
        )
        actual_incoming_options = snapshot.incoming_membership_options.get(
            role_name, frozenset()
        )
        allowed_incoming_options = (
            login_incoming_options,
            login_incoming_options | frozenset({SUPABASE_CREATOR_MEMBERSHIP}),
        )
        if (
            actual_incoming_options not in allowed_incoming_options
            or snapshot.incoming_members.get(role_name, frozenset())
            != frozenset(item[0] for item in actual_incoming_options)
            or snapshot.membership_options.get(role_name, frozenset())
        ):
            _fail("database-role-admin.membership-unsafe", role_name)
        if snapshot.owned_objects.get(role_name, frozenset()):
            _fail("database-role-admin.ownership-unsafe", role_name)
        if snapshot.direct_privileges.get(role_name, frozenset()) != _expected_direct_privileges(
            snapshot,
            role_name,
        ):
            _fail("database-role-admin.direct-privilege-unsafe", role_name)
    if snapshot.configured_search_paths:
        _fail("database-role-admin.namespace-unsafe", "configured-search-path")


def _require_login_roles_safe(snapshot: AdminRoleSnapshot) -> None:
    for login_role, capability_role in LOGIN_ROLE_CAPABILITY.items():
        attributes = snapshot.roles.get(login_role)
        if attributes is None:
            continue
        if not attributes.inherits or _has_elevated_attribute(attributes):
            _fail("database-role-admin.login-unsafe", login_role)
        if _has_search_path_setting(attributes.settings):
            _fail("database-role-admin.namespace-unsafe", login_role)
        memberships = snapshot.memberships.get(login_role, frozenset())
        if memberships != frozenset({capability_role}):
            _fail("database-role-admin.membership-unsafe", login_role)
        if snapshot.membership_options.get(login_role, frozenset()) != frozenset(
            {(capability_role, False, True, True)}
        ):
            _fail("database-role-admin.membership-unsafe", login_role)
        incoming_options = snapshot.incoming_membership_options.get(
            login_role,
            frozenset(),
        )
        if (
            incoming_options
            not in (
                frozenset(),
                frozenset({SUPABASE_CREATOR_MEMBERSHIP}),
            )
            or snapshot.incoming_members.get(login_role, frozenset())
            != frozenset(item[0] for item in incoming_options)
        ):
            _fail("database-role-admin.membership-unsafe", login_role)
        if snapshot.owned_objects.get(login_role, frozenset()):
            _fail("database-role-admin.ownership-unsafe", login_role)
        if snapshot.direct_privileges.get(login_role, frozenset()):
            _fail("database-role-admin.direct-privilege-unsafe", login_role)


def _has_search_path_setting(settings: tuple[str, ...]) -> bool:
    return any(setting.partition("=")[0].strip().lower() == "search_path" for setting in settings)


def _settings_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(setting) for setting in value)


def _expected_direct_privileges(
    snapshot: AdminRoleSnapshot,
    capability_role: str,
) -> frozenset[str]:
    contract = next(
        contract
        for contract in ROLE_CONTRACTS.values()
        if contract.capability_role == capability_role
    )
    expected = {
        f"database:{snapshot.database_name}:CONNECT",
        "schema:public:USAGE",
    }
    expected.update(
        f"relation:public.{table}:{privilege}"
        for table, privilege in contract.required_table_privileges
    )
    expected.update(
        f"routine:{_normalize_signature(signature)}:EXECUTE"
        for signature in contract.required_function_privileges
    )
    return frozenset(expected)


def _require_effective_capability_authority(
    cursor: Any,
    snapshot: AdminRoleSnapshot,
) -> None:
    for contract in ROLE_CONTRACTS.values():
        try:
            verify_effective_database_role_authority(
                cursor,
                contract,
                subject_role=contract.capability_role,
                expected_database=snapshot.database_name,
            )
        except DatabaseRoleContractError as exc:
            raise DatabaseRoleAdminError(
                "database-role-admin.authority-unsafe",
                contract.profile,
            ) from exc


def _normalize_signature(signature: str) -> str:
    canonical = signature.replace("timestamp with time zone", "timestamptz")
    canonical = canonical.replace("timestamp without time zone", "timestamp")
    return "".join(canonical.split())


def _normalize_object_identifier(identifier: str) -> str:
    if identifier.startswith("routine:"):
        return "routine:" + _normalize_signature(identifier.removeprefix("routine:"))
    return identifier


def _require_independent_passwords(
    runtime_password: str,
    qualification_password: str,
) -> None:
    if runtime_password.strip() == "" or qualification_password.strip() == "":
        _fail("database-role-admin.password-missing", "password-env")
    if runtime_password == qualification_password:
        _fail("database-role-admin.passwords-not-independent", "password-env")


def _create_or_rotate_login(
    cursor: Any,
    *,
    login_role: str,
    capability_role: str,
    password: str,
) -> None:
    snapshot = _read_single_login_snapshot(cursor, login_role)
    if snapshot is not None:
        _require_single_login_safe(snapshot, login_role, capability_role)
        attributes, _memberships = snapshot
        if not attributes.can_login:
            cursor.execute(
                sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(login_role))
            )
        cursor.execute(
            sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                sql.Identifier(login_role), sql.Literal(password)
            )
        )
    else:
        cursor.execute(
            sql.SQL(
                """
                CREATE ROLE {}
                LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
                PASSWORD {}
                """
            ).format(sql.Identifier(login_role), sql.Literal(password))
        )
    cursor.execute(
        sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
            sql.Identifier(capability_role),
            sql.Identifier(login_role),
        )
    )


def _connection_factory_from_dsn(dsn: str) -> ConnectionFactory:
    try:
        return scoped_connection_factory(dsn)
    except DatabaseRoleContractError as exc:
        raise DatabaseRoleAdminError(
            "database-role-admin.dsn-unsafe",
            "namespace",
        ) from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        result = _dispatch(args)
    except DatabaseRoleAdminError as exc:
        print(f"{exc.reason_code}: {exc.object_identifier}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"database-role-admin.failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        print(f"{result['status']}: {result['database']}")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="db-role-admin")
    subcommands = parser.add_subparsers(dest="command", required=True)
    preflight = subcommands.add_parser("preflight")
    preflight.add_argument("--expected-database", required=True)
    preflight.add_argument("--json", action="store_true")
    provision = subcommands.add_parser("provision")
    provision.add_argument("--enable", action="store_true")
    provision.add_argument("--expected-database", required=True)
    provision.add_argument("--json", action="store_true")
    verify = subcommands.add_parser("verify")
    verify.add_argument("--profile", choices=tuple(PROFILE_DSN_ENV), required=True)
    verify.add_argument("--expected-database", required=True)
    verify.add_argument("--json", action="store_true")
    disable = subcommands.add_parser("disable")
    disable.add_argument("--enable", action="store_true")
    disable.add_argument("--expected-database", required=True)
    disable.add_argument("--json", action="store_true")
    return parser


def _dispatch(args: argparse.Namespace) -> Mapping[str, object]:
    if args.command == "preflight":
        return preflight_capability_roles(
            _admin_connection_factory_from_env(),
            expected_database=args.expected_database,
        )
    if args.command == "provision":
        if not args.enable:
            _fail("database-role-admin.enable-required", "provision")
        return provision_login_roles(
            _admin_connection_factory_from_env(),
            expected_database=args.expected_database,
            runtime_password=_required_env(RUNTIME_PASSWORD_ENV),
            qualification_password=_required_env(QUALIFICATION_PASSWORD_ENV),
        )
    if args.command == "verify":
        profile = str(args.profile)
        dsn_env = PROFILE_DSN_ENV[profile]
        return verify_login_role(
            _connection_factory_from_dsn(_required_env(dsn_env)),
            profile,
            expected_database=args.expected_database,
        )
    if args.command == "disable":
        if not args.enable:
            _fail("database-role-admin.enable-required", "disable")
        return disable_login_roles(
            _admin_connection_factory_from_env(),
            expected_database=args.expected_database,
        )
    raise AssertionError(f"unknown command: {args.command}")


def _admin_connection_factory_from_env() -> ConnectionFactory:
    return _connection_factory_from_dsn(_required_env(ADMIN_DSN_ENV))


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if value.strip() == "":
        _fail("database-role-admin.env-missing", name)
    return value


def _read_single_login_snapshot(
    cursor: Any,
    login_role: str,
) -> tuple[RoleAttributes, frozenset[tuple[str, bool, bool, bool]]] | None:
    cursor.execute(
        """
        SELECT role.rolname, role.rolcanlogin, role.rolsuper,
               role.rolcreatedb, role.rolcreaterole, role.rolinherit,
               role.rolreplication, role.rolbypassrls, role.rolconfig
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = %s
        """,
        (login_role,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    cursor.execute(
        """
        SELECT member.rolname AS member_role, granted.rolname AS granted_role,
               membership.admin_option, membership.inherit_option,
               membership.set_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        WHERE member.rolname = %s
        ORDER BY granted.rolname
        """,
        (login_role,),
    )
    return _role_attributes_from_row(row), frozenset(
        (
            str(_row_value(membership, 1, "granted_role")),
            bool(_row_value(membership, 2, "admin_option")),
            bool(_row_value(membership, 3, "inherit_option")),
            bool(_row_value(membership, 4, "set_option")),
        )
        for membership in cursor.fetchall()
    )


def _require_single_login_safe(
    snapshot: tuple[RoleAttributes, frozenset[tuple[str, bool, bool, bool]]],
    login_role: str,
    capability_role: str,
) -> None:
    attributes, memberships = snapshot
    if attributes.role_name != login_role:
        _fail("database-role-admin.login-unsafe", attributes.role_name)
    if not attributes.inherits or _has_elevated_attribute(attributes):
        _fail("database-role-admin.login-unsafe", login_role)
    if memberships != frozenset({(capability_role, False, True, True)}):
        _fail("database-role-admin.membership-unsafe", login_role)
    if _has_search_path_setting(attributes.settings):
        _fail("database-role-admin.namespace-unsafe", login_role)


def _require_login_roles_present(snapshot: AdminRoleSnapshot) -> None:
    for role_name in LOGIN_ROLES:
        if role_name not in snapshot.roles:
            _fail("database-role-admin.login-missing", role_name)


def _role_attributes_from_row(row: object) -> RoleAttributes:
    return RoleAttributes(
        role_name=str(_row_value(row, 0, "rolname")),
        can_login=bool(_row_value(row, 1, "rolcanlogin")),
        superuser=bool(_row_value(row, 2, "rolsuper")),
        create_db=bool(_row_value(row, 3, "rolcreatedb")),
        create_role=bool(_row_value(row, 4, "rolcreaterole")),
        inherits=bool(_row_value(row, 5, "rolinherit")),
        replication=bool(_row_value(row, 6, "rolreplication")),
        bypass_rls=bool(_row_value(row, 7, "rolbypassrls")),
        settings=_settings_tuple(_row_value(row, 8, "rolconfig")),
    )


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, Mapping):
        return row[name]
    return row[index]  # type: ignore[index]


def _has_elevated_attribute(attributes: RoleAttributes) -> bool:
    return any(
        (
            attributes.superuser,
            attributes.create_db,
            attributes.create_role,
            attributes.replication,
            attributes.bypass_rls,
        )
    )


def _verification_mapping(verification: DatabaseRoleVerification) -> Mapping[str, object]:
    return {
        "profile": verification.profile,
        "session_user": verification.session_user,
        "capability_role": verification.capability_role,
        "database": verification.database_name,
        "status": verification.status,
    }


def _fail(reason_code: str, object_identifier: str) -> NoReturn:
    raise DatabaseRoleAdminError(reason_code, object_identifier)


__all__ = [
    "ADMIN_DSN_ENV",
    "CAPABILITY_ROLES",
    "DatabaseRoleAdminError",
    "LOGIN_ROLES",
    "QUALIFICATION_DSN_ENV",
    "QUALIFICATION_PASSWORD_ENV",
    "RUNTIME_DSN_ENV",
    "RUNTIME_PASSWORD_ENV",
    "_create_or_rotate_login",
    "_read_admin_role_snapshot",
    "_require_capability_roles_safe",
    "_require_database_and_revision",
    "_require_independent_passwords",
    "_require_login_roles_safe",
    "disable_login_roles",
    "main",
    "preflight_capability_roles",
    "provision_login_roles",
    "verify_login_role",
]


if __name__ == "__main__":
    raise SystemExit(main())
