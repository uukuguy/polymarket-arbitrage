"""Safe operator tooling for scoped control-plane database login roles."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, NoReturn

import psycopg
from psycopg import sql

from polyarb.control_plane.db_role_contract import (
    ROLE_CONTRACTS,
    ConnectionFactory,
    DatabaseRoleVerification,
    verify_daemon_database_role,
)

RUNTIME_PROFILE = "runtime-controller"
QUALIFICATION_PROFILE = "qualification-worker"
EXPECTED_REVISION = "026"
ADMIN_DSN_ENV = "POLYARB_SUPABASE_DB_DSN"
RUNTIME_PASSWORD_ENV = "POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD"
QUALIFICATION_PASSWORD_ENV = "POLYARB_QUALIFICATION_WORKER_DB_PASSWORD"
RUNTIME_DSN_ENV = "POLYARB_RUNTIME_CONTROLLER_DB_DSN"
QUALIFICATION_DSN_ENV = "POLYARB_QUALIFICATION_WORKER_DB_DSN"
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


@dataclass(frozen=True, slots=True)
class AdminRoleSnapshot:
    database_name: str
    revision: str
    roles: Mapping[str, RoleAttributes]
    memberships: Mapping[str, frozenset[str]]


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
        for role_name in LOGIN_ROLES:
            cursor.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(role_name)))
        connection.commit()
    return {"database": expected_database, "status": "disabled"}


def _read_admin_role_snapshot(cursor: Any) -> AdminRoleSnapshot:
    cursor.execute("SELECT current_database()")
    database_row = cursor.fetchone()
    if database_row is None:
        _fail("database-role-admin.database-unavailable", "current_database")

    cursor.execute("SELECT version_num FROM alembic_version")
    revision_row = cursor.fetchone()
    if revision_row is None:
        _fail("database-role-admin.revision-missing", "alembic_version")

    cursor.execute(
        """
        SELECT role.rolname, role.rolcanlogin, role.rolsuper,
               role.rolcreatedb, role.rolcreaterole, role.rolinherit,
               role.rolreplication, role.rolbypassrls
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
        SELECT member.rolname AS member_role, granted.rolname AS granted_role
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        WHERE member.rolname = ANY(%s)
        ORDER BY member.rolname, granted.rolname
        """,
        (list(LOGIN_ROLES),),
    )
    mutable_memberships: dict[str, set[str]] = {role: set() for role in LOGIN_ROLES}
    for row in cursor.fetchall():
        mutable_memberships.setdefault(str(_row_value(row, 0, "member_role")), set()).add(
            str(_row_value(row, 1, "granted_role"))
        )
    memberships = {
        role: frozenset(grants) for role, grants in mutable_memberships.items()
    }
    return AdminRoleSnapshot(
        database_name=str(_row_value(database_row, 0, "current_database")),
        revision=str(_row_value(revision_row, 0, "version_num")),
        roles=roles,
        memberships=memberships,
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


def _require_capability_roles_safe(snapshot: AdminRoleSnapshot) -> None:
    for role_name in CAPABILITY_ROLES:
        attributes = snapshot.roles.get(role_name)
        if attributes is None:
            _fail("database-role-admin.capability-missing", role_name)
        if attributes.can_login or attributes.inherits or _has_elevated_attribute(attributes):
            _fail("database-role-admin.capability-unsafe", role_name)


def _require_login_roles_safe(snapshot: AdminRoleSnapshot) -> None:
    for login_role, capability_role in LOGIN_ROLE_CAPABILITY.items():
        attributes = snapshot.roles.get(login_role)
        if attributes is None:
            continue
        if (
            not attributes.can_login
            or not attributes.inherits
            or _has_elevated_attribute(attributes)
        ):
            _fail("database-role-admin.login-unsafe", login_role)
        memberships = snapshot.memberships.get(login_role, frozenset())
        if memberships != frozenset({capability_role}):
            _fail("database-role-admin.membership-unsafe", login_role)


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
        cursor.execute(
            sql.SQL(
                """
                ALTER ROLE {}
                LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
                PASSWORD {}
                """
            ).format(sql.Identifier(login_role), sql.Literal(password))
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
        sql.SQL("GRANT {} TO {}").format(
            sql.Identifier(capability_role),
            sql.Identifier(login_role),
        )
    )


def _connection_factory_from_dsn(dsn: str) -> ConnectionFactory:
    return lambda: psycopg.connect(dsn)


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
) -> tuple[RoleAttributes, frozenset[str]] | None:
    cursor.execute(
        """
        SELECT role.rolname, role.rolcanlogin, role.rolsuper,
               role.rolcreatedb, role.rolcreaterole, role.rolinherit,
               role.rolreplication, role.rolbypassrls
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
        SELECT member.rolname AS member_role, granted.rolname AS granted_role
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        WHERE member.rolname = %s
        ORDER BY granted.rolname
        """,
        (login_role,),
    )
    return _role_attributes_from_row(row), frozenset(
        str(_row_value(membership, 1, "granted_role"))
        for membership in cursor.fetchall()
    )


def _require_single_login_safe(
    snapshot: tuple[RoleAttributes, frozenset[str]],
    login_role: str,
    capability_role: str,
) -> None:
    attributes, memberships = snapshot
    if attributes.role_name != login_role:
        _fail("database-role-admin.login-unsafe", attributes.role_name)
    if (
        not attributes.can_login
        or not attributes.inherits
        or _has_elevated_attribute(attributes)
    ):
        _fail("database-role-admin.login-unsafe", login_role)
    if memberships != frozenset({capability_role}):
        _fail("database-role-admin.membership-unsafe", login_role)


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
