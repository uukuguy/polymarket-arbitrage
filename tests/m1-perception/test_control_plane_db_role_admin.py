"""Operator-side admin tooling for scoped control-plane login roles."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql

from polyarb.control_plane.db_role_contract import (
    QUALIFICATION_ALLOWED,
    ROLE_CONTRACTS,
    RUNTIME_ALLOWED,
    ConnectionFactory,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
RUNTIME_LOGIN = "m1_runtime_controller_login"
QUALIFICATION_LOGIN = "m1_qualification_worker_login"
RUNTIME_CAPABILITY = "m1_runtime_controller_capability"
QUALIFICATION_CAPABILITY = "m1_qualification_worker_capability"


class FakeCursor:
    def __init__(self, factory: FakeAdminFactory) -> None:
        self.factory = factory
        self.rows: list[Any] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: object, params: object = ()) -> FakeCursor:
        rendered = _render_for_fake(statement)
        normalized = " ".join(rendered.lower().split())
        self.factory.calls.append((normalized, params))
        if self.factory.fail_after_first_password_change and self.factory.password_changes == 1:
            raise RuntimeError("simulated second role failure")
        answer = self.factory.answer(normalized, params)
        self.rows = answer if isinstance(answer, list) else [answer]
        return self

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[Any]:
        return self.rows


class FakeConnection:
    def __init__(self, factory: FakeAdminFactory) -> None:
        self.factory = factory

    def __enter__(self) -> FakeConnection:
        self.factory.begin()
        return self

    def __exit__(self, exc_type: object, *_exc: object) -> None:
        if exc_type is not None:
            self.factory.rollback()

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.factory)

    def commit(self) -> None:
        self.factory.commits += 1
        self.factory.commit()


class FakeAdminFactory:
    def __init__(self) -> None:
        self.database = "role_test"
        self.revision = "026"
        self.roles: dict[str, dict[str, Any]] = {
            RUNTIME_CAPABILITY: {
                "can_login": False,
                "superuser": False,
                "create_db": False,
                "create_role": False,
                "inherits": False,
                "replication": False,
                "bypass_rls": False,
                "settings": [],
                "memberships": set(),
                "password": None,
            },
            QUALIFICATION_CAPABILITY: {
                "can_login": False,
                "superuser": False,
                "create_db": False,
                "create_role": False,
                "inherits": False,
                "replication": False,
                "bypass_rls": False,
                "settings": [],
                "memberships": set(),
                "password": None,
            },
        }
        self.calls: list[tuple[str, object]] = []
        self.owned_objects: dict[str, set[str]] = {role: set() for role in self.roles}
        self.direct_privileges: dict[str, set[str]] = {
            RUNTIME_CAPABILITY: _fake_expected_direct("runtime-controller", self.database),
            QUALIFICATION_CAPABILITY: _fake_expected_direct("qualification-worker", self.database),
        }
        self.commits = 0
        self.password_changes = 0
        self.fail_after_first_password_change = False
        self.public_schema_create = False
        self._transaction_backup: dict[str, dict[str, Any]] | None = None

    def __call__(self) -> FakeConnection:
        return FakeConnection(self)

    def begin(self) -> None:
        self._transaction_backup = {
            role: {
                **attrs,
                "memberships": set(cast(set[str], attrs["memberships"])),
            }
            for role, attrs in self.roles.items()
        }

    def commit(self) -> None:
        self._transaction_backup = None

    def rollback(self) -> None:
        assert self._transaction_backup is not None
        self.roles = self._transaction_backup
        self._transaction_backup = None

    def add_login(
        self,
        role: str,
        *,
        capability: str,
        password: str,
        extra_membership: str | None = None,
    ) -> None:
        memberships = {capability}
        if extra_membership is not None:
            memberships.add(extra_membership)
        self.roles[role] = {
            "can_login": True,
            "superuser": False,
            "create_db": False,
            "create_role": False,
            "inherits": True,
            "replication": False,
            "bypass_rls": False,
            "settings": [],
            "memberships": memberships,
            "password": password,
        }
        self.owned_objects.setdefault(role, set())
        self.direct_privileges.setdefault(role, set())

    def answer(self, normalized: str, params: object) -> object:
        if normalized == "set transaction read only":
            return None
        if "select current_database()" in normalized:
            return (self.database,)
        if "select version_num from public.alembic_version" in normalized:
            return (self.revision,)
        if "from pg_catalog.pg_db_role_setting" in normalized:
            return []
        if (
            "select namespace.nspname" in normalized
            and "from pg_catalog.pg_namespace" in normalized
        ):
            return [("public",)]
        if "as relation_name" in normalized and "pg_catalog.pg_class" in normalized:
            names = set(RUNTIME_ALLOWED) | set(QUALIFICATION_ALLOWED)
            return [(f"public.{name}", name, "public") for name in sorted(names)]
        if "routine.prosecdef" in normalized and "pg_catalog.pg_proc" in normalized:
            return [
                (signature,)
                for signature in (
                    "public.l3_retention_cleanup(timestamptz,timestamptz,timestamptz)",
                    *ROLE_CONTRACTS["qualification-worker"].required_function_privileges,
                    *ROLE_CONTRACTS["runtime-controller"].forbidden_function_privileges,
                )
            ]
        if "select owner.rolname as owner_role" in normalized:
            return sorted(
                (role, item) for role, items in self.owned_objects.items() for item in items
            )
        if "with target_roles as" in normalized:
            return sorted(
                (role, privilege.rsplit(":", 1)[0], privilege.rsplit(":", 1)[1])
                for role, privileges in self.direct_privileges.items()
                for privilege in privileges
            )
        if "select owned_object from" in normalized:
            subject = str(_param_tuple(params)[0])
            items = sorted(self.owned_objects.get(subject, set()))
            return [(items[0],)] if items else []
        if "from pg_catalog.pg_roles as role" in normalized:
            names = _param_tuple(params)
            return [
                (
                    name,
                    attrs["can_login"],
                    attrs["superuser"],
                    attrs["create_db"],
                    attrs["create_role"],
                    attrs["inherits"],
                    attrs["replication"],
                    attrs["bypass_rls"],
                    attrs["settings"],
                )
                for name, attrs in sorted(self.roles.items())
                if name in names
            ]
        if "from pg_catalog.pg_auth_members" in normalized:
            rows: list[tuple[str, str, bool, bool, bool]] = []
            for member, attrs in self.roles.items():
                rows.extend(
                    (member, role, False, True, True)
                    for role in cast(set[str], attrs["memberships"])
                )
            if "where member.rolname = %s" in normalized:
                requested = str(_param_tuple(params)[0])
                return sorted(row for row in rows if row[0] == requested)
            requested = set(_param_tuple(params))
            return sorted(row for row in rows if row[0] in requested or row[1] in requested)
        if "has_database_privilege" in normalized:
            privilege = str(_param_tuple(params)[2]).upper()
            return (privilege != "CREATE",)
        if "as sequence_name" in normalized and "pg_catalog.pg_class" in normalized:
            return []
        if "has_schema_privilege" in normalized:
            privilege = str(_param_tuple(params)[2]).upper()
            return (self.public_schema_create if privilege == "CREATE" else True,)
        if "has_table_privilege" in normalized:
            subject, relation_name, privilege = _param_tuple(params)
            profile = _profile_for_role(subject)
            allowed = RUNTIME_ALLOWED if profile == "runtime-controller" else QUALIFICATION_ALLOWED
            table_name = relation_name.removeprefix("public.")
            return (privilege in allowed.get(table_name, frozenset()),)
        if "has_sequence_privilege" in normalized:
            return (False,)
        if "has_function_privilege" in normalized:
            subject, signature, _privilege = _param_tuple(params)
            profile = _profile_for_role(subject)
            allowed = {
                "".join(item.split())
                for item in ROLE_CONTRACTS[profile].required_function_privileges
            }
            return ("".join(signature.split()) in allowed,)
        if normalized.startswith("create role "):
            role = _identifier_after(rendered=normalized, keyword="create role")
            self.roles[role] = {
                "can_login": True,
                "superuser": False,
                "create_db": False,
                "create_role": False,
                "inherits": True,
                "replication": False,
                "bypass_rls": False,
                "settings": [],
                "memberships": set(),
                "password": _literal_password(normalized),
            }
            self.owned_objects.setdefault(role, set())
            self.direct_privileges.setdefault(role, set())
            self.password_changes += 1
            return None
        if normalized.startswith("alter role ") and " password " in normalized:
            role = _identifier_after(rendered=normalized, keyword="alter role")
            self.roles[role]["can_login"] = True
            self.roles[role]["password"] = _literal_password(normalized)
            self.password_changes += 1
            return None
        if normalized.startswith("alter role ") and normalized.endswith(" nologin"):
            role = _identifier_after(rendered=normalized, keyword="alter role")
            self.roles[role]["can_login"] = False
            return None
        if normalized.startswith("grant "):
            capability, login = normalized.removeprefix("grant ").split(" to ", 1)
            cast(set[str], self.roles[login]["memberships"]).add(capability)
            return None
        raise AssertionError(f"unexpected admin query: {normalized}")


def _as_connection_factory(factory: FakeAdminFactory) -> ConnectionFactory:
    return cast(ConnectionFactory, factory)


def _param_tuple(params: object) -> tuple[str, ...]:
    assert isinstance(params, tuple)
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        return tuple(str(value) for value in params[0])
    values: list[str] = []
    for value in params:
        if isinstance(value, (list, tuple)):
            values.extend(str(item) for item in value)
        else:
            values.append(str(value))
    return tuple(values)


def _profile_for_role(role: str) -> str:
    if "qualification" in role:
        return "qualification-worker"
    return "runtime-controller"


def _fake_expected_direct(profile: str, database: str) -> set[str]:
    contract = ROLE_CONTRACTS[profile]
    expected = {f"database:{database}:CONNECT", "schema:public:USAGE"}
    expected.update(
        f"relation:public.{table}:{privilege}"
        for table, privilege in contract.required_table_privileges
    )
    expected.update(
        f"routine:{''.join(signature.split())}:EXECUTE"
        for signature in contract.required_function_privileges
    )
    return expected


def _render_for_fake(statement: object) -> str:
    rendered = str(statement)
    if not rendered.startswith("Composed("):
        return rendered
    if "CREATE ROLE" in rendered:
        return _render_role_statement(rendered, "CREATE ROLE")
    if "ALTER ROLE" in rendered and "NOLOGIN" in rendered and "PASSWORD" not in rendered:
        role = _first_composed_value(rendered, "Identifier")
        return f"ALTER ROLE {role} NOLOGIN"
    if "ALTER ROLE" in rendered:
        return _render_role_statement(rendered, "ALTER ROLE")
    if "GRANT" in rendered:
        capability, login = _composed_values(rendered, "Identifier")[:2]
        return f"GRANT {capability} TO {login}"
    return rendered


def _render_role_statement(rendered: str, command: str) -> str:
    role = _first_composed_value(rendered, "Identifier")
    password = _first_composed_value(rendered, "Literal")
    return (
        f"{command} {role} "
        "LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS "
        f"PASSWORD '{password}'"
    )


def _first_composed_value(rendered: str, constructor: str) -> str:
    return _composed_values(rendered, constructor)[0]


def _composed_values(rendered: str, constructor: str) -> list[str]:
    prefix = f"{constructor}('"
    values: list[str] = []
    start = 0
    while True:
        index = rendered.find(prefix, start)
        if index == -1:
            return values
        value_start = index + len(prefix)
        value_end = rendered.find("')", value_start)
        values.append(rendered[value_start:value_end])
        start = value_end + 2


def _identifier_after(*, rendered: str, keyword: str) -> str:
    return rendered.removeprefix(keyword).strip().split()[0].strip('"')


def _literal_password(rendered: str) -> str:
    return rendered.rsplit(" password ", 1)[1].strip().strip("'")


def test_preflight_rejects_wrong_database_and_wrong_revision() -> None:
    from polyarb.control_plane.db_role_admin import (
        DatabaseRoleAdminError,
        preflight_capability_roles,
    )

    factory = FakeAdminFactory()
    factory.database = "wrong_database"
    with pytest.raises(DatabaseRoleAdminError, match="database-role-admin.database-mismatch"):
        preflight_capability_roles(_as_connection_factory(factory), expected_database="role_test")

    factory = FakeAdminFactory()
    factory.revision = "025"
    with pytest.raises(DatabaseRoleAdminError, match="database-role-admin.revision-mismatch"):
        preflight_capability_roles(_as_connection_factory(factory), expected_database="role_test")


def test_preflight_rejects_unsafe_capability_role_attributes() -> None:
    from polyarb.control_plane.db_role_admin import (
        DatabaseRoleAdminError,
        preflight_capability_roles,
    )

    factory = FakeAdminFactory()
    factory.roles[RUNTIME_CAPABILITY]["can_login"] = True

    with pytest.raises(DatabaseRoleAdminError, match="database-role-admin.capability-unsafe"):
        preflight_capability_roles(_as_connection_factory(factory), expected_database="role_test")


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        ("unexpected-member", "database-role-admin.membership-unsafe"),
        ("owned-object", "database-role-admin.ownership-unsafe"),
        ("direct-grant", "database-role-admin.direct-privilege-unsafe"),
        ("public-schema-create", "database-role-admin.authority-unsafe"),
    ),
)
def test_preflight_rejects_every_unexpected_capability_authority(
    mutation: str,
    reason_code: str,
) -> None:
    from polyarb.control_plane.db_role_admin import (
        DatabaseRoleAdminError,
        preflight_capability_roles,
    )

    factory = FakeAdminFactory()
    if mutation == "unexpected-member":
        factory.roles["unrelated_member"] = {
            "can_login": False,
            "superuser": False,
            "create_db": False,
            "create_role": False,
            "inherits": False,
            "replication": False,
            "bypass_rls": False,
            "memberships": {RUNTIME_CAPABILITY},
            "password": None,
        }
    elif mutation == "owned-object":
        factory.owned_objects[RUNTIME_CAPABILITY].add("relation:public.unrelated")
    elif mutation == "direct-grant":
        factory.direct_privileges[RUNTIME_CAPABILITY].add("relation:public.unrelated:SELECT")
    else:
        factory.public_schema_create = True

    with pytest.raises(DatabaseRoleAdminError, match=reason_code):
        preflight_capability_roles(
            _as_connection_factory(factory),
            expected_database="role_test",
        )


def test_provision_rejects_empty_or_equal_passwords() -> None:
    from polyarb.control_plane.db_role_admin import (
        DatabaseRoleAdminError,
        provision_login_roles,
    )

    with pytest.raises(DatabaseRoleAdminError, match="password-missing"):
        provision_login_roles(
            _as_connection_factory(FakeAdminFactory()),
            expected_database="role_test",
            runtime_password="",
            qualification_password="qualification-secret",
        )
    with pytest.raises(DatabaseRoleAdminError, match="passwords-not-independent"):
        provision_login_roles(
            _as_connection_factory(FakeAdminFactory()),
            expected_database="role_test",
            runtime_password="same-secret",
            qualification_password="same-secret",
        )


def test_provision_rejects_unsafe_login_attributes_and_unexpected_membership() -> None:
    from polyarb.control_plane.db_role_admin import (
        DatabaseRoleAdminError,
        provision_login_roles,
    )

    factory = FakeAdminFactory()
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="old-runtime",
    )
    factory.roles[RUNTIME_LOGIN]["superuser"] = True
    with pytest.raises(DatabaseRoleAdminError, match="database-role-admin.login-unsafe"):
        provision_login_roles(
            _as_connection_factory(factory),
            expected_database="role_test",
            runtime_password="new-runtime",
            qualification_password="new-qualification",
        )

    factory = FakeAdminFactory()
    factory.roles["other_role"] = {
        "can_login": False,
        "superuser": False,
        "create_db": False,
        "create_role": False,
        "inherits": False,
        "replication": False,
        "bypass_rls": False,
        "memberships": set(),
        "password": None,
    }
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="old-runtime",
        extra_membership="other_role",
    )
    with pytest.raises(DatabaseRoleAdminError, match="database-role-admin.membership-unsafe"):
        provision_login_roles(
            _as_connection_factory(factory),
            expected_database="role_test",
            runtime_password="new-runtime",
            qualification_password="new-qualification",
        )


def test_provision_success_idempotent_rotation_and_no_secret_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from polyarb.control_plane.db_role_admin import provision_login_roles

    factory = FakeAdminFactory()
    runtime_secret = "runtime-password-that-must-not-appear"
    result = provision_login_roles(
        _as_connection_factory(factory),
        expected_database="role_test",
        runtime_password=runtime_secret,
        qualification_password="independent-qualification-password",
    )

    captured = capsys.readouterr()
    assert result == {"database": "role_test", "status": "provisioned"}
    assert runtime_secret not in captured.out + captured.err
    assert factory.roles[RUNTIME_LOGIN]["password"] == runtime_secret
    assert factory.roles[QUALIFICATION_LOGIN]["password"] == "independent-qualification-password"
    assert factory.roles[RUNTIME_LOGIN]["memberships"] == {RUNTIME_CAPABILITY}
    assert factory.roles[QUALIFICATION_LOGIN]["memberships"] == {QUALIFICATION_CAPABILITY}

    provision_login_roles(
        _as_connection_factory(factory),
        expected_database="role_test",
        runtime_password="rotated-runtime-password",
        qualification_password="independent-qualification-password",
    )

    assert factory.roles[RUNTIME_LOGIN]["password"] == "rotated-runtime-password"
    assert factory.roles[QUALIFICATION_LOGIN]["password"] == "independent-qualification-password"
    assert factory.roles[RUNTIME_LOGIN]["memberships"] == {RUNTIME_CAPABILITY}
    assert factory.roles[QUALIFICATION_LOGIN]["memberships"] == {QUALIFICATION_CAPABILITY}


def test_provision_rolls_back_all_role_mutations_on_any_failure() -> None:
    from polyarb.control_plane.db_role_admin import provision_login_roles

    factory = FakeAdminFactory()
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="old-runtime-password",
    )
    factory.add_login(
        QUALIFICATION_LOGIN,
        capability=QUALIFICATION_CAPABILITY,
        password="old-qualification-password",
    )
    factory.fail_after_first_password_change = True

    with pytest.raises(RuntimeError, match="simulated second role failure"):
        provision_login_roles(
            _as_connection_factory(factory),
            expected_database="role_test",
            runtime_password="new-runtime-password",
            qualification_password="new-qualification-password",
        )

    assert factory.roles[RUNTIME_LOGIN]["password"] == "old-runtime-password"
    assert factory.roles[QUALIFICATION_LOGIN]["password"] == "old-qualification-password"
    assert factory.roles[RUNTIME_LOGIN]["memberships"] == {RUNTIME_CAPABILITY}
    assert factory.roles[QUALIFICATION_LOGIN]["memberships"] == {QUALIFICATION_CAPABILITY}
    assert factory.commits == 0


def test_disable_marks_both_login_roles_nologin() -> None:
    from polyarb.control_plane.db_role_admin import disable_login_roles

    factory = FakeAdminFactory()
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="runtime-password",
    )
    factory.add_login(
        QUALIFICATION_LOGIN,
        capability=QUALIFICATION_CAPABILITY,
        password="qualification-password",
    )

    result = disable_login_roles(_as_connection_factory(factory), expected_database="role_test")

    assert result == {"database": "role_test", "status": "disabled"}
    assert factory.roles[RUNTIME_LOGIN]["can_login"] is False
    assert factory.roles[QUALIFICATION_LOGIN]["can_login"] is False
    assert factory.roles[RUNTIME_LOGIN]["memberships"] == {RUNTIME_CAPABILITY}
    assert factory.roles[QUALIFICATION_LOGIN]["memberships"] == {QUALIFICATION_CAPABILITY}


def test_disable_is_repeatable_when_both_roles_are_already_nologin() -> None:
    from polyarb.control_plane.db_role_admin import disable_login_roles

    factory = FakeAdminFactory()
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="runtime-password",
    )
    factory.add_login(
        QUALIFICATION_LOGIN,
        capability=QUALIFICATION_CAPABILITY,
        password="qualification-password",
    )

    disable_login_roles(_as_connection_factory(factory), expected_database="role_test")
    result = disable_login_roles(_as_connection_factory(factory), expected_database="role_test")

    assert result == {"database": "role_test", "status": "disabled"}
    assert factory.roles[RUNTIME_LOGIN]["can_login"] is False
    assert factory.roles[QUALIFICATION_LOGIN]["can_login"] is False
    assert factory.roles[RUNTIME_LOGIN]["memberships"] == {RUNTIME_CAPABILITY}
    assert factory.roles[QUALIFICATION_LOGIN]["memberships"] == {QUALIFICATION_CAPABILITY}


def test_disable_accepts_one_role_already_nologin_and_disables_the_other() -> None:
    from polyarb.control_plane.db_role_admin import disable_login_roles

    factory = FakeAdminFactory()
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="runtime-password",
    )
    factory.add_login(
        QUALIFICATION_LOGIN,
        capability=QUALIFICATION_CAPABILITY,
        password="qualification-password",
    )
    factory.roles[RUNTIME_LOGIN]["can_login"] = False

    result = disable_login_roles(_as_connection_factory(factory), expected_database="role_test")

    assert result == {"database": "role_test", "status": "disabled"}
    assert factory.roles[RUNTIME_LOGIN]["can_login"] is False
    assert factory.roles[QUALIFICATION_LOGIN]["can_login"] is False


def test_provision_accepts_clean_nologin_roles_and_restores_login() -> None:
    from polyarb.control_plane.db_role_admin import provision_login_roles

    factory = FakeAdminFactory()
    factory.add_login(
        RUNTIME_LOGIN,
        capability=RUNTIME_CAPABILITY,
        password="old-runtime-password",
    )
    factory.add_login(
        QUALIFICATION_LOGIN,
        capability=QUALIFICATION_CAPABILITY,
        password="old-qualification-password",
    )
    factory.roles[RUNTIME_LOGIN]["can_login"] = False
    factory.roles[QUALIFICATION_LOGIN]["can_login"] = False

    result = provision_login_roles(
        _as_connection_factory(factory),
        expected_database="role_test",
        runtime_password="new-runtime-password",
        qualification_password="new-qualification-password",
    )

    assert result == {"database": "role_test", "status": "provisioned"}
    assert factory.roles[RUNTIME_LOGIN]["can_login"] is True
    assert factory.roles[QUALIFICATION_LOGIN]["can_login"] is True
    assert factory.roles[RUNTIME_LOGIN]["password"] == "new-runtime-password"
    assert factory.roles[QUALIFICATION_LOGIN]["password"] == "new-qualification-password"
    assert factory.roles[RUNTIME_LOGIN]["memberships"] == {RUNTIME_CAPABILITY}
    assert factory.roles[QUALIFICATION_LOGIN]["memberships"] == {QUALIFICATION_CAPABILITY}


def test_cli_requires_enable_and_reads_passwords_from_exact_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from polyarb.control_plane import db_role_admin

    calls: list[tuple[str, str, str]] = []

    def provision(
        _factory: object,
        *,
        expected_database: str,
        runtime_password: str,
        qualification_password: str,
    ) -> dict[str, str]:
        calls.append((expected_database, runtime_password, qualification_password))
        return {"database": expected_database, "status": "provisioned"}

    monkeypatch.setattr(db_role_admin, "provision_login_roles", provision)
    monkeypatch.setenv(
        "POLYARB_CONTROL_PLANE_DB_ADMIN_DSN",
        "postgresql://admin:secret@example/role_test",
    )
    monkeypatch.setenv("POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD", "runtime-env-secret")
    monkeypatch.setenv("POLYARB_QUALIFICATION_WORKER_DB_PASSWORD", "qualification-env-secret")
    monkeypatch.setenv("POLYARB_RUNTIME_PASSWORD", "wrong-env-secret")

    assert db_role_admin.main(["provision", "--expected-database", "role_test", "--json"]) == 2
    assert calls == []
    assert db_role_admin.main(["disable", "--expected-database", "role_test", "--json"]) == 2
    assert calls == []

    assert (
        db_role_admin.main(
            [
                "provision",
                "--enable",
                "--expected-database",
                "role_test",
                "--json",
            ]
        )
        == 0
    )

    assert calls == [("role_test", "runtime-env-secret", "qualification-env-secret")]
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"database": "role_test", "status": "provisioned"}
    assert "runtime-env-secret" not in captured.out + captured.err
    assert "qualification-env-secret" not in captured.out + captured.err
    assert "postgresql://" not in captured.out + captured.err


def test_admin_and_daemon_dsn_constants_are_distinct() -> None:
    from polyarb.control_plane import db_role_admin

    assert db_role_admin.ADMIN_DSN_ENV == "POLYARB_CONTROL_PLANE_DB_ADMIN_DSN"
    assert db_role_admin.RUNTIME_DSN_ENV == "POLYARB_SUPABASE_DB_DSN"
    assert db_role_admin.QUALIFICATION_DSN_ENV == "POLYARB_QUALIFICATION_DB_DSN"
    assert (
        len(
            {
                db_role_admin.ADMIN_DSN_ENV,
                db_role_admin.RUNTIME_DSN_ENV,
                db_role_admin.QUALIFICATION_DSN_ENV,
            }
        )
        == 3
    )


def test_cli_admin_to_scoped_handoff_reconnects_admin_for_disable_without_leaks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from polyarb.control_plane import db_role_admin

    admin_dsn = "postgresql://admin:admin-secret@example.test/role_test"
    runtime_dsn = "postgresql://runtime:runtime-secret@example.test/role_test"
    qualification_dsn = "postgresql://qualification:qualification-secret@example.test/role_test"
    factories: dict[str, object] = {}
    connected: list[str] = []
    operations: list[tuple[str, object, str | None]] = []

    def connection_factory(dsn: str) -> object:
        connected.append(dsn)
        return factories.setdefault(dsn, object())

    def preflight(factory: object, *, expected_database: str) -> dict[str, str]:
        operations.append(("preflight", factory, None))
        return {"database": expected_database, "status": "ready"}

    def provision(
        factory: object,
        *,
        expected_database: str,
        runtime_password: str,
        qualification_password: str,
    ) -> dict[str, str]:
        assert runtime_password == "runtime-password"
        assert qualification_password == "qualification-password"
        operations.append(("provision", factory, None))
        return {"database": expected_database, "status": "provisioned"}

    def verify(
        factory: object,
        profile: str,
        *,
        expected_database: str,
    ) -> db_role_admin.DatabaseRoleVerification:
        operations.append(("verify", factory, profile))
        return db_role_admin.DatabaseRoleVerification(
            profile=profile,
            session_user=f"{profile}-login",
            capability_role=f"{profile}-capability",
            database_name=expected_database,
        )

    def disable(factory: object, *, expected_database: str) -> dict[str, str]:
        operations.append(("disable", factory, None))
        return {"database": expected_database, "status": "disabled"}

    monkeypatch.setattr(db_role_admin, "_connection_factory_from_dsn", connection_factory)
    monkeypatch.setattr(db_role_admin, "preflight_capability_roles", preflight)
    monkeypatch.setattr(db_role_admin, "provision_login_roles", provision)
    monkeypatch.setattr(db_role_admin, "verify_daemon_database_role", verify)
    monkeypatch.setattr(db_role_admin, "disable_login_roles", disable)
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_DB_ADMIN_DSN", admin_dsn)
    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", runtime_dsn)
    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", qualification_dsn)
    monkeypatch.setenv("POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD", "runtime-password")
    monkeypatch.setenv("POLYARB_QUALIFICATION_WORKER_DB_PASSWORD", "qualification-password")

    invocations = (
        ["preflight", "--expected-database", "role_test", "--json"],
        ["provision", "--enable", "--expected-database", "role_test", "--json"],
        [
            "verify",
            "--profile",
            "runtime-controller",
            "--expected-database",
            "role_test",
            "--json",
        ],
        [
            "verify",
            "--profile",
            "qualification-worker",
            "--expected-database",
            "role_test",
            "--json",
        ],
        ["disable", "--enable", "--expected-database", "role_test", "--json"],
    )
    assert [db_role_admin.main(args) for args in invocations] == [0, 0, 0, 0, 0]

    assert connected == [admin_dsn, admin_dsn, runtime_dsn, qualification_dsn, admin_dsn]
    assert [operation[0] for operation in operations] == [
        "preflight",
        "provision",
        "verify",
        "verify",
        "disable",
    ]
    assert operations[0][1] is factories[admin_dsn]
    assert operations[1][1] is factories[admin_dsn]
    assert operations[2][1] is factories[runtime_dsn]
    assert operations[3][1] is factories[qualification_dsn]
    assert operations[4][1] is factories[admin_dsn]
    captured = capsys.readouterr()
    output = captured.out + captured.err
    for secret in (admin_dsn, runtime_dsn, qualification_dsn, "password"):
        assert secret not in output


def test_runbook_and_templates_keep_admin_dsn_out_of_app_secrets() -> None:
    runbook = (PROJECT_ROOT / "docs/dev/control-plane-runbook.md").read_text()
    runtime = (
        PROJECT_ROOT / "deploy/control-plane/fly-runtime-controller.toml.template"
    ).read_text()
    qualification = (
        PROJECT_ROOT / "deploy/control-plane/fly-qualification-worker.toml.template"
    ).read_text()

    assert "POLYARB_CONTROL_PLANE_DB_ADMIN_DSN" in runbook
    assert "POLYARB_SUPABASE_DB_DSN" in runbook
    assert "POLYARB_QUALIFICATION_DB_DSN" in runbook
    assert "POLYARB_CONTROL_PLANE_DB_ADMIN_DSN" not in runtime
    assert "POLYARB_CONTROL_PLANE_DB_ADMIN_DSN" not in qualification
    assert "POLYARB_SUPABASE_DB_DSN" in runtime
    assert "POLYARB_QUALIFICATION_DB_DSN" not in runtime
    assert "POLYARB_QUALIFICATION_DB_DSN" in qualification
    assert "POLYARB_SUPABASE_DB_DSN" not in qualification


def test_cli_verify_selects_profile_scoped_dsn_and_delegates(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from polyarb.control_plane import db_role_admin

    runtime_factory = object()
    qualification_factory = object()
    calls: list[tuple[object, str, str]] = []

    def connection_factory(dsn: str):
        if "runtime" in dsn:
            return runtime_factory
        if "qualification" in dsn:
            return qualification_factory
        raise AssertionError(f"unexpected DSN: {dsn}")

    def verify(factory: object, profile: str, *, expected_database: str):
        calls.append((factory, profile, expected_database))
        return db_role_admin.DatabaseRoleVerification(
            profile=profile,
            session_user=f"{profile}-login",
            capability_role=f"{profile}-capability",
            database_name=expected_database,
        )

    monkeypatch.setattr(db_role_admin, "_connection_factory_from_dsn", connection_factory)
    monkeypatch.setattr(db_role_admin, "verify_daemon_database_role", verify)
    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://runtime:secret@example/db")
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_DB_DSN",
        "postgresql://qualification:secret@example/db",
    )

    assert (
        db_role_admin.main(
            [
                "verify",
                "--profile",
                "qualification-worker",
                "--expected-database",
                "role_test",
                "--json",
            ]
        )
        == 0
    )

    assert calls == [(qualification_factory, "qualification-worker", "role_test")]
    captured = capsys.readouterr()
    assert "postgresql://" not in captured.out + captured.err
    assert json.loads(captured.out)["status"] == "pass"


def test_cli_verify_rejects_deprecated_daemon_dsn_aliases(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from polyarb.control_plane import db_role_admin

    monkeypatch.delenv("POLYARB_SUPABASE_DB_DSN", raising=False)
    monkeypatch.delenv("POLYARB_QUALIFICATION_DB_DSN", raising=False)
    monkeypatch.setenv(
        "POLYARB_RUNTIME_CONTROLLER_DB_DSN",
        "postgresql://deprecated-runtime:secret@example/db",
    )
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_WORKER_DB_DSN",
        "postgresql://deprecated-qualification:secret@example/db",
    )

    assert (
        db_role_admin.main(
            [
                "verify",
                "--profile",
                "qualification-worker",
                "--expected-database",
                "role_test",
            ]
        )
        == 2
    )
    captured = capsys.readouterr()
    assert "POLYARB_QUALIFICATION_DB_DSN" in captured.err
    assert "postgresql://" not in captured.err


def test_real_postgres_provisions_verifies_rotates_and_disables_roles(
    postgres_026_dsn: str,
) -> None:
    from polyarb.control_plane.db_role_admin import (
        disable_login_roles,
        provision_login_roles,
    )
    from polyarb.control_plane.db_role_contract import (
        scoped_connection_factory,
        verify_daemon_database_role,
    )

    def admin_factory() -> psycopg.Connection[object]:
        return psycopg.connect(postgres_026_dsn)

    provision_login_roles(
        admin_factory,
        expected_database="test",
        runtime_password="runtime-real-secret-a",
        qualification_password="qualification-real-secret-a",
    )

    runtime_dsn = _role_dsn(postgres_026_dsn, RUNTIME_LOGIN, "runtime-real-secret-a")
    qualification_dsn = _role_dsn(
        postgres_026_dsn,
        QUALIFICATION_LOGIN,
        "qualification-real-secret-a",
    )
    assert (
        verify_daemon_database_role(
            scoped_connection_factory(runtime_dsn),
            "runtime-controller",
            expected_database="test",
        ).status
        == "pass"
    )
    assert (
        verify_daemon_database_role(
            scoped_connection_factory(qualification_dsn),
            "qualification-worker",
            expected_database="test",
        ).status
        == "pass"
    )

    with psycopg.connect(postgres_026_dsn) as admin:
        assert _role_attributes(admin, RUNTIME_LOGIN) == (True, False, False, False, True)
        assert _role_attributes(admin, QUALIFICATION_LOGIN) == (True, False, False, False, True)
        assert _memberships(admin, RUNTIME_LOGIN) == [RUNTIME_CAPABILITY]
        assert _memberships(admin, QUALIFICATION_LOGIN) == [QUALIFICATION_CAPABILITY]
    provision_login_roles(
        admin_factory,
        expected_database="test",
        runtime_password="runtime-real-secret-b",
        qualification_password="qualification-real-secret-a",
    )

    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(runtime_dsn).close()
    assert (
        verify_daemon_database_role(
            scoped_connection_factory(
                _role_dsn(postgres_026_dsn, RUNTIME_LOGIN, "runtime-real-secret-b")
            ),
            "runtime-controller",
            expected_database="test",
        ).status
        == "pass"
    )
    assert psycopg.connect(qualification_dsn).close() is None

    disable_login_roles(admin_factory, expected_database="test")

    with psycopg.connect(postgres_026_dsn) as admin:
        assert _role_attributes(admin, RUNTIME_LOGIN)[0] is False
        assert _role_attributes(admin, QUALIFICATION_LOGIN)[0] is False
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(_role_dsn(postgres_026_dsn, RUNTIME_LOGIN, "runtime-real-secret-b")).close()
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(qualification_dsn).close()

    disable_login_roles(admin_factory, expected_database="test")

    provision_login_roles(
        admin_factory,
        expected_database="test",
        runtime_password="runtime-real-secret-c",
        qualification_password="qualification-real-secret-c",
    )

    restored_runtime_dsn = _role_dsn(
        postgres_026_dsn,
        RUNTIME_LOGIN,
        "runtime-real-secret-c",
    )
    restored_qualification_dsn = _role_dsn(
        postgres_026_dsn,
        QUALIFICATION_LOGIN,
        "qualification-real-secret-c",
    )
    assert (
        verify_daemon_database_role(
            scoped_connection_factory(restored_runtime_dsn),
            "runtime-controller",
            expected_database="test",
        ).status
        == "pass"
    )
    assert (
        verify_daemon_database_role(
            scoped_connection_factory(restored_qualification_dsn),
            "qualification-worker",
            expected_database="test",
        ).status
        == "pass"
    )
    with psycopg.connect(postgres_026_dsn) as admin:
        assert _role_attributes(admin, RUNTIME_LOGIN) == (True, False, False, False, True)
        assert _role_attributes(admin, QUALIFICATION_LOGIN) == (True, False, False, False, True)
        assert _memberships(admin, RUNTIME_LOGIN) == [RUNTIME_CAPABILITY]
        assert _memberships(admin, QUALIFICATION_LOGIN) == [QUALIFICATION_CAPABILITY]


def test_real_cli_admin_scoped_verify_admin_disable_handoff(
    postgres_026_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from polyarb.control_plane import db_role_admin

    runtime_password = "runtime-real-cli-secret"
    qualification_password = "qualification-real-cli-secret"
    admin_dsn = postgres_026_dsn
    runtime_dsn = _role_dsn(postgres_026_dsn, RUNTIME_LOGIN, runtime_password)
    qualification_dsn = _role_dsn(
        postgres_026_dsn,
        QUALIFICATION_LOGIN,
        qualification_password,
    )
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_DB_ADMIN_DSN", admin_dsn)
    monkeypatch.setenv("POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD", runtime_password)
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_WORKER_DB_PASSWORD",
        qualification_password,
    )

    assert db_role_admin.main(["preflight", "--expected-database", "test", "--json"]) == 0
    assert (
        db_role_admin.main(["provision", "--enable", "--expected-database", "test", "--json"]) == 0
    )
    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", runtime_dsn)
    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", qualification_dsn)
    for profile in ("runtime-controller", "qualification-worker"):
        assert (
            db_role_admin.main(
                [
                    "verify",
                    "--profile",
                    profile,
                    "--expected-database",
                    "test",
                    "--json",
                ]
            )
            == 0
        )
    assert db_role_admin.main(["disable", "--enable", "--expected-database", "test", "--json"]) == 0

    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(runtime_dsn).close()
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(qualification_dsn).close()
    captured = capsys.readouterr()
    output = captured.out + captured.err
    for secret in (
        admin_dsn,
        runtime_dsn,
        qualification_dsn,
        runtime_password,
        qualification_password,
    ):
        assert secret not in output


@pytest.fixture(scope="module")
def postgres_026_dsn() -> Iterator[str]:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real admin role operations")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        _create_supabase_roles(dsn)
        _run_alembic(dsn, "upgrade", "026")
        yield dsn


def _docker_available() -> bool:
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _normalize_dsn(dsn: str) -> str:
    return dsn.replace("postgresql+psycopg2://", "postgresql://")


def _create_supabase_roles(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        for role in ("anon", "authenticated", "service_role"):
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=PROJECT_ROOT,
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def _role_dsn(dsn: str, username: str, password: str) -> str:
    parts = urlsplit(dsn)
    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    return urlunsplit(
        (
            parts.scheme,
            f"{quote(username)}:{quote(password)}@{host}",
            parts.path,
            parts.query,
            parts.fragment,
        )
    )


def _role_attributes(
    connection: psycopg.Connection[object],
    role: str,
) -> tuple[bool, bool, bool, bool, bool]:
    row = connection.execute(
        """
        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit
        FROM pg_catalog.pg_roles
        WHERE rolname = %s
        """,
        (role,),
    ).fetchone()
    assert row is not None
    values = cast(tuple[object, ...], row)
    return (
        bool(values[0]),
        bool(values[1]),
        bool(values[2]),
        bool(values[3]),
        bool(values[4]),
    )


def _memberships(connection: psycopg.Connection[object], role: str) -> list[str]:
    return [
        str(cast(tuple[object, ...], row)[0])
        for row in connection.execute(
            """
            SELECT granted.rolname
            FROM pg_catalog.pg_auth_members AS membership
            JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
            JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
            WHERE member.rolname = %s
            ORDER BY granted.rolname
            """,
            (role,),
        ).fetchall()
    ]
