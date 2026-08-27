"""Fail-closed database identity contract for daemon roles."""

from __future__ import annotations

import inspect
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import pytest


class FakeCursor:
    def __init__(self, factory: FakeRoleFactory) -> None:
        self._factory = factory
        self._rows: list[Any] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: object, params: object = ()) -> FakeCursor:
        sql = str(statement)
        normalized = " ".join(sql.lower().split())
        if normalized.startswith(("insert ", "update ", "delete ", "truncate ", "alter ")):
            self._factory.write_count += 1
        self._factory.calls.append((normalized, params))
        answer = self._factory.answer(normalized, params)
        self._rows = answer if isinstance(answer, list) else [answer]
        return self

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[Any]:
        return self._rows


class FakeConnection:
    def __init__(self, factory: FakeRoleFactory) -> None:
        self._factory = factory

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._factory)

    def transaction(self, **options: object) -> FakeConnection:
        self._factory.transactions.append(options)
        return self


class FakeRoleFactory:
    def __init__(
        self,
        *,
        failure_code: str | None = None,
        profile: str = "runtime-controller",
        role_attribute_failure: str | None = None,
        sequence_privilege_source: str | None = None,
        authority_failure: str | None = None,
    ):
        self.failure_code = failure_code
        self.profile = profile
        self.role_attribute_failure = role_attribute_failure
        self.sequence_privilege_source = sequence_privilege_source
        self.authority_failure = authority_failure
        self.write_count = 0
        self.calls: list[tuple[str, object]] = []
        self.transactions: list[dict[str, object]] = []
        self.expected_database = "role_test"
        self.session_user = {
            "runtime-controller": "m1_runtime_controller_login",
            "qualification-worker": "m1_qualification_worker_login",
        }[profile]
        self.capability_role = {
            "runtime-controller": "m1_runtime_controller_capability",
            "qualification-worker": "m1_qualification_worker_capability",
        }[profile]
        self.other_capability_role = {
            "runtime-controller": "m1_qualification_worker_capability",
            "qualification-worker": "m1_runtime_controller_capability",
        }[profile]

    def __call__(self) -> FakeConnection:
        return FakeConnection(self)

    def answer(self, sql: str, params: object) -> object:
        if sql == "set transaction read only":
            return None
        if "select current_setting('search_path')" in sql:
            return ("pg_catalog,public", ["pg_catalog", "public"])
        if "from pg_catalog.pg_db_role_setting" in sql:
            if self.authority_failure == "database-search-path":
                return [(["search_path=evil, public"],)]
            return []
        if "select namespace.nspname" in sql and "from pg_catalog.pg_namespace" in sql:
            names = ["public"]
            if self.authority_failure == "nonpublic-schema":
                names.append("evil")
            return [(name,) for name in names]
        if "as relation_name" in sql and "pg_catalog.pg_class" in sql:
            names = list(_allowed_table_names(self.profile))
            names.append("unrelated_authority")
            rows = [(f"public.{name}", name, "public") for name in sorted(names)]
            if self.authority_failure == "nonpublic-relation":
                rows.append(("evil.shadow", "shadow", "evil"))
            if (
                self.authority_failure == "ambient-unreachable-relation"
                and "has_schema_privilege" not in sql
            ):
                rows.append(
                    (
                        "extensions.pg_stat_statements",
                        "pg_stat_statements",
                        "extensions",
                    )
                )
            return rows
        if "routine.prosecdef" in sql and "pg_catalog.pg_proc" in sql:
            return [
                (signature,)
                for signature in (
                    "public.l3_retention_cleanup(timestamptz,timestamptz,timestamptz)",
                    "public.m1_insert_qualification_certificate("
                    "text,text,text,text,jsonb,timestamptz,timestamptz,jsonb,"
                    "text,text,text,text)",
                    "public.m1_project_incident_qualification_ingress()",
                    "public.m1_project_recovery_qualification_ingress()",
                    "public.m1_project_runtime_qualification_ingress()",
                    "public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)",
                    "public.m1_record_qualification_ingress(text,text,text,timestamptz,jsonb)",
                    "public.m1_verify_qualification_certificate_insert()",
                )
            ]
        if "owner.rolname" in sql and "pg_catalog.pg_namespace" in sql:
            if self.authority_failure == "owned-object":
                return [("table:public.unrelated_owned",)]
            return []
        if "from pg_catalog.pg_class as sequence" in sql:
            return [("public.m1_qualification_ingress_ledger_ingest_seq",)]
        if "select pg_get_serial_sequence" in sql:
            table = str(_param(params, 0))
            column = str(_param(params, 1))
            if (table, column) == ("public.m1_qualification_ingress_ledger", "ingest_seq"):
                return ("public.m1_qualification_ingress_ledger_ingest_seq",)
            return (None,)
        if "where role.rolname = %s" in sql:
            role_name = str(_param(params, 0))
            can_login = role_name == self.session_user
            inherits = role_name == self.session_user
            unsafe = False
            if self.role_attribute_failure == "login-nologin" and role_name == self.session_user:
                can_login = False
            if self.role_attribute_failure == "login-noinherit" and role_name == self.session_user:
                inherits = False
            if (
                self.role_attribute_failure == "capability-login"
                and role_name == self.capability_role
            ):
                can_login = True
            if (
                self.role_attribute_failure == "capability-inherit"
                and role_name == self.capability_role
            ):
                inherits = True
            if (
                self.role_attribute_failure == "capability-unsafe"
                and role_name == self.capability_role
            ):
                unsafe = True
            settings = (
                ["search_path=evil, public"]
                if self.authority_failure == "role-search-path" and role_name == self.session_user
                else None
            )
            return (
                role_name,
                can_login,
                unsafe,
                False,
                False,
                inherits,
                False,
                False,
                settings,
            )
        if "from pg_catalog.pg_roles as role" in sql:
            database = (
                "wrong_database"
                if self.failure_code == "database-role.login-mismatch"
                else self.expected_database
            )
            session_user = (
                "wrong_login"
                if self.failure_code == "database-role.login-mismatch"
                else self.session_user
            )
            unsafe = self.failure_code == "database-role.unsafe-attribute"
            return (
                database,
                session_user,
                self.capability_role,
                unsafe,
                False,
                False,
                False,
                False,
            )
        if "from pg_catalog.pg_roles as inherited_role" in sql:
            if self.failure_code == "database-role.cross-capability":
                return (self.other_capability_role,)
            return None
        if "from pg_catalog.pg_auth_members" in sql:
            if "where granted.rolname = %s" in sql:
                row = (
                    self.session_user,
                    self.authority_failure == "membership-options",
                    True,
                    True,
                )
                rows = [row]
                if self.authority_failure == "unexpected-member":
                    rows.append(("unexpected_member", False, True, True))
                return rows
            if "where member.rolname = %s" in sql:
                return []
        if "pg_has_role" in sql:
            role = _param(params, 1)
            if role == self.capability_role:
                return (self.failure_code != "database-role.capability-missing",)
            return (self.failure_code == "database-role.cross-capability",)
        if "has_database_privilege" in sql:
            privilege = str(_param(params, 2)).upper()
            if privilege == "CREATE":
                return (self.authority_failure == "database-create",)
            return (True,)
        if "has_schema_privilege" in sql:
            schema = str(_param(params, 1))
            privilege = str(_param(params, 2))
            if schema != "public":
                return (self.authority_failure == "nonpublic-schema" and privilege == "USAGE",)
            if privilege == "CREATE":
                return (self.authority_failure == "schema-create",)
            return (True,)
        if "has_table_privilege" in sql:
            privilege = str(_param(params, 2))
            table = str(_param(params, 1)).removeprefix("public.")
            allowed = privilege in _allowed_table_privileges(self.profile, table)
            if str(_param(params, 1)).startswith("extensions."):
                allowed = (
                    self.authority_failure == "ambient-unreachable-relation"
                    and privilege == "SELECT"
                )
            if str(_param(params, 1)).startswith("evil."):
                allowed = self.authority_failure == "nonpublic-relation" and privilege == "SELECT"
            if table == "unrelated_authority" and privilege == "SELECT":
                allowed = self.authority_failure == "unrelated-relation"
            if self.failure_code == "database-role.required-privilege-missing" and allowed:
                return (False,)
            if self.failure_code == "database-role.forbidden-privilege-present" and not allowed:
                return (True,)
            return (allowed,)
        if "has_function_privilege" in sql:
            function_signature = str(_param(params, 1))
            allowed = function_signature in _allowed_functions(self.profile)
            if function_signature.startswith("public.l3_retention_cleanup"):
                allowed = self.authority_failure == "security-definer-execute"
            if self.failure_code == "database-role.required-privilege-missing" and allowed:
                return (False,)
            if self.failure_code == "database-role.forbidden-privilege-present" and not allowed:
                return (True,)
            return (allowed,)
        if "has_sequence_privilege" in sql:
            sequence = str(_param(params, 1))
            privilege = str(_param(params, 2))
            if self.sequence_privilege_source and privilege == "USAGE":
                return (sequence in _forbidden_sequences(self.profile),)
            return (False,)
        raise AssertionError(f"unexpected verifier query: {sql}")


def _param(params: object, index: int) -> object:
    assert isinstance(params, tuple)
    return params[index]


def _allowed_table_privileges(profile: str, table: str) -> frozenset[str]:
    allowed: dict[str, dict[str, frozenset[str]]] = {
        "runtime-controller": {
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
        },
        "qualification-worker": {
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
        },
    }
    return allowed[profile].get(table, frozenset())


def _allowed_table_names(profile: str) -> Iterable[str]:
    if profile == "runtime-controller":
        return (
            "m1_runtime_controller_leases",
            "m1_runtime_observe_decisions",
            "m1_job_runtime_state",
            "m1_jobs",
            "m1_job_circuits",
            "m1_job_attempts",
            "m1_recovery_target_budgets",
            "m1_recovery_actions",
            "m1_job_runtime_events",
            "m1_incidents",
            "m1_incident_events",
            "m1_alert_outbox",
        )
    return (
        "m1_qualification_ingress_ledger",
        "m1_qualification_source_cursors",
        "m1_qualification_epochs",
        "m1_qualification_recovery_observations",
        "m1_qualification_certificates",
        "m1_publication_pointers",
        "m1_generation_manifests",
        "m1_opportunity_publication_pointers",
        "m1_opportunity_projections",
        "m1_job_runtime_events",
        "m1_incidents",
        "m1_incident_events",
        "m1_recovery_actions",
        "m1_alert_outbox",
    )


def _allowed_functions(profile: str) -> Iterable[str]:
    if profile != "qualification-worker":
        return ()
    return (
        "public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)",
        "public.m1_insert_qualification_certificate("
        "text,text,text,text,jsonb,timestamptz,timestamptz,jsonb,text,text,text,text)",
    )


def _forbidden_sequences(profile: str) -> frozenset[str]:
    return frozenset({"public.m1_qualification_ingress_ledger_ingest_seq"})


def test_database_role_contract_accepts_exact_runtime_controller_identity() -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory()

    verification = verify_daemon_database_role(
        cast(ConnectionFactory, factory),
        "runtime-controller",
        expected_database="role_test",
    )

    assert verification.status == "pass"
    assert verification.profile == "runtime-controller"
    assert verification.session_user == "m1_runtime_controller_login"
    assert verification.capability_role == "m1_runtime_controller_capability"
    assert verification.database_name == "role_test"
    assert factory.write_count == 0
    assert factory.transactions == [{}]
    assert any(call[0] == "set transaction read only" for call in factory.calls)


def test_database_role_contract_accepts_exact_qualification_worker_identity() -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(profile="qualification-worker")

    verification = verify_daemon_database_role(
        cast(ConnectionFactory, factory),
        "qualification-worker",
        expected_database="role_test",
    )

    assert verification.status == "pass"
    assert verification.session_user == "m1_qualification_worker_login"
    assert verification.capability_role == "m1_qualification_worker_capability"
    assert factory.write_count == 0


def test_database_role_contract_ignores_ambient_acl_in_unreachable_schema() -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(authority_failure="ambient-unreachable-relation")

    verification = verify_daemon_database_role(
        cast(ConnectionFactory, factory),
        "runtime-controller",
        expected_database="role_test",
    )

    assert verification.status == "pass"
    reachable_catalog_queries = [
        call
        for call in factory.calls
        if any(
            marker in call[0]
            for marker in (
                "as relation_name",
                "pg_catalog.pg_class as sequence",
                "routine.prosecdef",
            )
        )
    ]
    assert len(reachable_catalog_queries) == 3
    assert all("has_schema_privilege" in call[0] for call in reachable_catalog_queries)
    assert all(call[1] == (factory.session_user,) for call in reachable_catalog_queries)


@pytest.mark.parametrize(
    "failure_code",
    (
        "database-role.login-mismatch",
        "database-role.capability-missing",
        "database-role.cross-capability",
        "database-role.unsafe-attribute",
        "database-role.required-privilege-missing",
        "database-role.forbidden-privilege-present",
    ),
)
def test_database_role_contract_fails_closed(failure_code: str) -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        DatabaseRoleContractError,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(failure_code=failure_code)

    with pytest.raises(DatabaseRoleContractError, match=failure_code):
        verify_daemon_database_role(
            cast(ConnectionFactory, factory),
            "runtime-controller",
            expected_database="role_test",
        )

    assert factory.write_count == 0


def test_database_role_contract_sanitizes_database_errors() -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        DatabaseRoleContractError,
        verify_daemon_database_role,
    )

    class BoomFactory:
        write_count = 0

        def __call__(self) -> FakeConnection:
            raise RuntimeError("postgresql://operator:secret@example.test/control exploded")

    with pytest.raises(DatabaseRoleContractError) as exc_info:
        verify_daemon_database_role(
            cast(ConnectionFactory, BoomFactory()),
            "runtime-controller",
            expected_database="role_test",
        )

    message = str(exc_info.value)
    assert "database-role.unavailable" in message
    assert "postgresql://" not in message
    assert "secret" not in message


@pytest.mark.parametrize(
    "profile,source",
    (
        ("runtime-controller", "public"),
        ("runtime-controller", "inherited"),
        ("qualification-worker", "public"),
        ("qualification-worker", "inherited"),
    ),
)
def test_database_role_contract_rejects_forbidden_sequence_privilege(
    profile: str,
    source: str,
) -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        DatabaseRoleContractError,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(profile=profile, sequence_privilege_source=source)

    with pytest.raises(
        DatabaseRoleContractError,
        match="database-role.forbidden-privilege-present",
    ):
        verify_daemon_database_role(
            cast(ConnectionFactory, factory),
            profile,
            expected_database="role_test",
        )

    assert factory.write_count == 0
    assert any("has_sequence_privilege" in call[0] for call in factory.calls)


def test_runtime_sequence_denial_uses_public_catalog_enumeration() -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        DatabaseRoleContractError,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(
        profile="runtime-controller",
        sequence_privilege_source="public",
    )

    with pytest.raises(
        DatabaseRoleContractError,
        match="database-role.forbidden-privilege-present",
    ) as exc_info:
        verify_daemon_database_role(
            cast(ConnectionFactory, factory),
            "runtime-controller",
            expected_database="role_test",
        )

    assert "application-sequence" in str(exc_info.value)
    assert factory.write_count == 0
    sequence_queries = [
        call for call in factory.calls if "pg_catalog.pg_class as sequence" in call[0]
    ]
    assert len(sequence_queries) == 1
    assert sequence_queries[0][1] == (factory.session_user,)


@pytest.mark.parametrize(
    "role_attribute_failure,reason_code",
    (
        ("login-nologin", "database-role.login-attribute"),
        ("login-noinherit", "database-role.login-attribute"),
        ("capability-login", "database-role.capability-attribute"),
        ("capability-inherit", "database-role.capability-attribute"),
        ("capability-unsafe", "database-role.capability-attribute"),
    ),
)
def test_database_role_contract_rejects_role_attribute_violations(
    role_attribute_failure: str,
    reason_code: str,
) -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        DatabaseRoleContractError,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(role_attribute_failure=role_attribute_failure)

    with pytest.raises(DatabaseRoleContractError, match=reason_code):
        verify_daemon_database_role(
            cast(ConnectionFactory, factory),
            "runtime-controller",
            expected_database="role_test",
        )

    assert factory.write_count == 0


@pytest.mark.parametrize(
    "authority_failure,reason_code",
    (
        ("unrelated-relation", "database-role.forbidden-privilege-present"),
        ("security-definer-execute", "database-role.forbidden-privilege-present"),
        ("schema-create", "database-role.forbidden-privilege-present"),
        ("owned-object", "database-role.forbidden-privilege-present"),
        ("database-create", "database-role.forbidden-privilege-present"),
        ("nonpublic-schema", "database-role.forbidden-privilege-present"),
        ("nonpublic-relation", "database-role.forbidden-privilege-present"),
        ("membership-options", "database-role.forbidden-privilege-present"),
        ("role-search-path", "database-role.namespace-unsafe"),
        ("database-search-path", "database-role.namespace-unsafe"),
    ),
)
def test_database_role_contract_rejects_uncatalogued_effective_authority(
    authority_failure: str,
    reason_code: str,
) -> None:
    from polyarb.control_plane.db_role_contract import (
        ConnectionFactory,
        DatabaseRoleContractError,
        verify_daemon_database_role,
    )

    factory = FakeRoleFactory(authority_failure=authority_failure)

    with pytest.raises(
        DatabaseRoleContractError,
        match=reason_code,
    ):
        verify_daemon_database_role(
            cast(ConnectionFactory, factory),
            "runtime-controller",
            expected_database="role_test",
        )

    assert factory.write_count == 0


@pytest.mark.parametrize(
    "dsn",
    (
        "postgresql://runtime:secret@example.test/role_test?options=-csearch_path%3Devil",
        "postgresql://runtime:secret@example.test/role_test?search_path=evil,public",
        "host=example.test dbname=role_test user=runtime options='-c search_path=evil'",
    ),
)
def test_scoped_connection_factory_rejects_dsn_namespace_override_before_connect(
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
) -> None:
    from polyarb.control_plane import db_role_contract

    calls: list[object] = []
    monkeypatch.setattr(
        db_role_contract.psycopg,
        "connect",
        lambda *args, **kwargs: calls.append(args),
    )

    with pytest.raises(
        db_role_contract.DatabaseRoleContractError,
        match="database-role.dsn-override",
    ):
        db_role_contract.scoped_connection_factory(dsn)

    assert calls == []


def test_scoped_connection_factory_binds_controlled_search_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane import db_role_contract

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    sentinel = object()

    def connect(*args: object, **kwargs: object) -> object:
        calls.append((args, kwargs))
        return sentinel

    monkeypatch.setattr(db_role_contract.psycopg, "connect", connect)
    factory = db_role_contract.scoped_connection_factory(
        "postgresql://runtime:secret@example.test/role_test"
    )

    assert factory() is sentinel
    assert calls == [
        (
            ("postgresql://runtime:secret@example.test/role_test",),
            {
                "connect_timeout": 5,
                "options": "-csearch_path=pg_catalog,public",
            },
        )
    ]


def test_database_role_contract_declares_namespace_and_pg16_membership_closure() -> None:
    source = Path("src/polyarb/control_plane/db_role_contract.py").read_text()
    for fragment in (
        "pg_db_role_setting",
        "rolconfig",
        "admin_option",
        "inherit_option",
        "set_option",
        "has_database_privilege",
        "TEMPORARY",
    ):
        assert fragment in source


def test_daemon_database_sql_is_public_schema_qualified() -> None:
    unqualified = re.compile(
        r"(?im)\b(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?|"
        r"TABLE|FUNCTION)\s+(?!public\.)(?:m1_|l3_)"
    )
    paths = (
        Path("src/polyarb/control_plane/runtime_observe.py"),
        Path("src/polyarb/control_plane/recovery_store.py"),
        Path("src/polyarb/control_plane/runtime_store.py"),
        Path("src/polyarb/control_plane/qualification_service.py"),
        Path("src/polyarb/control_plane/qualification_store.py"),
    )
    bare_application_identifier = re.compile(r"(?<!public\.)\bm1_[a-z0-9_]+")
    failures = {
        path.as_posix(): sorted(
            set(match.group(0) for match in unqualified.finditer(path.read_text()))
        )
        for path in paths
        if unqualified.search(path.read_text())
    }
    assert failures == {}
    bare_failures = {
        path.as_posix(): sorted(
            set(match.group(0) for match in bare_application_identifier.finditer(path.read_text()))
        )
        for path in paths
        if bare_application_identifier.search(path.read_text())
    }
    assert bare_failures == {}

    from polyarb.control_plane.postgres import PostgresControlPlane

    scoped_source = "\n".join(
        inspect.getsource(getattr(PostgresControlPlane, method))
        for method in (
            "_append_retry_runtime_events_cursor",
            "_recovery_job_lease_cursor",
            "_finish_retryable_with_incident_cursor",
            "_heartbeat_recovering_job_cursor",
            "_cancel_stalled_job_cursor",
            "_release_retryable_job_cursor",
            "_reclaim_expired_job_cursor",
            "_release_one_circuit_probe_cursor",
            "_execute_recovery_action_cursor",
            "_record_incident_event",
        )
    )
    assert sorted(set(match.group(0) for match in unqualified.finditer(scoped_source))) == []
    assert (
        sorted(set(match.group(0) for match in bare_application_identifier.finditer(scoped_source)))
        == []
    )
