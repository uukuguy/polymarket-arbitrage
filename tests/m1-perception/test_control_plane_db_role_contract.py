"""Fail-closed database identity contract for daemon roles."""

from __future__ import annotations

from collections.abc import Iterable
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
        self._rows = [self._factory.answer(normalized, params)]
        return self

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


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
    def __init__(self, *, failure_code: str | None = None, profile: str = "runtime-controller"):
        self.failure_code = failure_code
        self.profile = profile
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
        if "pg_has_role" in sql:
            role = _param(params, 1)
            if role == self.capability_role:
                return (self.failure_code != "database-role.capability-missing",)
            return (self.failure_code == "database-role.cross-capability",)
        if "has_database_privilege" in sql or "has_schema_privilege" in sql:
            return (True,)
        if "has_table_privilege" in sql:
            privilege = str(_param(params, 2))
            table = str(_param(params, 1)).removeprefix("public.")
            allowed = privilege in _allowed_table_privileges(self.profile, table)
            if self.failure_code == "database-role.required-privilege-missing" and allowed:
                return (False,)
            if self.failure_code == "database-role.forbidden-privilege-present" and not allowed:
                return (True,)
            return (allowed,)
        if "has_function_privilege" in sql:
            function_signature = str(_param(params, 1))
            allowed = function_signature in _allowed_functions(self.profile)
            if self.failure_code == "database-role.required-privilege-missing" and allowed:
                return (False,)
            if self.failure_code == "database-role.forbidden-privilege-present" and not allowed:
                return (True,)
            return (allowed,)
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
    return allowed[profile][table]


def _allowed_functions(profile: str) -> Iterable[str]:
    if profile != "qualification-worker":
        return ()
    return (
        "public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)",
        "public.m1_insert_qualification_certificate("
        "text,text,text,text,jsonb,timestamptz,timestamptz,jsonb,text,text,text,text)",
    )


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
