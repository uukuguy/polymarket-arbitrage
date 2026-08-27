"""Contracts for revision 026 scoped runtime capability roles."""

from __future__ import annotations

import ast
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from polyarb.control_plane.qualification_store import (
    canonical_certificate_bytes,
    certificate_digest,
)
from polyarb.control_plane.recovery_store import claim_controller
from polyarb.control_plane.runtime_observe import (
    build_runtime_observe_idle_record,
    insert_runtime_observe_decision,
)

MIGRATION_PATH = Path("alembic/versions/026_m1_runtime_scoped_roles.py")
NOW = datetime(2026, 8, 25, 0, 0, tzinfo=UTC)

RUNTIME_ROLE = "m1_runtime_controller_capability"
QUALIFICATION_ROLE = "m1_qualification_worker_capability"
RUNTIME_LOGIN = "m1_runtime_controller_test"
QUALIFICATION_LOGIN = "m1_qualification_worker_test"
SOURCE_LOGIN = "m1_source_projection_test"
PLAIN_LOGIN = "m1_plain_login_test"
HARDENED_TRIGGER_FUNCTIONS = (
    "public.m1_project_runtime_qualification_ingress()",
    "public.m1_project_incident_qualification_ingress()",
    "public.m1_project_recovery_qualification_ingress()",
    "public.m1_verify_qualification_certificate_insert()",
)


def test_026_declares_exact_capability_roles_and_chain() -> None:
    text = MIGRATION_PATH.read_text()
    assert 'revision = "026"' in text
    assert 'down_revision = "025"' in text
    for role in (
        "m1_runtime_controller_capability",
        "m1_qualification_worker_capability",
    ):
        assert role in text
    for attribute in (
        "NOLOGIN",
        "NOSUPERUSER",
        "NOCREATEDB",
        "NOCREATEROLE",
        "NOINHERIT",
        "NOREPLICATION",
        "NOBYPASSRLS",
    ):
        assert attribute in text


def test_026_closes_all_application_namespaces_and_database_creation() -> None:
    text = MIGRATION_PATH.read_text()
    for fragment in (
        "rolconfig",
        "pg_db_role_setting",
        "has_database_privilege",
        "'CREATE'",
        "'TEMPORARY'",
        "namespace.nspname NOT IN ('pg_catalog', 'information_schema')",
        "namespace.nspname !~ '^pg_(toast|temp)(_|$)'",
    ):
        assert fragment in text
    assert text.count("'{role}', namespace.oid, 'USAGE'") == 3


def test_026_accepts_ambient_acl_only_in_an_unreachable_schema() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove reachable authority boundary")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        _create_supabase_roles(dsn)
        _run_alembic(dsn, "upgrade", "025")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("CREATE SCHEMA extensions")
            admin.execute("REVOKE ALL ON SCHEMA extensions FROM PUBLIC")
            admin.execute(
                "CREATE VIEW extensions.pg_stat_statements AS "
                "SELECT 1::bigint AS queryid"
            )
            admin.execute(
                "CREATE VIEW extensions.pg_stat_statements_info AS "
                "SELECT 1::bigint AS dealloc"
            )
            admin.execute(
                "GRANT SELECT ON extensions.pg_stat_statements, "
                "extensions.pg_stat_statements_info TO PUBLIC"
            )

        _run_alembic(dsn, "upgrade", "026")
        assert _current_revision(dsn) == "026"
        with psycopg.connect(dsn) as admin:
            row = admin.execute(
                "SELECT "
                "has_table_privilege(%s, 'extensions.pg_stat_statements', 'SELECT'), "
                "has_schema_privilege(%s, 'extensions', 'USAGE')",
                (RUNTIME_ROLE, RUNTIME_ROLE),
            ).fetchone()
            assert row == (True, False)

        _run_alembic(dsn, "downgrade", "025")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("GRANT USAGE ON SCHEMA extensions TO PUBLIC")

        result = _run_alembic_result(dsn, "upgrade", "026")
        assert result.returncode != 0
        assert "authority envelope is not exact" in result.stderr + result.stdout
        assert _current_revision(dsn) == "025"


def test_026_keeps_observe_role_out_of_recovery_mutation() -> None:
    text = MIGRATION_PATH.read_text()
    assert "m1_runtime_observe_decisions" in text
    assert "m1_runtime_controller_leases" in text
    assert "m1_recovery_actions" in text
    assert "RUNTIME_CONTROLLER_WRITE_TABLES" in text
    assert "m1_recovery_actions" not in _runtime_write_table_tuple(text)


def test_026_declares_exact_permission_matrix_and_function_hardening() -> None:
    text = MIGRATION_PATH.read_text()

    assert _literal_tuple(text, "RUNTIME_CONTROLLER_READ_TABLES") == (
        "m1_runtime_controller_leases",
        "m1_runtime_observe_decisions",
        "m1_job_runtime_state",
        "m1_jobs",
        "m1_job_circuits",
        "m1_job_attempts",
        "m1_recovery_target_budgets",
        "m1_recovery_actions",
    )
    assert _literal_tuple(text, "RUNTIME_CONTROLLER_WRITE_TABLES") == (
        "m1_runtime_controller_leases",
        "m1_runtime_observe_decisions",
    )
    assert _literal_tuple(text, "QUALIFICATION_READ_TABLES") == (
        "m1_qualification_ingress_ledger",
        "m1_qualification_source_cursors",
        "m1_qualification_epochs",
        "m1_qualification_recovery_observations",
        "m1_qualification_certificates",
        "m1_publication_pointers",
        "m1_generation_manifests",
        "m1_opportunity_publication_pointers",
        "m1_opportunity_projections",
    )
    assert _literal_tuple(text, "QUALIFICATION_INSERT_TABLES") == (
        "m1_qualification_source_cursors",
        "m1_qualification_epochs",
        "m1_qualification_recovery_observations",
    )
    assert _literal_tuple(text, "QUALIFICATION_UPDATE_TABLES") == (
        "m1_qualification_source_cursors",
        "m1_qualification_epochs",
    )

    assert "REVOKE EXECUTE ON FUNCTION public.m1_record_qualification_ingress" in text
    for grantee in (
        "PUBLIC",
        "anon",
        "authenticated",
        "service_role",
        "m1_runtime_controller_capability",
        "m1_qualification_worker_capability",
    ):
        assert grantee in text
    assert "m1_record_qualification_freshness_ingress" in text
    assert "SECURITY DEFINER" in text
    assert "SET search_path = pg_catalog" in text
    assert "qualification freshness product is unsupported" in text
    assert "qualification freshness identity conflicts" in text
    assert "qualification freshness payload is invalid" in text
    assert "pg_column_size(p_payload) > 8192" in text
    assert "DROP ROLE IF EXISTS m1_runtime_controller_capability" in text
    assert "DROP ROLE IF EXISTS m1_qualification_worker_capability" in text


def test_026_rejects_unsafe_or_authorized_preexisting_capability_roles() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 026 role guard")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        _create_supabase_roles(dsn)
        _run_alembic(dsn, "upgrade", "025")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(RUNTIME_ROLE)))

        result = _run_alembic_result(dsn, "upgrade", "026")

        assert result.returncode != 0
        assert "m1_runtime_controller_capability exists with unsafe attributes" in (
            result.stderr + result.stdout
        )

        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(RUNTIME_ROLE)))
            admin.execute("CREATE ROLE m1_collision_peer NOLOGIN")
            admin.execute("CREATE SCHEMA m1_collision_namespace")

        collision_setups = (
            (
                "direct-grant",
                sql.SQL("GRANT SELECT ON TABLE public.m1_jobs TO {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
                sql.SQL("REVOKE SELECT ON TABLE public.m1_jobs FROM {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
            ),
            (
                "incoming-member",
                sql.SQL("GRANT {} TO m1_collision_peer").format(sql.Identifier(RUNTIME_ROLE)),
                sql.SQL("REVOKE {} FROM m1_collision_peer").format(sql.Identifier(RUNTIME_ROLE)),
            ),
            (
                "outgoing-member",
                sql.SQL("GRANT m1_collision_peer TO {}").format(sql.Identifier(RUNTIME_ROLE)),
                sql.SQL("REVOKE m1_collision_peer FROM {}").format(sql.Identifier(RUNTIME_ROLE)),
            ),
            (
                "owned-object",
                sql.SQL("ALTER TABLE public.m1_jobs OWNER TO {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
                sql.SQL("REASSIGN OWNED BY {} TO CURRENT_USER").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
            ),
            (
                "database-create",
                sql.SQL("GRANT CREATE ON DATABASE test TO {}").format(sql.Identifier(RUNTIME_ROLE)),
                sql.SQL("REVOKE CREATE ON DATABASE test FROM {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
            ),
            (
                "role-search-path",
                sql.SQL("ALTER ROLE {} SET search_path = evil, public").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
                sql.SQL("ALTER ROLE {} RESET search_path").format(sql.Identifier(RUNTIME_ROLE)),
            ),
            (
                "database-search-path",
                sql.SQL("ALTER DATABASE test SET search_path = evil, public"),
                sql.SQL("ALTER DATABASE test RESET search_path"),
            ),
            (
                "nonpublic-grant",
                sql.SQL("GRANT USAGE ON SCHEMA m1_collision_namespace TO {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
                sql.SQL("REVOKE USAGE ON SCHEMA m1_collision_namespace FROM {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
            ),
            (
                "nonpublic-owner",
                sql.SQL("ALTER SCHEMA m1_collision_namespace OWNER TO {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                ),
                sql.SQL("ALTER SCHEMA m1_collision_namespace OWNER TO CURRENT_USER"),
            ),
        )
        for case, setup, cleanup in collision_setups:
            with psycopg.connect(dsn, autocommit=True) as admin:
                admin.execute(
                    sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(RUNTIME_ROLE))
                )
                admin.execute(setup)
            result = _run_alembic_result(dsn, "upgrade", "026")
            assert result.returncode != 0, case
            assert "m1_runtime_controller_capability exists with unexpected authority" in (
                result.stderr + result.stdout
            )
            with psycopg.connect(dsn, autocommit=True) as admin:
                admin.execute(cleanup)
                admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(RUNTIME_ROLE)))

        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("DROP SCHEMA m1_collision_namespace")
            admin.execute("DROP ROLE m1_collision_peer")
        _run_alembic(dsn, "upgrade", "026")
        assert _current_revision(dsn) == "026"


def test_026_scoped_roles_harden_ingress_and_replay_across_downgrade() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 025<->026 role contract")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        _create_supabase_roles(dsn)
        _run_alembic(dsn, "upgrade", "025")
        revision_024_function_projection = _revision_024_function_projection(dsn)
        _run_alembic(dsn, "upgrade", "026")
        assert _current_revision(dsn) == "026"

        runtime_dsn = _role_dsn(dsn, RUNTIME_LOGIN, "runtime-test")
        qualification_dsn = _role_dsn(dsn, QUALIFICATION_LOGIN, "qualification-test")
        source_dsn = _role_dsn(dsn, SOURCE_LOGIN, "source-test")
        plain_dsn = _role_dsn(dsn, PLAIN_LOGIN, "plain-test")
        with psycopg.connect(dsn, autocommit=True) as admin:
            _create_test_login(admin, RUNTIME_LOGIN, "runtime-test")
            _create_test_login(admin, QUALIFICATION_LOGIN, "qualification-test")
            _create_test_login(admin, SOURCE_LOGIN, "source-test")
            _create_test_login(admin, PLAIN_LOGIN, "plain-test")
            admin.execute(
                sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(RUNTIME_ROLE),
                    sql.Identifier(RUNTIME_LOGIN),
                )
            )
            admin.execute(
                sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(QUALIFICATION_ROLE),
                    sql.Identifier(QUALIFICATION_LOGIN),
                )
            )
            _grant_source_projection_permissions(admin)
            _grant_plain_login_permissions(admin)
            _assert_role_attributes(admin, RUNTIME_ROLE)
            _assert_role_attributes(admin, QUALIFICATION_ROLE)
            _assert_runtime_permissions(admin)
            _assert_qualification_permissions(admin)
            _assert_source_projection_cannot_touch_ledger(admin)
            _assert_hardened_trigger_functions_not_executable(admin)

        _exercise_runtime_controller(runtime_dsn)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(runtime_dsn) as runtime:
                runtime.execute("UPDATE m1_recovery_actions SET state = state WHERE false")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(runtime_dsn) as runtime:
                runtime.execute(
                    "SELECT public.m1_record_qualification_freshness_ingress(%s, %s, %s, %s)",
                    (
                        "freshness:structure:runtime",
                        "structure",
                        NOW,
                        Jsonb(_freshness_payload("structure")),
                    ),
                )

        _exercise_source_projection_ingress(source_dsn, dsn)
        _assert_plain_login_cannot_spoof_trigger_ingress(plain_dsn, dsn)
        _exercise_qualification_worker(qualification_dsn, dsn)

        with psycopg.connect(dsn) as admin:
            _assert_append_only_triggers(admin)

        with psycopg.connect(dsn, autocommit=True) as admin:
            _drop_test_login(admin, RUNTIME_LOGIN)
            _drop_test_login(admin, QUALIFICATION_LOGIN)
            _drop_test_login(admin, SOURCE_LOGIN)
            _drop_test_login(admin, PLAIN_LOGIN)
        _run_alembic(dsn, "downgrade", "025")
        assert _current_revision(dsn) == "025"
        with psycopg.connect(dsn) as admin:
            assert not _role_exists(admin, RUNTIME_ROLE)
            assert not _role_exists(admin, QUALIFICATION_ROLE)
            _assert_revision_024_function_security_restored(admin)
        assert _revision_024_function_projection(dsn) == revision_024_function_projection
        _exercise_source_projection_after_downgrade(source_dsn, dsn)
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(SOURCE_LOGIN)))
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(SOURCE_LOGIN)))
        _run_alembic(dsn, "upgrade", "026")
        assert _current_revision(dsn) == "026"
        with psycopg.connect(dsn) as admin:
            _assert_role_attributes(admin, RUNTIME_ROLE)
            _assert_role_attributes(admin, QUALIFICATION_ROLE)
            _assert_runtime_permissions(admin)
            _assert_qualification_permissions(admin)


def test_026_real_pg16_exact_authority_adversarial_matrix() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove exact authority matrix")

    from testcontainers.postgres import PostgresContainer

    from polyarb.control_plane.db_role_admin import (
        DatabaseRoleAdminError,
        preflight_capability_roles,
        provision_login_roles,
    )
    from polyarb.control_plane.db_role_contract import (
        DatabaseRoleContractError,
        scoped_connection_factory,
        verify_daemon_database_role,
    )

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        _create_supabase_roles(dsn)
        _run_alembic(dsn, "upgrade", "026")

        def admin_factory() -> psycopg.Connection[Any]:
            return psycopg.connect(dsn)

        provision_login_roles(
            admin_factory,
            expected_database="test",
            runtime_password="runtime-matrix-secret",
            qualification_password="qualification-matrix-secret",
        )
        runtime_dsn = _role_dsn(dsn, "m1_runtime_controller_login", "runtime-matrix-secret")
        qualification_dsn = _role_dsn(
            dsn, "m1_qualification_worker_login", "qualification-matrix-secret"
        )

        profiles = {
            "runtime-controller": runtime_dsn,
            "qualification-worker": qualification_dsn,
        }
        assert preflight_capability_roles(admin_factory, expected_database="test")["status"] == (
            "ready"
        )
        for profile, role_dsn in profiles.items():
            assert (
                verify_daemon_database_role(
                    scoped_connection_factory(role_dsn),
                    profile,
                    expected_database="test",
                ).status
                == "pass"
            )

        def assert_rejected(profile: str) -> None:
            before = _scoped_daemon_mutation_counts(dsn)
            with pytest.raises(DatabaseRoleContractError) as daemon_error:
                verify_daemon_database_role(
                    scoped_connection_factory(profiles[profile]),
                    profile,
                    expected_database="test",
                )
            with pytest.raises(DatabaseRoleAdminError) as admin_error:
                preflight_capability_roles(admin_factory, expected_database="test")
            for error in (daemon_error.value, admin_error.value):
                rendered = str(error)
                assert len(rendered) <= 256
                assert "postgresql://" not in rendered
                assert "matrix-secret" not in rendered
            assert _scoped_daemon_mutation_counts(dsn) == before

        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("CREATE TABLE public.m1_unrelated_authority(id bigint)")
            admin.execute(
                sql.SQL("GRANT SELECT ON TABLE public.m1_unrelated_authority TO {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                )
            )
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("REVOKE SELECT ON TABLE public.m1_unrelated_authority FROM {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                )
            )

            admin.execute(
                sql.SQL(
                    "GRANT EXECUTE ON FUNCTION public.l3_retention_cleanup("
                    "timestamptz,timestamptz,timestamptz) TO {}"
                ).format(sql.Identifier(QUALIFICATION_ROLE))
            )
        assert_rejected("qualification-worker")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL(
                    "REVOKE EXECUTE ON FUNCTION public.l3_retention_cleanup("
                    "timestamptz,timestamptz,timestamptz) FROM {}"
                ).format(sql.Identifier(QUALIFICATION_ROLE))
            )

            admin.execute("GRANT CREATE ON SCHEMA public TO PUBLIC")
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            admin.execute("CREATE ROLE m1_unexpected_capability_member NOLOGIN")
            admin.execute(
                sql.SQL("GRANT {} TO m1_unexpected_capability_member").format(
                    sql.Identifier(RUNTIME_ROLE)
                )
            )
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("REVOKE {} FROM m1_unexpected_capability_member").format(
                    sql.Identifier(RUNTIME_ROLE)
                )
            )
            admin.execute("DROP ROLE m1_unexpected_capability_member")
            admin.execute(
                sql.SQL("ALTER TABLE public.m1_unrelated_authority OWNER TO {}").format(
                    sql.Identifier(RUNTIME_ROLE)
                )
            )
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("DROP TABLE public.m1_unrelated_authority")

            admin.execute(
                sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                    sql.Identifier("m1_runtime_controller_login"),
                    sql.Identifier("m1_runtime_controller_login"),
                )
            )
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(
                    sql.Identifier("m1_runtime_controller_login")
                )
            )

            admin.execute("ALTER ROLE m1_runtime_controller_login SET search_path = evil, public")
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("ALTER ROLE m1_runtime_controller_login RESET search_path")

            admin.execute("ALTER DATABASE test SET search_path = evil, public")
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("ALTER DATABASE test RESET search_path")

            admin.execute("GRANT CREATE ON DATABASE test TO m1_runtime_controller_login")
        assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("REVOKE CREATE ON DATABASE test FROM m1_runtime_controller_login")

            admin.execute("CREATE SCHEMA m1_unrelated_namespace")
            admin.execute("CREATE TABLE m1_unrelated_namespace.shadow(id bigint)")
            admin.execute(
                "GRANT USAGE ON SCHEMA m1_unrelated_namespace TO m1_qualification_worker_login"
            )
            admin.execute(
                "GRANT SELECT ON TABLE m1_unrelated_namespace.shadow "
                "TO m1_qualification_worker_login"
            )
        assert_rejected("qualification-worker")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                "REVOKE SELECT ON TABLE m1_unrelated_namespace.shadow "
                "FROM m1_qualification_worker_login"
            )
            admin.execute(
                "REVOKE USAGE ON SCHEMA m1_unrelated_namespace FROM m1_qualification_worker_login"
            )
            admin.execute("DROP SCHEMA m1_unrelated_namespace CASCADE")

        membership_options = (
            "WITH ADMIN TRUE, INHERIT TRUE, SET TRUE",
            "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE",
            "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE",
        )
        for options in membership_options:
            with psycopg.connect(dsn, autocommit=True) as admin:
                admin.execute(
                    "REVOKE m1_runtime_controller_capability FROM m1_runtime_controller_login"
                )
                admin.execute(
                    "GRANT m1_runtime_controller_capability "
                    f"TO m1_runtime_controller_login {options}"
                )
            assert_rejected("runtime-controller")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                "REVOKE m1_runtime_controller_capability FROM m1_runtime_controller_login"
            )
            admin.execute(
                "GRANT m1_runtime_controller_capability "
                "TO m1_runtime_controller_login "
                "WITH ADMIN FALSE, INHERIT TRUE, SET TRUE"
            )

        unsafe_runtime_dsn = runtime_dsn + "?options=-csearch_path%3Devil"
        before = _scoped_daemon_mutation_counts(dsn)
        with pytest.raises(DatabaseRoleContractError, match="database-role.dsn-override"):
            scoped_connection_factory(unsafe_runtime_dsn)
        assert _scoped_daemon_mutation_counts(dsn) == before

        assert preflight_capability_roles(admin_factory, expected_database="test")["status"] == (
            "ready"
        )
        for profile, role_dsn in profiles.items():
            assert (
                verify_daemon_database_role(
                    scoped_connection_factory(role_dsn),
                    profile,
                    expected_database="test",
                ).status
                == "pass"
            )
        with psycopg.connect(dsn) as admin:
            temporary_row = admin.execute(
                "SELECT has_database_privilege(%s, current_database(), 'TEMPORARY')",
                ("m1_runtime_controller_login",),
            ).fetchone()
            assert temporary_row is not None
            assert bool(_row_value(temporary_row, 0, "has_database_privilege"))
            assert {
                str(row[0])
                for row in admin.execute(
                    "SELECT rolname FROM pg_catalog.pg_roles "
                    "WHERE rolname IN ('anon', 'authenticated', 'service_role')"
                ).fetchall()
            } == {"anon", "authenticated", "service_role"}


def _scoped_daemon_mutation_counts(dsn: str) -> tuple[int, ...]:
    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM public.m1_runtime_controller_leases),
              (SELECT COUNT(*) FROM public.m1_runtime_observe_decisions),
              (SELECT COUNT(*) FROM public.m1_qualification_source_cursors),
              (SELECT COUNT(*) FROM public.m1_qualification_epochs),
              (SELECT COUNT(*) FROM public.m1_qualification_recovery_observations),
              (SELECT COUNT(*) FROM public.m1_qualification_certificates)
            """
        ).fetchone()
    assert row is not None
    values = cast(Sequence[object], row)
    return tuple(int(str(value)) for value in values)


def _runtime_write_table_tuple(text: str) -> tuple[str, ...]:
    return _literal_tuple(text, "RUNTIME_CONTROLLER_WRITE_TABLES")


def _literal_tuple(text: str, name: str) -> tuple[str, ...]:
    module = ast.parse(text)
    for node in module.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                value = ast.literal_eval(node.value)
                assert isinstance(value, tuple)
                return tuple(cast(tuple[str, ...], value))
    raise AssertionError(f"{name} assignment not found")


def _row_value(row: object, index: int, name: str) -> object:
    if isinstance(row, Mapping):
        return row[name]
    values = cast(Sequence[object], row)
    return values[index]


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
    result = _run_alembic_result(dsn, *args)
    assert result.returncode == 0, result.stderr


def _run_alembic_result(dsn: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )


def _current_revision(dsn: str) -> str:
    with psycopg.connect(dsn) as connection:
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row is not None
    return str(_row_value(row, 0, "version_num"))


def _create_supabase_roles(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        for role in ("anon", "authenticated", "service_role"):
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))


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


def _create_test_login(
    admin: psycopg.Connection[object],
    role_name: str,
    password: str,
) -> None:
    admin.execute(
        sql.SQL("CREATE ROLE {} LOGIN INHERIT PASSWORD {}").format(
            sql.Identifier(role_name),
            sql.Literal(password),
        )
    )


def _drop_test_login(admin: psycopg.Connection[object], role_name: str) -> None:
    for capability in (RUNTIME_ROLE, QUALIFICATION_ROLE):
        admin.execute(
            sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(capability),
                sql.Identifier(role_name),
            )
        )
    admin.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role_name)))
    admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role_name)))


def _grant_source_projection_permissions(admin: psycopg.Connection[object]) -> None:
    admin.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(admin.info.dbname),
            sql.Identifier(SOURCE_LOGIN),
        )
    )
    admin.execute(
        sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(SOURCE_LOGIN))
    )
    for table_name in ("m1_job_runtime_events", "m1_incident_events"):
        admin.execute(
            sql.SQL("GRANT INSERT ON TABLE {} TO {}").format(
                sql.Identifier(table_name),
                sql.Identifier(SOURCE_LOGIN),
            )
        )
    admin.execute(
        sql.SQL("GRANT INSERT, UPDATE ON TABLE m1_recovery_actions TO {}").format(
            sql.Identifier(SOURCE_LOGIN)
        )
    )


def _grant_plain_login_permissions(admin: psycopg.Connection[object]) -> None:
    admin.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(admin.info.dbname),
            sql.Identifier(PLAIN_LOGIN),
        )
    )
    admin.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(PLAIN_LOGIN)))


def _assert_role_attributes(admin: psycopg.Connection[object], role_name: str) -> None:
    row = admin.execute(
        """
        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit,
               rolreplication, rolbypassrls
        FROM pg_catalog.pg_roles
        WHERE rolname = %s
        """,
        (role_name,),
    ).fetchone()
    assert row == (False, False, False, False, False, False, False)


def _assert_runtime_permissions(admin: psycopg.Connection[object]) -> None:
    for table in (
        "m1_runtime_controller_leases",
        "m1_runtime_observe_decisions",
        "m1_job_runtime_state",
        "m1_jobs",
        "m1_job_circuits",
        "m1_job_attempts",
        "m1_recovery_target_budgets",
        "m1_recovery_actions",
    ):
        assert _has_table(admin, RUNTIME_ROLE, table, "SELECT")
    for table in ("m1_runtime_controller_leases", "m1_runtime_observe_decisions"):
        assert _has_table(admin, RUNTIME_ROLE, table, "INSERT")
    assert _has_table(admin, RUNTIME_ROLE, "m1_runtime_controller_leases", "UPDATE")
    assert not _has_table(admin, RUNTIME_ROLE, "m1_recovery_actions", "INSERT")
    assert not _has_table(admin, RUNTIME_ROLE, "m1_recovery_actions", "UPDATE")
    assert not _has_table(admin, RUNTIME_ROLE, "m1_runtime_observe_decisions", "UPDATE")
    assert not _has_table(admin, RUNTIME_ROLE, "m1_runtime_controller_leases", "DELETE")
    assert not _has_function(
        admin,
        RUNTIME_ROLE,
        "public.m1_record_qualification_ingress(text,text,text,timestamptz,jsonb)",
    )


def _assert_qualification_permissions(admin: psycopg.Connection[object]) -> None:
    for table in (
        "m1_qualification_ingress_ledger",
        "m1_qualification_source_cursors",
        "m1_qualification_epochs",
        "m1_qualification_recovery_observations",
        "m1_qualification_certificates",
        "m1_publication_pointers",
        "m1_generation_manifests",
        "m1_opportunity_publication_pointers",
        "m1_opportunity_projections",
    ):
        assert _has_table(admin, QUALIFICATION_ROLE, table, "SELECT")
    for table in (
        "m1_qualification_source_cursors",
        "m1_qualification_epochs",
        "m1_qualification_recovery_observations",
    ):
        assert _has_table(admin, QUALIFICATION_ROLE, table, "INSERT")
    for table in ("m1_qualification_source_cursors", "m1_qualification_epochs"):
        assert _has_table(admin, QUALIFICATION_ROLE, table, "UPDATE")
    assert not _has_table(admin, QUALIFICATION_ROLE, "m1_qualification_ingress_ledger", "INSERT")
    assert not _has_table(admin, QUALIFICATION_ROLE, "m1_qualification_certificates", "INSERT")
    assert not _has_table(admin, QUALIFICATION_ROLE, "m1_qualification_certificates", "UPDATE")
    assert not _has_sequence(admin, QUALIFICATION_ROLE, _ledger_sequence(admin), "USAGE")
    assert not _has_function(
        admin,
        QUALIFICATION_ROLE,
        "public.m1_record_qualification_ingress(text,text,text,timestamptz,jsonb)",
    )
    assert _has_function(
        admin,
        QUALIFICATION_ROLE,
        "public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)",
    )
    assert _has_function(
        admin,
        QUALIFICATION_ROLE,
        "public.m1_insert_qualification_certificate("
        "text,text,text,text,jsonb,timestamptz,timestamptz,jsonb,text,text,text,text)",
    )


def _assert_source_projection_cannot_touch_ledger(admin: psycopg.Connection[object]) -> None:
    assert not _has_table(admin, SOURCE_LOGIN, "m1_qualification_ingress_ledger", "INSERT")
    assert not _has_sequence(admin, SOURCE_LOGIN, _ledger_sequence(admin), "USAGE")


def _assert_hardened_trigger_functions_not_executable(
    admin: psycopg.Connection[object],
) -> None:
    for function_signature in HARDENED_TRIGGER_FUNCTIONS:
        assert not _public_has_function(admin, function_signature)
    for grantee in (
        "anon",
        "authenticated",
        "service_role",
        RUNTIME_ROLE,
        QUALIFICATION_ROLE,
        PLAIN_LOGIN,
    ):
        for function_signature in HARDENED_TRIGGER_FUNCTIONS:
            assert not _has_function(admin, grantee, function_signature)


def _has_table(
    connection: psycopg.Connection[object],
    grantee: str,
    table: str,
    privilege: str,
) -> bool:
    row = connection.execute(
        "SELECT has_table_privilege(%s, %s, %s)",
        (grantee, f"public.{table}", privilege),
    ).fetchone()
    return bool(row and _row_value(row, 0, "has_table_privilege"))


def _has_sequence(
    connection: psycopg.Connection[object],
    grantee: str,
    sequence: str,
    privilege: str,
) -> bool:
    row = connection.execute(
        "SELECT has_sequence_privilege(%s, %s, %s)",
        (grantee, sequence, privilege),
    ).fetchone()
    return bool(row and _row_value(row, 0, "has_sequence_privilege"))


def _has_function(
    connection: psycopg.Connection[object],
    grantee: str,
    function_signature: str,
) -> bool:
    row = connection.execute(
        "SELECT has_function_privilege(%s, %s, 'EXECUTE')",
        (grantee, function_signature),
    ).fetchone()
    return bool(row and _row_value(row, 0, "has_function_privilege"))


def _public_has_function(
    connection: psycopg.Connection[object],
    function_signature: str,
) -> bool:
    row = connection.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS p
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
            ) AS acl
            WHERE p.oid = %s::regprocedure
              AND acl.grantee = 0
              AND acl.privilege_type = 'EXECUTE'
        )
        """,
        (function_signature,),
    ).fetchone()
    return bool(row and _row_value(row, 0, "exists"))


def _ledger_sequence(connection: psycopg.Connection[object]) -> str:
    row = connection.execute(
        "SELECT pg_get_serial_sequence('public.m1_qualification_ingress_ledger', 'ingest_seq')"
    ).fetchone()
    assert row is not None
    return str(_row_value(row, 0, "pg_get_serial_sequence"))


def _exercise_runtime_controller(runtime_dsn: str) -> None:
    def runtime_connection() -> psycopg.Connection[object]:
        return psycopg.connect(runtime_dsn)

    controller = claim_controller(
        runtime_connection,
        controller_id="scoped-controller",
        owner_id="scoped-owner",
        lease_seconds=60,
        now=NOW,
    )
    insert_runtime_observe_decision(
        runtime_connection,
        build_runtime_observe_idle_record(
            controller_id=controller.controller_id,
            controller_owner_id=controller.owner_id,
            controller_epoch=controller.lease_epoch,
            observed_at=NOW,
            next_check_at=NOW + timedelta(seconds=30),
            observed_by=controller.owner_id,
        ),
    )


def _exercise_source_projection_ingress(source_dsn: str, admin_dsn: str) -> None:
    _seed_required_job_facts(admin_dsn)
    with psycopg.connect(source_dsn) as source:
        source.execute(
            """
            INSERT INTO m1_job_runtime_events (
                event_id, job_key, attempt_id, lease_epoch, worker_id,
                event_sequence, kind, stage, progress_sequence, progress_current,
                progress_total, detail, occurred_at, idempotency_key
            ) VALUES (
                'runtime-source-026', 'job-source-026', 'attempt-source-026', 1,
                'worker-source-026', 1, 'job.succeeded', 'done', 1, 1, 1,
                %s, %s, 'runtime-source-026'
            )
            """,
            (Jsonb({"reason_code": "healthy"}), NOW),
        )
        source.execute(
            """
            INSERT INTO m1_incident_events (
                incident_event_id, incident_key, kind, detail, idempotency_key, occurred_at
            ) VALUES (
                'incident-event-source-026', 'incident-source-026', 'detected',
                %s, 'incident-event-source-026', %s
            )
            """,
            (Jsonb({"reason_code": "incident.p1"}), NOW + timedelta(seconds=1)),
        )
        source.execute(
            """
            INSERT INTO m1_recovery_actions (
                action_id, controller_id, controller_owner_id, incident_key,
                target_type, target_id, action_type, expected_controller_epoch,
                expected_attempt_id, expected_lease_epoch, requested_at, started_at,
                finished_at, state, result_code, next_allowed_at, detail, idempotency_key
            ) VALUES (
                'recovery-source-026', 'source-controller-026', 'source-owner-026',
                NULL, 'job', 'job-source-026', 'retry-job', 1, 'attempt-source-026',
                1, %s, %s, %s, 'completed', 'succeeded', %s, %s, 'recovery-source-026'
            )
            """,
            (
                NOW,
                NOW + timedelta(seconds=1),
                NOW + timedelta(seconds=2),
                NOW + timedelta(minutes=1),
                Jsonb({"reason_code": "upstream.timeout"}),
            ),
        )

    with psycopg.connect(admin_dsn) as admin:
        row = admin.execute(
            """
            SELECT array_agg(source ORDER BY source)
            FROM m1_qualification_ingress_ledger
            WHERE source_id IN (
                'runtime-source-026',
                'incident-event-source-026',
                'recovery-source-026'
            )
            """
        ).fetchone()
    assert row == (["incident", "recovery", "runtime"],)


def _assert_plain_login_cannot_spoof_trigger_ingress(plain_dsn: str, admin_dsn: str) -> None:
    _seed_spoof_incident(admin_dsn)
    for attack in (
        _attempt_runtime_trigger_spoof,
        _attempt_incident_trigger_spoof,
        _attempt_recovery_trigger_spoof,
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(plain_dsn) as plain:
                attack(plain)

    with psycopg.connect(admin_dsn) as admin:
        row = admin.execute(
            """
            SELECT COUNT(*)
            FROM m1_qualification_ingress_ledger
            WHERE source_id IN (
                'spoof-runtime-via-temp-trigger',
                'spoof-incident-via-temp-trigger',
                'spoof-recovery-via-temp-trigger'
            )
            """
        ).fetchone()
    assert row == (0,)


def _attempt_runtime_trigger_spoof(connection: psycopg.Connection[object]) -> None:
    connection.execute("CREATE TEMP TABLE spoof_runtime(event_id text, occurred_at timestamptz)")
    connection.execute(
        """
        CREATE TRIGGER spoof_runtime_ingress
        AFTER INSERT ON spoof_runtime
        FOR EACH ROW EXECUTE FUNCTION public.m1_project_runtime_qualification_ingress()
        """
    )
    connection.execute(
        """
        INSERT INTO spoof_runtime(event_id, occurred_at)
        VALUES ('spoof-runtime-via-temp-trigger', %s)
        """,
        (NOW,),
    )


def _attempt_incident_trigger_spoof(connection: psycopg.Connection[object]) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE spoof_incident(
            incident_event_id text,
            incident_key text,
            occurred_at timestamptz
        )
        """
    )
    connection.execute(
        """
        CREATE TRIGGER spoof_incident_ingress
        AFTER INSERT ON spoof_incident
        FOR EACH ROW EXECUTE FUNCTION public.m1_project_incident_qualification_ingress()
        """
    )
    connection.execute(
        """
        INSERT INTO spoof_incident(incident_event_id, incident_key, occurred_at)
        VALUES ('spoof-incident-via-temp-trigger', 'incident-spoof-026', %s)
        """,
        (NOW,),
    )


def _attempt_recovery_trigger_spoof(connection: psycopg.Connection[object]) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE spoof_recovery(
            action_id text,
            state text,
            result_code text,
            finished_at timestamptz,
            started_at timestamptz,
            requested_at timestamptz
        )
        """
    )
    connection.execute(
        """
        CREATE TRIGGER spoof_recovery_ingress
        AFTER INSERT ON spoof_recovery
        FOR EACH ROW EXECUTE FUNCTION public.m1_project_recovery_qualification_ingress()
        """
    )
    connection.execute(
        """
        INSERT INTO spoof_recovery(
            action_id, state, result_code, finished_at, started_at, requested_at
        ) VALUES (
            'spoof-recovery-via-temp-trigger', 'completed', 'succeeded', %s, %s, %s
        )
        """,
        (NOW + timedelta(seconds=2), NOW + timedelta(seconds=1), NOW),
    )


def _exercise_qualification_worker(qualification_dsn: str, admin_dsn: str) -> None:
    valid_payload = _freshness_payload("structure")
    with psycopg.connect(qualification_dsn) as qualification:
        qualification.execute(
            """
            SELECT public.m1_record_qualification_freshness_ingress(%s, %s, %s, %s)
            """,
            (
                cast(str, valid_payload["fact_id"]),
                "structure",
                NOW,
                Jsonb(valid_payload),
            ),
        )
        qualification.commit()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            qualification.execute(
                """
                SELECT public.m1_record_qualification_ingress(%s, %s, %s, %s, %s)
                """,
                ("runtime", "spoof-runtime-026", "v1", NOW, Jsonb({"fact_id": "x"})),
            )
        qualification.rollback()
        for params in (
            (
                "bad-product",
                "freshness:bad-product:026",
                "bad-product",
                Jsonb({"data_product": "bad-product"}),
            ),
            (
                "bad-prefix",
                "runtime:structure:026",
                "structure",
                Jsonb(_freshness_payload("structure", fact_id="runtime:structure:026")),
            ),
            (
                "bad-json",
                "freshness:structure:not-object",
                "structure",
                Jsonb(["not-object"]),
            ),
            (
                "bad-size",
                "freshness:structure:oversize",
                "structure",
                Jsonb({"data_product": "structure", "padding": "x" * 9000}),
            ),
        ):
            _case, source_id, source_version, payload = params
            with pytest.raises(psycopg.errors.RaiseException):
                qualification.execute(
                    """
                    SELECT public.m1_record_qualification_freshness_ingress(%s, %s, %s, %s)
                    """,
                    (source_id, source_version, NOW, payload),
                )
            qualification.rollback()

        _insert_cursor_epoch_observation_and_certificate(qualification)

    with psycopg.connect(admin_dsn) as admin:
        row = admin.execute(
            """
            SELECT COUNT(*)
            FROM m1_qualification_ingress_ledger
            WHERE source = 'freshness' AND source_id = 'freshness:structure:026'
            """
        ).fetchone()
        assert row == (1,)


def _insert_cursor_epoch_observation_and_certificate(
    qualification: psycopg.Connection[object],
) -> None:
    role_identity = ["m1", "structure"]
    qualification.execute(
        """
        INSERT INTO m1_qualification_source_cursors (
            identity_key, policy_version, release_id, config_id, role_identity,
            source_cursor, writer_id
        ) VALUES (
            'identity-026-cursor', 'm1-rolling-qualification-v1', 'release-a',
            'config-a', %s, %s, 'qualification-worker'
        )
        """,
        (Jsonb(role_identity), Jsonb({"ingest_seq": 1})),
    )
    qualification.execute(
        """
        UPDATE m1_qualification_source_cursors
        SET source_cursor = %s
        WHERE identity_key = 'identity-026-cursor'
        """,
        (Jsonb({"ingest_seq": 2}),),
    )
    qualification.execute(
        """
        INSERT INTO m1_qualification_epochs (
            epoch_id, state, version, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, last_fact_at, invalidated_at,
            invalidation_reason, previous_epoch_id, coverage_seconds,
            max_gap_seconds, required_seconds, fact_records
        ) VALUES (
            'epoch-026-previous', 'invalidated', 1, 'identity-026-previous',
            'm1-rolling-qualification-v1', 'release-a', 'config-a',
            %s, %s, %s, %s, 'lease.expired', NULL, 12, 900, 86400, %s
        )
        """,
        (
            Jsonb(role_identity),
            NOW - timedelta(minutes=3),
            NOW - timedelta(minutes=2),
            NOW - timedelta(minutes=1),
            Jsonb([]),
        ),
    )
    qualification.execute(
        """
        INSERT INTO m1_qualification_epochs (
            epoch_id, state, version, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, previous_epoch_id,
            coverage_seconds, max_gap_seconds, required_seconds, fact_records
        ) VALUES (
            'epoch-026-recovering', 'recovering', 1, 'identity-026-recovering',
            'm1-rolling-qualification-v1', 'release-a', 'config-a',
            %s, %s, 'epoch-026-previous', 0, 900, 86400, %s
        )
        """,
        (Jsonb(role_identity), NOW, Jsonb([])),
    )
    qualification.execute(
        """
        INSERT INTO m1_qualification_recovery_observations (
            observation_id, recovering_epoch_id, ingest_seq, fact_id,
            reason, observed_at, fact_record, fact_record_sha256
        )
        SELECT 'observation-026', 'epoch-026-recovering', ingest_seq,
               'freshness:structure:026', 'healthy', %s, %s, %s
        FROM m1_qualification_ingress_ledger
        WHERE source = 'freshness' AND source_id = 'freshness:structure:026'
        """,
        (
            NOW,
            Jsonb({"fact": {"fact_id": "freshness:structure:026"}}),
            sha256(b'{"fact":{"fact_id":"freshness:structure:026"}}').hexdigest(),
        ),
    )
    qualification.execute(
        """
        UPDATE m1_qualification_epochs
        SET version = version + 1
        WHERE epoch_id = 'epoch-026-recovering'
        """
    )
    payload = _certificate_payload("epoch-026-qualified")
    identity = cast(dict[str, object], payload["identity"])
    bounds = cast(dict[str, object], payload["bounds"])
    digest = certificate_digest(payload)
    qualification.execute(
        """
        INSERT INTO m1_qualification_epochs (
            epoch_id, state, version, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, last_fact_at, qualified_at,
            fact_digests, contained_recoveries, coverage_seconds, max_gap_seconds,
            progress_count, successful_count, evidence_digest, required_seconds,
            slo, contained_incident_details, recovery_action_details, fact_records
        ) VALUES (
            %s, 'qualified', 2, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, 86400, 900, 12, 12, %s, 86400, %s, %s, %s, %s
        )
        """,
        (
            cast(str, identity["epoch_id"]),
            _certificate_identity_key(payload),
            cast(str, identity["policy_version"]),
            cast(str, identity["release_id"]),
            cast(str, identity["config_id"]),
            Jsonb(cast(list[object], identity["role_identity"])),
            cast(str, bounds["started_at"]),
            cast(str, bounds["qualified_at"]),
            cast(str, bounds["qualified_at"]),
            Jsonb([]),
            Jsonb([]),
            cast(str, payload["evidence_digest"]),
            Jsonb(payload["slo"]),
            Jsonb([]),
            Jsonb([]),
            Jsonb([]),
        ),
    )
    row = qualification.execute(
        """
        SELECT certificate_id, certificate_digest
        FROM public.m1_insert_qualification_certificate(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            cast(str, identity["epoch_id"]),
            cast(str, identity["policy_version"]),
            cast(str, identity["release_id"]),
            cast(str, identity["config_id"]),
            Jsonb(cast(list[object], identity["role_identity"])),
            cast(str, bounds["started_at"]),
            cast(str, bounds["qualified_at"]),
            Jsonb(payload),
            canonical_certificate_bytes(payload).decode("utf-8"),
            digest,
            digest,
            cast(str, payload["evidence_digest"]),
        ),
    ).fetchone()
    assert row == (f"qualification-certificate:{digest}", digest)
    qualification.commit()
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        qualification.execute(
            "INSERT INTO m1_qualification_certificates SELECT * FROM m1_qualification_certificates"
        )
    qualification.rollback()


def _seed_required_job_facts(dsn: str) -> None:
    with psycopg.connect(dsn) as admin:
        admin.execute(
            """
            INSERT INTO m1_jobs (
                job_key, job_type, input_identity, state, created_at, updated_at
            ) VALUES (
                'job-source-026', 'quote-batch', 'job-source-026:input',
                'leased', %s, %s
            )
            """,
            (NOW, NOW),
        )
        admin.execute(
            """
            INSERT INTO m1_job_attempts (
                attempt_id, job_key, lease_epoch, worker_id, state, started_at
            ) VALUES (
                'attempt-source-026', 'job-source-026', 1, 'worker-source-026',
                'running', %s
            )
            """,
            (NOW,),
        )
        admin.execute(
            """
            INSERT INTO m1_runtime_controller_leases (
                controller_id, owner_id, lease_epoch, lease_expires_at, claimed_at, updated_at
            ) VALUES (
                'source-controller-026', 'source-owner-026', 1, %s, %s, %s
            )
            """,
            (NOW + timedelta(minutes=5), NOW, NOW),
        )
        admin.execute(
            """
            INSERT INTO m1_incidents (
                incident_key, dedupe_key, component, severity, state, summary,
                diagnosis, opened_at, updated_at
            ) VALUES (
                'incident-source-026', 'incident-source-026', 'm1', 'critical',
                'open', 'source projection test', %s, %s, %s
            )
            """,
            (Jsonb({}), NOW, NOW),
        )


def _seed_spoof_incident(dsn: str) -> None:
    with psycopg.connect(dsn) as admin:
        admin.execute(
            """
            INSERT INTO m1_incidents (
                incident_key, dedupe_key, component, severity, state, summary,
                diagnosis, opened_at, updated_at
            ) VALUES (
                'incident-spoof-026', 'incident-spoof-026', 'm1', 'critical',
                'open', 'spoof trigger test', %s, %s, %s
            )
            """,
            (Jsonb({}), NOW, NOW),
        )


def _assert_append_only_triggers(connection: psycopg.Connection[object]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        connection.execute(
            "UPDATE m1_qualification_certificates SET evidence_digest = evidence_digest"
        )
    connection.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        connection.execute("DELETE FROM m1_qualification_certificates")
    connection.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        connection.execute("UPDATE m1_qualification_recovery_observations SET reason = reason")
    connection.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        connection.execute("DELETE FROM m1_qualification_recovery_observations")
    connection.rollback()


def _assert_revision_024_function_security_restored(
    connection: psycopg.Connection[object],
) -> None:
    raw_rows = connection.execute(
        """
            SELECT p.proname, p.prosecdef
            FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public'
              AND p.proname IN (
                  'm1_record_qualification_ingress',
                  'm1_project_runtime_qualification_ingress',
                  'm1_project_incident_qualification_ingress',
                  'm1_project_recovery_qualification_ingress',
                  'm1_verify_qualification_certificate_insert',
                  'm1_insert_qualification_certificate'
              )
            """
    ).fetchall()
    rows = {
        str(_row_value(row, 0, "proname")): bool(_row_value(row, 1, "prosecdef"))
        for row in raw_rows
    }
    assert rows["m1_record_qualification_ingress"] is False
    assert rows["m1_project_runtime_qualification_ingress"] is False
    assert rows["m1_project_incident_qualification_ingress"] is False
    assert rows["m1_project_recovery_qualification_ingress"] is False
    assert rows["m1_verify_qualification_certificate_insert"] is False
    assert rows["m1_insert_qualification_certificate"] is True

    public_execute_functions = (
        "public.m1_record_qualification_ingress(text,text,text,timestamptz,jsonb)",
        "public.m1_project_runtime_qualification_ingress()",
        "public.m1_project_incident_qualification_ingress()",
        "public.m1_project_recovery_qualification_ingress()",
        "public.m1_canonical_jsonb(jsonb)",
        "public.m1_verify_qualification_certificate_insert()",
    )
    for function_signature in public_execute_functions:
        assert _public_has_function(connection, function_signature)
    certificate_function = (
        "public.m1_insert_qualification_certificate("
        "text,text,text,text,jsonb,timestamptz,timestamptz,jsonb,text,text,text,text)"
    )
    assert not _public_has_function(connection, certificate_function)
    assert _has_function(connection, "service_role", certificate_function)


def _revision_024_function_projection(
    dsn: str,
) -> dict[str, tuple[bool, tuple[str, ...], tuple[str, ...]]]:
    with psycopg.connect(dsn) as connection:
        rows = connection.execute(
            """
            SELECT pg_catalog.format(
                       '%I.%I(%s)', namespace.nspname, routine.proname,
                       pg_catalog.pg_get_function_identity_arguments(routine.oid)
                   ) AS signature,
                   routine.prosecdef,
                   COALESCE(routine.proconfig, ARRAY[]::text[]) AS proconfig,
                   ARRAY(
                       SELECT pg_catalog.format(
                                  '%%s:%%s:%%s',
                                  CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                       ELSE grantee.rolname END,
                                  acl.privilege_type,
                                  acl.is_grantable
                              )
                       FROM pg_catalog.aclexplode(
                           COALESCE(
                               routine.proacl,
                               pg_catalog.acldefault('f', routine.proowner)
                           )
                       ) AS acl
                       LEFT JOIN pg_catalog.pg_roles AS grantee
                         ON grantee.oid = acl.grantee
                       ORDER BY 1
                   ) AS acl_projection
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
            WHERE namespace.nspname = 'public'
              AND routine.proname IN (
                  'm1_record_qualification_ingress',
                  'm1_project_runtime_qualification_ingress',
                  'm1_project_incident_qualification_ingress',
                  'm1_project_recovery_qualification_ingress',
                  'm1_canonical_jsonb',
                  'm1_verify_qualification_certificate_insert',
                  'm1_insert_qualification_certificate'
              )
            ORDER BY signature
            """
        ).fetchall()
    return {
        str(_row_value(row, 0, "signature")): (
            bool(_row_value(row, 1, "prosecdef")),
            tuple(str(item) for item in cast(Sequence[object], _row_value(row, 2, "proconfig"))),
            tuple(
                str(item) for item in cast(Sequence[object], _row_value(row, 3, "acl_projection"))
            ),
        )
        for row in rows
    }


def _exercise_source_projection_after_downgrade(source_dsn: str, admin_dsn: str) -> None:
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        _create_test_login(admin, SOURCE_LOGIN, "source-test")
        _grant_source_projection_permissions(admin)
        admin.execute(
            sql.SQL("GRANT SELECT, INSERT ON TABLE m1_qualification_ingress_ledger TO {}").format(
                sql.Identifier(SOURCE_LOGIN)
            )
        )
        admin.execute(
            sql.SQL("GRANT USAGE ON SEQUENCE {} TO {}").format(
                sql.Identifier(_ledger_sequence(admin).split(".")[-1]),
                sql.Identifier(SOURCE_LOGIN),
            )
        )
    with psycopg.connect(source_dsn) as source:
        source.execute(
            """
            INSERT INTO m1_job_runtime_events (
                event_id, job_key, attempt_id, lease_epoch, worker_id,
                event_sequence, kind, stage, progress_sequence, progress_current,
                progress_total, detail, occurred_at, idempotency_key
            ) VALUES (
                'runtime-source-after-026-downgrade', 'job-source-026',
                'attempt-source-026', 1, 'worker-source-026', 2, 'job.stage-changed',
                'after-downgrade', 2, 1, 1, %s, %s,
                'runtime-source-after-026-downgrade'
            )
            """,
            (Jsonb({"reason_code": "after-downgrade"}), NOW + timedelta(seconds=3)),
        )
    with psycopg.connect(admin_dsn) as admin:
        row = admin.execute(
            """
            SELECT COUNT(*) FROM m1_qualification_ingress_ledger
            WHERE source = 'runtime'
              AND source_id = 'runtime-source-after-026-downgrade'
            """
        ).fetchone()
    assert row == (1,)


def _role_exists(connection: psycopg.Connection[object], role_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s",
        (role_name,),
    ).fetchone()
    return row is not None


def _freshness_payload(product: str, *, fact_id: str | None = None) -> dict[str, object]:
    return {
        "data_product": product,
        "fact_id": fact_id or f"freshness:{product}:026",
        "freshness_seconds": 1,
        "freshness_slo_seconds": 900,
        "observed_at": NOW.isoformat(),
        "progress_count": 1,
        "successful_count": 1,
    }


def _certificate_payload(epoch_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "bounds": {
            "max_gap_seconds": 900,
            "qualified_at": "2026-08-26T00:00:00+00:00",
            "required_seconds": 86400,
            "started_at": "2026-08-25T00:00:00+00:00",
        },
        "contained_incidents": [],
        "counts": {"progress_count": 12, "successful_count": 12},
        "identity": {
            "config_id": "config-a",
            "epoch_id": epoch_id,
            "policy_version": "m1-rolling-qualification-v1",
            "release_id": "release-a",
            "role_identity": ["m1", "structure"],
        },
        "policy_version": "m1-rolling-qualification-v1",
        "recovery_actions": [],
        "slo": {
            "evidence_gap_seconds": 900,
            "evidence_gap_status": "pass",
            "freshness": "pass",
            "recovery": "pass",
            "required_seconds": 86400,
        },
    }
    payload["evidence_digest"] = sha256(
        json.dumps(
            {
                "contained_incidents": [],
                "epoch_id": epoch_id,
                "fact_digests": [],
                "recovery_actions": [],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return payload


def _certificate_identity_key(payload: dict[str, object]) -> str:
    return sha256(
        json.dumps(
            {"bounds": payload["bounds"], "identity": payload["identity"]},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
