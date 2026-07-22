"""Alembic 007 contract tests for the append-only L3 soak evidence schema."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

MIGRATION_PATH = Path("alembic/versions/007_l3_soak_evidence.py")
EVIDENCE_TABLES = {
    "l3_runtime_boots",
    "l3_promote_runs",
    "l3_health_samples",
    "l3_market_samples",
    "l3_runtime_events",
}
RUNTIME_EVENT_KINDS = {
    "watchdog_stale",
    "reconnect_reserved",
    "reconnect_deferred",
    "reconnect_started",
    "reconnect_succeeded",
    "reconnect_failed",
    "ws_generation_changed",
    "subscription_control_failed",
    "subscription_compensated",
    "evidence_writer_failed",
    "evidence_writer_recovered",
    "shutdown_signal",
    "soak_manifest_bound",
    "checkpoint_report_bound",
}


def test_007_revision_chains_directly_to_006() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    assert 'revision = "007"' in text
    assert 'down_revision = "006"' in text


def test_007_upgrade_source_is_add_only() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    upgrade = text[text.index("def upgrade(") : text.index("def downgrade(")]
    assert "op.drop_" not in upgrade
    assert "DROP TABLE" not in upgrade.upper()
    assert "ALTER COLUMN" not in upgrade.upper()


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def pg_dsn() -> str:
    if not _docker_available():
        pytest.skip("Docker daemon unavailable; 007 replay acceptance is BLOCKED")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if url.startswith(prefix):
                url = "postgresql://" + url[len(prefix) :]
                break
        asyncio.run(_create_supabase_roles(url))
        yield url


async def _create_supabase_roles(dsn: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        for role in ("anon", "authenticated", "service_role"):
            await conn.execute(f"CREATE ROLE {role} NOLOGIN")
    finally:
        await conn.close()


def _run_alembic(dsn: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )


async def _fetch(dsn: str, query: str, *args: object) -> list[dict]:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        return [dict(row) for row in await conn.fetch(query, *args)]
    finally:
        await conn.close()


async def _execute(dsn: str, *statements: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        for statement in statements:
            await conn.execute(statement)
    finally:
        await conn.close()


def _q(dsn: str, query: str, *args: object) -> list[dict]:
    return asyncio.run(_fetch(dsn, query, *args))


def _schema_signature(dsn: str) -> dict[str, list[dict]]:
    return {
        "tables": _q(
            dsn,
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
        ),
        "columns": _q(
            dsn,
            "SELECT table_name, column_name, data_type, is_nullable "
            "FROM information_schema.columns WHERE table_schema='public' "
            "ORDER BY table_name, ordinal_position",
        ),
        "indexes": _q(
            dsn,
            "SELECT tablename, indexname, indexdef FROM pg_indexes "
            "WHERE schemaname='public' ORDER BY tablename, indexname",
        ),
        "views": _q(
            dsn,
            "SELECT table_name, view_definition FROM information_schema.views "
            "WHERE table_schema='public' ORDER BY table_name",
        ),
    }


def _assert_catalog_contract(dsn: str) -> None:
    columns = _q(
        dsn,
        "SELECT table_name, column_name, data_type, udt_name, is_nullable, "
        "column_default FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name = ANY($1::text[]) "
        "ORDER BY table_name, ordinal_position",
        sorted(EVIDENCE_TABLES),
    )
    by_table: dict[str, dict[str, dict]] = {}
    for row in columns:
        by_table.setdefault(row["table_name"], {})[row["column_name"]] = row
    assert set(by_table) == EVIDENCE_TABLES

    expected_columns = {
        "l3_runtime_boots": {
            "boot_id",
            "started_at",
            "stopped_at",
            "machine_id",
            "machine_version",
            "image_ref",
            "release_id",
            "code_version",
            "acceptance_config_hash",
            "recorded_at",
        },
        "l3_promote_runs": {
            "id",
            "boot_id",
            "run_seq",
            "scheduled_at",
            "started_at",
            "finished_at",
            "status",
            "reason_code",
            "selected_count",
            "desired_count",
            "committed_count",
            "evidenced_count",
            "add_count",
            "remove_count",
            "mapping_hash",
            "desired_hash",
            "committed_hash",
            "acceptance_config_hash",
            "ws_generation",
            "add_succeeded",
            "remove_succeeded",
            "mirror_succeeded",
            "duration_ms",
            "recorded_at",
        },
        "l3_health_samples": {
            "boot_id",
            "sample_seq",
            "sampled_at",
            "desired_count",
            "committed_count",
            "evidenced_count",
            "promote_age_ms",
            "global_book_age_ms",
            "ws_age_ms",
            "mirror_age_ms",
            "candidate_age_ms",
            "reconciliation_age_ms",
            "listener_state",
            "cursor_lag",
            "watchdog_count",
            "reconnect_count",
            "ws_generation",
            "mapping_hash",
            "acceptance_config_hash",
            "status",
            "reason_code",
            "recorded_at",
        },
        "l3_market_samples": {
            "boot_id",
            "sample_seq",
            "sampled_at",
            "market_id",
            "yes_token_id",
            "no_token_id",
            "yes_desired",
            "no_desired",
            "yes_committed",
            "no_committed",
            "yes_evidenced",
            "no_evidenced",
            "evidence_generation",
            "yes_book_at",
            "no_book_at",
            "yes_book_age_ms",
            "no_book_age_ms",
            "worst_book_age_ms",
            "yes_ohlc_at",
            "yes_ohlc_age_ms",
            "status",
            "reason_code",
            "recorded_at",
        },
        "l3_runtime_events": {
            "event_id",
            "boot_id",
            "event_seq",
            "occurred_at",
            "kind",
            "severity",
            "generation",
            "reason_code",
            "detail",
            "recorded_at",
        },
    }
    assert {table: set(cols) for table, cols in by_table.items()} == expected_columns

    for table in EVIDENCE_TABLES:
        recorded = by_table[table]["recorded_at"]
        assert recorded["data_type"] == "timestamp with time zone"
        assert recorded["is_nullable"] == "NO"
        assert recorded["column_default"] == "clock_timestamp()"
    assert by_table["l3_runtime_boots"]["boot_id"]["udt_name"] == "uuid"
    assert by_table["l3_runtime_events"]["event_id"]["udt_name"] == "uuid"
    assert by_table["l3_promote_runs"]["id"]["data_type"] == "bigint"
    assert by_table["l3_runtime_events"]["detail"]["udt_name"] == "jsonb"
    assert by_table["l3_runtime_boots"]["stopped_at"]["is_nullable"] == "YES"
    assert {
        by_table["l3_promote_runs"][column]["is_nullable"]
        for column in ("scheduled_at", "started_at", "finished_at")
    } == {"NO"}

    index_rows = _q(
        dsn,
        "SELECT tablename, indexname, indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename = ANY($1::text[])",
        sorted(EVIDENCE_TABLES),
    )
    indexes = {row["indexname"]: row for row in index_rows}
    expected_indexes = {
        "idx_l3_runtime_boots_started_boot": ("started_at", "boot_id"),
        "idx_l3_runtime_boots_machine_version": ("machine_id", "machine_version"),
        "idx_l3_promote_runs_scheduled_boot": ("scheduled_at", "boot_id"),
        "idx_l3_promote_runs_boot_started": ("boot_id", "started_at"),
        "idx_l3_promote_runs_status": ("status",),
        "idx_l3_health_samples_sampled_boot": ("sampled_at", "boot_id"),
        "idx_l3_health_samples_boot_seq": ("boot_id", "sample_seq"),
        "idx_l3_market_samples_sampled_market": ("sampled_at", "market_id"),
        "idx_l3_market_samples_yes_sampled": ("yes_token_id", "sampled_at"),
        "idx_l3_market_samples_no_sampled": ("no_token_id", "sampled_at"),
        "idx_l3_runtime_events_occurred_kind": ("occurred_at", "kind"),
        "idx_l3_runtime_events_boot_seq": ("boot_id", "event_seq"),
    }
    constraint_indexes = {
        "pk_l3_runtime_boots",
        "pk_l3_promote_runs",
        "uq_l3_promote_runs_boot_seq",
        "pk_l3_health_samples",
        "pk_l3_market_samples",
        "uq_l3_market_samples_yes_token",
        "uq_l3_market_samples_no_token",
        "pk_l3_runtime_events",
        "uq_l3_runtime_events_boot_seq",
    }
    assert set(indexes) == set(expected_indexes) | constraint_indexes
    for name, columns in expected_indexes.items():
        assert f"({', '.join(columns)})" in indexes[name]["indexdef"]

    constraints = _q(
        dsn,
        "SELECT conname, contype, pg_get_constraintdef(oid) AS definition "
        "FROM pg_constraint WHERE conrelid::regclass::text = ANY($1::text[])",
        sorted(EVIDENCE_TABLES),
    )
    names = {row["conname"] for row in constraints}
    expected_named = {
        "pk_l3_runtime_boots",
        "ck_l3_runtime_boots_occurrence_window",
        "ck_l3_runtime_boots_hash_len",
        "pk_l3_promote_runs",
        "fk_l3_promote_runs_boot",
        "uq_l3_promote_runs_boot_seq",
        "ck_l3_promote_runs_nonnegative",
        "ck_l3_promote_runs_status",
        "ck_l3_promote_runs_occurrence_window",
        "ck_l3_promote_runs_hash_lengths",
        "pk_l3_health_samples",
        "fk_l3_health_samples_boot",
        "ck_l3_health_samples_nonnegative",
        "ck_l3_health_samples_status",
        "ck_l3_health_samples_occurrence_window",
        "ck_l3_health_samples_hash_lengths",
        "pk_l3_market_samples",
        "fk_l3_market_samples_health",
        "uq_l3_market_samples_yes_token",
        "uq_l3_market_samples_no_token",
        "ck_l3_market_samples_nonnegative",
        "ck_l3_market_samples_status",
        "ck_l3_market_samples_occurrence_window",
        "pk_l3_runtime_events",
        "fk_l3_runtime_events_boot",
        "uq_l3_runtime_events_boot_seq",
        "ck_l3_runtime_events_nonnegative",
        "ck_l3_runtime_events_kind",
        "ck_l3_runtime_events_severity",
        "ck_l3_runtime_events_detail_size",
        "ck_l3_runtime_events_occurrence_window",
    }
    assert expected_named <= names
    promote_status = next(
        row for row in constraints if row["conname"] == "ck_l3_promote_runs_status"
    )["definition"]
    assert all(
        status in promote_status for status in ("success", "frozen", "underfilled", "failed")
    )
    runtime_event_kind = next(
        row for row in constraints if row["conname"] == "ck_l3_runtime_events_kind"
    )["definition"]
    assert set(re.findall(r"'([^']+)'", runtime_event_kind)) == RUNTIME_EVENT_KINDS
    market_fk = next(row for row in constraints if row["conname"] == "fk_l3_market_samples_health")
    assert "ON DELETE CASCADE" in market_fk["definition"]

    trigger_rows = _q(
        dsn,
        "SELECT event_object_table, trigger_name, action_statement "
        "FROM information_schema.triggers WHERE trigger_schema='public' "
        "AND event_object_table = ANY($1::text[]) ORDER BY event_object_table",
        sorted(EVIDENCE_TABLES),
    )
    assert {row["event_object_table"] for row in trigger_rows} == EVIDENCE_TABLES
    assert {row["trigger_name"] for row in trigger_rows} == {"trg_l3_evidence_append_only"}
    assert all("l3_evidence_append_only_guard" in row["action_statement"] for row in trigger_rows)


async def _assert_write_and_retention_contract(dsn: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    boot_id = "11111111-1111-4111-8111-111111111111"
    protected_boot_id = "22222222-2222-4222-8222-222222222222"
    eligible_boot_id = "33333333-3333-4333-8333-333333333333"
    try:
        await conn.execute(
            "INSERT INTO l3_runtime_boots "
            "(boot_id, started_at, machine_id, machine_version, image_ref, "
            " release_id, code_version, acceptance_config_hash) "
            "VALUES ($1, clock_timestamp(), 'machine-1', 'v1', 'image@sha256:test', "
            "'release-1', 'code-1', $2)",
            boot_id,
            "a" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_promote_runs "
            "(boot_id, run_seq, scheduled_at, started_at, finished_at, status, "
            " reason_code, selected_count, desired_count, committed_count, "
            " evidenced_count, add_count, remove_count, mapping_hash, desired_hash, "
            " committed_hash, acceptance_config_hash, ws_generation, add_succeeded, "
            " remove_succeeded, mirror_succeeded, duration_ms) VALUES "
            "($1, 0, clock_timestamp(), clock_timestamp(), clock_timestamp(), "
            " 'success', 'initial', 1, 2, 2, 2, 2, 0, $2, $2, $2, $2, 0, "
            " true, true, true, 1)",
            boot_id,
            "b" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_health_samples "
            "(boot_id, sample_seq, sampled_at, desired_count, committed_count, "
            " evidenced_count, listener_state, cursor_lag, watchdog_count, "
            " reconnect_count, ws_generation, mapping_hash, acceptance_config_hash, "
            " status, reason_code) VALUES "
            "($1, 0, clock_timestamp(), 2, 2, 2, 'connected', 0, 0, 0, 0, "
            " $2, $2, 'pass', 'healthy')",
            boot_id,
            "c" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_market_samples "
            "(boot_id, sample_seq, sampled_at, market_id, yes_token_id, no_token_id, "
            " yes_desired, no_desired, yes_committed, no_committed, yes_evidenced, "
            " no_evidenced, evidence_generation, yes_book_at, no_book_at, "
            " yes_book_age_ms, no_book_age_ms, worst_book_age_ms, yes_ohlc_at, "
            " yes_ohlc_age_ms, status, reason_code) VALUES "
            "($1, 0, clock_timestamp(), 'market-1', 'yes-1', 'no-1', true, true, "
            " true, true, true, true, 0, clock_timestamp(), clock_timestamp(), "
            " 0, 0, 0, clock_timestamp(), 0, 'pass', 'healthy')",
            boot_id,
        )
        for seq in (0, 1):
            await conn.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, "
                " generation, reason_code, detail) VALUES "
                "(gen_random_uuid(), $1, $2, clock_timestamp(), 'shutdown_signal', "
                " 'info', 0, 'test', $3::jsonb)",
                boot_id,
                seq,
                '{"signal":"TERM"}',
            )
        await conn.execute(
            "INSERT INTO l3_runtime_boots "
            "(boot_id, started_at, machine_id, machine_version, image_ref, "
            " release_id, code_version, acceptance_config_hash) "
            "VALUES ($1, clock_timestamp(), 'machine-2', 'v1', 'image@sha256:test', "
            "'release-1', 'code-1', $2)",
            protected_boot_id,
            "d" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_runtime_events "
            "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
            "VALUES (gen_random_uuid(), $1, 0, clock_timestamp(), "
            "'shutdown_signal', 'info', '{}'::jsonb)",
            protected_boot_id,
        )
        await conn.execute(
            "INSERT INTO l3_runtime_boots "
            "(boot_id, started_at, machine_id, machine_version, image_ref, "
            " release_id, code_version, acceptance_config_hash) "
            "VALUES ($1, clock_timestamp(), 'machine-3', 'v1', 'image@sha256:test', "
            "'release-1', 'code-1', $2)",
            eligible_boot_id,
            "e" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_promote_runs "
            "(boot_id, run_seq, scheduled_at, started_at, finished_at, status, "
            " reason_code, selected_count, desired_count, committed_count, "
            " evidenced_count, add_count, remove_count, mapping_hash, desired_hash, "
            " committed_hash, acceptance_config_hash, ws_generation, add_succeeded, "
            " remove_succeeded, mirror_succeeded, duration_ms) VALUES "
            "($1, 0, clock_timestamp(), clock_timestamp(), clock_timestamp(), "
            " 'success', 'cleanup', 1, 2, 2, 2, 2, 0, $2, $2, $2, $2, 0, "
            " true, true, true, 1)",
            eligible_boot_id,
            "f" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_health_samples "
            "(boot_id, sample_seq, sampled_at, desired_count, committed_count, "
            " evidenced_count, listener_state, cursor_lag, watchdog_count, "
            " reconnect_count, ws_generation, mapping_hash, acceptance_config_hash, "
            " status, reason_code) VALUES "
            "($1, 0, clock_timestamp(), 2, 2, 2, 'connected', 0, 0, 0, 0, "
            " $2, $2, 'pass', 'cleanup')",
            eligible_boot_id,
            "1" * 64,
        )
        await conn.execute(
            "INSERT INTO l3_market_samples "
            "(boot_id, sample_seq, sampled_at, market_id, yes_token_id, no_token_id, "
            " yes_desired, no_desired, yes_committed, no_committed, yes_evidenced, "
            " no_evidenced, evidence_generation, yes_book_age_ms, no_book_age_ms, "
            " worst_book_age_ms, yes_ohlc_age_ms, status, reason_code) VALUES "
            "($1, 0, clock_timestamp(), 'market-cleanup', 'yes-cleanup', 'no-cleanup', "
            " true, true, true, true, true, true, 0, 0, 0, 0, 0, 'pass', 'cleanup')",
            eligible_boot_id,
        )
        await conn.execute(
            "INSERT INTO l3_runtime_events "
            "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
            "VALUES (gen_random_uuid(), $1, 0, clock_timestamp(), "
            "'shutdown_signal', 'info', '{}'::jsonb)",
            eligible_boot_id,
        )

        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
                "VALUES (gen_random_uuid(), $1, 9, clock_timestamp() - interval '25 hours', "
                "'shutdown_signal', 'info', '{}'::jsonb)",
                boot_id,
            )
        server_owned = await conn.fetchval(
            "WITH inserted AS ("
            "  INSERT INTO l3_runtime_events "
            "  (event_id, boot_id, event_seq, occurred_at, kind, severity, detail, "
            "   recorded_at) VALUES (gen_random_uuid(), $1, 12, clock_timestamp(), "
            "   'shutdown_signal', 'info', '{}'::jsonb, "
            "   clock_timestamp()+interval '25 seconds') RETURNING recorded_at"
            ") SELECT recorded_at <= clock_timestamp()+interval '2 seconds' "
            "FROM inserted",
            boot_id,
        )
        assert server_owned, "recorded_at must be overwritten by the database"
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail, "
                " recorded_at) VALUES (gen_random_uuid(), $1, 13, "
                "clock_timestamp()+interval '55 seconds', 'shutdown_signal', "
                "'info', '{}'::jsonb, clock_timestamp()+interval '29 seconds')",
                boot_id,
            )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail, recorded_at) "
                "VALUES (gen_random_uuid(), $1, 14, clock_timestamp()-interval '25 hours', "
                "'shutdown_signal', 'info', '{}'::jsonb, "
                "clock_timestamp()-interval '25 hours')",
                boot_id,
            )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
                "VALUES (gen_random_uuid(), $1, 10, clock_timestamp() + interval '31 seconds', "
                "'shutdown_signal', 'info', '{}'::jsonb)",
                boot_id,
            )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
                "VALUES (gen_random_uuid(), $1, 11, clock_timestamp(), "
                "'shutdown_signal', 'info', jsonb_build_object('payload', repeat('x', 2100)))",
                boot_id,
            )
        with pytest.raises(asyncpg.exceptions.RaiseError):
            await conn.execute(
                "UPDATE l3_runtime_boots SET stopped_at=clock_timestamp() WHERE boot_id=$1",
                boot_id,
            )
        with pytest.raises(asyncpg.exceptions.RaiseError):
            await conn.execute(
                "DELETE FROM l3_runtime_events WHERE boot_id=$1 AND event_seq=0",
                boot_id,
            )

        await conn.execute("BEGIN")
        await conn.execute("SET LOCAL polyarb.retention_cleanup = 'on'")
        await conn.execute(
            "UPDATE l3_runtime_events SET recorded_at=clock_timestamp()-interval '40 days', "
            "occurred_at=clock_timestamp()-interval '40 days' "
            "WHERE boot_id=$1 AND event_seq=0",
            boot_id,
        )
        await conn.execute(
            "UPDATE l3_runtime_events SET recorded_at=clock_timestamp()-interval '35 days', "
            "occurred_at=clock_timestamp()-interval '35 days' "
            "WHERE boot_id=$1 AND event_seq=1",
            boot_id,
        )
        await conn.execute(
            "UPDATE l3_runtime_boots SET recorded_at=clock_timestamp()-interval '40 days', "
            "started_at=clock_timestamp()-interval '40 days' WHERE boot_id=$1",
            protected_boot_id,
        )
        await conn.execute(
            "UPDATE l3_runtime_events SET recorded_at=clock_timestamp()-interval '35 days', "
            "occurred_at=clock_timestamp()-interval '35 days' WHERE boot_id=$1",
            protected_boot_id,
        )
        await conn.execute(
            "UPDATE l3_runtime_boots SET recorded_at=clock_timestamp()-interval '40 days', "
            "started_at=clock_timestamp()-interval '40 days' WHERE boot_id=$1",
            eligible_boot_id,
        )
        await conn.execute(
            "UPDATE l3_promote_runs SET recorded_at=clock_timestamp()-interval '40 days', "
            "scheduled_at=clock_timestamp()-interval '40 days', "
            "started_at=clock_timestamp()-interval '40 days', "
            "finished_at=clock_timestamp()-interval '40 days' WHERE boot_id=$1",
            eligible_boot_id,
        )
        await conn.execute(
            "UPDATE l3_health_samples SET recorded_at=clock_timestamp()-interval '40 days', "
            "sampled_at=clock_timestamp()-interval '40 days' WHERE boot_id=$1",
            eligible_boot_id,
        )
        await conn.execute(
            "UPDATE l3_market_samples SET recorded_at=clock_timestamp()-interval '40 days', "
            "sampled_at=clock_timestamp()-interval '40 days' WHERE boot_id=$1",
            eligible_boot_id,
        )
        await conn.execute(
            "UPDATE l3_runtime_events SET recorded_at=clock_timestamp()-interval '40 days', "
            "occurred_at=clock_timestamp()-interval '40 days' WHERE boot_id=$1",
            eligible_boot_id,
        )
        await conn.execute("COMMIT")

        assert not await conn.fetchval(
            "SELECT has_function_privilege('service_role', "
            "'l3_retention_cleanup(timestamptz,timestamptz,timestamptz)', 'EXECUTE')"
        )
        assert await conn.fetchval(
            "SELECT has_function_privilege('l3_retention_operator', "
            "'l3_retention_cleanup(timestamptz,timestamptz,timestamptz)', 'EXECUTE')"
        )
        assert not await conn.fetchval(
            "SELECT pg_has_role('service_role', 'l3_retention_operator', 'MEMBER')"
        )

        await conn.execute("SET ROLE service_role")
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await conn.fetch(
                "SELECT * FROM l3_retention_cleanup("
                "clock_timestamp()-interval '31 days', "
                "clock_timestamp()-interval '36 days', "
                "clock_timestamp()-interval '34 days')"
            )
        await conn.execute("RESET ROLE")

        await conn.execute("SET ROLE l3_retention_operator")
        with pytest.raises(asyncpg.exceptions.RaiseError):
            await conn.fetch(
                "SELECT * FROM l3_retention_cleanup("
                "clock_timestamp()-interval '29 days', "
                "clock_timestamp()-interval '36 days', "
                "clock_timestamp()-interval '34 days')"
            )
        with pytest.raises(asyncpg.exceptions.RaiseError):
            await conn.fetch(
                "SELECT * FROM l3_retention_cleanup("
                "clock_timestamp()-interval '31 days', "
                "clock_timestamp()-interval '34 days', "
                "clock_timestamp()-interval '36 days')"
            )
        counts = dict(
            await conn.fetchrow(
                "SELECT * FROM l3_retention_cleanup("
                "clock_timestamp()-interval '31 days', "
                "clock_timestamp()-interval '36 days', "
                "clock_timestamp()-interval '34 days')"
            )
        )
        await conn.execute("RESET ROLE")
        assert counts == {
            "runtime_boots_deleted": 1,
            "promote_runs_deleted": 1,
            "health_samples_deleted": 1,
            "market_samples_deleted": 1,
            "runtime_events_deleted": 2,
        }
        remaining = await conn.fetch(
            "SELECT event_seq FROM l3_runtime_events WHERE boot_id=$1 ORDER BY event_seq",
            boot_id,
        )
        assert [row["event_seq"] for row in remaining] == [1, 12]
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM l3_runtime_events WHERE boot_id=$1",
                protected_boot_id,
            )
            == 1
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM l3_runtime_boots WHERE boot_id=$1)",
            eligible_boot_id,
        )

        privileges = await conn.fetch(
            "SELECT table_name, role_name, privilege_type, "
            "has_table_privilege(role_name, table_name, privilege_type) AS allowed "
            "FROM unnest($1::text[]) table_name "
            "CROSS JOIN unnest(ARRAY['anon','authenticated','service_role']) role_name "
            "CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE']) privilege_type "
            "ORDER BY table_name, role_name, privilege_type",
            sorted(EVIDENCE_TABLES),
        )
        allowed = {
            (row["table_name"], row["role_name"], row["privilege_type"])
            for row in privileges
            if row["allowed"]
        }
        assert allowed == {
            (table, "service_role", privilege)
            for table in EVIDENCE_TABLES
            for privilege in ("SELECT", "INSERT")
        }
    finally:
        await conn.close()


@pytest.mark.slow
def test_007_upgrade_downgrade_upgrade_roundtrip(pg_dsn: str) -> None:
    to_006 = _run_alembic(pg_dsn, "upgrade", "006")
    assert to_006.returncode == 0, to_006.stderr
    before = _schema_signature(pg_dsn)

    asyncio.run(
        _execute(
            pg_dsn,
            "CREATE ROLE l3_retention_operator LOGIN SUPERUSER",
            "GRANT l3_retention_operator TO service_role",
        )
    )
    collision = _run_alembic(pg_dsn, "upgrade", "007")
    assert collision.returncode != 0
    assert "already exists" in collision.stderr
    assert _q(pg_dsn, "SELECT version_num FROM alembic_version") == [{"version_num": "006"}]
    assert _q(
        pg_dsn,
        "SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname='l3_retention_operator'",
    ) == [{"rolcanlogin": True, "rolsuper": True}]
    assert _q(
        pg_dsn,
        "SELECT pg_has_role('service_role', 'l3_retention_operator', 'MEMBER') AS member",
    ) == [{"member": True}]
    asyncio.run(
        _execute(
            pg_dsn,
            "REVOKE l3_retention_operator FROM service_role",
            "DROP ROLE l3_retention_operator",
        )
    )

    first = _run_alembic(pg_dsn, "upgrade", "007")
    assert first.returncode == 0, first.stderr
    assert _q(
        pg_dsn,
        "SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit, "
        "rolreplication FROM pg_roles WHERE rolname='l3_retention_operator'",
    ) == [
        {
            "rolcanlogin": False,
            "rolsuper": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolinherit": False,
            "rolreplication": False,
        }
    ]
    after_tables = {
        row["tablename"]
        for row in _q(
            pg_dsn,
            "SELECT tablename FROM pg_tables WHERE schemaname='public'",
        )
    }
    before_tables = {row["tablename"] for row in before["tables"]}
    assert after_tables - before_tables == EVIDENCE_TABLES
    _assert_catalog_contract(pg_dsn)
    asyncio.run(_assert_write_and_retention_contract(pg_dsn))

    down = _run_alembic(pg_dsn, "downgrade", "006")
    assert down.returncode == 0, down.stderr
    assert _schema_signature(pg_dsn) == before
    assert (
        _q(
            pg_dsn,
            "SELECT 1 FROM pg_proc WHERE proname IN "
            "('l3_retention_cleanup', 'l3_evidence_append_only_guard')",
        )
        == []
    )
    assert (
        _q(
            pg_dsn,
            "SELECT 1 FROM pg_roles WHERE rolname='l3_retention_operator'",
        )
        == []
    )

    second = _run_alembic(pg_dsn, "upgrade", "007")
    assert second.returncode == 0, second.stderr
    assert _q(pg_dsn, "SELECT version_num FROM alembic_version") == [{"version_num": "007"}]
    _assert_catalog_contract(pg_dsn)
