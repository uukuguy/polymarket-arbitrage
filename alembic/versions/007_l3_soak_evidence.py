"""Add the append-only L3 continuous-soak evidence schema.

Revision ID: 007
Revises: 006
Create Date: 2026-07-22

The five tables in this revision are evidence, not mutable application state.
Daemon credentials may append and read evidence.  Destructive retention is a
separate capability exposed only through a SECURITY DEFINER function.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None

EVIDENCE_TABLES = (
    "l3_runtime_boots",
    "l3_promote_runs",
    "l3_health_samples",
    "l3_market_samples",
    "l3_runtime_events",
)


def _recorded_at_column() -> sa.Column:
    return sa.Column(
        "recorded_at",
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("clock_timestamp()"),
    )


def _occurrence_window(column: str, *, nullable: bool = False) -> str:
    window = (
        f"{column} >= recorded_at - interval '24 hours' "
        f"AND {column} <= recorded_at + interval '30 seconds'"
    )
    return f"({column} IS NULL OR ({window}))" if nullable else window


def _create_append_only_trigger(table: str) -> None:
    op.execute(
        f"CREATE TRIGGER trg_l3_evidence_append_only "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW "
        f"EXECUTE FUNCTION l3_evidence_append_only_guard()"
    )


def upgrade() -> None:
    op.create_table(
        "l3_runtime_boots",
        sa.Column("boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("machine_id", sa.Text, nullable=False),
        sa.Column("machine_version", sa.Text, nullable=False),
        sa.Column("image_ref", sa.Text, nullable=False),
        sa.Column("release_id", sa.Text, nullable=False),
        sa.Column("code_version", sa.Text, nullable=False),
        sa.Column("acceptance_config_hash", sa.String(64), nullable=False),
        _recorded_at_column(),
        sa.PrimaryKeyConstraint("boot_id", name="pk_l3_runtime_boots"),
        sa.CheckConstraint(
            _occurrence_window("started_at") + " AND stopped_at IS NULL",
            name="ck_l3_runtime_boots_occurrence_window",
        ),
        sa.CheckConstraint(
            "acceptance_config_hash ~ '^[0-9a-f]{64}$'",
            name="ck_l3_runtime_boots_hash_len",
        ),
    )
    op.create_index(
        "idx_l3_runtime_boots_started_boot",
        "l3_runtime_boots",
        ["started_at", "boot_id"],
    )
    op.create_index(
        "idx_l3_runtime_boots_machine_version",
        "l3_runtime_boots",
        ["machine_id", "machine_version"],
    )

    op.create_table(
        "l3_promote_runs",
        sa.Column(
            "id",
            sa.BigInteger,
            sa.Identity(always=False),
            nullable=False,
        ),
        sa.Column("boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_seq", sa.BigInteger, nullable=False),
        sa.Column("scheduled_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=False),
        sa.Column("selected_count", sa.Integer, nullable=False),
        sa.Column("desired_count", sa.Integer, nullable=False),
        sa.Column("committed_count", sa.Integer, nullable=False),
        sa.Column("evidenced_count", sa.Integer, nullable=False),
        sa.Column("add_count", sa.Integer, nullable=False),
        sa.Column("remove_count", sa.Integer, nullable=False),
        sa.Column("mapping_hash", sa.String(64), nullable=False),
        sa.Column("desired_hash", sa.String(64), nullable=False),
        sa.Column("committed_hash", sa.String(64), nullable=False),
        sa.Column("acceptance_config_hash", sa.String(64), nullable=False),
        sa.Column("ws_generation", sa.BigInteger, nullable=False),
        sa.Column("add_succeeded", sa.Boolean, nullable=True),
        sa.Column("remove_succeeded", sa.Boolean, nullable=True),
        sa.Column("mirror_succeeded", sa.Boolean, nullable=False),
        sa.Column("duration_ms", sa.BigInteger, nullable=False),
        _recorded_at_column(),
        sa.PrimaryKeyConstraint("id", name="pk_l3_promote_runs"),
        sa.ForeignKeyConstraint(
            ["boot_id"],
            ["l3_runtime_boots.boot_id"],
            name="fk_l3_promote_runs_boot",
        ),
        sa.UniqueConstraint("boot_id", "run_seq", name="uq_l3_promote_runs_boot_seq"),
        sa.CheckConstraint(
            "run_seq >= 0 AND selected_count >= 0 AND desired_count >= 0 "
            "AND committed_count >= 0 AND evidenced_count >= 0 "
            "AND add_count >= 0 AND remove_count >= 0 AND ws_generation >= 0 "
            "AND duration_ms >= 0",
            name="ck_l3_promote_runs_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('success', 'frozen', 'underfilled', 'failed')",
            name="ck_l3_promote_runs_status",
        ),
        sa.CheckConstraint(
            _occurrence_window("scheduled_at")
            + " AND "
            + _occurrence_window("started_at")
            + " AND "
            + _occurrence_window("finished_at"),
            name="ck_l3_promote_runs_occurrence_window",
        ),
        sa.CheckConstraint(
            "mapping_hash ~ '^[0-9a-f]{64}$' "
            "AND desired_hash ~ '^[0-9a-f]{64}$' "
            "AND committed_hash ~ '^[0-9a-f]{64}$' "
            "AND acceptance_config_hash ~ '^[0-9a-f]{64}$'",
            name="ck_l3_promote_runs_hash_lengths",
        ),
    )
    op.create_index(
        "idx_l3_promote_runs_scheduled_boot",
        "l3_promote_runs",
        ["scheduled_at", "boot_id"],
    )
    op.create_index(
        "idx_l3_promote_runs_boot_started",
        "l3_promote_runs",
        ["boot_id", "started_at"],
    )
    op.create_index("idx_l3_promote_runs_status", "l3_promote_runs", ["status"])

    op.create_table(
        "l3_health_samples",
        sa.Column("boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sample_seq", sa.BigInteger, nullable=False),
        sa.Column("sampled_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("desired_count", sa.Integer, nullable=False),
        sa.Column("committed_count", sa.Integer, nullable=False),
        sa.Column("evidenced_count", sa.Integer, nullable=False),
        sa.Column("promote_age_ms", sa.BigInteger, nullable=True),
        sa.Column("global_book_age_ms", sa.BigInteger, nullable=True),
        sa.Column("ws_age_ms", sa.BigInteger, nullable=True),
        sa.Column("mirror_age_ms", sa.BigInteger, nullable=True),
        sa.Column("candidate_age_ms", sa.BigInteger, nullable=True),
        sa.Column("reconciliation_age_ms", sa.BigInteger, nullable=True),
        sa.Column("listener_state", sa.String(32), nullable=False),
        sa.Column("cursor_lag", sa.BigInteger, nullable=False),
        sa.Column("watchdog_count", sa.BigInteger, nullable=False),
        sa.Column("reconnect_count", sa.BigInteger, nullable=False),
        sa.Column("ws_generation", sa.BigInteger, nullable=False),
        sa.Column("mapping_hash", sa.String(64), nullable=False),
        sa.Column("acceptance_config_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=False),
        _recorded_at_column(),
        sa.PrimaryKeyConstraint("boot_id", "sample_seq", name="pk_l3_health_samples"),
        sa.ForeignKeyConstraint(
            ["boot_id"],
            ["l3_runtime_boots.boot_id"],
            name="fk_l3_health_samples_boot",
        ),
        sa.CheckConstraint(
            "sample_seq >= 0 AND desired_count >= 0 AND committed_count >= 0 "
            "AND evidenced_count >= 0 "
            "AND (promote_age_ms IS NULL OR promote_age_ms >= 0) "
            "AND (global_book_age_ms IS NULL OR global_book_age_ms >= 0) "
            "AND (ws_age_ms IS NULL OR ws_age_ms >= 0) "
            "AND (mirror_age_ms IS NULL OR mirror_age_ms >= 0) "
            "AND (candidate_age_ms IS NULL OR candidate_age_ms >= 0) "
            "AND (reconciliation_age_ms IS NULL OR reconciliation_age_ms >= 0) "
            "AND cursor_lag >= 0 AND watchdog_count >= 0 "
            "AND reconnect_count >= 0 AND ws_generation >= 0",
            name="ck_l3_health_samples_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('pass', 'warn', 'fail')",
            name="ck_l3_health_samples_status",
        ),
        sa.CheckConstraint(
            _occurrence_window("sampled_at"),
            name="ck_l3_health_samples_occurrence_window",
        ),
        sa.CheckConstraint(
            "mapping_hash ~ '^[0-9a-f]{64}$' AND acceptance_config_hash ~ '^[0-9a-f]{64}$'",
            name="ck_l3_health_samples_hash_lengths",
        ),
    )
    op.create_index(
        "idx_l3_health_samples_sampled_boot",
        "l3_health_samples",
        ["sampled_at", "boot_id"],
    )
    op.create_index(
        "idx_l3_health_samples_boot_seq",
        "l3_health_samples",
        ["boot_id", "sample_seq"],
    )

    op.create_table(
        "l3_market_samples",
        sa.Column("boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sample_seq", sa.BigInteger, nullable=False),
        sa.Column("sampled_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("market_id", sa.Text, nullable=False),
        sa.Column("yes_token_id", sa.Text, nullable=False),
        sa.Column("no_token_id", sa.Text, nullable=False),
        sa.Column("yes_desired", sa.Boolean, nullable=False),
        sa.Column("no_desired", sa.Boolean, nullable=False),
        sa.Column("yes_committed", sa.Boolean, nullable=False),
        sa.Column("no_committed", sa.Boolean, nullable=False),
        sa.Column("yes_evidenced", sa.Boolean, nullable=False),
        sa.Column("no_evidenced", sa.Boolean, nullable=False),
        sa.Column("evidence_generation", sa.BigInteger, nullable=False),
        sa.Column("yes_book_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("no_book_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("yes_book_age_ms", sa.BigInteger, nullable=True),
        sa.Column("no_book_age_ms", sa.BigInteger, nullable=True),
        sa.Column("worst_book_age_ms", sa.BigInteger, nullable=True),
        sa.Column("yes_ohlc_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("yes_ohlc_age_ms", sa.BigInteger, nullable=True),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=False),
        _recorded_at_column(),
        sa.PrimaryKeyConstraint("boot_id", "sample_seq", "market_id", name="pk_l3_market_samples"),
        sa.ForeignKeyConstraint(
            ["boot_id", "sample_seq"],
            ["l3_health_samples.boot_id", "l3_health_samples.sample_seq"],
            name="fk_l3_market_samples_health",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "boot_id",
            "sample_seq",
            "yes_token_id",
            name="uq_l3_market_samples_yes_token",
        ),
        sa.UniqueConstraint(
            "boot_id",
            "sample_seq",
            "no_token_id",
            name="uq_l3_market_samples_no_token",
        ),
        sa.CheckConstraint(
            "sample_seq >= 0 AND evidence_generation >= 0 "
            "AND (yes_book_age_ms IS NULL OR yes_book_age_ms >= 0) "
            "AND (no_book_age_ms IS NULL OR no_book_age_ms >= 0) "
            "AND (worst_book_age_ms IS NULL OR worst_book_age_ms >= 0) "
            "AND (yes_ohlc_age_ms IS NULL OR yes_ohlc_age_ms >= 0)",
            name="ck_l3_market_samples_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('pass', 'warn', 'fail')",
            name="ck_l3_market_samples_status",
        ),
        sa.CheckConstraint(
            _occurrence_window("sampled_at"),
            name="ck_l3_market_samples_occurrence_window",
        ),
    )
    op.create_index(
        "idx_l3_market_samples_sampled_market",
        "l3_market_samples",
        ["sampled_at", "market_id"],
    )
    op.create_index(
        "idx_l3_market_samples_yes_sampled",
        "l3_market_samples",
        ["yes_token_id", "sampled_at"],
    )
    op.create_index(
        "idx_l3_market_samples_no_sampled",
        "l3_market_samples",
        ["no_token_id", "sampled_at"],
    )

    op.create_table(
        "l3_runtime_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("boot_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_seq", sa.BigInteger, nullable=False),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("kind", sa.String(64), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("generation", sa.BigInteger, nullable=True),
        sa.Column("reason_code", sa.String(64), nullable=True),
        sa.Column("detail", postgresql.JSONB, nullable=True),
        _recorded_at_column(),
        sa.PrimaryKeyConstraint("event_id", name="pk_l3_runtime_events"),
        sa.ForeignKeyConstraint(
            ["boot_id"],
            ["l3_runtime_boots.boot_id"],
            name="fk_l3_runtime_events_boot",
        ),
        sa.UniqueConstraint("boot_id", "event_seq", name="uq_l3_runtime_events_boot_seq"),
        sa.CheckConstraint(
            "event_seq >= 0 AND (generation IS NULL OR generation >= 0)",
            name="ck_l3_runtime_events_nonnegative",
        ),
        sa.CheckConstraint(
            "kind IN ('watchdog_stale', 'reconnect_reserved', "
            "'reconnect_deferred', 'reconnect_started', 'reconnect_succeeded', "
            "'reconnect_failed', 'ws_generation_changed', "
            "'subscription_control_failed', 'subscription_compensated', "
            "'evidence_writer_failed', 'evidence_writer_recovered', "
            "'shutdown_signal', 'soak_manifest_bound', "
            "'checkpoint_report_bound')",
            name="ck_l3_runtime_events_kind",
        ),
        sa.CheckConstraint(
            "severity IN ('info', 'warning', 'critical')",
            name="ck_l3_runtime_events_severity",
        ),
        sa.CheckConstraint(
            "detail IS NULL OR (jsonb_typeof(detail) = 'object' "
            "AND octet_length(detail::text) <= 2048)",
            name="ck_l3_runtime_events_detail_size",
        ),
        sa.CheckConstraint(
            _occurrence_window("occurred_at"),
            name="ck_l3_runtime_events_occurrence_window",
        ),
    )
    op.create_index(
        "idx_l3_runtime_events_occurred_kind",
        "l3_runtime_events",
        ["occurred_at", "kind"],
    )
    op.create_index(
        "idx_l3_runtime_events_boot_seq",
        "l3_runtime_events",
        ["boot_id", "event_seq"],
    )

    op.execute(
        """
        CREATE FUNCTION l3_evidence_append_only_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            table_owner name;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.recorded_at := clock_timestamp();
                RETURN NEW;
            END IF;

            SELECT pg_get_userbyid(c.relowner) INTO table_owner
            FROM pg_class AS c WHERE c.oid = TG_RELID;
            IF current_setting('polyarb.retention_cleanup', true) IS DISTINCT FROM 'on'
               OR current_user IS DISTINCT FROM table_owner
            THEN
                RAISE EXCEPTION 'L3 soak evidence is append-only';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    for table in EVIDENCE_TABLES:
        _create_append_only_trigger(table)

    op.execute(
        """
        CREATE FUNCTION l3_retention_cleanup(
            cutoff timestamptz,
            protected_start timestamptz,
            protected_end timestamptz
        )
        RETURNS TABLE (
            runtime_boots_deleted bigint,
            promote_runs_deleted bigint,
            health_samples_deleted bigint,
            market_samples_deleted bigint,
            runtime_events_deleted bigint
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF cutoff IS NULL OR cutoff > clock_timestamp() - interval '30 days' THEN
                RAISE EXCEPTION 'retention cutoff must be at least 30 days old';
            END IF;
            IF protected_start IS NULL OR protected_end IS NULL
               OR protected_start >= protected_end
            THEN
                RAISE EXCEPTION 'invalid protected retention interval';
            END IF;

            PERFORM set_config('polyarb.retention_cleanup', 'on', true);

            DELETE FROM public.l3_market_samples
            WHERE recorded_at < cutoff AND sampled_at < cutoff
              AND NOT (sampled_at >= protected_start AND sampled_at < protected_end);
            GET DIAGNOSTICS market_samples_deleted = ROW_COUNT;

            DELETE FROM public.l3_health_samples
            WHERE recorded_at < cutoff AND sampled_at < cutoff
              AND NOT (sampled_at >= protected_start AND sampled_at < protected_end)
              AND NOT EXISTS (
                  SELECT 1 FROM public.l3_market_samples AS market
                  WHERE market.boot_id = l3_health_samples.boot_id
                    AND market.sample_seq = l3_health_samples.sample_seq
              );
            GET DIAGNOSTICS health_samples_deleted = ROW_COUNT;

            DELETE FROM public.l3_promote_runs
            WHERE recorded_at < cutoff AND scheduled_at < cutoff
              AND NOT (scheduled_at >= protected_start AND scheduled_at < protected_end);
            GET DIAGNOSTICS promote_runs_deleted = ROW_COUNT;

            DELETE FROM public.l3_runtime_events
            WHERE recorded_at < cutoff AND occurred_at < cutoff
              AND NOT (occurred_at >= protected_start AND occurred_at < protected_end);
            GET DIAGNOSTICS runtime_events_deleted = ROW_COUNT;

            DELETE FROM public.l3_runtime_boots
            WHERE recorded_at < cutoff AND started_at < cutoff
              AND NOT (started_at >= protected_start AND started_at < protected_end)
              AND NOT EXISTS (
                  SELECT 1 FROM public.l3_promote_runs AS promote
                  WHERE promote.boot_id = l3_runtime_boots.boot_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM public.l3_health_samples AS health
                  WHERE health.boot_id = l3_runtime_boots.boot_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM public.l3_runtime_events AS event
                  WHERE event.boot_id = l3_runtime_boots.boot_id
              );
            GET DIAGNOSTICS runtime_boots_deleted = ROW_COUNT;

            RETURN NEXT;
        END;
        $function$;
        """
    )

    capability_attributes = (
        "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
    )
    op.execute(f"CREATE ROLE l3_evidence_daemon {capability_attributes}")
    op.execute(f"CREATE ROLE l3_retention_operator {capability_attributes}")
    tables = ", ".join(EVIDENCE_TABLES)
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {tables} "
        "FROM PUBLIC, anon, authenticated, service_role, l3_evidence_daemon"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE {tables} TO service_role")
    op.execute(f"GRANT SELECT, INSERT ON TABLE {tables} TO l3_evidence_daemon")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE l3_promote_runs_id_seq TO service_role")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE l3_promote_runs_id_seq TO l3_evidence_daemon")
    op.execute(
        "GRANT SELECT ON TABLE l2_book_levels, l2_top_of_book, l2_ohlc_1m, snapshots "
        "TO l3_evidence_daemon"
    )
    op.execute("GRANT SELECT ON TABLE markets_latest TO l3_evidence_daemon")
    op.execute("GRANT SELECT, INSERT, UPDATE ON TABLE l2_event_cursor TO l3_evidence_daemon")
    op.execute("ALTER POLICY anon_read ON l2_event_cursor TO anon")
    op.execute(
        "CREATE POLICY l3_candidate_cursor_select ON l2_event_cursor "
        "FOR SELECT TO l3_evidence_daemon "
        "USING (consumer = 'l2-candidate-refresh')"
    )
    op.execute(
        "CREATE POLICY l3_candidate_cursor_insert ON l2_event_cursor "
        "FOR INSERT TO l3_evidence_daemon "
        "WITH CHECK (consumer = 'l2-candidate-refresh')"
    )
    op.execute(
        "CREATE POLICY l3_candidate_cursor_update ON l2_event_cursor "
        "FOR UPDATE TO l3_evidence_daemon "
        "USING (consumer = 'l2-candidate-refresh') "
        "WITH CHECK (consumer = 'l2-candidate-refresh')"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "l3_retention_cleanup(timestamptz,timestamptz,timestamptz) "
        "FROM PUBLIC, anon, authenticated, service_role, l3_evidence_daemon"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION "
        "l3_retention_cleanup(timestamptz,timestamptz,timestamptz) "
        "TO l3_retention_operator"
    )


def downgrade() -> None:
    op.execute("DROP POLICY l3_candidate_cursor_update ON l2_event_cursor")
    op.execute("DROP POLICY l3_candidate_cursor_insert ON l2_event_cursor")
    op.execute("DROP POLICY l3_candidate_cursor_select ON l2_event_cursor")
    op.execute("ALTER POLICY anon_read ON l2_event_cursor TO PUBLIC")
    op.execute("REVOKE SELECT, INSERT, UPDATE ON TABLE l2_event_cursor FROM l3_evidence_daemon")
    op.execute(
        "REVOKE SELECT ON TABLE l2_book_levels, l2_top_of_book, l2_ohlc_1m, snapshots "
        "FROM l3_evidence_daemon"
    )
    op.execute("REVOKE SELECT ON TABLE markets_latest FROM l3_evidence_daemon")
    for table in reversed(EVIDENCE_TABLES):
        op.execute(f"DROP TRIGGER trg_l3_evidence_append_only ON {table}")
    op.execute("DROP FUNCTION l3_retention_cleanup(timestamptz,timestamptz,timestamptz)")
    op.execute("DROP FUNCTION l3_evidence_append_only_guard()")
    op.drop_table("l3_runtime_events")
    op.drop_table("l3_market_samples")
    op.drop_table("l3_health_samples")
    op.drop_table("l3_promote_runs")
    op.drop_table("l3_runtime_boots")
    op.execute("DROP ROLE l3_retention_operator")
    op.execute("DROP ROLE l3_evidence_daemon")
