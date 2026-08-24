"""Persist task-local runtime state and immutable lifecycle evidence.

Revision ID: 022
Revises: 021

The two tables are deliberately additive.  ``m1_job_runtime_state`` is the
bounded current projection used by a reconciler, while
``m1_job_runtime_events`` is the append-only historical evidence consumed by
the incident and qualification layers.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_job_runtime_state",
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("attempt_id", sa.Text, nullable=False),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False),
        sa.Column("worker_id", sa.Text, nullable=False),
        sa.Column("stage", sa.Text, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_heartbeat_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_progress_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("progress_sequence", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("progress_current", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("progress_total", sa.BigInteger, nullable=True),
        sa.Column("lease_deadline_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("heartbeat_deadline_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("progress_deadline_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("attempt_deadline_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("recovery_state", sa.Text, nullable=False, server_default="active"),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("job_key", name="pk_m1_job_runtime_state"),
        sa.UniqueConstraint("attempt_id", name="uq_m1_job_runtime_state_attempt"),
        sa.ForeignKeyConstraint(
            ["job_key"], ["m1_jobs.job_key"], name="fk_m1_runtime_state_job"
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"], ["m1_job_attempts.attempt_id"], name="fk_m1_runtime_state_attempt"
        ),
        sa.CheckConstraint("lease_epoch > 0", name="ck_m1_runtime_state_epoch"),
        sa.CheckConstraint(
            "progress_sequence >= 0 AND progress_current >= 0 AND "
            "(progress_total IS NULL OR (progress_total >= 0 AND "
            "progress_current <= progress_total))",
            name="ck_m1_runtime_state_progress",
        ),
        sa.CheckConstraint(
            "recovery_state IN ('active', 'suspect', 'recovering', 'recovered', 'terminal')",
            name="ck_m1_runtime_state_recovery",
        ),
    )
    op.create_index(
        "m1_job_runtime_state_deadlines",
        "m1_job_runtime_state",
        ["lease_deadline_at", "heartbeat_deadline_at", "progress_deadline_at"],
    )

    op.create_table(
        "m1_job_runtime_events",
        sa.Column("event_id", sa.Text, nullable=False),
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("attempt_id", sa.Text, nullable=False),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False),
        sa.Column("worker_id", sa.Text, nullable=False),
        sa.Column("event_sequence", sa.BigInteger, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("stage", sa.Text, nullable=False),
        sa.Column("progress_sequence", sa.BigInteger, nullable=True),
        sa.Column("progress_current", sa.BigInteger, nullable=True),
        sa.Column("progress_total", sa.BigInteger, nullable=True),
        sa.Column(
            "detail",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("event_id", name="pk_m1_job_runtime_events"),
        sa.ForeignKeyConstraint(
            ["job_key"], ["m1_jobs.job_key"], name="fk_m1_runtime_events_job"
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"], ["m1_job_attempts.attempt_id"], name="fk_m1_runtime_events_attempt"
        ),
        sa.UniqueConstraint(
            "attempt_id", "event_sequence", name="uq_m1_runtime_events_attempt_sequence"
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_m1_runtime_events_idempotency"),
        sa.CheckConstraint("lease_epoch > 0", name="ck_m1_runtime_events_epoch"),
        sa.CheckConstraint("event_sequence > 0", name="ck_m1_runtime_events_sequence"),
        sa.CheckConstraint(
            "progress_sequence IS NULL OR progress_sequence >= 0",
            name="ck_m1_runtime_events_progress_sequence",
        ),
        sa.CheckConstraint(
            "(progress_sequence IS NULL) = (progress_current IS NULL)",
            name="ck_m1_runtime_events_progress_pair",
        ),
        sa.CheckConstraint(
            "progress_current IS NULL OR progress_current >= 0",
            name="ck_m1_runtime_events_progress_current",
        ),
        sa.CheckConstraint(
            "progress_total IS NULL OR (progress_total >= 0 AND "
            "progress_current IS NOT NULL AND progress_current <= progress_total)",
            name="ck_m1_runtime_events_progress_total",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(detail) = 'object' AND octet_length(detail::text) <= 4096 "
            "AND pg_column_size(detail) <= 4096",
            name="ck_m1_runtime_events_detail_size",
        ),
        sa.CheckConstraint(
            "kind IN ('job.started', 'job.stage-changed', 'job.lease-at-risk', "
            "'job.progress-stalled', 'job.retryable-failed', 'job.retry-scheduled', "
            "'job.recovery-started', 'job.recovered', 'job.terminal-failed', 'job.succeeded')",
            name="ck_m1_runtime_events_kind",
        ),
    )
    op.create_index(
        "m1_job_runtime_events_job_occurred",
        "m1_job_runtime_events",
        ["job_key", "occurred_at", "event_sequence"],
    )
    op.create_index(
        "m1_job_runtime_events_attempt_sequence",
        "m1_job_runtime_events",
        ["attempt_id", "event_sequence"],
    )

    op.execute(
        """
        CREATE FUNCTION m1_reject_runtime_event_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'runtime events are append-only';
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER m1_runtime_events_immutable
        BEFORE UPDATE OR DELETE ON m1_job_runtime_events
        FOR EACH ROW EXECUTE FUNCTION m1_reject_runtime_event_mutation();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER m1_runtime_events_immutable ON m1_job_runtime_events")
    op.execute("DROP FUNCTION m1_reject_runtime_event_mutation()")
    op.drop_index("m1_job_runtime_events_attempt_sequence", table_name="m1_job_runtime_events")
    op.drop_index("m1_job_runtime_events_job_occurred", table_name="m1_job_runtime_events")
    op.drop_table("m1_job_runtime_events")
    op.drop_index("m1_job_runtime_state_deadlines", table_name="m1_job_runtime_state")
    op.drop_table("m1_job_runtime_state")
