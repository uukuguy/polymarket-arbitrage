"""Persist append-only cloud evidence for formal transactional M1 soak runs.

Revision ID: 017
Revises: 016
Create Date: 2026-08-16

The observer is deliberately separate from the M1 data-plane workers.  Once a
run or observation is recorded it must not be edited into a healthy history.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_soak_runs",
        sa.Column("run_id", sa.Text, nullable=False),
        sa.Column("control_api_url", sa.Text, nullable=False),
        sa.Column("machine_ids", postgresql.JSONB, nullable=False),
        sa.Column("baseline_record", postgresql.JSONB, nullable=False),
        sa.Column("baseline_snapshot_sha256", sa.Text, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("run_id", name="pk_m1_soak_runs"),
        sa.CheckConstraint("run_id <> ''", name="ck_m1_soak_runs_id"),
        sa.CheckConstraint("control_api_url <> ''", name="ck_m1_soak_runs_api"),
        sa.CheckConstraint(
            "jsonb_typeof(machine_ids) = 'array' AND jsonb_array_length(machine_ids) > 0",
            name="ck_m1_soak_runs_machine_ids",
        ),
        sa.CheckConstraint(
            "baseline_snapshot_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_m1_soak_runs_baseline_digest",
        ),
    )
    op.create_table(
        "m1_soak_observations",
        sa.Column("run_id", sa.Text, nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("record", postgresql.JSONB, nullable=False),
        sa.Column("snapshot_sha256", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("run_id", "observed_at", name="pk_m1_soak_observations"),
        sa.ForeignKeyConstraint(
            ["run_id"], ["m1_soak_runs.run_id"], name="fk_m1_soak_observations_run"
        ),
        sa.UniqueConstraint("run_id", "snapshot_sha256", name="uq_m1_soak_observations_digest"),
        sa.CheckConstraint(
            "snapshot_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_m1_soak_observations_digest",
        ),
    )
    op.create_index(
        "idx_m1_soak_observations_run_time",
        "m1_soak_observations",
        ["run_id", "observed_at"],
    )
    op.execute(
        """
        CREATE FUNCTION m1_reject_soak_evidence_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'm1 soak evidence is append-only';
        END;
        $$;
        """
    )
    for table in ("m1_soak_runs", "m1_soak_observations"):
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_immutable
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION m1_reject_soak_evidence_mutation();
            """
        )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_m1_soak_observations_immutable ON m1_soak_observations")
    op.execute("DROP TRIGGER trg_m1_soak_runs_immutable ON m1_soak_runs")
    op.execute("DROP FUNCTION m1_reject_soak_evidence_mutation()")
    op.drop_index("idx_m1_soak_observations_run_time", table_name="m1_soak_observations")
    op.drop_table("m1_soak_observations")
    op.drop_table("m1_soak_runs")
