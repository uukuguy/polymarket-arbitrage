"""Add fenced runtime controller leases and recovery action ledger.

Revision ID: 023
Revises: 022

The tables are additive runtime-control authority for the reconciler.  A
monotonically increasing controller lease fences scheduling decisions, and the
action ledger records both executable commands and durable stale no-ops.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_runtime_controller_leases",
        sa.Column("controller_id", sa.Text, nullable=False),
        sa.Column("owner_id", sa.Text, nullable=False),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "claimed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("controller_id", name="pk_m1_runtime_controller_leases"),
        sa.UniqueConstraint(
            "owner_id",
            "lease_epoch",
            name="uq_m1_runtime_controller_leases_owner_epoch",
        ),
        sa.CheckConstraint("lease_epoch > 0", name="ck_m1_runtime_controller_epoch"),
    )
    op.create_index(
        "m1_runtime_controller_leases_expiry",
        "m1_runtime_controller_leases",
        ["lease_expires_at"],
    )

    op.create_table(
        "m1_recovery_actions",
        sa.Column("action_id", sa.Text, nullable=False),
        sa.Column("controller_id", sa.Text, nullable=False),
        sa.Column("controller_owner_id", sa.Text, nullable=False),
        sa.Column("incident_key", sa.Text, nullable=True),
        sa.Column("target_type", sa.Text, nullable=False),
        sa.Column("target_id", sa.Text, nullable=False),
        sa.Column("action_type", sa.Text, nullable=False),
        sa.Column("expected_controller_epoch", sa.BigInteger, nullable=False),
        sa.Column("expected_attempt_id", sa.Text, nullable=False),
        sa.Column("expected_lease_epoch", sa.BigInteger, nullable=False),
        sa.Column("requested_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("result_code", sa.Text, nullable=True),
        sa.Column("next_allowed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("worker_id", sa.Text, nullable=True),
        sa.Column("worker_epoch", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("worker_lease_expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "detail",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("action_id", name="pk_m1_recovery_actions"),
        sa.ForeignKeyConstraint(
            ["controller_id"],
            ["m1_runtime_controller_leases.controller_id"],
            name="fk_m1_recovery_actions_controller",
        ),
        sa.ForeignKeyConstraint(
            ["incident_key"],
            ["m1_incidents.incident_key"],
            name="fk_m1_recovery_actions_incident",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["expected_attempt_id"],
            ["m1_job_attempts.attempt_id"],
            name="fk_m1_recovery_actions_attempt",
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_m1_recovery_actions_idempotency"),
        sa.CheckConstraint(
            "target_type IN ('job', 'circuit', 'worker-process', 'machine')",
            name="ck_m1_recovery_actions_target_type",
        ),
        sa.CheckConstraint(
            "action_type IN ('heartbeat-job', 'cancel-job', 'retry-job', 'reclaim-job', "
            "'probe-circuit', 'restart-worker-process', 'restart-machine')",
            name="ck_m1_recovery_actions_type",
        ),
        sa.CheckConstraint(
            "expected_controller_epoch > 0 AND expected_lease_epoch > 0 "
            "AND worker_epoch >= 0",
            name="ck_m1_recovery_actions_epochs",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'running', 'completed')",
            name="ck_m1_recovery_actions_state",
        ),
        sa.CheckConstraint(
            "((state IN ('pending', 'running') AND result_code IS NULL AND finished_at IS NULL) "
            "OR (state = 'completed' AND result_code IN "
            "('succeeded', 'failed', 'stale-noop', 'disabled-action')))",
            name="ck_m1_recovery_actions_result_code",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(detail) = 'object' AND octet_length(detail::text) <= 4096 "
            "AND pg_column_size(detail) <= 4096",
            name="ck_m1_recovery_actions_detail_size",
        ),
    )
    op.create_index(
        "m1_recovery_actions_target_requested",
        "m1_recovery_actions",
        ["target_type", "target_id", "requested_at"],
    )
    op.create_index(
        "m1_recovery_actions_claimable",
        "m1_recovery_actions",
        ["state", "worker_lease_expires_at", "requested_at"],
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_m1_recovery_action_active_target
        ON m1_recovery_actions(target_type, target_id)
        WHERE state IN ('pending', 'running')
        """
    )

    op.create_table(
        "m1_recovery_target_budgets",
        sa.Column("controller_id", sa.Text, nullable=False),
        sa.Column("target_type", sa.Text, nullable=False),
        sa.Column("target_id", sa.Text, nullable=False),
        sa.Column("max_actions", sa.BigInteger, nullable=False),
        sa.Column("remaining_actions", sa.BigInteger, nullable=False),
        sa.Column("last_next_allowed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "controller_id",
            "target_type",
            "target_id",
            name="pk_m1_recovery_target_budgets",
        ),
        sa.ForeignKeyConstraint(
            ["controller_id"],
            ["m1_runtime_controller_leases.controller_id"],
            name="fk_m1_recovery_target_budgets_controller",
        ),
        sa.CheckConstraint(
            "target_type IN ('job', 'circuit', 'worker-process', 'machine')",
            name="ck_m1_recovery_target_budgets_target_type",
        ),
        sa.CheckConstraint(
            "max_actions >= 0 AND remaining_actions >= 0 "
            "AND remaining_actions <= max_actions",
            name="ck_m1_recovery_target_budgets_nonnegative",
        ),
    )
    op.create_index(
        "m1_recovery_target_budgets_cooldown",
        "m1_recovery_target_budgets",
        ["controller_id", "target_type", "target_id", "last_next_allowed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "m1_recovery_target_budgets_cooldown",
        table_name="m1_recovery_target_budgets",
    )
    op.drop_table("m1_recovery_target_budgets")
    op.drop_index("uq_m1_recovery_action_active_target", table_name="m1_recovery_actions")
    op.drop_index("m1_recovery_actions_claimable", table_name="m1_recovery_actions")
    op.drop_index("m1_recovery_actions_target_requested", table_name="m1_recovery_actions")
    op.drop_table("m1_recovery_actions")
    op.drop_index(
        "m1_runtime_controller_leases_expiry",
        table_name="m1_runtime_controller_leases",
    )
    op.drop_table("m1_runtime_controller_leases")
