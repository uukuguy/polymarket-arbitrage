"""Persist observe-only runtime reconciliation decisions.

Revision ID: 025
Revises: 024

This migration is deliberately independent from rolling qualification. It
records what an observe-only runtime controller would have decided, without
creating recovery actions or triggering qualification ingress.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None

_REASON_CODES = (
    "job.healthy",
    "job.lease-at-risk",
    "job.progress-stalled",
    "job.heartbeat-missing-fence",
    "job.heartbeat-missing",
    "job.lease-expired",
    "job.attempt-deadline",
    "circuit.probe-due",
    "circuit.cooldown",
    "recovery.budget-exhausted",
    "recovery.stale-fence",
    "failure.integrity",
    "failure.authentication",
    "failure.schema",
    "failure.credential",
    "failure.capacity",
)
_ACTION_TYPES = (
    "heartbeat-job",
    "cancel-job",
    "retry-job",
    "reclaim-job",
    "probe-circuit",
    "restart-worker-process",
    "restart-machine",
)


def upgrade() -> None:
    op.create_table(
        "m1_runtime_observe_decisions",
        sa.Column("decision_id", sa.Text, nullable=False),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("controller_id", sa.Text, nullable=False),
        sa.Column("controller_owner_id", sa.Text, nullable=False),
        sa.Column("controller_epoch", sa.BigInteger, nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("decision_kind", sa.Text, nullable=False),
        sa.Column("target_type", sa.Text, nullable=True),
        sa.Column("target_id", sa.Text, nullable=True),
        sa.Column("action_type", sa.Text, nullable=True),
        sa.Column("reason_code", sa.Text, nullable=False),
        sa.Column("incident_severity", sa.Text, nullable=False),
        sa.Column("qualification_breaking", sa.Boolean, nullable=False),
        sa.Column("next_check_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("runtime_state_digest", sa.Text, nullable=True),
        sa.Column("decision_digest", sa.Text, nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("payload_sha256", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("decision_id", name="pk_m1_runtime_observe_decisions"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_m1_runtime_observe_idempotency",
        ),
        sa.CheckConstraint(
            "decision_id ~ '^runtime-observe:[0-9a-f]{64}$'",
            name="ck_m1_runtime_observe_decision_id",
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^runtime-observe-idempotency:[0-9a-f]{64}$'",
            name="ck_m1_runtime_observe_idempotency",
        ),
        sa.CheckConstraint(
            "length(controller_owner_id) > 0 AND controller_epoch > 0",
            name="ck_m1_runtime_observe_controller_identity",
        ),
        sa.CheckConstraint(
            "decision_kind IN ('decision', 'idle')",
            name="ck_m1_runtime_observe_kind",
        ),
        sa.CheckConstraint(
            "target_type IS NULL OR target_type IN ('job', 'circuit')",
            name="ck_m1_runtime_observe_target_type",
        ),
        sa.CheckConstraint(
            "action_type IS NULL OR action_type IN ("
            + ", ".join(f"'{action}'" for action in _ACTION_TYPES)
            + ")",
            name="ck_m1_runtime_observe_action_type",
        ),
        sa.CheckConstraint(
            "reason_code IN (" + ", ".join(f"'{reason}'" for reason in _REASON_CODES) + ")",
            name="ck_m1_runtime_observe_reason_code",
        ),
        sa.CheckConstraint(
            "incident_severity IN ('warning', 'critical')",
            name="ck_m1_runtime_observe_severity",
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_m1_runtime_observe_payload_digest",
        ),
        sa.CheckConstraint(
            "runtime_state_digest IS NULL OR runtime_state_digest ~ '^[0-9a-f]{64}$'",
            name="ck_m1_runtime_observe_state_digest",
        ),
        sa.CheckConstraint(
            "decision_digest ~ '^[0-9a-f]{64}$'",
            name="ck_m1_runtime_observe_decision_digest",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND pg_column_size(payload) <= 8192",
            name="ck_m1_runtime_observe_payload_shape",
        ),
        sa.CheckConstraint(
            "next_check_at >= observed_at",
            name="ck_m1_runtime_observe_next_check",
        ),
        sa.CheckConstraint(
            "("
            "decision_kind = 'idle' "
            "AND target_type IS NULL AND target_id IS NULL AND action_type IS NULL "
            "AND runtime_state_digest IS NULL"
            ") OR ("
            "decision_kind = 'decision' "
            "AND target_type IS NOT NULL AND length(target_id) > 0 "
            "AND runtime_state_digest IS NOT NULL"
            ")",
            name="ck_m1_runtime_observe_kind_identity",
        ),
    )
    op.create_index(
        "m1_runtime_observe_controller_target_observed",
        "m1_runtime_observe_decisions",
        [
            "controller_id",
            "controller_owner_id",
            "controller_epoch",
            "target_type",
            "target_id",
            "observed_at",
        ],
    )
    op.create_index(
        "m1_runtime_observe_controller_observed",
        "m1_runtime_observe_decisions",
        ["controller_id", "controller_owner_id", "controller_epoch", "observed_at"],
    )
    op.execute(
        """
        CREATE FUNCTION m1_runtime_observe_decisions_reject_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'm1_runtime_observe_decisions is append-only';
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER m1_runtime_observe_decisions_immutable
        BEFORE UPDATE OR DELETE ON m1_runtime_observe_decisions
        FOR EACH ROW EXECUTE FUNCTION m1_runtime_observe_decisions_reject_mutation();
        """
    )
    op.execute("REVOKE ALL ON TABLE m1_runtime_observe_decisions FROM PUBLIC")
    op.execute("GRANT SELECT ON TABLE m1_runtime_observe_decisions TO authenticated")
    op.execute("GRANT SELECT, INSERT ON TABLE m1_runtime_observe_decisions TO service_role")


def downgrade() -> None:
    op.execute(
        """
        DROP TRIGGER IF EXISTS m1_runtime_observe_decisions_immutable
        ON m1_runtime_observe_decisions
        """
    )
    op.execute("DROP FUNCTION IF EXISTS m1_runtime_observe_decisions_reject_mutation")
    op.drop_index(
        "m1_runtime_observe_controller_target_observed",
        table_name="m1_runtime_observe_decisions",
    )
    op.drop_index(
        "m1_runtime_observe_controller_observed",
        table_name="m1_runtime_observe_decisions",
    )
    op.drop_table("m1_runtime_observe_decisions")
