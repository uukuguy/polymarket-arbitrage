"""Add the durable, fenced M1 transactional control plane.

Revision ID: 009
Revises: 008
Create Date: 2026-08-11

This is additive authority for M1 work coordination.  It deliberately does
not alter the existing SQLite mirror or publication tables: those remain live
until a shadow migration has demonstrated parity.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def _timestamp(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(
        name,
        sa.TIMESTAMP(timezone=True),
        nullable=nullable,
        server_default=None if nullable else sa.text("clock_timestamp()"),
    )


def upgrade() -> None:
    op.create_table(
        "m1_jobs",
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("job_type", sa.Text, nullable=False),
        sa.Column("input_identity", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False, server_default="runnable"),
        sa.Column("checkpoint_cursor", sa.Text, nullable=True),
        sa.Column("checkpoint_digest", sa.Text, nullable=True),
        sa.Column("lease_owner", sa.Text, nullable=True),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("last_error_class", sa.Text, nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("job_key", name="pk_m1_jobs"),
        sa.UniqueConstraint("job_type", "input_identity", name="uq_m1_jobs_identity"),
        sa.CheckConstraint(
            "state IN ('runnable', 'leased', 'retryable', 'checkpointed', "
            "'succeeded', 'quarantined')",
            name="ck_m1_jobs_state",
        ),
        sa.CheckConstraint(
            "lease_epoch >= 0 AND attempt_count >= 0",
            name="ck_m1_jobs_nonnegative",
        ),
    )
    op.create_index("m1_jobs_state", "m1_jobs", ["state"])
    op.create_index("m1_jobs_runnable", "m1_jobs", ["state", "next_attempt_at", "updated_at"])
    op.create_index("m1_jobs_lease_expiry", "m1_jobs", ["state", "lease_expires_at"])

    op.create_table(
        "m1_job_attempts",
        sa.Column("attempt_id", sa.Text, nullable=False),
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False),
        sa.Column("worker_id", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("error_class", sa.Text, nullable=True),
        sa.Column("error_detail", postgresql.JSONB, nullable=True),
        _timestamp("recorded_at"),
        sa.PrimaryKeyConstraint("attempt_id", name="pk_m1_job_attempts"),
        sa.ForeignKeyConstraint(["job_key"], ["m1_jobs.job_key"], name="fk_m1_attempts_job"),
        sa.UniqueConstraint("job_key", "lease_epoch", name="uq_m1_attempts_job_epoch"),
        sa.CheckConstraint(
            "state IN ('running', 'checkpointed', 'succeeded', 'retryable', 'quarantined')",
            name="ck_m1_job_attempts_state",
        ),
    )
    op.create_index("m1_job_attempts_job_started", "m1_job_attempts", ["job_key", "started_at"])

    op.create_table(
        "m1_checkpoint_receipts",
        sa.Column("receipt_id", sa.Text, nullable=False),
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        sa.Column("checkpoint_cursor", sa.Text, nullable=False),
        sa.Column("checkpoint_digest", sa.Text, nullable=False),
        sa.Column("artifact_key", sa.Text, nullable=True),
        _timestamp("committed_at"),
        sa.PrimaryKeyConstraint("receipt_id", name="pk_m1_checkpoint_receipts"),
        sa.ForeignKeyConstraint(["job_key"], ["m1_jobs.job_key"], name="fk_m1_receipts_job"),
        sa.UniqueConstraint("idempotency_key", name="uq_m1_receipts_idempotency"),
        sa.UniqueConstraint(
            "job_key", "lease_epoch", "checkpoint_cursor", name="uq_m1_receipts_cursor"
        ),
    )

    op.create_table(
        "m1_quote_batch_receipts",
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("structure_receipt_digest", sa.Text, nullable=False),
        sa.Column("universe_hash", sa.Text, nullable=False),
        sa.Column("token_range_digest", sa.Text, nullable=False),
        sa.Column("quote_digest", sa.Text, nullable=False),
        sa.Column("successful_response_count", sa.BigInteger, nullable=False),
        sa.Column("quoted_at", sa.TIMESTAMP(timezone=True), nullable=False),
        _timestamp("committed_at"),
        sa.PrimaryKeyConstraint("job_key", name="pk_m1_quote_batch_receipts"),
        sa.ForeignKeyConstraint(
            ["job_key"], ["m1_jobs.job_key"], name="fk_m1_quote_batch_receipts_job"
        ),
        sa.CheckConstraint(
            "successful_response_count >= 0", name="ck_m1_quote_batch_receipts_nonnegative"
        ),
    )

    op.create_table(
        "m1_generation_manifests",
        sa.Column("generation_key", sa.Text, nullable=False),
        sa.Column("producer_job_key", sa.Text, nullable=False),
        sa.Column("input_digest", sa.Text, nullable=False),
        sa.Column("artifact_key", sa.Text, nullable=False),
        sa.Column("artifact_digest", sa.Text, nullable=False),
        sa.Column("record_count", sa.BigInteger, nullable=False),
        _timestamp("published_at"),
        sa.PrimaryKeyConstraint("generation_key", name="pk_m1_generation_manifests"),
        sa.ForeignKeyConstraint(
            ["producer_job_key"], ["m1_jobs.job_key"], name="fk_m1_manifests_job"
        ),
        sa.CheckConstraint("record_count >= 0", name="ck_m1_manifests_record_count"),
    )

    op.create_table(
        "m1_publication_pointers",
        sa.Column("pointer_key", sa.Text, nullable=False),
        sa.Column("generation_key", sa.Text, nullable=False),
        sa.Column("expected_generation_key", sa.Text, nullable=True),
        sa.Column("lease_epoch", sa.BigInteger, nullable=False),
        _timestamp("published_at"),
        sa.PrimaryKeyConstraint("pointer_key", name="pk_m1_publication_pointers"),
        sa.ForeignKeyConstraint(
            ["generation_key"],
            ["m1_generation_manifests.generation_key"],
            name="fk_m1_pointers_generation",
        ),
        sa.CheckConstraint("lease_epoch >= 0", name="ck_m1_pointers_epoch"),
    )

    op.create_table(
        "m1_incidents",
        sa.Column("incident_key", sa.Text, nullable=False),
        sa.Column("dedupe_key", sa.Text, nullable=False),
        sa.Column("component", sa.Text, nullable=False),
        sa.Column("severity", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False, server_default="open"),
        sa.Column("summary", sa.Text, nullable=False),
        sa.Column("diagnosis", postgresql.JSONB, nullable=True),
        _timestamp("opened_at"),
        sa.Column("resolved_at", sa.TIMESTAMP(timezone=True), nullable=True),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("incident_key", name="pk_m1_incidents"),
        sa.UniqueConstraint("dedupe_key", name="uq_m1_incidents_dedupe"),
        sa.CheckConstraint(
            "severity IN ('info', 'warning', 'critical')", name="ck_m1_incidents_severity"
        ),
        sa.CheckConstraint(
            "state IN ('open', 'acknowledged', 'resolved')", name="ck_m1_incidents_state"
        ),
    )
    op.create_index("m1_incidents_open", "m1_incidents", ["state", "severity", "updated_at"])

    op.create_table(
        "m1_incident_events",
        sa.Column("incident_event_id", sa.Text, nullable=False),
        sa.Column("incident_key", sa.Text, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("detail", postgresql.JSONB, nullable=False),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        _timestamp("occurred_at"),
        sa.PrimaryKeyConstraint("incident_event_id", name="pk_m1_incident_events"),
        sa.ForeignKeyConstraint(
            ["incident_key"], ["m1_incidents.incident_key"], name="fk_m1_events_incident"
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_m1_events_idempotency"),
    )
    op.create_index(
        "m1_incident_events_incident", "m1_incident_events", ["incident_key", "occurred_at"]
    )

    op.create_table(
        "m1_alert_outbox",
        sa.Column("outbox_id", sa.Text, nullable=False),
        sa.Column("incident_event_id", sa.Text, nullable=False),
        sa.Column("channel", sa.Text, nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("state", sa.Text, nullable=False, server_default="pending"),
        sa.Column("next_attempt_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.BigInteger, nullable=False, server_default="0"),
        _timestamp("created_at"),
        sa.PrimaryKeyConstraint("outbox_id", name="pk_m1_alert_outbox"),
        sa.ForeignKeyConstraint(
            ["incident_event_id"],
            ["m1_incident_events.incident_event_id"],
            name="fk_m1_outbox_incident_event",
        ),
        sa.UniqueConstraint("incident_event_id", "channel", name="uq_m1_outbox_event_channel"),
        sa.CheckConstraint(
            "state IN ('pending', 'delivered', 'retryable', 'failed')", name="ck_m1_outbox_state"
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_m1_outbox_attempt_count"),
    )
    op.create_index(
        "m1_alert_outbox_event_channel", "m1_alert_outbox", ["incident_event_id", "channel"]
    )
    op.create_index("m1_alert_outbox_pending", "m1_alert_outbox", ["state", "next_attempt_at"])

    op.create_table(
        "m1_alert_deliveries",
        sa.Column("delivery_id", sa.Text, nullable=False),
        sa.Column("outbox_id", sa.Text, nullable=False),
        sa.Column("attempt_number", sa.BigInteger, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("provider_receipt", sa.Text, nullable=True),
        sa.Column("error_class", sa.Text, nullable=True),
        sa.Column("error_detail", postgresql.JSONB, nullable=True),
        _timestamp("attempted_at"),
        sa.PrimaryKeyConstraint("delivery_id", name="pk_m1_alert_deliveries"),
        sa.ForeignKeyConstraint(
            ["outbox_id"], ["m1_alert_outbox.outbox_id"], name="fk_m1_deliveries_outbox"
        ),
        sa.UniqueConstraint("outbox_id", "attempt_number", name="uq_m1_deliveries_attempt"),
        sa.CheckConstraint("attempt_number > 0", name="ck_m1_deliveries_attempt_number"),
        sa.CheckConstraint(
            "state IN ('delivered', 'retryable', 'failed')", name="ck_m1_deliveries_state"
        ),
    )


def downgrade() -> None:
    op.drop_table("m1_alert_deliveries")
    op.drop_table("m1_alert_outbox")
    op.drop_table("m1_incident_events")
    op.drop_table("m1_incidents")
    op.drop_table("m1_publication_pointers")
    op.drop_table("m1_generation_manifests")
    op.drop_table("m1_quote_batch_receipts")
    op.drop_table("m1_checkpoint_receipts")
    op.drop_table("m1_job_attempts")
    op.drop_table("m1_jobs")
