"""Persist rolling qualification epochs and immutable certificates.

Revision ID: 024
Revises: 023

The migration is deliberately additive.  Epoch rows are a fenced mutable
projection with state/version CAS, while certificates are immutable replay
artifacts once sealed.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_qualification_epochs",
        sa.Column("epoch_id", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("version", sa.BigInteger, nullable=False, server_default="1"),
        sa.Column("identity_key", sa.Text, nullable=False),
        sa.Column("policy_version", sa.Text, nullable=False),
        sa.Column("release_id", sa.Text, nullable=False),
        sa.Column("config_id", sa.Text, nullable=False),
        sa.Column("role_identity", postgresql.JSONB, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_fact_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("invalidated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("invalidation_reason", sa.Text, nullable=True),
        sa.Column("qualified_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("previous_epoch_id", sa.Text, nullable=True),
        sa.Column(
            "fact_digests",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "contained_recoveries",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("coverage_seconds", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("max_gap_seconds", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("progress_count", sa.BigInteger, nullable=True),
        sa.Column("successful_count", sa.BigInteger, nullable=True),
        sa.Column("writer_id", sa.Text, nullable=True),
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
        sa.PrimaryKeyConstraint("epoch_id", name="pk_m1_qualification_epochs"),
        sa.ForeignKeyConstraint(
            ["previous_epoch_id"],
            ["m1_qualification_epochs.epoch_id"],
            name="fk_m1_qualification_epochs_previous",
        ),
        sa.CheckConstraint(
            "state IN ('accumulating', 'invalidated', 'recovering', 'qualified')",
            name="ck_m1_qualification_epochs_state",
        ),
        sa.CheckConstraint("version > 0", name="ck_m1_qualification_epochs_version"),
        sa.CheckConstraint(
            "jsonb_typeof(role_identity) = 'array' AND jsonb_array_length(role_identity) > 0",
            name="ck_m1_qualification_epochs_role_identity",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(fact_digests) = 'array' "
            "AND jsonb_typeof(contained_recoveries) = 'array'",
            name="ck_m1_qualification_epochs_evidence_json",
        ),
        sa.CheckConstraint(
            "coverage_seconds >= 0 AND max_gap_seconds >= 0 "
            "AND (progress_count IS NULL OR progress_count >= 0) "
            "AND (successful_count IS NULL OR successful_count >= 0)",
            name="ck_m1_qualification_epochs_counts",
        ),
        sa.CheckConstraint(
            "("
            "state = 'accumulating' AND invalidated_at IS NULL "
            "AND invalidation_reason IS NULL AND qualified_at IS NULL "
            "OR state = 'invalidated' AND invalidated_at IS NOT NULL "
            "AND invalidation_reason IS NOT NULL AND qualified_at IS NULL "
            "OR state = 'recovering' AND previous_epoch_id IS NOT NULL "
            "AND invalidated_at IS NULL AND invalidation_reason IS NULL "
            "AND qualified_at IS NULL "
            "OR state = 'qualified' AND qualified_at IS NOT NULL "
            "AND invalidated_at IS NULL AND invalidation_reason IS NULL"
            ")",
            name="ck_m1_qualification_epochs_terminal_fields",
        ),
    )
    op.create_index(
        "m1_qualification_epochs_identity_started",
        "m1_qualification_epochs",
        ["identity_key", "started_at"],
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_m1_qualification_active_identity
        ON m1_qualification_epochs(identity_key)
        WHERE state IN ('accumulating', 'recovering')
        """
    )

    op.create_table(
        "m1_qualification_certificates",
        sa.Column("certificate_id", sa.Text, nullable=False),
        sa.Column("epoch_id", sa.Text, nullable=False),
        sa.Column("identity_key", sa.Text, nullable=False),
        sa.Column("policy_version", sa.Text, nullable=False),
        sa.Column("release_id", sa.Text, nullable=False),
        sa.Column("config_id", sa.Text, nullable=False),
        sa.Column("role_identity", postgresql.JSONB, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("qualified_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("payload_sha256", sa.Text, nullable=False),
        sa.Column("certificate_digest", sa.Text, nullable=False),
        sa.Column("evidence_digest", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "certificate_id",
            name="pk_m1_qualification_certificates",
        ),
        sa.ForeignKeyConstraint(
            ["epoch_id"],
            ["m1_qualification_epochs.epoch_id"],
            name="fk_m1_qualification_certificates_epoch",
        ),
        sa.UniqueConstraint(
            "identity_key",
            name="uq_m1_qualification_certificates_identity",
        ),
        sa.UniqueConstraint(
            "certificate_digest",
            name="uq_m1_qualification_certificates_digest",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(role_identity) = 'array' AND jsonb_array_length(role_identity) > 0",
            name="ck_m1_qualification_certificates_role_identity",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="ck_m1_qualification_certificates_payload",
        ),
        sa.CheckConstraint(
            "started_at < qualified_at",
            name="ck_m1_qualification_certificates_time_bounds",
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND certificate_digest ~ '^[0-9a-f]{64}$' "
            "AND evidence_digest ~ '^[0-9a-f]{64}$'",
            name="ck_m1_qualification_certificates_digests",
        ),
    )
    op.create_index(
        "m1_qualification_certificates_created",
        "m1_qualification_certificates",
        ["created_at"],
    )

    op.execute(
        """
        CREATE FUNCTION m1_reject_qualification_certificate_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'qualification certificates are append-only';
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER m1_qualification_certificates_immutable
        BEFORE UPDATE OR DELETE ON m1_qualification_certificates
        FOR EACH ROW EXECUTE FUNCTION m1_reject_qualification_certificate_mutation();
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER m1_qualification_certificates_immutable "
        "ON m1_qualification_certificates"
    )
    op.execute("DROP FUNCTION m1_reject_qualification_certificate_mutation()")
    op.drop_index(
        "m1_qualification_certificates_created",
        table_name="m1_qualification_certificates",
    )
    op.drop_table("m1_qualification_certificates")
    op.drop_index("uq_m1_qualification_active_identity", table_name="m1_qualification_epochs")
    op.drop_index(
        "m1_qualification_epochs_identity_started",
        table_name="m1_qualification_epochs",
    )
    op.drop_table("m1_qualification_epochs")
