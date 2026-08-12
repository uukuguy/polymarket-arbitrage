"""Add fenced Gamma source windows for transactional M1 Structure.

Revision ID: 010
Revises: 009
Create Date: 2026-08-12

The tables are additive.  They do not read, alter, or replace the legacy
SQLite Structure pipeline; they make the cloud source boundary independently
durable before any pointer switch is considered.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "010"
down_revision = "009"
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
        "m1_structure_source_windows",
        sa.Column("window_key", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False, server_default="running"),
        _timestamp("admitted_at"),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("window_key", name="pk_m1_structure_source_windows"),
        sa.CheckConstraint(
            "state IN ('running', 'events-complete', 'complete', 'quarantined')",
            name="ck_m1_structure_source_windows_state",
        ),
    )
    op.create_table(
        "m1_structure_source_page_inputs",
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("window_key", sa.Text, nullable=False),
        sa.Column("stream", sa.Text, nullable=False),
        sa.Column("ordinal", sa.BigInteger, nullable=False),
        sa.Column("requested_cursor", sa.Text, nullable=True),
        _timestamp("admitted_at"),
        sa.PrimaryKeyConstraint("job_key", name="pk_m1_structure_source_page_inputs"),
        sa.ForeignKeyConstraint(
            ["job_key"], ["m1_jobs.job_key"], name="fk_m1_structure_source_page_input_job"
        ),
        sa.ForeignKeyConstraint(
            ["window_key"], ["m1_structure_source_windows.window_key"],
            name="fk_m1_structure_source_page_input_window",
        ),
        sa.UniqueConstraint(
            "window_key", "stream", "ordinal", name="uq_m1_structure_source_page_ordinal"
        ),
        sa.CheckConstraint("stream IN ('events', 'markets')", name="ck_m1_structure_source_stream"),
        sa.CheckConstraint("ordinal >= 0", name="ck_m1_structure_source_page_ordinal"),
    )
    op.create_table(
        "m1_structure_source_page_receipts",
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("artifact_key", sa.Text, nullable=False),
        sa.Column("artifact_digest", sa.Text, nullable=False),
        sa.Column("next_cursor", sa.Text, nullable=True),
        sa.Column("completed", sa.Boolean, nullable=False),
        sa.Column("record_count", sa.BigInteger, nullable=False),
        _timestamp("committed_at"),
        sa.PrimaryKeyConstraint("job_key", name="pk_m1_structure_source_page_receipts"),
        sa.ForeignKeyConstraint(
            ["job_key"], ["m1_structure_source_page_inputs.job_key"],
            name="fk_m1_structure_source_page_receipt_input",
        ),
        sa.CheckConstraint("record_count >= 0", name="ck_m1_structure_source_page_receipt_count"),
        sa.CheckConstraint("length(artifact_digest) = 64", name="ck_m1_structure_source_digest"),
    )
    op.create_index(
        "m1_structure_source_page_window_stream",
        "m1_structure_source_page_inputs",
        ["window_key", "stream", "ordinal"],
    )


def downgrade() -> None:
    op.drop_index(
        "m1_structure_source_page_window_stream",
        table_name="m1_structure_source_page_inputs",
    )
    op.drop_table("m1_structure_source_page_receipts")
    op.drop_table("m1_structure_source_page_inputs")
    op.drop_table("m1_structure_source_windows")
