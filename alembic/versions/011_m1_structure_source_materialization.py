"""Record fenced source-window bundle materialization.

Revision ID: 011
Revises: 010
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def _timestamp(name: str) -> sa.Column:
    return sa.Column(
        name,
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("clock_timestamp()"),
    )


def upgrade() -> None:
    op.create_table(
        "m1_structure_source_window_bundles",
        sa.Column("window_key", sa.Text, nullable=False),
        sa.Column("producer_job_key", sa.Text, nullable=False),
        sa.Column("source_digest", sa.Text, nullable=False),
        sa.Column("bundle_key", sa.Text, nullable=False),
        sa.Column("bundle_digest", sa.Text, nullable=False),
        _timestamp("committed_at"),
        sa.PrimaryKeyConstraint("window_key", name="pk_m1_structure_source_window_bundles"),
        sa.ForeignKeyConstraint(
            ["window_key"], ["m1_structure_source_windows.window_key"],
            name="fk_m1_structure_source_bundle_window",
        ),
        sa.ForeignKeyConstraint(
            ["producer_job_key"], ["m1_jobs.job_key"],
            name="fk_m1_structure_source_bundle_producer",
        ),
        sa.UniqueConstraint("bundle_digest", name="uq_m1_structure_source_bundle_digest"),
        sa.CheckConstraint(
            "length(source_digest) = 64", name="ck_m1_structure_source_bundle_source"
        ),
        sa.CheckConstraint(
            "length(bundle_digest) = 64", name="ck_m1_structure_source_bundle_digest"
        ),
    )


def downgrade() -> None:
    op.drop_table("m1_structure_source_window_bundles")
