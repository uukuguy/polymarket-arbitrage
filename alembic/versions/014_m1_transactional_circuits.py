"""Add durable, job-scoped retry circuit state for M1 workers.

Revision ID: 014
Revises: 013
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_job_circuits",
        sa.Column("job_key", sa.Text, sa.ForeignKey("m1_jobs.job_key"), primary_key=True),
        sa.Column("consecutive_failures", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("state", sa.Text, nullable=False, server_default="closed"),
        sa.Column("opened_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("next_probe_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint("consecutive_failures >= 0", name="m1_job_circuits_failure_count"),
        sa.CheckConstraint("state IN ('closed', 'open')", name="m1_job_circuits_state"),
    )
    op.create_index(
        "m1_job_circuits_open_probe", "m1_job_circuits", ["state", "next_probe_at", "updated_at"]
    )


def downgrade() -> None:
    op.drop_index("m1_job_circuits_open_probe", table_name="m1_job_circuits")
    op.drop_table("m1_job_circuits")
