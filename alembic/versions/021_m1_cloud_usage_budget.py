"""Add durable M1 cloud-egress observations.

Revision ID: 021
Revises: 020
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_cloud_usage_observations",
        sa.Column("observation_id", sa.Text, primary_key=True),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("budget_day", sa.Date, nullable=False),
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("operation", sa.Text, nullable=False),
        sa.Column("bytes_received", sa.BigInteger, nullable=False),
        sa.Column("item_count", sa.Integer, nullable=False),
        sa.Column("artifact_key", sa.Text, nullable=False),
        sa.Column("artifact_digest", sa.Text, nullable=False),
        sa.CheckConstraint("bytes_received >= 0"),
        sa.CheckConstraint("item_count >= 0"),
        sa.CheckConstraint("length(artifact_digest) = 64"),
    )
    op.create_index("m1_cloud_usage_budget_day", "m1_cloud_usage_observations", ["budget_day"])


def downgrade() -> None:
    op.drop_table("m1_cloud_usage_observations")
