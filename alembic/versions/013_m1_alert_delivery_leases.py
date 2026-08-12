"""Add fenced leases for durable M1 alert-outbox delivery.

Revision ID: 013
Revises: 012
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("m1_alert_outbox", sa.Column("lease_owner", sa.Text, nullable=True))
    op.add_column(
        "m1_alert_outbox",
        sa.Column("lease_epoch", sa.BigInteger, nullable=False, server_default="0"),
    )
    op.add_column(
        "m1_alert_outbox", sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True), nullable=True)
    )
    op.create_index(
        "m1_alert_outbox_lease_expiry", "m1_alert_outbox", ["state", "lease_expires_at"]
    )


def downgrade() -> None:
    op.drop_index("m1_alert_outbox_lease_expiry", table_name="m1_alert_outbox")
    op.drop_column("m1_alert_outbox", "lease_expires_at")
    op.drop_column("m1_alert_outbox", "lease_epoch")
    op.drop_column("m1_alert_outbox", "lease_owner")
