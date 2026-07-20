"""Add no_token_id to markets_latest for complete L3 outcome identity.

Revision ID: 006
Revises: 005
Create Date: 2026-07-20

The L1 normalizer, SQLite store, and Parquet projection already retain both
binary outcome token IDs.  This add-only revision closes the narrower Supabase
projection so the L3 promoter can expand five selected Yes assets to five
complete Yes/No pairs without an external lookup.
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "markets_latest",
        sa.Column("no_token_id", sa.Text, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("markets_latest", "no_token_id")
