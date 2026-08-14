"""Add immutable exact-ID inputs for event-rooted Structure source batches.

Revision ID: 015
Revises: 014
Create Date: 2026-08-15
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "m1_structure_source_page_inputs",
        sa.Column("market_ids_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "m1_structure_source_page_inputs",
        sa.Column("market_ids_digest", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("m1_structure_source_page_inputs", "market_ids_digest")
    op.drop_column("m1_structure_source_page_inputs", "market_ids_json")
