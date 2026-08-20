"""Permit compact R2-authoritative Quote inputs.

Revision ID: 020
Revises: 019
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "m1_quote_batch_inputs",
        "token_ids",
        existing_type=sa.dialects.postgresql.JSONB(),
        nullable=True,
    )


def downgrade() -> None:
    op.execute(
        """UPDATE m1_quote_batch_inputs
           SET token_ids = '[]'::jsonb
           WHERE token_ids IS NULL"""
    )
    op.alter_column(
        "m1_quote_batch_inputs",
        "token_ids",
        existing_type=sa.dialects.postgresql.JSONB(),
        nullable=False,
    )
