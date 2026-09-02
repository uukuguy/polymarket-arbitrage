"""Add a fenced candidate relation for Quote business research.

Revision ID: 045
Revises: 044
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "045"
down_revision = "044"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "m1_runtime_controller_capability"
TABLE = "m1_business_quote_staging_rows"
PAGE_INDEX = "m1_business_quote_staging_rows_page"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("generation_key", sa.Text(), nullable=False),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", "token_id", name=f"pk_{TABLE}"),
    )
    op.create_index(PAGE_INDEX, TABLE, ["generation_key", "token_id"])
    op.execute(f"REVOKE ALL ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, DELETE, TRUNCATE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}")


def downgrade() -> None:
    raise RuntimeError("revision 045 is production-forward-only")
