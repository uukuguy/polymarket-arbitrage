"""Add bounded, generation-bound read indexes for M1 business research.

Revision ID: 040
Revises: 039
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "040"
down_revision = "039"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "m1_runtime_controller_capability"
STRUCTURE_PAGE_INDEX = "m1_business_structure_rows_page"
QUOTE_PAGE_INDEX = "m1_business_quote_rows_page"


def upgrade() -> None:
    for table, identity_column, page_index in (
        ("m1_business_structure_rows", "entity_id", STRUCTURE_PAGE_INDEX),
        ("m1_business_quote_rows", "token_id", QUOTE_PAGE_INDEX),
    ):
        op.create_table(
            table,
            sa.Column("generation_key", sa.Text(), nullable=False),
            sa.Column(identity_column, sa.Text(), nullable=False),
            sa.Column("payload", postgresql.JSONB(), nullable=False),
            sa.PrimaryKeyConstraint("generation_key", identity_column, name=f"pk_{table}"),
        )
        op.create_index(page_index, table, ["generation_key", identity_column])
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")
        op.execute(f"GRANT SELECT, INSERT ON TABLE public.{table} TO {RUNTIME_ROLE}")


def downgrade() -> None:
    for table, page_index in (
        ("m1_business_quote_rows", QUOTE_PAGE_INDEX),
        ("m1_business_structure_rows", STRUCTURE_PAGE_INDEX),
    ):
        op.execute(f"REVOKE SELECT, INSERT ON TABLE public.{table} FROM {RUNTIME_ROLE}")
        op.drop_index(page_index, table_name=table)
        op.drop_table(table)
