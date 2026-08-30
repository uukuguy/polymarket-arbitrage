"""Let many immutable Quote admissions share one Structure generation.

Revision ID: 039
Revises: 038
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "039"
down_revision = "038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("m1_quote_admission_inputs", sa.Column("quote_generation_key", sa.Text()))
    op.execute(
        """
        UPDATE public.m1_quote_admission_inputs
        SET quote_generation_key = 'quote:' || bundle_digest
        WHERE quote_generation_key IS NULL
        """
    )
    op.alter_column("m1_quote_admission_inputs", "quote_generation_key", nullable=False)
    op.drop_constraint(
        "uq_m1_quote_admission_input_generation",
        "m1_quote_admission_inputs",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_m1_quote_admission_input_quote_generation",
        "m1_quote_admission_inputs",
        ["quote_generation_key"],
    )
    op.create_check_constraint(
        "ck_m1_quote_admission_quote_generation",
        "m1_quote_admission_inputs",
        "quote_generation_key ~ '^quote:[0-9a-f]{64}$'",
    )
    op.create_index(
        "m1_quote_admission_inputs_structure",
        "m1_quote_admission_inputs",
        ["generation_key", "admitted_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "m1_quote_admission_inputs_structure",
        table_name="m1_quote_admission_inputs",
    )
    op.drop_constraint(
        "ck_m1_quote_admission_quote_generation",
        "m1_quote_admission_inputs",
        type_="check",
    )
    op.drop_constraint(
        "uq_m1_quote_admission_input_quote_generation",
        "m1_quote_admission_inputs",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_m1_quote_admission_input_generation",
        "m1_quote_admission_inputs",
        ["generation_key"],
    )
    op.drop_column("m1_quote_admission_inputs", "quote_generation_key")
