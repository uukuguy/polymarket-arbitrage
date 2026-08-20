"""Stage authenticated R2 references for compact Quote-batch inputs.

Revision ID: 019
Revises: 018
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("m1_quote_batch_inputs", sa.Column("input_artifact_key", sa.Text()))
    op.add_column("m1_quote_batch_inputs", sa.Column("input_artifact_digest", sa.Text()))
    op.add_column("m1_quote_batch_inputs", sa.Column("leg_count", sa.BigInteger()))
    op.create_check_constraint(
        "ck_m1_quote_batch_input_artifact_digest",
        "m1_quote_batch_inputs",
        "input_artifact_digest IS NULL OR length(input_artifact_digest) = 64",
    )
    op.create_check_constraint(
        "ck_m1_quote_batch_input_leg_count",
        "m1_quote_batch_inputs",
        "leg_count IS NULL OR leg_count > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_m1_quote_batch_input_leg_count", "m1_quote_batch_inputs", type_="check"
    )
    op.drop_constraint(
        "ck_m1_quote_batch_input_artifact_digest", "m1_quote_batch_inputs", type_="check"
    )
    op.drop_column("m1_quote_batch_inputs", "leg_count")
    op.drop_column("m1_quote_batch_inputs", "input_artifact_digest")
    op.drop_column("m1_quote_batch_inputs", "input_artifact_key")
