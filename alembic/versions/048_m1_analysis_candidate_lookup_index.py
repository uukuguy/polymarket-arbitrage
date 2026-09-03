"""Index the bounded analysis candidate Quote-to-group lookup.

Revision ID: 048
Revises: 047
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "048"
down_revision = "047"
branch_labels = None
depends_on = None

INDEX = "m1_business_quote_rows_candidate_group"


def upgrade() -> None:
    op.create_index(
        INDEX,
        "m1_business_quote_rows",
        ["generation_key", sa.text("(payload ->> 'neg_risk_market_id')")],
    )


def downgrade() -> None:
    raise RuntimeError("revision 048 is production-forward-only")
