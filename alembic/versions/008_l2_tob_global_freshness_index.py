"""Bound global latest-top-of-book reads as the history table grows.

Revision ID: 008
Revises: 007
Create Date: 2026-07-31
"""

from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # L2 writes continuously in production, so this index must not take the
    # table-wide write lock used by a regular CREATE INDEX.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_l2_tob_ts_desc "
            "ON l2_top_of_book (ts DESC)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_l2_tob_ts_desc")
