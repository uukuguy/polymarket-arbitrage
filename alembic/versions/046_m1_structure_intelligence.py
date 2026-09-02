"""Add bounded Structure Market Intelligence projections.

Revision ID: 046
Revises: 045
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "046"
down_revision = "045"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "m1_runtime_controller_capability"
EVENTS = "m1_structure_intelligence_events"
GROUPS = "m1_structure_intelligence_groups"
SUMMARIES = "m1_structure_intelligence_summaries"


def upgrade() -> None:
    op.create_table(
        EVENTS,
        sa.Column("generation_key", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("sort_end_time_ms", sa.BigInteger(), nullable=True),
        sa.Column("is_open", sa.Boolean(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_octets", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", "event_id", name=f"pk_{EVENTS}"),
        sa.CheckConstraint("payload_octets BETWEEN 2 AND 4096", name=f"ck_{EVENTS}_payload_octets"),
    )
    op.create_index(f"{EVENTS}_page", EVENTS, ["generation_key", "is_open", "sort_end_time_ms", "event_id"])
    op.create_table(
        GROUPS,
        sa.Column("generation_key", sa.Text(), nullable=False),
        sa.Column("group_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("quality", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_octets", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", "group_id", name=f"pk_{GROUPS}"),
        sa.CheckConstraint("payload_octets BETWEEN 2 AND 4096", name=f"ck_{GROUPS}_payload_octets"),
    )
    op.create_index(f"{GROUPS}_risk", GROUPS, ["generation_key", "quality", "group_id"])
    op.create_table(
        SUMMARIES,
        sa.Column("generation_key", sa.Text(), primary_key=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_octets", sa.Integer(), nullable=False),
        sa.CheckConstraint("payload_octets BETWEEN 2 AND 4096", name=f"ck_{SUMMARIES}_payload_octets"),
    )
    for table in (EVENTS, GROUPS, SUMMARIES):
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")
        op.execute(f"GRANT SELECT, INSERT, DELETE, TRUNCATE ON TABLE public.{table} TO {RUNTIME_ROLE}")


def downgrade() -> None:
    raise RuntimeError("revision 046 is production-forward-only")
