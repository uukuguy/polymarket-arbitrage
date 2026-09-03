"""Add bounded, lineage-fenced M1 analysis candidate projections.

Revision ID: 047
Revises: 046

Candidates are deliberately distinct from certified opportunities: they retain
both positive and rejected group-level analysis facts, but never authorize
execution.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "047"
down_revision = "046"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "m1_runtime_controller_capability"
PROJECTIONS = "m1_analysis_candidate_projections"
ROWS = "m1_analysis_candidate_rows"


def upgrade() -> None:
    op.create_table(
        PROJECTIONS,
        sa.Column("generation_key", sa.Text(), nullable=False),
        sa.Column("structure_generation_key", sa.Text(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("positive_edge_count", sa.Integer(), nullable=False),
        sa.Column("materialized_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", name=f"pk_{PROJECTIONS}"),
        sa.CheckConstraint("record_count BETWEEN 0 AND 20000", name=f"ck_{PROJECTIONS}_count"),
        sa.CheckConstraint(
            "positive_edge_count BETWEEN 0 AND record_count",
            name=f"ck_{PROJECTIONS}_positive_count",
        ),
    )
    op.create_table(
        ROWS,
        sa.Column("generation_key", sa.Text(), nullable=False),
        sa.Column("group_id", sa.Text(), nullable=False),
        sa.Column("candidate_state", sa.Text(), nullable=False),
        sa.Column("gross_edge_bps", sa.Numeric(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_octets", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", "group_id", name=f"pk_{ROWS}"),
        sa.ForeignKeyConstraint(
            ["generation_key"], [f"{PROJECTIONS}.generation_key"], name=f"fk_{ROWS}_projection"
        ),
        sa.CheckConstraint("payload_octets BETWEEN 2 AND 2048", name=f"ck_{ROWS}_payload_octets"),
    )
    op.create_index(
        f"{ROWS}_page",
        ROWS,
        [
            "generation_key",
            "candidate_state",
            sa.text("gross_edge_bps DESC NULLS LAST"),
            "group_id",
        ],
    )
    for table in (PROJECTIONS, ROWS):
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")
        op.execute(
            f"GRANT SELECT, INSERT, DELETE, TRUNCATE ON TABLE public.{table} TO {RUNTIME_ROLE}"
        )


def downgrade() -> None:
    raise RuntimeError("revision 047 is production-forward-only")
