"""Add the certified Postgres opportunity projection for formal M1 reads.

Revision ID: 016
Revises: 015
Create Date: 2026-08-16

The rows are immutable per certified Quote generation.  Readers follow the
separate current pointer, so a partially built next projection can never be
reported as an empty current opportunity set.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_opportunity_projections",
        sa.Column("generation_key", sa.Text, nullable=False),
        sa.Column("structure_generation_key", sa.Text, nullable=False),
        sa.Column("projection_digest", sa.Text, nullable=False),
        sa.Column("record_count", sa.BigInteger, nullable=False),
        sa.Column("certified_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", name="pk_m1_opportunity_projections"),
        sa.ForeignKeyConstraint(
            ["generation_key"],
            ["m1_generation_manifests.generation_key"],
            name="fk_m1_opportunity_projection_quote_manifest",
        ),
        sa.ForeignKeyConstraint(
            ["structure_generation_key"],
            ["m1_generation_manifests.generation_key"],
            name="fk_m1_opportunity_projection_structure_manifest",
        ),
        sa.CheckConstraint("record_count >= 0", name="ck_m1_opportunity_projection_count"),
    )
    op.create_table(
        "m1_opportunity_projection_rows",
        sa.Column("generation_key", sa.Text, nullable=False),
        sa.Column("group_id", sa.Text, nullable=False),
        sa.Column("event_id", sa.Text, nullable=False),
        sa.Column("membership_hash", sa.Text, nullable=False),
        sa.Column("bundle_cost", sa.Numeric, nullable=False),
        sa.Column("gross_edge_bps", sa.Numeric, nullable=False),
        sa.Column("max_bundle_size", sa.Numeric, nullable=False),
        sa.Column("legs", postgresql.JSONB, nullable=False),
        sa.Column("structure_observed_at_ms", sa.BigInteger, nullable=False),
        sa.Column("quote_started_at_ms", sa.BigInteger, nullable=False),
        sa.Column("quote_quoted_at_ms", sa.BigInteger, nullable=False),
        sa.PrimaryKeyConstraint(
            "generation_key", "group_id", name="pk_m1_opportunity_projection_rows"
        ),
        sa.ForeignKeyConstraint(
            ["generation_key"],
            ["m1_opportunity_projections.generation_key"],
            name="fk_m1_opportunity_row_projection",
        ),
        sa.CheckConstraint("bundle_cost > 0", name="ck_m1_opportunity_bundle_cost"),
        sa.CheckConstraint("gross_edge_bps > 0", name="ck_m1_opportunity_edge"),
        sa.CheckConstraint("max_bundle_size > 0", name="ck_m1_opportunity_size"),
        sa.CheckConstraint(
            "structure_observed_at_ms >= 0 AND quote_started_at_ms >= 0 "
            "AND quote_quoted_at_ms >= quote_started_at_ms",
            name="ck_m1_opportunity_times",
        ),
    )
    op.create_index(
        "idx_m1_opportunity_projection_rows_page",
        "m1_opportunity_projection_rows",
        ["generation_key", "group_id"],
    )
    op.create_table(
        "m1_opportunity_publication_pointers",
        sa.Column("pointer_key", sa.Text, nullable=False),
        sa.Column("generation_key", sa.Text, nullable=False),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("pointer_key", name="pk_m1_opportunity_publication_pointers"),
        sa.ForeignKeyConstraint(
            ["generation_key"],
            ["m1_opportunity_projections.generation_key"],
            name="fk_m1_opportunity_pointer_projection",
        ),
    )


def downgrade() -> None:
    op.drop_table("m1_opportunity_publication_pointers")
    op.drop_index("idx_m1_opportunity_projection_rows_page", table_name="m1_opportunity_projection_rows")
    op.drop_table("m1_opportunity_projection_rows")
    op.drop_table("m1_opportunity_projections")
