"""Add durable Quote-admission inputs downstream of Structure certification.

Revision ID: 012
Revises: 011
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "m1_quote_admission_inputs",
        sa.Column("job_key", sa.Text, nullable=False),
        sa.Column("generation_key", sa.Text, nullable=False),
        sa.Column("bundle_key", sa.Text, nullable=False),
        sa.Column("bundle_digest", sa.Text, nullable=False),
        sa.Column(
            "admitted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("job_key", name="pk_m1_quote_admission_inputs"),
        sa.ForeignKeyConstraint(
            ["job_key"], ["m1_jobs.job_key"], name="fk_m1_quote_admission_input_job"
        ),
        sa.ForeignKeyConstraint(
            ["generation_key"],
            ["m1_structure_generation_inputs.generation_key"],
            name="fk_m1_quote_admission_input_generation",
        ),
        sa.UniqueConstraint("generation_key", name="uq_m1_quote_admission_input_generation"),
        sa.CheckConstraint(
            "length(bundle_digest) = 64", name="ck_m1_quote_admission_bundle_digest"
        ),
    )


def downgrade() -> None:
    op.drop_table("m1_quote_admission_inputs")
