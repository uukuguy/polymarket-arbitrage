"""Add authoritative recurring Quote-to-Structure lineage.

Revision ID: 038
Revises: 037
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "038"
down_revision = "037"
branch_labels = None
depends_on = None

QUALIFICATION_ROLE = "m1_qualification_worker_capability"


def upgrade() -> None:
    op.create_table(
        "m1_quote_generation_inputs",
        sa.Column("generation_key", sa.Text(), nullable=False),
        sa.Column("structure_generation_key", sa.Text(), nullable=False),
        sa.Column("universe_hash", sa.Text(), nullable=False),
        sa.Column("cadence_seconds", sa.BigInteger()),
        sa.Column("cadence_bucket", sa.BigInteger()),
        sa.Column("admitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("generation_key", name="pk_m1_quote_generation_inputs"),
        sa.ForeignKeyConstraint(
            ["structure_generation_key"],
            ["m1_generation_manifests.generation_key"],
            name="fk_m1_quote_generation_inputs_structure",
        ),
        sa.CheckConstraint(
            "generation_key ~ '^quote:[0-9a-f]{64}$'",
            name="ck_m1_quote_generation_key",
        ),
        sa.CheckConstraint(
            "structure_generation_key ~ '^structure:[0-9a-f]{64}$'",
            name="ck_m1_quote_structure_generation_key",
        ),
        sa.CheckConstraint(
            "universe_hash ~ '^[0-9a-f]{64}$'",
            name="ck_m1_quote_universe_hash",
        ),
        sa.CheckConstraint(
            "(cadence_seconds IS NULL AND cadence_bucket IS NULL) OR "
            "(cadence_seconds > 0 AND cadence_bucket >= 0)",
            name="ck_m1_quote_cadence_identity",
        ),
    )
    op.execute("REVOKE ALL ON TABLE public.m1_quote_generation_inputs FROM PUBLIC")
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM public.m1_generation_manifests AS quote
                WHERE quote.generation_key ~ '^quote:[0-9a-f]{64}$'
                  AND NOT EXISTS (
                      SELECT 1 FROM public.m1_generation_manifests AS structure
                      WHERE structure.generation_key =
                            'structure:' || substr(quote.generation_key, 7)
                  )
            ) THEN
                RAISE EXCEPTION 'legacy Quote manifest lacks exact Structure lineage';
            END IF;
        END
        $$
        """
    )
    op.execute(
        """
        INSERT INTO public.m1_quote_generation_inputs (
            generation_key, structure_generation_key, universe_hash,
            cadence_seconds, cadence_bucket, admitted_at
        )
        SELECT quote.generation_key,
               'structure:' || substr(quote.generation_key, 7),
               quote.input_digest,
               NULL,
               NULL,
               quote.published_at
        FROM public.m1_generation_manifests AS quote
        JOIN public.m1_generation_manifests AS structure
          ON structure.generation_key = 'structure:' || substr(quote.generation_key, 7)
        WHERE quote.generation_key ~ '^quote:[0-9a-f]{64}$'
        """
    )
    op.execute(f"GRANT SELECT ON TABLE public.m1_quote_generation_inputs TO {QUALIFICATION_ROLE}")


def downgrade() -> None:
    op.execute(
        f"REVOKE SELECT ON TABLE public.m1_quote_generation_inputs FROM {QUALIFICATION_ROLE}"
    )
    op.drop_table("m1_quote_generation_inputs")
