"""Persist bounded generated projections for qualification operator status.

Revision ID: 029
Revises: 028
"""

from __future__ import annotations

from alembic import op

revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # These values are derived when the epoch row is written. Status reads can
    # therefore remain independent of the monotonically growing evidence JSON.
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        ADD COLUMN status_last_fact_record jsonb
        GENERATED ALWAYS AS (fact_records -> -1) STORED
        """
    )
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        ADD COLUMN status_recovery_count bigint
        GENERATED ALWAYS AS (
            jsonb_array_length(contained_recoveries)::bigint
        ) STORED
        """
    )
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        ADD COLUMN status_recent_recoveries jsonb
        GENERATED ALWAYS AS (
            jsonb_path_query_array(
                contained_recoveries,
                '$[last - 19 to last]'::jsonpath
            )
        ) STORED
        """
    )


def downgrade() -> None:
    op.drop_column("m1_qualification_epochs", "status_recent_recoveries")
    op.drop_column("m1_qualification_epochs", "status_recovery_count")
    op.drop_column("m1_qualification_epochs", "status_last_fact_record")
