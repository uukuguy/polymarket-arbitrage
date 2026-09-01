"""Reset maximum observation gap when a new continuous session begins.

Revision ID: 044
Revises: 043
"""

from __future__ import annotations

from alembic import op

revision = "044"
down_revision = "043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        DECLARE definition text;
        BEGIN
            SELECT pg_get_functiondef(
                'public.m1_runtime_observe_apply_turn(jsonb)'::regprocedure
            ) INTO definition;
            definition := replace(
                definition,
                $old$WHEN EXCLUDED.max_gap_seconds > 90 THEN EXCLUDED.max_gap_seconds$old$,
                $new$WHEN EXCLUDED.max_gap_seconds > 90
                         OR m1_runtime_observe_status.continuous_since
                            = m1_runtime_observe_status.last_completed_at
                    THEN EXCLUDED.max_gap_seconds$new$
            );
            EXECUTE definition;
        END;
        $$;
        """
    )


def downgrade() -> None:
    raise RuntimeError("revision 044 is production-forward-only")
