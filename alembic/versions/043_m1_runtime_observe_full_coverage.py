"""Raise bounded runtime-observe coverage to its 500-target current-state cap.

Revision ID: 043
Revises: 042
"""

from __future__ import annotations

from alembic import op

revision = "043"
down_revision = "042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.m1_runtime_observe_status
            DROP CONSTRAINT m1_runtime_observe_status_candidate_count_check,
            DROP CONSTRAINT m1_runtime_observe_status_actionable_count_check,
            DROP CONSTRAINT m1_runtime_observe_status_critical_count_check,
            ADD CONSTRAINT m1_runtime_observe_status_candidate_count_check
                CHECK (candidate_count BETWEEN 0 AND 500),
            ADD CONSTRAINT m1_runtime_observe_status_actionable_count_check
                CHECK (actionable_count BETWEEN 0 AND 500),
            ADD CONSTRAINT m1_runtime_observe_status_critical_count_check
                CHECK (critical_count BETWEEN 0 AND 500);
        """
    )
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
                $needle$jsonb_array_length(turn->'candidates') > 100$needle$,
                $replacement$jsonb_array_length(turn->'candidates') > 500$replacement$
            );
            EXECUTE definition;
        END;
        $$;
        """
    )


def downgrade() -> None:
    raise RuntimeError("revision 043 is production-forward-only")
