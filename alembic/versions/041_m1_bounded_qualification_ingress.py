"""Project only high-signal runtime evidence into qualification ingress.

Revision ID: 041
Revises: 040
"""

from __future__ import annotations

from alembic import op

revision = "041"
down_revision = "040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.m1_project_runtime_qualification_ingress()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $$
        DECLARE
            runtime_job_type text;
        BEGIN
            SELECT job_type INTO runtime_job_type
            FROM public.m1_jobs
            WHERE job_key = NEW.job_key;
            IF runtime_job_type IN (
                'structure-fetch',
                'structure-normalize',
                'structure-materialize',
                'quote-batch'
            ) AND NEW.kind IN ('job.started', 'job.stage-changed', 'job.succeeded') THEN
                RETURN NEW;
            END IF;
            PERFORM public.m1_record_qualification_ingress(
                'runtime', NEW.event_id, 'v1', NEW.occurred_at, pg_catalog.to_jsonb(NEW)
            );
            RETURN NEW;
        END;
        $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.m1_project_runtime_qualification_ingress()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $$
        BEGIN
            PERFORM public.m1_record_qualification_ingress(
                'runtime', NEW.event_id, 'v1', NEW.occurred_at, pg_catalog.to_jsonb(NEW)
            );
            RETURN NEW;
        END;
        $$;
        """
    )
