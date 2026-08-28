"""Normalize active qualification facts into an append-only relation.

Revision ID: 030
Revises: 029
"""

from __future__ import annotations

from alembic import op

revision = "030"
down_revision = "029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        ADD COLUMN runtime_fact_count bigint NOT NULL DEFAULT 0,
        ADD COLUMN runtime_contained_recovery_count bigint NOT NULL DEFAULT 0,
        ADD CONSTRAINT ck_m1_qualification_epochs_runtime_fact_count
            CHECK (runtime_fact_count >= 0),
        ADD CONSTRAINT ck_m1_qualification_epochs_runtime_recovery_count
            CHECK (
                runtime_contained_recovery_count >= 0
                AND runtime_contained_recovery_count <= runtime_fact_count
            )
        """
    )
    op.execute(
        """
        CREATE TABLE public.m1_qualification_epoch_facts (
            epoch_id text NOT NULL
                REFERENCES public.m1_qualification_epochs(epoch_id) ON DELETE RESTRICT,
            ordinal bigint NOT NULL,
            fact_id text NOT NULL,
            reason text NOT NULL,
            observed_at timestamptz NOT NULL,
            fact_record jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_m1_qualification_epoch_facts PRIMARY KEY (epoch_id, ordinal),
            CONSTRAINT uq_m1_qualification_epoch_fact_id UNIQUE (epoch_id, fact_id),
            CONSTRAINT ck_m1_qualification_epoch_fact_ordinal CHECK (ordinal > 0),
            CONSTRAINT ck_m1_qualification_epoch_fact_identity
                CHECK (length(fact_id) > 0 AND length(reason) > 0),
            CONSTRAINT ck_m1_qualification_epoch_fact_record
                CHECK (jsonb_typeof(fact_record) = 'object')
        )
        """
    )
    op.execute(
        """
        CREATE INDEX m1_qualification_epoch_facts_recent
        ON public.m1_qualification_epoch_facts (epoch_id, ordinal DESC)
        """
    )
    op.execute(
        """
        INSERT INTO public.m1_qualification_epoch_facts (
            epoch_id, ordinal, fact_id, reason, observed_at, fact_record, created_at
        )
        SELECT epoch.epoch_id,
               item.ordinality::bigint,
               item.record #>> '{fact,fact_id}',
               item.record #>> '{fact,reason}',
               (item.record #>> '{fact,observed_at}')::timestamptz,
               item.record,
               epoch.updated_at
        FROM public.m1_qualification_epochs AS epoch
        CROSS JOIN LATERAL jsonb_array_elements(epoch.fact_records)
            WITH ORDINALITY AS item(record, ordinality)
        """
    )
    op.execute(
        """
        UPDATE public.m1_qualification_epochs AS epoch
        SET runtime_fact_count = counts.fact_count,
            runtime_contained_recovery_count = counts.recovery_count
        FROM (
            SELECT facts.epoch_id,
                   count(*)::bigint AS fact_count,
                   count(*) FILTER (
                       WHERE facts.reason IN (
                           'recovery.heartbeat', 'recovery.retry', 'recovery.reclaim',
                           'recovery.machine-replacement', 'recovery.process-replacement',
                           'recovery.circuit-probe'
                       )
                   )::bigint AS recovery_count
            FROM public.m1_qualification_epoch_facts AS facts
            GROUP BY facts.epoch_id
        ) AS counts
        WHERE counts.epoch_id = epoch.epoch_id
        """
    )
    op.execute(
        """
        CREATE FUNCTION m1_reject_qualification_epoch_fact_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'qualification epoch facts are append-only';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER m1_qualification_epoch_facts_immutable
        BEFORE UPDATE OR DELETE ON public.m1_qualification_epoch_facts
        FOR EACH ROW EXECUTE FUNCTION m1_reject_qualification_epoch_fact_mutation()
        """
    )
    op.execute(
        """
        GRANT SELECT, INSERT ON TABLE public.m1_qualification_epoch_facts
        TO m1_qualification_worker_capability
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE SELECT, INSERT ON TABLE public.m1_qualification_epoch_facts
        FROM m1_qualification_worker_capability
        """
    )
    op.execute(
        "DROP TRIGGER IF EXISTS m1_qualification_epoch_facts_immutable "
        "ON public.m1_qualification_epoch_facts"
    )
    op.execute("DROP FUNCTION IF EXISTS m1_reject_qualification_epoch_fact_mutation()")
    op.execute("DROP TABLE public.m1_qualification_epoch_facts")
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        DROP CONSTRAINT ck_m1_qualification_epochs_runtime_recovery_count,
        DROP CONSTRAINT ck_m1_qualification_epochs_runtime_fact_count,
        DROP COLUMN runtime_contained_recovery_count,
        DROP COLUMN runtime_fact_count
        """
    )
