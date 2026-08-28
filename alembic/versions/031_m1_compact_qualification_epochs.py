"""Compact legacy qualification evidence after normalized-fact validation.

Revision ID: 031
Revises: 030
"""

from __future__ import annotations

from alembic import op

revision = "031"
down_revision = "030"
branch_labels = None
depends_on = None

_CONTAINED_REASONS = """
    'recovery.heartbeat', 'recovery.retry', 'recovery.reclaim',
    'recovery.machine-replacement', 'recovery.process-replacement',
    'recovery.circuit-probe'
"""


def upgrade() -> None:
    # A schema migration must not wait indefinitely behind a live writer.
    op.execute("SET LOCAL lock_timeout = '1000ms'")
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM public.m1_qualification_epochs AS epoch
                LEFT JOIN (
                    SELECT fact.epoch_id,
                           count(*)::bigint AS fact_count,
                           min(fact.ordinal)::bigint AS min_ordinal,
                           max(fact.ordinal)::bigint AS max_ordinal,
                           count(*) FILTER (
                               WHERE fact.reason IN ({_CONTAINED_REASONS})
                           )::bigint AS recovery_count
                    FROM public.m1_qualification_epoch_facts AS fact
                    GROUP BY fact.epoch_id
                ) AS normalized ON normalized.epoch_id = epoch.epoch_id
                WHERE epoch.runtime_fact_count
                          IS DISTINCT FROM COALESCE(normalized.fact_count, 0)
                   OR epoch.runtime_contained_recovery_count
                          IS DISTINCT FROM COALESCE(normalized.recovery_count, 0)
                   OR (
                       COALESCE(normalized.fact_count, 0) > 0
                       AND (
                           normalized.min_ordinal IS DISTINCT FROM 1
                           OR normalized.max_ordinal
                                  IS DISTINCT FROM normalized.fact_count
                       )
                   )
            ) THEN
                RAISE EXCEPTION
                    'qualification epoch normalized fact count conflicts';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM public.m1_qualification_epochs AS epoch
                WHERE jsonb_array_length(epoch.fact_records) > 0
                  AND epoch.fact_records IS DISTINCT FROM (
                      SELECT COALESCE(
                          jsonb_agg(fact.fact_record ORDER BY fact.ordinal),
                          '[]'::jsonb
                      )
                      FROM public.m1_qualification_epoch_facts AS fact
                      WHERE fact.epoch_id = epoch.epoch_id
                  )
            ) THEN
                RAISE EXCEPTION
                    'qualification epoch normalized fact payload conflicts';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM public.m1_qualification_epochs AS epoch
                WHERE jsonb_array_length(epoch.fact_records) > 0
                  AND (
                      jsonb_array_length(epoch.fact_digests)
                          IS DISTINCT FROM epoch.runtime_fact_count
                      OR EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(epoch.fact_digests)
                              WITH ORDINALITY AS digest_item(value, ordinal)
                          LEFT JOIN public.m1_qualification_epoch_facts AS fact
                            ON fact.epoch_id = epoch.epoch_id
                           AND fact.ordinal = digest_item.ordinal
                          WHERE jsonb_typeof(digest_item.value) <> 'array'
                             OR jsonb_array_length(digest_item.value) <> 2
                             OR digest_item.value ->> 0 IS DISTINCT FROM fact.fact_id
                             OR jsonb_typeof(digest_item.value -> 1) <> 'string'
                      )
                  )
            ) THEN
                RAISE EXCEPTION
                    'qualification epoch normalized fact digest index conflicts';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM public.m1_qualification_epochs AS epoch
                WHERE jsonb_array_length(epoch.fact_records) > 0
                  AND epoch.contained_recoveries IS DISTINCT FROM (
                      SELECT COALESCE(
                          jsonb_agg(to_jsonb(fact.fact_id) ORDER BY fact.ordinal)
                              FILTER (WHERE fact.reason IN ({_CONTAINED_REASONS})),
                          '[]'::jsonb
                      )
                      FROM public.m1_qualification_epoch_facts AS fact
                      WHERE fact.epoch_id = epoch.epoch_id
                  )
            ) THEN
                RAISE EXCEPTION
                    'qualification epoch normalized recovery index conflicts';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        """
        UPDATE public.m1_qualification_epochs
        SET fact_records = '[]'::jsonb,
            fact_digests = '[]'::jsonb,
            contained_recoveries = '[]'::jsonb,
            updated_at = clock_timestamp()
        WHERE jsonb_array_length(fact_records) <> 0
           OR jsonb_array_length(fact_digests) <> 0
           OR jsonb_array_length(contained_recoveries) <> 0
        """
    )
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        ADD CONSTRAINT ck_m1_qualification_epochs_fact_records_compact
            CHECK (jsonb_array_length(fact_records) = 0),
        ADD CONSTRAINT ck_m1_qualification_epochs_fact_digests_compact
            CHECK (jsonb_array_length(fact_digests) = 0),
        ADD CONSTRAINT ck_m1_qualification_epochs_contained_recoveries_compact
            CHECK (jsonb_array_length(contained_recoveries) = 0)
        """
    )


def downgrade() -> None:
    # Normalized facts remain canonical; dropping the fences is lossless and
    # deliberately does not recreate the redundant, growth-prone JSON arrays.
    op.execute("SET LOCAL lock_timeout = '1000ms'")
    op.execute(
        """
        ALTER TABLE public.m1_qualification_epochs
        DROP CONSTRAINT ck_m1_qualification_epochs_contained_recoveries_compact,
        DROP CONSTRAINT ck_m1_qualification_epochs_fact_digests_compact,
        DROP CONSTRAINT ck_m1_qualification_epochs_fact_records_compact
        """
    )
