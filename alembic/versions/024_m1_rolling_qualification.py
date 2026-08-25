"""Persist rolling qualification epochs and immutable certificates.

Revision ID: 024
Revises: 023

The migration is deliberately additive.  Epoch rows are a fenced mutable
projection with state/version CAS, while certificates are immutable replay
artifacts once sealed.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.create_table(
        "m1_qualification_epochs",
        sa.Column("epoch_id", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("version", sa.BigInteger, nullable=False, server_default="1"),
        sa.Column("identity_key", sa.Text, nullable=False),
        sa.Column("policy_version", sa.Text, nullable=False),
        sa.Column("release_id", sa.Text, nullable=False),
        sa.Column("config_id", sa.Text, nullable=False),
        sa.Column("role_identity", postgresql.JSONB, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_fact_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("invalidated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("invalidation_reason", sa.Text, nullable=True),
        sa.Column("qualified_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("previous_epoch_id", sa.Text, nullable=True),
        sa.Column(
            "fact_digests",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "contained_recoveries",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("coverage_seconds", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("max_gap_seconds", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("progress_count", sa.BigInteger, nullable=True),
        sa.Column("successful_count", sa.BigInteger, nullable=True),
        sa.Column("evidence_digest", sa.Text, nullable=True),
        sa.Column("required_seconds", sa.BigInteger, nullable=True),
        sa.Column(
            "slo",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "contained_incident_details",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "recovery_action_details",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("source_cursor", postgresql.JSONB, nullable=True),
        sa.Column(
            "fact_records",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("writer_id", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("epoch_id", name="pk_m1_qualification_epochs"),
        sa.ForeignKeyConstraint(
            ["previous_epoch_id"],
            ["m1_qualification_epochs.epoch_id"],
            name="fk_m1_qualification_epochs_previous",
        ),
        sa.CheckConstraint(
            "state IN ('accumulating', 'invalidated', 'recovering', 'qualified')",
            name="ck_m1_qualification_epochs_state",
        ),
        sa.CheckConstraint("version > 0", name="ck_m1_qualification_epochs_version"),
        sa.CheckConstraint(
            "jsonb_typeof(role_identity) = 'array' AND jsonb_array_length(role_identity) > 0",
            name="ck_m1_qualification_epochs_role_identity",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(fact_digests) = 'array' "
            "AND jsonb_typeof(contained_recoveries) = 'array'",
            name="ck_m1_qualification_epochs_evidence_json",
        ),
        sa.CheckConstraint(
            "coverage_seconds >= 0 AND max_gap_seconds >= 0 "
            "AND (progress_count IS NULL OR progress_count >= 0) "
            "AND (successful_count IS NULL OR successful_count >= 0) "
            "AND (required_seconds IS NULL OR required_seconds > 0)",
            name="ck_m1_qualification_epochs_counts",
        ),
        sa.CheckConstraint(
            "evidence_digest IS NULL OR evidence_digest ~ '^[0-9a-f]{64}$'",
            name="ck_m1_qualification_epochs_evidence_digest",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(slo) = 'object' "
            "AND jsonb_typeof(contained_incident_details) = 'array' "
            "AND jsonb_typeof(recovery_action_details) = 'array'",
            name="ck_m1_qualification_epochs_derived_evidence",
        ),
        sa.CheckConstraint(
            "source_cursor IS NULL OR jsonb_typeof(source_cursor) = 'object'",
            name="ck_m1_qualification_epochs_source_cursor",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(fact_records) = 'array'",
            name="ck_m1_qualification_epochs_fact_records",
        ),
        sa.CheckConstraint(
            "("
            "state = 'accumulating' AND invalidated_at IS NULL "
            "AND invalidation_reason IS NULL AND qualified_at IS NULL "
            "OR state = 'invalidated' AND invalidated_at IS NOT NULL "
            "AND invalidation_reason IS NOT NULL AND qualified_at IS NULL "
            "OR state = 'recovering' AND previous_epoch_id IS NOT NULL "
            "AND invalidated_at IS NULL AND invalidation_reason IS NULL "
            "AND qualified_at IS NULL "
            "OR state = 'qualified' AND qualified_at IS NOT NULL "
            "AND invalidated_at IS NULL AND invalidation_reason IS NULL "
            "AND progress_count IS NOT NULL AND successful_count IS NOT NULL "
            "AND evidence_digest IS NOT NULL AND required_seconds IS NOT NULL"
            ")",
            name="ck_m1_qualification_epochs_terminal_fields",
        ),
    )
    op.create_index(
        "m1_qualification_epochs_identity_started",
        "m1_qualification_epochs",
        ["identity_key", "started_at"],
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_m1_qualification_active_identity
        ON m1_qualification_epochs(identity_key)
        WHERE state IN ('accumulating', 'recovering')
        """
    )

    op.create_table(
        "m1_qualification_certificates",
        sa.Column("certificate_id", sa.Text, nullable=False),
        sa.Column("epoch_id", sa.Text, nullable=False),
        sa.Column("identity_key", sa.Text, nullable=False),
        sa.Column("policy_version", sa.Text, nullable=False),
        sa.Column("release_id", sa.Text, nullable=False),
        sa.Column("config_id", sa.Text, nullable=False),
        sa.Column("role_identity", postgresql.JSONB, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("qualified_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("canonical_payload", sa.Text, nullable=False),
        sa.Column("payload_sha256", sa.Text, nullable=False),
        sa.Column("certificate_digest", sa.Text, nullable=False),
        sa.Column("evidence_digest", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "certificate_id",
            name="pk_m1_qualification_certificates",
        ),
        sa.ForeignKeyConstraint(
            ["epoch_id"],
            ["m1_qualification_epochs.epoch_id"],
            name="fk_m1_qualification_certificates_epoch",
        ),
        sa.UniqueConstraint(
            "identity_key",
            name="uq_m1_qualification_certificates_identity",
        ),
        sa.UniqueConstraint(
            "certificate_digest",
            name="uq_m1_qualification_certificates_digest",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(role_identity) = 'array' AND jsonb_array_length(role_identity) > 0",
            name="ck_m1_qualification_certificates_role_identity",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="ck_m1_qualification_certificates_payload",
        ),
        sa.CheckConstraint(
            "octet_length(canonical_payload) > 0",
            name="ck_m1_qualification_certificates_canonical_payload",
        ),
        sa.CheckConstraint(
            "started_at < qualified_at",
            name="ck_m1_qualification_certificates_time_bounds",
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND certificate_digest ~ '^[0-9a-f]{64}$' "
            "AND evidence_digest ~ '^[0-9a-f]{64}$'",
            name="ck_m1_qualification_certificates_digests",
        ),
    )
    op.create_index(
        "m1_qualification_certificates_created",
        "m1_qualification_certificates",
        ["created_at"],
    )

    op.execute(
        """
        CREATE FUNCTION m1_canonical_jsonb(p_value jsonb) RETURNS text
        LANGUAGE sql
        IMMUTABLE
        STRICT
        AS $$
            SELECT CASE jsonb_typeof(p_value)
                WHEN 'object' THEN
                    '{' || COALESCE(
                        (
                            SELECT string_agg(
                                m1_canonical_jsonb(to_jsonb(key)) || ':'
                                || m1_canonical_jsonb(value),
                                ',' ORDER BY key
                            )
                            FROM jsonb_each(p_value)
                        ),
                        ''
                    ) || '}'
                WHEN 'array' THEN
                    '[' || COALESCE(
                        (
                            SELECT string_agg(
                                m1_canonical_jsonb(value),
                                ',' ORDER BY ordinality
                            )
                            FROM jsonb_array_elements(p_value)
                                 WITH ORDINALITY AS item(value, ordinality)
                        ),
                        ''
                    ) || ']'
                WHEN 'string' THEN to_jsonb(p_value #>> '{}')::text
                ELSE p_value::text
            END
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION m1_verify_qualification_certificate_insert() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            epoch_row m1_qualification_epochs%ROWTYPE;
            canonical_json jsonb;
            canonical_digest text;
            canonical_identity_key text;
            payload_started_at timestamptz;
            payload_qualified_at timestamptz;
            payload_required_seconds bigint;
            payload_max_gap_seconds bigint;
            payload_progress_count bigint;
            payload_successful_count bigint;
        BEGIN
            canonical_json := NEW.canonical_payload::jsonb;
            canonical_digest := encode(
                digest(convert_to(NEW.canonical_payload, 'UTF8'), 'sha256'),
                'hex'
            );
            IF canonical_json <> NEW.payload THEN
                RAISE EXCEPTION 'qualification certificate payload/canonical mismatch';
            END IF;
            IF NEW.canonical_payload <> m1_canonical_jsonb(canonical_json) THEN
                RAISE EXCEPTION 'qualification certificate canonical bytes mismatch';
            END IF;
            IF NEW.payload_sha256 <> canonical_digest
               OR NEW.certificate_digest <> canonical_digest THEN
                RAISE EXCEPTION 'qualification certificate digest mismatch';
            END IF;
            IF NEW.certificate_id <> 'qualification-certificate:' || canonical_digest THEN
                RAISE EXCEPTION 'qualification certificate id mismatch';
            END IF;
            canonical_identity_key := encode(
                digest(
                    convert_to(
                        m1_canonical_jsonb(
                            jsonb_build_object(
                                'bounds', canonical_json -> 'bounds',
                                'identity', canonical_json -> 'identity'
                            )
                        ),
                        'UTF8'
                    ),
                    'sha256'
                ),
                'hex'
            );
            IF NEW.identity_key <> canonical_identity_key THEN
                RAISE EXCEPTION 'qualification certificate identity key mismatch';
            END IF;

            SELECT * INTO epoch_row
            FROM m1_qualification_epochs
            WHERE epoch_id = NEW.epoch_id
            FOR SHARE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'qualification certificate epoch is missing';
            END IF;
            IF epoch_row.state <> 'qualified' OR epoch_row.qualified_at IS NULL THEN
                RAISE EXCEPTION 'qualification certificate epoch is not qualified';
            END IF;

            IF NEW.policy_version <> epoch_row.policy_version
               OR NEW.release_id <> epoch_row.release_id
               OR NEW.config_id <> epoch_row.config_id
               OR NEW.role_identity <> epoch_row.role_identity
               OR NEW.started_at <> epoch_row.started_at
               OR NEW.qualified_at <> epoch_row.qualified_at
               OR NEW.evidence_digest <> epoch_row.evidence_digest THEN
                RAISE EXCEPTION 'qualification certificate columns conflict with epoch';
            END IF;

            IF NEW.payload #>> '{identity,epoch_id}' <> epoch_row.epoch_id
               OR NEW.payload #>> '{identity,policy_version}' <> epoch_row.policy_version
               OR NEW.payload #>> '{identity,release_id}' <> epoch_row.release_id
               OR NEW.payload #>> '{identity,config_id}' <> epoch_row.config_id
               OR NEW.payload -> 'identity' -> 'role_identity' <> epoch_row.role_identity
               OR NEW.payload ->> 'policy_version' <> epoch_row.policy_version
               OR NEW.payload ->> 'evidence_digest' <> epoch_row.evidence_digest THEN
                RAISE EXCEPTION 'qualification certificate identity conflicts with epoch';
            END IF;

            IF NEW.payload #>> '{bounds,started_at}' !~ '(Z|\\+00:00)$'
               OR NEW.payload #>> '{bounds,qualified_at}' !~ '(Z|\\+00:00)$' THEN
                RAISE EXCEPTION 'qualification certificate bounds must be UTC';
            END IF;
            payload_started_at := (NEW.payload #>> '{bounds,started_at}')::timestamptz;
            payload_qualified_at := (NEW.payload #>> '{bounds,qualified_at}')::timestamptz;
            payload_required_seconds := (NEW.payload #>> '{bounds,required_seconds}')::bigint;
            payload_max_gap_seconds := (NEW.payload #>> '{bounds,max_gap_seconds}')::bigint;
            payload_progress_count := (NEW.payload #>> '{counts,progress_count}')::bigint;
            payload_successful_count := (NEW.payload #>> '{counts,successful_count}')::bigint;

            IF payload_started_at <> epoch_row.started_at
               OR payload_qualified_at <> epoch_row.qualified_at
               OR payload_required_seconds <> epoch_row.required_seconds
               OR payload_max_gap_seconds <> epoch_row.max_gap_seconds
               OR payload_progress_count <> epoch_row.progress_count
               OR payload_successful_count <> epoch_row.successful_count
               OR NEW.payload -> 'slo' <> epoch_row.slo
               OR NEW.payload -> 'contained_incidents' <> epoch_row.contained_incident_details
               OR NEW.payload -> 'recovery_actions' <> epoch_row.recovery_action_details THEN
                RAISE EXCEPTION 'qualification certificate payload conflicts with epoch';
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER m1_qualification_certificates_verify_insert
        BEFORE INSERT ON m1_qualification_certificates
        FOR EACH ROW EXECUTE FUNCTION m1_verify_qualification_certificate_insert();
        """
    )

    op.execute(
        """
        CREATE FUNCTION m1_reject_qualification_certificate_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'qualification certificates are append-only';
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION m1_insert_qualification_certificate(
            p_epoch_id text,
            p_policy_version text,
            p_release_id text,
            p_config_id text,
            p_role_identity jsonb,
            p_started_at timestamptz,
            p_qualified_at timestamptz,
            p_payload jsonb,
            p_canonical_payload text,
            p_payload_sha256 text,
            p_certificate_digest text,
            p_evidence_digest text
        ) RETURNS SETOF m1_qualification_certificates
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public
        AS $$
        DECLARE
            derived_certificate_id text;
            derived_identity_key text;
        BEGIN
            derived_certificate_id := 'qualification-certificate:' || p_certificate_digest;
            derived_identity_key := encode(
                digest(
                    convert_to(
                        m1_canonical_jsonb(
                            jsonb_build_object(
                                'bounds', p_payload -> 'bounds',
                                'identity', p_payload -> 'identity'
                            )
                        ),
                        'UTF8'
                    ),
                    'sha256'
                ),
                'hex'
            );

            INSERT INTO m1_qualification_certificates (
                certificate_id, epoch_id, identity_key, policy_version, release_id,
                config_id, role_identity, started_at, qualified_at, payload,
                canonical_payload, payload_sha256, certificate_digest, evidence_digest
            ) VALUES (
                derived_certificate_id, p_epoch_id, derived_identity_key, p_policy_version,
                p_release_id, p_config_id, p_role_identity, p_started_at,
                p_qualified_at, p_payload, p_canonical_payload, p_payload_sha256,
                p_certificate_digest, p_evidence_digest
            )
            ON CONFLICT (identity_key) DO NOTHING;

            RETURN QUERY
            SELECT *
            FROM m1_qualification_certificates
            WHERE identity_key = derived_identity_key;
        END;
        $$;
        """
    )
    op.execute("REVOKE ALL ON TABLE m1_qualification_certificates FROM PUBLIC")
    op.execute(
        """
        REVOKE ALL ON FUNCTION m1_insert_qualification_certificate(
            text, text, text, text, jsonb, timestamptz, timestamptz,
            jsonb, text, text, text, text
        ) FROM PUBLIC
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
                REVOKE ALL ON TABLE m1_qualification_certificates FROM anon;
                GRANT SELECT ON TABLE m1_qualification_certificates TO anon;
                REVOKE ALL ON FUNCTION m1_insert_qualification_certificate(
                    text, text, text, text, jsonb, timestamptz, timestamptz,
                    jsonb, text, text, text, text
                ) FROM anon;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
                REVOKE ALL ON TABLE m1_qualification_certificates FROM authenticated;
                GRANT SELECT ON TABLE m1_qualification_certificates TO authenticated;
                REVOKE ALL ON FUNCTION m1_insert_qualification_certificate(
                    text, text, text, text, jsonb, timestamptz, timestamptz,
                    jsonb, text, text, text, text
                ) FROM authenticated;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
                REVOKE ALL ON TABLE m1_qualification_certificates FROM service_role;
                GRANT SELECT ON TABLE m1_qualification_certificates TO service_role;
                REVOKE ALL ON FUNCTION m1_insert_qualification_certificate(
                    text, text, text, text, jsonb, timestamptz, timestamptz,
                    jsonb, text, text, text, text
                ) FROM service_role;
                GRANT EXECUTE ON FUNCTION m1_insert_qualification_certificate(
                    text, text, text, text, jsonb, timestamptz, timestamptz,
                    jsonb, text, text, text, text
                ) TO service_role;
            END IF;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER m1_qualification_certificates_immutable
        BEFORE UPDATE OR DELETE ON m1_qualification_certificates
        FOR EACH ROW EXECUTE FUNCTION m1_reject_qualification_certificate_mutation();
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER m1_qualification_certificates_immutable "
        "ON m1_qualification_certificates"
    )
    op.execute(
        "DROP TRIGGER m1_qualification_certificates_verify_insert "
        "ON m1_qualification_certificates"
    )
    op.execute(
        "DROP FUNCTION m1_insert_qualification_certificate("
        "text, text, text, text, jsonb, timestamptz, "
        "timestamptz, jsonb, text, text, text, text)"
    )
    op.execute("DROP FUNCTION m1_reject_qualification_certificate_mutation()")
    op.execute("DROP FUNCTION m1_verify_qualification_certificate_insert()")
    op.execute("DROP FUNCTION m1_canonical_jsonb(jsonb)")
    op.drop_index(
        "m1_qualification_certificates_created",
        table_name="m1_qualification_certificates",
    )
    op.drop_table("m1_qualification_certificates")
    op.drop_index("uq_m1_qualification_active_identity", table_name="m1_qualification_epochs")
    op.drop_index(
        "m1_qualification_epochs_identity_started",
        table_name="m1_qualification_epochs",
    )
    op.drop_table("m1_qualification_epochs")
