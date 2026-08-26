"""Add scoped runtime and qualification database capability roles.

Revision ID: 026
Revises: 025

This migration is a least-privilege hardening layer over the runtime and
qualification tables introduced through revision 025.  It is local-test
rollback safe only: production must not downgrade through this revision after
external login roles have been bound to the capabilities.
"""

from __future__ import annotations

from alembic import op

revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None

RUNTIME_CONTROLLER_READ_TABLES = (
    "m1_runtime_controller_leases",
    "m1_runtime_observe_decisions",
    "m1_job_runtime_state",
    "m1_jobs",
    "m1_job_circuits",
    "m1_job_attempts",
    "m1_recovery_target_budgets",
    "m1_recovery_actions",
)
RUNTIME_CONTROLLER_WRITE_TABLES = (
    "m1_runtime_controller_leases",
    "m1_runtime_observe_decisions",
)
QUALIFICATION_READ_TABLES = (
    "m1_qualification_ingress_ledger",
    "m1_qualification_source_cursors",
    "m1_qualification_epochs",
    "m1_qualification_recovery_observations",
    "m1_qualification_certificates",
    "m1_publication_pointers",
    "m1_generation_manifests",
    "m1_opportunity_publication_pointers",
    "m1_opportunity_projections",
)
QUALIFICATION_INSERT_TABLES = (
    "m1_qualification_source_cursors",
    "m1_qualification_epochs",
    "m1_qualification_recovery_observations",
)
QUALIFICATION_UPDATE_TABLES = (
    "m1_qualification_source_cursors",
    "m1_qualification_epochs",
)

CAPABILITY_ATTRIBUTES = (
    "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
    "NOINHERIT NOREPLICATION NOBYPASSRLS"
)
RUNTIME_ROLE = "m1_runtime_controller_capability"
QUALIFICATION_ROLE = "m1_qualification_worker_capability"
SUPABASE_ROLES = ("anon", "authenticated", "service_role")
CERTIFICATE_FUNCTION_SIGNATURE = (
    "text, text, text, text, jsonb, timestamptz, timestamptz, "
    "jsonb, text, text, text, text"
)
HARDENED_TRIGGER_FUNCTION_SIGNATURES = (
    "public.m1_project_runtime_qualification_ingress()",
    "public.m1_project_incident_qualification_ingress()",
    "public.m1_project_recovery_qualification_ingress()",
    "public.m1_verify_qualification_certificate_insert()",
)


def upgrade() -> None:
    _ensure_capability_role(RUNTIME_ROLE)
    _ensure_capability_role(QUALIFICATION_ROLE)
    _harden_qualification_functions()
    _grant_runtime_controller()
    _grant_qualification_worker()


def downgrade() -> None:
    # Isolated-test-only downgrade: production bindings to these capabilities
    # must be removed by an operator before rollback.
    _revoke_qualification_worker()
    _revoke_runtime_controller()
    _restore_revision_024_function_security()
    op.execute(
        """
        DROP FUNCTION IF EXISTS public.m1_record_qualification_freshness_ingress(
            text, text, timestamptz, jsonb
        )
        """
    )
    op.execute("DROP ROLE IF EXISTS m1_qualification_worker_capability")
    op.execute("DROP ROLE IF EXISTS m1_runtime_controller_capability")


def _ensure_capability_role(role: str) -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            existing record;
        BEGIN
            SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit,
                   rolreplication, rolbypassrls
            INTO existing
            FROM pg_catalog.pg_roles
            WHERE rolname = '{role}';

            IF NOT FOUND THEN
                CREATE ROLE {role} {CAPABILITY_ATTRIBUTES};
                RETURN;
            END IF;

            IF existing.rolcanlogin
               OR existing.rolsuper
               OR existing.rolcreatedb
               OR existing.rolcreaterole
               OR existing.rolinherit
               OR existing.rolreplication
               OR existing.rolbypassrls THEN
                RAISE EXCEPTION '{role} exists with unsafe attributes';
            END IF;
        END;
        $$;
        """
    )


def _grant_runtime_controller() -> None:
    _grant_current_database_connect(RUNTIME_ROLE)
    op.execute(f"GRANT USAGE ON SCHEMA public TO {RUNTIME_ROLE}")
    for table in RUNTIME_CONTROLLER_READ_TABLES:
        op.execute(f"GRANT SELECT ON TABLE public.{table} TO {RUNTIME_ROLE}")
    for table in RUNTIME_CONTROLLER_WRITE_TABLES:
        op.execute(f"GRANT INSERT ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE ON TABLE public.m1_runtime_controller_leases TO {RUNTIME_ROLE}"
    )
    _revoke_general_recorder_execute(RUNTIME_ROLE)


def _grant_qualification_worker() -> None:
    _grant_current_database_connect(QUALIFICATION_ROLE)
    op.execute(f"GRANT USAGE ON SCHEMA public TO {QUALIFICATION_ROLE}")
    for table in QUALIFICATION_READ_TABLES:
        op.execute(f"GRANT SELECT ON TABLE public.{table} TO {QUALIFICATION_ROLE}")
    for table in QUALIFICATION_INSERT_TABLES:
        op.execute(f"GRANT INSERT ON TABLE public.{table} TO {QUALIFICATION_ROLE}")
    for table in QUALIFICATION_UPDATE_TABLES:
        op.execute(f"GRANT UPDATE ON TABLE public.{table} TO {QUALIFICATION_ROLE}")
    _revoke_general_recorder_execute(QUALIFICATION_ROLE)
    op.execute(
        f"""
        GRANT EXECUTE ON FUNCTION public.m1_record_qualification_freshness_ingress(
            text, text, timestamptz, jsonb
        ) TO {QUALIFICATION_ROLE}
        """
    )
    op.execute(
        f"""
        GRANT EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
            {CERTIFICATE_FUNCTION_SIGNATURE}
        ) TO {QUALIFICATION_ROLE}
        """
    )


def _revoke_runtime_controller() -> None:
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {RUNTIME_ROLE}")
    _revoke_current_database_connect(RUNTIME_ROLE)
    op.execute(f"REVOKE UPDATE ON TABLE public.m1_runtime_controller_leases FROM {RUNTIME_ROLE}")
    for table in RUNTIME_CONTROLLER_WRITE_TABLES:
        op.execute(f"REVOKE INSERT ON TABLE public.{table} FROM {RUNTIME_ROLE}")
    for table in RUNTIME_CONTROLLER_READ_TABLES:
        op.execute(f"REVOKE SELECT ON TABLE public.{table} FROM {RUNTIME_ROLE}")
    _revoke_general_recorder_execute(RUNTIME_ROLE)


def _revoke_qualification_worker() -> None:
    op.execute(
        f"""
        REVOKE EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
            {CERTIFICATE_FUNCTION_SIGNATURE}
        ) FROM {QUALIFICATION_ROLE}
        """
    )
    op.execute(
        f"""
        REVOKE EXECUTE ON FUNCTION public.m1_record_qualification_freshness_ingress(
            text, text, timestamptz, jsonb
        ) FROM {QUALIFICATION_ROLE}
        """
    )
    for table in QUALIFICATION_UPDATE_TABLES:
        op.execute(f"REVOKE UPDATE ON TABLE public.{table} FROM {QUALIFICATION_ROLE}")
    for table in QUALIFICATION_INSERT_TABLES:
        op.execute(f"REVOKE INSERT ON TABLE public.{table} FROM {QUALIFICATION_ROLE}")
    for table in QUALIFICATION_READ_TABLES:
        op.execute(f"REVOKE SELECT ON TABLE public.{table} FROM {QUALIFICATION_ROLE}")
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {QUALIFICATION_ROLE}")
    _revoke_current_database_connect(QUALIFICATION_ROLE)
    _revoke_general_recorder_execute(QUALIFICATION_ROLE)


def _revoke_general_recorder_execute(grantee: str) -> None:
    op.execute(
        f"""
        REVOKE EXECUTE ON FUNCTION public.m1_record_qualification_ingress(
            text, text, text, timestamptz, jsonb
        ) FROM {grantee}
        """
    )


def _revoke_hardened_trigger_execute(grantee: str) -> None:
    for function_signature in HARDENED_TRIGGER_FUNCTION_SIGNATURES:
        op.execute(f"REVOKE EXECUTE ON FUNCTION {function_signature} FROM {grantee}")


def _grant_current_database_connect(role: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            EXECUTE pg_catalog.format('GRANT CONNECT ON DATABASE %I TO {role}',
                                      pg_catalog.current_database());
        END;
        $$;
        """
    )


def _revoke_current_database_connect(role: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            EXECUTE pg_catalog.format('REVOKE CONNECT ON DATABASE %I FROM {role}',
                                      pg_catalog.current_database());
        END;
        $$;
        """
    )


def _harden_qualification_functions() -> None:
    _create_general_recorder(security_definer=True, search_path="pg_catalog")
    _create_freshness_recorder()
    _create_runtime_projection(security_definer=True, search_path="pg_catalog")
    _create_incident_projection(security_definer=True, search_path="pg_catalog")
    _create_recovery_projection(security_definer=True, search_path="pg_catalog")
    _create_canonical_jsonb(search_path="pg_catalog", schema_qualified_recursion=True)
    _create_certificate_verifier(security_definer=True, search_path="pg_catalog")
    _create_certificate_inserter(search_path="pg_catalog")
    for grantee in ("PUBLIC", *SUPABASE_ROLES, RUNTIME_ROLE, QUALIFICATION_ROLE):
        _revoke_hardened_trigger_execute(grantee)
    for grantee in ("PUBLIC", *SUPABASE_ROLES, RUNTIME_ROLE, QUALIFICATION_ROLE):
        _revoke_general_recorder_execute(grantee)
    op.execute(
        f"""
        REVOKE EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
            {CERTIFICATE_FUNCTION_SIGNATURE}
        ) FROM PUBLIC
        """
    )
    for role in SUPABASE_ROLES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{role}') THEN
                    REVOKE EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
                        {CERTIFICATE_FUNCTION_SIGNATURE}
                    ) FROM {role};
                END IF;
            END;
            $$;
            """
        )


def _restore_revision_024_function_security() -> None:
    for grantee in ("PUBLIC", *SUPABASE_ROLES, RUNTIME_ROLE, QUALIFICATION_ROLE):
        _revoke_general_recorder_execute(grantee)
    op.execute(
        f"""
        REVOKE EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
            {CERTIFICATE_FUNCTION_SIGNATURE}
        ) FROM {RUNTIME_ROLE}
        """
    )
    op.execute(
        f"""
        REVOKE EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
            {CERTIFICATE_FUNCTION_SIGNATURE}
        ) FROM {QUALIFICATION_ROLE}
        """
    )
    _create_general_recorder(security_definer=False, search_path=None)
    _create_runtime_projection(security_definer=False, search_path=None)
    _create_incident_projection(security_definer=False, search_path=None)
    _create_recovery_projection(security_definer=False, search_path=None)
    _create_canonical_jsonb(search_path=None, schema_qualified_recursion=False)
    _create_certificate_verifier(security_definer=False, search_path=None)
    _create_certificate_inserter(search_path="public")
    op.execute(
        """
        REVOKE ALL ON FUNCTION public.m1_insert_qualification_certificate(
            text, text, text, text, jsonb, timestamptz, timestamptz,
            jsonb, text, text, text, text
        ) FROM PUBLIC
        """
    )
    for role in SUPABASE_ROLES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{role}') THEN
                    REVOKE ALL ON FUNCTION public.m1_insert_qualification_certificate(
                        text, text, text, text, jsonb, timestamptz, timestamptz,
                        jsonb, text, text, text, text
                    ) FROM {role};
                END IF;
            END;
            $$;
            """
        )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'service_role') THEN
                GRANT EXECUTE ON FUNCTION public.m1_insert_qualification_certificate(
                    text, text, text, text, jsonb, timestamptz, timestamptz,
                    jsonb, text, text, text, text
                ) TO service_role;
            END IF;
        END;
        $$;
        """
    )


def _security_clause(*, security_definer: bool, search_path: str | None) -> str:
    clause = "SECURITY DEFINER" if security_definer else ""
    if search_path is not None:
        clause += f"\n        SET search_path = {search_path}"
    return clause


def _create_general_recorder(*, security_definer: bool, search_path: str | None) -> None:
    security = _security_clause(security_definer=security_definer, search_path=search_path)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_record_qualification_ingress(
            p_source text,
            p_source_id text,
            p_source_version text,
            p_original_observed_at timestamptz,
            p_payload jsonb
        ) RETURNS void
        LANGUAGE plpgsql
        {security}
        AS $$
        DECLARE
            payload_digest text;
            existing_digest text;
        BEGIN
            IF p_payload IS NULL OR pg_catalog.jsonb_typeof(p_payload) <> 'object' THEN
                RAISE EXCEPTION 'qualification ingress payload must be an object';
            END IF;
            payload_digest := pg_catalog.encode(
                public.digest(pg_catalog.convert_to(p_payload::text, 'UTF8'), 'sha256'),
                'hex'
            );
            INSERT INTO public.m1_qualification_ingress_ledger (
                source, source_id, source_version, original_observed_at,
                payload, payload_sha256
            ) VALUES (
                p_source, p_source_id, p_source_version, p_original_observed_at,
                p_payload, payload_digest
            )
            ON CONFLICT (source, source_id, source_version) DO NOTHING;

            SELECT payload_sha256 INTO existing_digest
            FROM public.m1_qualification_ingress_ledger
            WHERE source = p_source
              AND source_id = p_source_id
              AND source_version = p_source_version;
            IF existing_digest IS NULL THEN
                RAISE EXCEPTION 'qualification ingress idempotency raced';
            END IF;
            IF existing_digest <> payload_digest THEN
                RAISE EXCEPTION 'qualification ingress source version conflicts';
            END IF;
        END;
        $$;
        """
    )


def _create_freshness_recorder() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.m1_record_qualification_freshness_ingress(
            p_source_id text,
            p_source_version text,
            p_original_observed_at timestamptz,
            p_payload jsonb
        ) RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $$
        BEGIN
            IF p_source_version NOT IN ('structure', 'quote', 'opportunity') THEN
                RAISE EXCEPTION 'qualification freshness product is unsupported';
            END IF;
            IF p_source_id NOT LIKE 'freshness:' || p_source_version || ':%' THEN
                RAISE EXCEPTION 'qualification freshness identity conflicts';
            END IF;
            IF p_payload IS NULL
               OR jsonb_typeof(p_payload) <> 'object'
               OR pg_column_size(p_payload) > 8192
               OR p_payload ->> 'data_product' <> p_source_version THEN
                RAISE EXCEPTION 'qualification freshness payload is invalid';
            END IF;
            PERFORM public.m1_record_qualification_ingress(
                'freshness', p_source_id, p_source_version,
                p_original_observed_at, p_payload
            );
        END;
        $$;
        """
    )
    op.execute(
        """
        REVOKE EXECUTE ON FUNCTION public.m1_record_qualification_freshness_ingress(
            text, text, timestamptz, jsonb
        ) FROM PUBLIC
        """
    )


def _create_runtime_projection(*, security_definer: bool, search_path: str | None) -> None:
    security = _security_clause(security_definer=security_definer, search_path=search_path)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_project_runtime_qualification_ingress()
        RETURNS trigger
        LANGUAGE plpgsql
        {security}
        AS $$
        BEGIN
            PERFORM public.m1_record_qualification_ingress(
                'runtime',
                NEW.event_id,
                'v1',
                NEW.occurred_at,
                pg_catalog.to_jsonb(NEW)
            );
            RETURN NEW;
        END;
        $$;
        """
    )


def _create_incident_projection(*, security_definer: bool, search_path: str | None) -> None:
    security = _security_clause(security_definer=security_definer, search_path=search_path)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_project_incident_qualification_ingress()
        RETURNS trigger
        LANGUAGE plpgsql
        {security}
        AS $$
        DECLARE
            incident_row public.m1_incidents%ROWTYPE;
        BEGIN
            SELECT * INTO incident_row
            FROM public.m1_incidents
            WHERE incident_key = NEW.incident_key;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'qualification incident ingress missing incident row';
            END IF;
            PERFORM public.m1_record_qualification_ingress(
                'incident',
                NEW.incident_event_id,
                'v1',
                NEW.occurred_at,
                pg_catalog.to_jsonb(NEW) || pg_catalog.jsonb_build_object(
                    'severity', incident_row.severity,
                    'state', incident_row.state
                )
            );
            RETURN NEW;
        END;
        $$;
        """
    )


def _create_recovery_projection(*, security_definer: bool, search_path: str | None) -> None:
    security = _security_clause(security_definer=security_definer, search_path=search_path)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_project_recovery_qualification_ingress()
        RETURNS trigger
        LANGUAGE plpgsql
        {security}
        AS $$
        DECLARE
            observed_at timestamptz;
            projected_version text;
        BEGIN
            observed_at := COALESCE(NEW.finished_at, NEW.started_at, NEW.requested_at);
            projected_version := NEW.state || ':' || COALESCE(NEW.result_code, 'none')
                || ':' || COALESCE(
                    NEW.finished_at::text,
                    NEW.started_at::text,
                    NEW.requested_at::text
                );
            PERFORM public.m1_record_qualification_ingress(
                'recovery',
                NEW.action_id,
                projected_version,
                observed_at,
                pg_catalog.to_jsonb(NEW)
            );
            RETURN NEW;
        END;
        $$;
        """
    )


def _create_certificate_verifier(
    *,
    security_definer: bool,
    search_path: str | None,
) -> None:
    security = _security_clause(security_definer=security_definer, search_path=search_path)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_verify_qualification_certificate_insert()
        RETURNS trigger
        LANGUAGE plpgsql
        {security}
        AS $$
        DECLARE
            epoch_row public.m1_qualification_epochs%ROWTYPE;
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
            canonical_digest := pg_catalog.encode(
                public.digest(pg_catalog.convert_to(NEW.canonical_payload, 'UTF8'), 'sha256'),
                'hex'
            );
            IF canonical_json <> NEW.payload THEN
                RAISE EXCEPTION 'qualification certificate payload/canonical mismatch';
            END IF;
            IF NEW.canonical_payload <> public.m1_canonical_jsonb(canonical_json) THEN
                RAISE EXCEPTION 'qualification certificate canonical bytes mismatch';
            END IF;
            IF NEW.payload_sha256 <> canonical_digest
               OR NEW.certificate_digest <> canonical_digest THEN
                RAISE EXCEPTION 'qualification certificate digest mismatch';
            END IF;
            IF NEW.certificate_id <> 'qualification-certificate:' || canonical_digest THEN
                RAISE EXCEPTION 'qualification certificate id mismatch';
            END IF;
            canonical_identity_key := pg_catalog.encode(
                public.digest(
                    pg_catalog.convert_to(
                        public.m1_canonical_jsonb(
                            pg_catalog.jsonb_build_object(
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
            FROM public.m1_qualification_epochs
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

            IF NEW.payload #>> '{{identity,epoch_id}}' <> epoch_row.epoch_id
               OR NEW.payload #>> '{{identity,policy_version}}' <> epoch_row.policy_version
               OR NEW.payload #>> '{{identity,release_id}}' <> epoch_row.release_id
               OR NEW.payload #>> '{{identity,config_id}}' <> epoch_row.config_id
               OR NEW.payload -> 'identity' -> 'role_identity' <> epoch_row.role_identity
               OR NEW.payload ->> 'policy_version' <> epoch_row.policy_version
               OR NEW.payload ->> 'evidence_digest' <> epoch_row.evidence_digest THEN
                RAISE EXCEPTION 'qualification certificate identity conflicts with epoch';
            END IF;

            IF NEW.payload #>> '{{bounds,started_at}}' !~ '(Z|\\+00:00)$'
               OR NEW.payload #>> '{{bounds,qualified_at}}' !~ '(Z|\\+00:00)$' THEN
                RAISE EXCEPTION 'qualification certificate bounds must be UTC';
            END IF;
            payload_started_at := (NEW.payload #>> '{{bounds,started_at}}')::timestamptz;
            payload_qualified_at := (NEW.payload #>> '{{bounds,qualified_at}}')::timestamptz;
            payload_required_seconds := (NEW.payload #>> '{{bounds,required_seconds}}')::bigint;
            payload_max_gap_seconds := (NEW.payload #>> '{{bounds,max_gap_seconds}}')::bigint;
            payload_progress_count := (NEW.payload #>> '{{counts,progress_count}}')::bigint;
            payload_successful_count := (NEW.payload #>> '{{counts,successful_count}}')::bigint;

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


def _create_canonical_jsonb(
    *,
    search_path: str | None,
    schema_qualified_recursion: bool,
) -> None:
    path_clause = "" if search_path is None else f"\n        SET search_path = {search_path}"
    recursive_call = (
        "public.m1_canonical_jsonb" if schema_qualified_recursion else "m1_canonical_jsonb"
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_canonical_jsonb(p_value jsonb) RETURNS text
        LANGUAGE sql
        IMMUTABLE
        STRICT
        {path_clause}
        AS $$
            SELECT CASE jsonb_typeof(p_value)
                WHEN 'object' THEN
                    '{{' || COALESCE(
                        (
                            SELECT string_agg(
                                {recursive_call}(to_jsonb(key)) || ':'
                                || {recursive_call}(value),
                                ',' ORDER BY key
                            )
                            FROM jsonb_each(p_value)
                        ),
                        ''
                    ) || '}}'
                WHEN 'array' THEN
                    '[' || COALESCE(
                        (
                            SELECT string_agg(
                                {recursive_call}(value),
                                ',' ORDER BY ordinality
                            )
                            FROM jsonb_array_elements(p_value)
                                 WITH ORDINALITY AS item(value, ordinality)
                        ),
                        ''
                    ) || ']'
                WHEN 'string' THEN to_jsonb(p_value #>> '{{}}')::text
                ELSE p_value::text
            END
        $$;
        """
    )


def _create_certificate_inserter(*, search_path: str) -> None:
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.m1_insert_qualification_certificate(
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
        ) RETURNS SETOF public.m1_qualification_certificates
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = {search_path}
        AS $$
        DECLARE
            derived_certificate_id text;
            derived_identity_key text;
        BEGIN
            derived_certificate_id := 'qualification-certificate:' || p_certificate_digest;
            derived_identity_key := pg_catalog.encode(
                public.digest(
                    pg_catalog.convert_to(
                        public.m1_canonical_jsonb(
                            pg_catalog.jsonb_build_object(
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

            INSERT INTO public.m1_qualification_certificates (
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
            FROM public.m1_qualification_certificates
            WHERE identity_key = derived_identity_key;
        END;
        $$;
        """
    )
