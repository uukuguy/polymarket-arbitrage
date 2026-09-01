"""Replace unbounded runtime observe rows with bounded semantic projections.

Revision ID: 042
Revises: 041
"""

# ruff: noqa: E501

from __future__ import annotations

from alembic import op

revision = "042"
down_revision = "041"
branch_labels = None
depends_on = None

_RUNTIME_ROLE = "m1_runtime_controller_capability"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE public.m1_runtime_observe_status (
            controller_id text PRIMARY KEY,
            controller_owner_id text NOT NULL,
            controller_epoch bigint NOT NULL CHECK (controller_epoch > 0),
            continuous_since timestamptz NOT NULL,
            last_completed_at timestamptz NOT NULL,
            max_gap_seconds integer NOT NULL CHECK (max_gap_seconds >= 0),
            candidate_count integer NOT NULL CHECK (candidate_count BETWEEN 0 AND 500),
            actionable_count integer NOT NULL CHECK (actionable_count BETWEEN 0 AND 500),
            critical_count integer NOT NULL CHECK (critical_count BETWEEN 0 AND 500),
            coverage_truncated boolean NOT NULL,
            storage_limited boolean NOT NULL,
            suppressed_transition_count integer NOT NULL CHECK (suppressed_transition_count >= 0),
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE public.m1_runtime_observe_current (
            controller_id text NOT NULL,
            target_type text NOT NULL CHECK (target_type IN ('job', 'circuit')),
            target_id text NOT NULL CHECK (length(target_id) BETWEEN 1 AND 512),
            semantic_digest text NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
            action_type text,
            reason_code text NOT NULL,
            severity text NOT NULL CHECK (severity IN ('warning', 'critical')),
            qualification_breaking boolean NOT NULL,
            payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object' AND pg_column_size(payload) <= 2048),
            first_seen_at timestamptz NOT NULL,
            last_seen_at timestamptz NOT NULL,
            PRIMARY KEY (controller_id, target_type, target_id)
        );
        CREATE INDEX m1_runtime_observe_current_seen
        ON public.m1_runtime_observe_current (controller_id, last_seen_at DESC);
        CREATE TABLE public.m1_runtime_observe_transitions (
            transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            controller_id text NOT NULL,
            observed_at timestamptz NOT NULL,
            event_kind text NOT NULL CHECK (event_kind IN ('entered', 'changed', 'recovered', 'overflow')),
            target_type text,
            target_id text,
            semantic_digest text,
            detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object' AND pg_column_size(detail) <= 2048)
        );
        CREATE INDEX m1_runtime_observe_transitions_recent
        ON public.m1_runtime_observe_transitions (controller_id, observed_at DESC, transition_id DESC);
        CREATE TABLE public.m1_runtime_observe_hourly (
            controller_id text NOT NULL,
            hour_start timestamptz NOT NULL,
            turn_count integer NOT NULL CHECK (turn_count >= 0),
            candidate_count integer NOT NULL CHECK (candidate_count >= 0),
            actionable_count integer NOT NULL CHECK (actionable_count >= 0),
            critical_count integer NOT NULL CHECK (critical_count >= 0),
            transition_count integer NOT NULL CHECK (transition_count >= 0),
            recovery_count integer NOT NULL CHECK (recovery_count >= 0),
            suppressed_transition_count integer NOT NULL CHECK (suppressed_transition_count >= 0),
            max_candidate_count integer NOT NULL CHECK (max_candidate_count >= 0),
            PRIMARY KEY (controller_id, hour_start)
        );
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.m1_runtime_observe_apply_turn(turn jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            v_controller_id text := turn->>'controller_id';
            v_owner_id text := turn->>'controller_owner_id';
            v_epoch bigint := (turn->>'controller_epoch')::bigint;
            v_now timestamptz := (turn->>'observed_at')::timestamptz;
            v_coverage_truncated boolean := COALESCE((turn->>'coverage_truncated')::boolean, false);
            v_candidate jsonb;
            v_candidate_count integer := 0;
            v_actionable_count integer := 0;
            v_critical_count integer := 0;
            v_transition_count integer := 0;
            v_recovery_count integer := 0;
            v_suppressed integer := 0;
            v_storage_limited boolean := false;
            v_existing record;
            v_current_count integer;
            v_target_type text;
            v_target_id text;
            v_digest text;
            v_changed boolean;
            v_event_kind text;
            v_gap integer := 0;
            v_prior_status record;
        BEGIN
            IF jsonb_typeof(turn) <> 'object'
               OR v_controller_id IS NULL OR v_controller_id = ''
               OR v_owner_id IS NULL OR v_owner_id = ''
               OR v_epoch IS NULL OR v_epoch < 1 OR v_now IS NULL
               OR jsonb_typeof(COALESCE(turn->'candidates', '[]'::jsonb)) <> 'array' THEN
                RAISE EXCEPTION 'invalid bounded runtime observe turn';
            END IF;
            IF jsonb_array_length(turn->'candidates') > 500 THEN
                RAISE EXCEPTION 'runtime observe candidate limit exceeded';
            END IF;
            PERFORM 1 FROM public.m1_runtime_controller_leases
            WHERE controller_id = v_controller_id
              AND owner_id = v_owner_id
              AND lease_epoch = v_epoch
              AND lease_expires_at >= v_now
            FOR SHARE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'runtime observe controller lease is stale';
            END IF;

            DELETE FROM public.m1_runtime_observe_transitions
            WHERE controller_id = v_controller_id AND observed_at < v_now - interval '24 hours';
            DELETE FROM public.m1_runtime_observe_transitions
            WHERE controller_id = v_controller_id
              AND transition_id NOT IN (
                SELECT transition_id FROM (
                    SELECT transition_id FROM public.m1_runtime_observe_transitions
                    WHERE controller_id = v_controller_id
                    ORDER BY observed_at DESC, transition_id DESC LIMIT 5000
                ) retained_transitions
            );
            DELETE FROM public.m1_runtime_observe_hourly
            WHERE controller_id = v_controller_id AND hour_start < date_trunc('hour', v_now) - interval '30 days';

            FOR v_candidate IN SELECT value FROM jsonb_array_elements(turn->'candidates') LOOP
                v_target_type := v_candidate->>'target_type';
                v_target_id := v_candidate->>'target_id';
                v_digest := v_candidate->>'semantic_digest';
                IF v_target_type NOT IN ('job', 'circuit') OR v_target_id IS NULL
                   OR length(v_target_id) = 0 OR length(v_target_id) > 512
                   OR v_digest !~ '^[0-9a-f]{64}$'
                   OR (v_candidate->>'severity') NOT IN ('warning', 'critical')
                   OR v_candidate->>'reason_code' IS NULL
                   OR jsonb_typeof(COALESCE(v_candidate->'payload', '{}'::jsonb)) <> 'object'
                   OR pg_column_size(v_candidate->'payload') > 2048 THEN
                    RAISE EXCEPTION 'invalid bounded runtime observe candidate';
                END IF;
                v_candidate_count := v_candidate_count + 1;
                IF COALESCE(v_candidate->>'action_type', '') <> '' THEN
                    v_actionable_count := v_actionable_count + 1;
                END IF;
                IF v_candidate->>'severity' = 'critical' THEN
                    v_critical_count := v_critical_count + 1;
                END IF;
                SELECT * INTO v_existing FROM public.m1_runtime_observe_current
                WHERE controller_id = v_controller_id AND target_type = v_target_type AND target_id = v_target_id
                FOR UPDATE;
                v_changed := NOT FOUND OR v_existing.semantic_digest <> v_digest;
                IF NOT FOUND THEN
                    SELECT count(*) INTO v_current_count FROM public.m1_runtime_observe_current
                    WHERE controller_id = v_controller_id;
                    IF v_current_count >= 500 THEN
                        v_storage_limited := true;
                        v_suppressed := v_suppressed + 1;
                        CONTINUE;
                    END IF;
                    v_event_kind := 'entered';
                    INSERT INTO public.m1_runtime_observe_current (
                        controller_id,target_type,target_id,semantic_digest,action_type,reason_code,severity,
                        qualification_breaking,payload,first_seen_at,last_seen_at
                    ) VALUES (
                        v_controller_id,v_target_type,v_target_id,v_digest,NULLIF(v_candidate->>'action_type',''),
                        v_candidate->>'reason_code',v_candidate->>'severity',
                        COALESCE((v_candidate->>'qualification_breaking')::boolean,false),v_candidate->'payload',v_now,v_now
                    );
                ELSE
                    v_event_kind := 'changed';
                    UPDATE public.m1_runtime_observe_current SET
                        semantic_digest = v_digest, action_type = NULLIF(v_candidate->>'action_type',''),
                        reason_code = v_candidate->>'reason_code', severity = v_candidate->>'severity',
                        qualification_breaking = COALESCE((v_candidate->>'qualification_breaking')::boolean,false),
                        payload = v_candidate->'payload', last_seen_at = v_now
                    WHERE controller_id = v_controller_id AND target_type = v_target_type AND target_id = v_target_id;
                END IF;
                IF v_changed THEN
                    IF v_transition_count < 20 THEN
                        INSERT INTO public.m1_runtime_observe_transitions (
                            controller_id,observed_at,event_kind,target_type,target_id,semantic_digest,detail
                        ) VALUES (v_controller_id,v_now,v_event_kind,v_target_type,v_target_id,v_digest,v_candidate->'payload');
                        v_transition_count := v_transition_count + 1;
                    ELSE
                        v_suppressed := v_suppressed + 1;
                    END IF;
                END IF;
            END LOOP;

            IF NOT v_coverage_truncated THEN
                FOR v_existing IN SELECT * FROM public.m1_runtime_observe_current current_row
                    WHERE current_row.controller_id = v_controller_id
                      AND NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements(turn->'candidates') candidate_row
                        WHERE candidate_row->>'target_type' = current_row.target_type
                          AND candidate_row->>'target_id' = current_row.target_id
                    ) LOOP
                    DELETE FROM public.m1_runtime_observe_current
                    WHERE controller_id = v_controller_id AND target_type = v_existing.target_type AND target_id = v_existing.target_id;
                    IF v_transition_count < 20 THEN
                        INSERT INTO public.m1_runtime_observe_transitions (
                            controller_id,observed_at,event_kind,target_type,target_id,semantic_digest,detail
                        ) VALUES (v_controller_id,v_now,'recovered',v_existing.target_type,v_existing.target_id,
                                  v_existing.semantic_digest,jsonb_build_object('reason_code',v_existing.reason_code));
                        v_transition_count := v_transition_count + 1;
                        v_recovery_count := v_recovery_count + 1;
                    ELSE
                        v_suppressed := v_suppressed + 1;
                    END IF;
                END LOOP;
            END IF;
            IF v_suppressed > 0 THEN
                INSERT INTO public.m1_runtime_observe_transitions (
                    controller_id,observed_at,event_kind,detail
                ) VALUES (v_controller_id,v_now,'overflow',jsonb_build_object('suppressed_transition_count',v_suppressed));
            END IF;
            INSERT INTO public.m1_runtime_observe_hourly (
                controller_id,hour_start,turn_count,candidate_count,actionable_count,critical_count,
                transition_count,recovery_count,suppressed_transition_count,max_candidate_count
            ) VALUES (
                v_controller_id,date_trunc('hour',v_now),1,v_candidate_count,v_actionable_count,v_critical_count,
                v_transition_count,v_recovery_count,v_suppressed,v_candidate_count
            ) ON CONFLICT (controller_id,hour_start) DO UPDATE SET
                turn_count = m1_runtime_observe_hourly.turn_count + 1,
                candidate_count = m1_runtime_observe_hourly.candidate_count + EXCLUDED.candidate_count,
                actionable_count = m1_runtime_observe_hourly.actionable_count + EXCLUDED.actionable_count,
                critical_count = m1_runtime_observe_hourly.critical_count + EXCLUDED.critical_count,
                transition_count = m1_runtime_observe_hourly.transition_count + EXCLUDED.transition_count,
                recovery_count = m1_runtime_observe_hourly.recovery_count + EXCLUDED.recovery_count,
                suppressed_transition_count = m1_runtime_observe_hourly.suppressed_transition_count + EXCLUDED.suppressed_transition_count,
                max_candidate_count = greatest(m1_runtime_observe_hourly.max_candidate_count, EXCLUDED.max_candidate_count);

            SELECT * INTO v_prior_status FROM public.m1_runtime_observe_status WHERE controller_id = v_controller_id FOR UPDATE;
            IF FOUND THEN
                v_gap := greatest(0, floor(extract(epoch FROM v_now - v_prior_status.last_completed_at))::integer);
            END IF;
            INSERT INTO public.m1_runtime_observe_status (
                controller_id,controller_owner_id,controller_epoch,continuous_since,last_completed_at,max_gap_seconds,
                candidate_count,actionable_count,critical_count,coverage_truncated,storage_limited,
                suppressed_transition_count,updated_at
            ) VALUES (
                v_controller_id,v_owner_id,v_epoch,v_now,v_now,v_gap,v_candidate_count,v_actionable_count,
                v_critical_count,v_coverage_truncated,v_storage_limited,v_suppressed,v_now
            ) ON CONFLICT (controller_id) DO UPDATE SET
                controller_owner_id = EXCLUDED.controller_owner_id, controller_epoch = EXCLUDED.controller_epoch,
                continuous_since = CASE WHEN EXCLUDED.max_gap_seconds > 90 THEN EXCLUDED.last_completed_at ELSE m1_runtime_observe_status.continuous_since END,
                last_completed_at = EXCLUDED.last_completed_at,
                max_gap_seconds = CASE
                    WHEN EXCLUDED.max_gap_seconds > 90 THEN EXCLUDED.max_gap_seconds
                    ELSE greatest(m1_runtime_observe_status.max_gap_seconds, EXCLUDED.max_gap_seconds)
                END,
                candidate_count = EXCLUDED.candidate_count, actionable_count = EXCLUDED.actionable_count,
                critical_count = EXCLUDED.critical_count, coverage_truncated = EXCLUDED.coverage_truncated,
                storage_limited = EXCLUDED.storage_limited, suppressed_transition_count = EXCLUDED.suppressed_transition_count,
                updated_at = EXCLUDED.updated_at;
            RETURN jsonb_build_object(
                'candidate_count',v_candidate_count,'actionable_count',v_actionable_count,
                'critical_count',v_critical_count,'transition_count',v_transition_count,
                'recovery_count',v_recovery_count,'suppressed_transition_count',v_suppressed,
                'coverage_truncated',v_coverage_truncated,'storage_limited',v_storage_limited
            );
        END;
        $$;
        """
    )
    op.execute("DROP TRIGGER IF EXISTS m1_runtime_observe_decisions_immutable ON public.m1_runtime_observe_decisions")
    op.execute("DROP FUNCTION IF EXISTS public.m1_runtime_observe_decisions_reject_mutation()")
    op.execute("DROP TABLE public.m1_runtime_observe_decisions")
    op.execute(
        f"REVOKE ALL ON TABLE "
        f"public.m1_runtime_observe_status, public.m1_runtime_observe_current, "
        f"public.m1_runtime_observe_transitions, public.m1_runtime_observe_hourly "
        f"FROM {_RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT SELECT ON TABLE public.m1_runtime_observe_status TO {_RUNTIME_ROLE}"
    )
    op.execute(f"GRANT USAGE ON SCHEMA public TO {_RUNTIME_ROLE}")
    op.execute("REVOKE ALL ON FUNCTION public.m1_runtime_observe_apply_turn(jsonb) FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION public.m1_runtime_observe_apply_turn(jsonb) TO {_RUNTIME_ROLE}"
    )


def downgrade() -> None:
    raise RuntimeError("revision 042 is production-forward-only")
