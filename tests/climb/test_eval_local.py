from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.climb import eval_local  # noqa: E402
from tools.climb.eval_local import (  # noqa: E402
    GATE_COMMANDS,
    GateResult,
    build_score,
    evaluate_gates,
)


def test_living_doc_contract_selects_focused_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "living-doc-contract"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_m1_manual_contract.py",
            "-q",
        ],
        "integration": ["make", "docs-m1-check"],
        "cli": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_makefile_contract.py",
            "tests/test_makefile.py",
            "-q",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_m1_manual_contract.py",
            "-k",
            "precommit",
            "-q",
        ],
    }


def test_opportunity_feed_chain_truth_profile_is_dedicated() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "opportunity-feed-chain-truth"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/routing/test_opportunity_diagnosis.py",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/cli/test_arbitrage_cli_process.py",
            "-k",
            "diagnose_feed",
            "-q",
        ],
        "cli": ["make", "docs-m1-check"],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_m1_manual_contract.py",
            "-k",
            "opportunity_diagnosis",
            "-q",
        ],
    }


def test_opportunity_feed_cadence_sla_profile_is_fixture_only() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "opportunity-feed-cadence-sla"})

    assert tuple(commands) == ("planning", "unit", "integration", "cli", "restart")
    required_tests = {
        "tests/routing/test_neg_risk_quote_store.py",
        "tests/routing/test_neg_risk_quote_collector.py",
        "tests/routing/test_opportunity_scanner.py",
        "tests/m1-perception/test_arbitrage_opportunities_http.py",
    }
    flattened = [argument for command in commands.values() for argument in command]
    assert required_tests <= set(flattened)
    assert ["make", "-n", "collect-neg-risk-quotes"] in commands.values()
    assert ["make", "-n", "scan-arb-quotes"] in commands.values()
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("http://", "https://", "flyctl", "deploy", "cron")
        )
    }


def test_l3_prerequisite_profile_uses_only_local_relevant_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "l3-prerequisite-chain-truth"})

    flattened = [argument for command in commands.values() for argument in command]
    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/alembic/test_006.py",
        "tests/storage/test_supabase_mirror.py",
        "tests/observation/test_l2_candidate_refresh.py",
        "tests/m1-perception/test_l3_promoter.py",
        "tests/m1-perception/test_l3_promote_dry_run.py",
        "tests/m1-perception/test_candidate_refresh_l3_protection.py",
    ):
        assert required in flattened
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("http://", "https://", "flyctl", "deploy", "migrate")
        )
    }


def test_checkpointed_structure_recovery_profile_uses_bounded_local_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "checkpointed-structure-recovery"})

    flattened = [argument for command in commands.values() for argument in command]
    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/m1-perception/test_structure_generation_publication.py",
        "tests/m1-perception/test_control_plane_postgres.py",
        "tests/m1-perception/test_control_plane_shadow.py",
    ):
        assert required in flattened
    assert commands["unit"][-3:] == [
        "-k",
        "expired_read_budget or preserves_prior_checkpoint or certification_rejects",
        "-q",
    ]
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("http://", "https://", "flyctl", "deploy", "migrate")
        )
    }


def test_transactional_production_promotion_profile_uses_only_local_proof_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "transactional-production-promotion"})

    flattened = [argument for command in commands.values() for argument in command]

    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/m1-perception/test_control_plane_postgres.py",
        "tests/m1-perception/test_control_plane_rollout.py",
        "tests/m1-perception/test_control_plane_shadow.py",
    ):
        assert required in flattened
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("flyctl", "deploy", "migrate", "http://", "https://")
        )
    }


def test_event_driven_runtime_self_healing_profile_uses_local_runtime_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "event-driven-runtime-self-healing"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_runtime_models.py",
            "tests/m1-perception/test_transactional_runtime_coverage.py::test_runtime_registry_has_exact_eight_job_types_with_meaningful_stage_names",
            "tests/m1-perception/test_transactional_runtime_coverage.py::test_runtime_coverage_gate_uses_real_terminal_boundaries_and_fails_closed",
            "tests/m1-perception/test_transactional_runtime_coverage.py::test_runtime_reporter_rejects_secret_like_detail_keys_before_persistence",
            "tests/m1-perception/test_transactional_runtime_coverage.py::test_runtime_reporter_rejects_unbounded_detail_before_persistence",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_transactional_runtime_coverage.py",
            "-q",
        ],
        "cli": [
            "uv",
            "run",
            "ruff",
            "check",
            "src/polyarb/control_plane",
            "tests/m1-perception",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_transactional_quote_admission.py::test_quote_admitter_long_runtime_keeps_lease_live_for_207_simulated_seconds",
            "tests/m1-perception/test_transactional_quote_admission.py::test_quote_admitter_stale_heartbeat_drains_blocking_read_before_return",
            "tests/m1-perception/test_transactional_quote_admission.py::test_quote_admitter_external_cancellation_drains_blocking_read_before_return",
            "tests/m1-perception/test_transactional_quote_admission.py::test_quote_admitter_blocking_recovery_reports_pending_after_terminal_success",
            "tests/m1-perception/test_transactional_quote_worker.py::test_quote_batch_stale_heartbeat_cancels_owner_and_drains_reader",
            "tests/m1-perception/test_transactional_quote_worker.py::test_quote_batch_scheduler_cancellation_drains_reader_without_late_receipt",
            "tests/m1-perception/test_transactional_quote_worker.py::test_quote_certifier_scheduler_cancellation_drains_terminal_thread",
            "tests/m1-perception/test_transactional_opportunity_projection.py::test_opportunity_scheduler_cancellation_drains_db_call_without_late_publish",
            "-q",
        ],
    }

    flattened = [argument for command in commands.values() for argument in command]
    for required in (
        "test_transactional_runtime_coverage.py",
        "test_runtime_reporter_rejects_secret_like_detail_keys_before_persistence",
        "test_runtime_reporter_rejects_unbounded_detail_before_persistence",
        "test_quote_admitter_long_runtime_keeps_lease_live_for_207_simulated_seconds",
        "test_quote_batch_stale_heartbeat_cancels_owner_and_drains_reader",
        "test_quote_batch_scheduler_cancellation_drains_reader_without_late_receipt",
        "test_quote_certifier_scheduler_cancellation_drains_terminal_thread",
        "test_opportunity_scheduler_cancellation_drains_db_call_without_late_publish",
    ):
        assert any(required in argument for argument in flattened)
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in (
                "flyctl",
                "deploy",
                "migrate",
                "http://",
                "https://",
                "production",
                "dsn",
            )
        )
    }


def test_fenced_deadline_reconciler_profile_uses_exact_local_recovery_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "fenced-deadline-reconciler"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_reconciler.py::test_reconciler_classifies_bounded_runtime_recovery_table",
            "tests/m1-perception/test_control_plane_reconciler.py::test_missing_heartbeat_waits_for_fence_before_reclaim",
            "tests/m1-perception/test_control_plane_reconciler.py::test_expired_lease_fence_outranks_owner_authority_actions",
            "tests/m1-perception/test_control_plane_reconciler.py::test_expired_lease_respects_higher_precedence_no_action_safety_branches",
            "tests/m1-perception/test_control_plane_reconciler.py::test_exact_deadline_boundaries_are_inclusive_at_policy_thresholds",
            "tests/m1-perception/test_control_plane_reconciler.py::test_deadline_boundaries_are_not_triggered_before_policy_thresholds",
            "tests/m1-perception/test_control_plane_reconciler.py::test_integrity_auth_schema_credential_and_capacity_are_human_only",
            "tests/m1-perception/test_control_plane_reconciler.py::test_precedence_fencing_budget_and_safety_outrank_retry_convenience",
            "tests/m1-perception/test_control_plane_reconciler.py::test_process_and_machine_actions_exist_but_are_not_chosen_automatically",
            "tests/m1-perception/test_control_plane_reconciler.py::test_recovery_types_reject_naive_times_invalid_types_and_negative_counts",
            "tests/m1-perception/test_control_plane_reconciler.py::test_recovery_decision_enforces_closed_reason_codes_and_invariants",
            "tests/m1-perception/test_control_plane_reconciler.py::test_recovery_decision_allows_only_exact_action_reason_pairs",
            "tests/m1-perception/test_control_plane_reconciler.py::test_recovery_decision_rejects_wrong_action_reason_pairs",
            "tests/m1-perception/test_control_plane_reconciler.py::test_next_check_at_is_deterministic_from_inputs",
            "tests/m1-perception/test_transactional_runtime_coverage.py::test_retry_circuit_budget_and_backoff_have_one_runtime_policy_authority",
            "tests/m1-perception/test_transactional_runtime_coverage.py::test_structure_certifier_gets_bounded_long_attempt_without_weakening_liveness",
            "tests/m1-perception/test_gamma_client.py::test_gamma_explicit_close_bounds_hung_http_close",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/alembic/test_023.py::test_023_upgrades_from_022_downgrades_and_reupgrades_with_expected_schema",
            "tests/m1-perception/test_control_plane_postgres.py::test_controller_claims_are_monotonic_and_only_latest_schedules_recovery_action",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_active_target_race_persists_one_stale_noop",
            "tests/m1-perception/test_control_plane_postgres.py::test_runtime_controller_status_and_facts_are_read_only",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_stale_controller_does_not_create_budget_or_poison_schedule",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_schedule_is_idempotent_and_conflicting_replay_fails_closed",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_concurrent_exact_schedule_replay_is_atomic",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_persisted_budget_does_not_reset_on_controller_reclaim",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_concurrent_last_budget_unit_is_consumed_once",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_statement_timeout_rolls_back_action_event_incident_and_alert",
            "tests/m1-perception/test_control_plane_postgres.py::test_action_terminal_uses_db_clock_and_rolls_back_after_worker_lease_expires",
            "tests/m1-perception/test_control_plane_postgres.py::test_concurrent_terminal_structure_receipts_cannot_lose_certifier_wakeup",
            "tests/m1-perception/test_control_plane_postgres.py::test_concurrent_terminal_quote_receipts_cannot_lose_certifier_wakeup",
            "tests/m1-perception/test_control_plane_postgres.py::test_retry_circuit_opens_on_third_failure_with_bounded_probe_delay",
            "tests/m1-perception/test_control_plane_postgres.py::test_runtime_reconcile_exact_target_is_filtered_before_sample_limit",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_executor_heartbeats_exact_attempt_without_business_receipt",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_executor_cancel_is_cooperative_retry_and_exactly_fenced",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_executor_reclaims_expired_lease_without_claiming_another_job",
            "-q",
        ],
        "cli": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_controller_status_is_read_only_and_bounded",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_requires_enable_before_database_or_controller",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_evaluates_schedules_and_executes_one_action",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_exact_selector_fails_before_wrong_action",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_store_conflicts_fail_loud",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_serve_store_conflicts_exit_current_turn",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_serve_stops_cleanly_on_signal_and_is_sequential",
            "tests/m1-perception/test_makefile_contract.py::test_make_runtime_controller_targets_are_wired",
            "tests/m1-perception/test_makefile_contract.py::test_make_runtime_mutation_target_has_enable_guard",
            "tests/m1-perception/test_makefile_contract.py::test_make_runtime_status_is_read_only_dry_run",
            "tests/m1-perception/test_makefile_contract.py::test_make_runtime_reconcile_once_forwards_exact_target_selector",
            "tests/m1-perception/test_makefile_contract.py::test_runtime_image_build_binds_exact_revision_and_is_build_only",
            "-q",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_recovery_executor.py::test_executor_crash_leaves_running_action_for_expiry_reclaim",
            "tests/m1-perception/test_control_plane_recovery_executor.py::test_expired_old_action_worker_cannot_execute_after_reclaim_epoch_bump",
            "tests/m1-perception/test_control_plane_recovery_executor.py::test_process_and_machine_actions_are_durable_disabled_noops",
            "tests/m1-perception/test_control_plane_recovery_executor.py::test_recovery_action_result_never_exposes_receipt_or_pointer_postconditions",
            "tests/m1-perception/test_l2_main_book_levels.py::test_dispatch_reports_tob_and_depth_outcomes_separately",
            "tests/m1-perception/test_l2_main_book_levels.py::test_dispatcher_close_owns_and_drains_pending_top_of_book",
            "tests/m1-perception/test_daemon_shutdown.py::test_l2_shutdown_uses_one_budget_below_the_platform_window",
            "tests/m1-perception/test_daemon_shutdown.py::test_http_startup_gate_is_shared_deadline_based_and_stop_aware",
            "tests/m1-perception/test_daemon_shutdown.py::test_l1_and_l2_startup_use_the_same_named_authority",
            "tests/m1-perception/test_daemon_shutdown.py::test_daemon_task_exit_wins_the_l1_supervision_race_without_orphan_waiter",
            "tests/m1-perception/test_daemon_shutdown.py::test_l1_main_supervises_all_daemon_tasks_after_the_startup_commit",
            "tests/daemon/test_l2_main_startup.py::test_shutdown_uses_shared_daemon_drain_authority",
            "tests/m1-perception/test_scheduler.py::test_structure_slice_outer_budget_is_derived_from_work_and_shutdown",
            "tests/m1-perception/test_makefile_contract.py::test_docker_targets_scope_context_without_mutating_global_default",
            "tests/m1-perception/test_daemon_shutdown.py::test_daemon_readiness_grace_matches_platform_cap_and_outlives_startup",
            "tests/m1-perception/test_makefile_contract.py::test_runtime_image_build_treats_fly_config_as_release_input",
            "tests/m1-perception/test_structure_drift_end_to_end.py::test_actual_drift_child_defers_on_real_sqlite_writer_contention",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_claim_reclaims_expired_worker_lease_and_unwedges_active_index",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_old_worker_cannot_mutate_after_action_lease_reclaim",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_atomic_rollback_keeps_business_and_action_running",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_schedule_is_idempotent_and_conflicting_replay_fails_closed",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_active_target_race_persists_one_stale_noop",
            "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_concurrent_exact_schedule_replay_is_atomic",
            "-q",
        ],
    }

    flattened = [argument for command in commands.values() for argument in command]
    assert commands["planning"] == ["make", "planning-status"]
    pytest_nodes = [argument for argument in flattened if "::test_" in argument]
    assert len(pytest_nodes) >= 30
    assert all(
        argument.rsplit("::", maxsplit=1)[-1].startswith("test_") for argument in pytest_nodes
    )
    assert all(
        any("::test_" in argument for argument in commands[gate])
        for gate in ("unit", "integration", "cli", "restart")
    )
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in (
                "flyctl",
                "deploy",
                "http://",
                "https://",
                "production",
                "dsn",
                "migrate",
            )
        )
    }


def test_fenced_deadline_reconciler_gate_nodes_collect_nonzero() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "fenced-deadline-reconciler"})

    for gate in ("unit", "integration", "cli", "restart"):
        command = [argument for argument in commands[gate] if argument != "-q"]
        command.extend(("--collect-only", "-q"))
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert completed.returncode == 0, (
            f"{gate} collection failed:\n{completed.stdout}\n{completed.stderr}"
        )
        assert "collected 0 items" not in completed.stdout


def test_rolling_qualification_certificates_profile_uses_exact_local_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "rolling-qualification-certificates"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_qualification.py::test_contained_retry_keeps_epoch_accumulating",
            "tests/m1-perception/test_control_plane_qualification.py::test_integrity_or_expired_lease_invalidates_exact_epoch",
            "tests/m1-perception/test_control_plane_qualification.py::test_recovery_confirmation_opens_new_epoch_automatically",
            "tests/m1-perception/test_control_plane_qualification.py::test_recovery_confirmation_identity_drift_fails_closed[policy_version-policy-b]",
            "tests/m1-perception/test_control_plane_qualification.py::test_recovery_confirmation_identity_drift_fails_closed[release_id-release-b]",
            "tests/m1-perception/test_control_plane_qualification.py::test_recovery_confirmation_identity_drift_fails_closed[config_id-config-b]",
            "tests/m1-perception/test_control_plane_qualification.py::test_recovery_confirmation_identity_drift_fails_closed[role_identity-value3]",
            "tests/m1-perception/test_control_plane_qualification.py::test_exact_24_hour_boundary_qualifies_only_with_coverage",
            "tests/m1-perception/test_control_plane_qualification.py::test_gap_equal_to_limit_is_allowed_but_gap_over_limit_breaks",
            "tests/m1-perception/test_control_plane_qualification.py::test_breaking_reason_matrix_invalidates[evidence.gap]",
            "tests/m1-perception/test_control_plane_qualification.py::test_breaking_reason_matrix_invalidates[freshness.structure]",
            "tests/m1-perception/test_control_plane_qualification.py::test_breaking_reason_matrix_invalidates[integrity.conflict]",
            "tests/m1-perception/test_control_plane_qualification.py::test_breaking_reason_matrix_invalidates[lease.expired]",
            "tests/m1-perception/test_control_plane_qualification.py::test_unresolved_p1_and_three_freshness_classes_break",
            "tests/m1-perception/test_control_plane_qualification.py::test_contained_process_replacement_must_finish_within_slo",
            "tests/m1-perception/test_control_plane_qualification.py::test_duplicate_fact_is_idempotent_and_conflict_fails_closed",
            "tests/m1-perception/test_control_plane_qualification.py::test_terminal_epochs_replay_exact_fact_but_reject_new_mutation",
            "tests/m1-perception/test_control_plane_qualification.py::test_decision_rejects_impossible_four_state_combinations",
            "tests/m1-perception/test_control_plane_qualification.py::test_invalid_fact_values_are_rejected[fact_kwargs0]",
            "tests/m1-perception/test_control_plane_qualification.py::test_invalid_fact_values_are_rejected[fact_kwargs1]",
            "tests/m1-perception/test_control_plane_qualification.py::test_invalid_fact_values_are_rejected[fact_kwargs2]",
            "tests/m1-perception/test_control_plane_qualification.py::test_out_of_order_fact_is_rejected",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_virtual_26h_recovery_replay_seals_one_reproducible_certificate",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_qualified_without_certificate_is_sealed_on_next_tick",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/alembic/test_024.py::test_024_chains_after_023_and_declares_qualification_tables",
            "tests/alembic/test_024.py::test_024_schema_declares_state_version_cas_and_certificate_uniqueness",
            "tests/alembic/test_024.py::test_024_upgrades_from_023_downgrades_and_reupgrades_with_append_only_trigger",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_epoch_transition_is_state_version_cas_and_rolls_back_old_writers",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_service_first_tick_initializes_sql_null_cursor",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_ingress_late_runtime_commit_is_consumed_after_cursor",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_recovery_restart_keeps_epoch_fact_history_local",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_same_batch_recovery_keeps_recovering_epoch_empty",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_recovering_observes_second_breaker_status_and_restart",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_freshness_reobserves_same_pointer_and_invalidates_on_aging",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_certificate_is_canonical_idempotent_and_conflict_loud",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_certificate_api_rejects_forged_payload_and_bad_decision_types",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_certificate_db_rejects_direct_forgery_and_app_role_insert",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_certificate_function_privileges_and_derived_ids",
            "tests/m1-perception/test_control_plane_postgres.py::test_read_qualification_certificate_recomputes_canonical_digest_and_fails_on_tamper",
            "tests/m1-perception/test_control_plane_postgres.py::test_read_qualification_certificate_rejects_tampered_ids",
            "-q",
        ],
        "cli": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_cli.py::test_qualification_status_uses_scoped_dsn_and_is_read_only",
            "tests/m1-perception/test_control_plane_cli.py::test_qualification_certificates_reverify_read_only_limit",
            "tests/m1-perception/test_control_plane_cli.py::test_qualification_serve_requires_enable_before_connect",
            "tests/m1-perception/test_control_plane_cli.py::test_qualification_serve_stops_on_tick_error_without_overlap",
            "tests/m1-perception/test_makefile_contract.py::test_make_qualification_read_targets_execute_fake_uv_only[make_args0-expected_argv0]",
            "tests/m1-perception/test_makefile_contract.py::test_make_qualification_read_targets_execute_fake_uv_only[make_args1-expected_argv1]",
            "tests/m1-perception/test_makefile_contract.py::test_make_qualification_read_targets_execute_fake_uv_only[make_args2-expected_argv2]",
            "tests/m1-perception/test_makefile_contract.py::test_make_qualification_serve_requires_enable_before_cli",
            "tests/m1-perception/test_makefile_contract.py::test_make_qualification_serve_executes_fake_uv_after_enable_guard[make_args0-30]",
            "tests/m1-perception/test_makefile_contract.py::test_make_qualification_serve_executes_fake_uv_after_enable_guard[make_args1-5]",
            "-q",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_tick_cursor_is_total_ordered_and_crash_replay_is_exact",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_virtual_26h_recovery_replay_seals_one_reproducible_certificate",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_recovering_nonconfirmation_facts_are_observed_without_entering_epoch",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_qualified_without_certificate_is_sealed_on_next_tick",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_recovery_restart_keeps_epoch_fact_history_local",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_recovering_observes_second_breaker_status_and_restart",
            "tests/alembic/test_024.py::test_024_upgrades_from_023_downgrades_and_reupgrades_with_append_only_trigger",
            "-q",
        ],
    }

    flattened = [argument for command in commands.values() for argument in command]
    for gate in ("unit", "integration", "cli", "restart"):
        assert all(
            argument in {"uv", "run", "pytest", "-q"} or "::test_" in argument
            for argument in commands[gate]
        )
    assert all(
        any("::test_" in argument for argument in commands[gate])
        for gate in ("unit", "integration", "cli", "restart")
    )
    mutation_argv = [
        argument.lower()
        for argument in flattened
        if "::test_" not in argument
        and any(
            forbidden in argument.lower()
            for forbidden in (
                "flyctl",
                "deploy",
                "http://",
                "https://",
                "production",
                "dsn",
                "fly",
                "r2",
                "machine",
                "migrate",
            )
        )
    ]
    assert mutation_argv == []


def test_rolling_qualification_certificates_gate_nodes_collect_nonzero() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "rolling-qualification-certificates"})

    for gate in ("unit", "integration", "cli", "restart"):
        command = [argument for argument in commands[gate] if argument != "-q"]
        command.extend(("--collect-only", "-q"))
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert completed.returncode == 0, (
            f"{gate} collection failed:\n{completed.stdout}\n{completed.stderr}"
        )
        assert "collected 0 items" not in completed.stdout


def test_bounded_operator_truth_surfaces_profile_uses_exact_local_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "bounded-operator-truth-surfaces"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_dashboard_contract.py::test_control_plane_decoder_exports_strict_operator_types",
            "tests/m1-perception/test_control_plane_dashboard_contract.py::test_control_plane_decoder_rejects_malformed_operator_facts",
            "tests/m1-perception/test_control_plane_postgres.py::test_runtime_read_model_rejects_unknown_review_vocab_from_postgres",
            "tests/m1-perception/test_control_plane_postgres.py::test_runtime_read_model_rejects_unknown_active_task_registry_values",
            "tests/m1-perception/test_control_plane_qualification_service.py::test_qualification_status_never_transfers_unbounded_epoch_evidence",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_postgres.py::test_runtime_event_writer_concurrent_first_detected_records_one_event_and_two_outbox",
            "tests/m1-perception/test_control_plane_postgres.py::test_incident_event_and_alert_outbox_are_one_idempotent_transaction",
            "tests/m1-perception/test_control_plane_postgres.py::test_retryable_finish_creates_one_durable_incident_and_alert_intent",
            "tests/m1-perception/test_control_plane_postgres.py::test_operational_snapshot_reads_fenced_work_and_alert_intent",
            "tests/m1-perception/test_control_plane_postgres.py::test_runtime_read_model_projects_self_healing_state_bounded_and_read_only",
            "tests/m1-perception/test_control_plane_postgres.py::test_qualification_read_model_uses_persisted_coverage_not_wall_clock",
            "-q",
        ],
        "cli": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_http.py::test_control_plane_route_returns_durable_operator_snapshot",
            "tests/m1-perception/test_control_plane_http.py::test_control_plane_route_never_reports_missing_dependency_as_empty",
            "tests/m1-perception/test_control_plane_http.py::test_control_plane_route_redacts_malformed_read_model_failures",
            "tests/m1-perception/test_makefile_contract.py::test_make_help_lists_control_plane_dashboard_smoke",
            "tests/m1-perception/test_makefile_contract.py::test_smoke_control_plane_dashboard_requires_authenticated_input_before_curl",
            "tests/m1-perception/test_makefile_contract.py::test_smoke_control_plane_dashboard_rejects_auth_or_empty_200_page",
            "tests/m1-perception/test_makefile_contract.py::test_smoke_control_plane_dashboard_accepts_authenticated_operator_body",
            "-q",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_runtime_event_writer.py::test_runtime_transition_writer_suppresses_restart_duplicate_from_open_incident",
            "tests/m1-perception/test_runtime_event_writer.py::test_runtime_transition_writer_returns_escalated_payload_after_durable_reminder_gaps",
            "tests/m1-perception/test_runtime_event_writer.py::test_runtime_transition_writer_records_recovered_once_and_suppresses_replay",
            "tests/m1-perception/test_runtime_event_writer.py::test_runtime_transition_writer_rejects_stale_recovered_before_latest_detected",
            "tests/m1-perception/test_runtime_event_writer.py::test_runtime_transition_writer_rejects_stale_detected_before_latest_recovered",
            "tests/m1-perception/test_runtime_event_writer.py::test_runtime_transition_writer_uses_recovery_started_as_ordering_not_reminder_cursor",
            "tests/m1-perception/test_control_plane_watchdog.py::test_runtime_transition_watchdog_persists_normalized_payload_before_telegram",
            "tests/m1-perception/test_control_plane_watchdog.py::test_runtime_transition_watchdog_uses_persisted_duplicate_result_to_skip_delivery",
            "tests/m1-perception/test_control_plane_watchdog.py::test_runtime_transition_watchdog_delegates_reminder_timing_to_writer",
            "-q",
        ],
    }

    flattened = [argument for command in commands.values() for argument in command]
    assert commands["planning"] == ["make", "planning-status"]
    assert all(
        any("::test_" in argument for argument in commands[gate])
        for gate in ("unit", "integration", "cli", "restart")
    )
    assert all(
        argument.rsplit("::", maxsplit=1)[-1].startswith("test_")
        for argument in flattened
        if "::test_" in argument
    )
    forbidden_argv = [
        argument.lower()
        for argument in flattened
        if "::test_" not in argument
        and any(
            forbidden in argument.lower()
            for forbidden in (
                "flyctl",
                "deploy",
                "migrate",
                "http://",
                "https://",
                "production",
                "dsn",
                "r2",
                "machine",
            )
        )
    ]
    assert forbidden_argv == []


def test_bounded_operator_truth_surfaces_gate_nodes_collect_nonzero() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "bounded-operator-truth-surfaces"})

    for gate in ("unit", "integration", "cli", "restart"):
        command = [argument for argument in commands[gate] if argument != "-q"]
        command.extend(("--collect-only", "-q"))
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert completed.returncode == 0, (
            f"{gate} collection failed:\n{completed.stdout}\n{completed.stderr}"
        )
        assert "collected 0 items" not in completed.stdout


def test_deterministic_runtime_production_enablement_profile_uses_exact_local_gates() -> None:
    commands = eval_local.gate_commands_for(
        {"paradigm": "deterministic-runtime-production-enablement"}
    )

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_fly_recovery.py",
            "tests/m1-perception/test_control_plane_fly_topology_audit.py",
            "tests/m1-perception/test_control_plane_deployment_templates.py::test_runtime_controller_template_is_private_observe_only_recovery_topology",
            "tests/m1-perception/test_control_plane_deployment_templates.py::test_qualification_worker_template_has_only_scoped_database_and_no_recovery_authority",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/alembic/test_026.py",
            "tests/m1-perception/test_control_plane_db_role_contract.py",
            "tests/m1-perception/test_control_plane_db_role_admin.py",
            "tests/m1-perception/test_control_plane_qualification_identity.py",
            "tests/m1-perception/test_control_plane_runtime_fault_matrix.py::test_runtime_fault_matrix_is_canonical_ordered_and_cleans_temp_database",
            "tests/m1-perception/test_control_plane_runtime_fault_matrix.py::test_runtime_fault_matrix_exercises_real_migrated_authority_paths",
            "-q",
        ],
        "cli": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_observe_verify_derives_exact_current_identity_and_never_mutates",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_observe_only_records_every_candidate_without_recovery_mutation",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_observe_only_records_idle_without_executor",
            "tests/m1-perception/test_makefile_contract.py::test_make_runtime_observe_verify_is_read_only_and_bounded",
            "tests/m1-perception/test_makefile_contract.py::test_make_render_rollout_exposes_exact_six_app_topology",
            "tests/m1-perception/test_makefile_contract.py::test_make_fly_topology_audit_exposes_exact_read_only_argv",
            "-q",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_runtime_observe.py::test_insert_rejects_stale_controller_identity_or_conflicting_idempotency",
            "tests/m1-perception/test_control_plane_runtime_observe.py::test_verifier_fails_on_gap_recovery_mutation_mixed_identity_or_replay_mismatch",
            "tests/m1-perception/test_control_plane_runtime_observe.py::test_real_postgres_records_idempotent_idle_window_and_verifies_read_only",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_store_conflicts_fail_loud",
            "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_serve_store_conflicts_exit_current_turn",
            "-q",
        ],
        "scoped-runtime-controller": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_db_role_contract.py",
            "tests/m1-perception/test_control_plane_db_role_admin.py",
            "tests/m1-perception/test_control_plane_runtime_fault_matrix.py",
            "-q",
        ],
        "scoped-qualification-worker": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_db_role_contract.py",
            "tests/m1-perception/test_control_plane_db_role_admin.py",
            "tests/m1-perception/test_control_plane_qualification_identity.py",
            "tests/m1-perception/test_control_plane_runtime_fault_matrix.py",
            "-q",
        ],
        "zero-recovery-actions": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_runtime_fault_matrix.py::test_runtime_fault_matrix_is_canonical_ordered_and_cleans_temp_database",
            "tests/m1-perception/test_control_plane_runtime_observe.py::test_real_postgres_records_idempotent_idle_window_and_verifies_read_only",
            "-q",
        ],
        "qualification-identity-digest": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_control_plane_qualification_identity.py",
            "tests/m1-perception/test_control_plane_runtime_fault_matrix.py::test_runtime_fault_matrix_is_canonical_ordered_and_cleans_temp_database",
            "-q",
        ],
    }

    flattened = [argument for command in commands.values() for argument in command]
    for output_node in (
        "scoped-runtime-controller",
        "scoped-qualification-worker",
        "zero-recovery-actions",
        "qualification-identity-digest",
    ):
        assert output_node in commands
    for required_file in (
        "tests/alembic/test_026.py",
        "tests/m1-perception/test_control_plane_db_role_contract.py",
        "tests/m1-perception/test_control_plane_db_role_admin.py",
        "tests/m1-perception/test_control_plane_fly_topology_audit.py",
        "tests/m1-perception/test_control_plane_qualification_identity.py",
        "tests/m1-perception/test_control_plane_runtime_fault_matrix.py",
    ):
        assert any(argument.startswith(required_file) for argument in flattened)
    assert all(
        any("::test_" in argument for argument in commands[gate])
        for gate in ("integration", "cli", "restart")
    )
    assert not any(
        forbidden in argument.lower()
        for argument in flattened
        if "::test_" not in argument
        for forbidden in ("flyctl", "deploy", "production", "http://", "https://")
    )


def test_deterministic_runtime_production_enablement_gate_nodes_collect_nonzero() -> None:
    commands = eval_local.gate_commands_for(
        {"paradigm": "deterministic-runtime-production-enablement"}
    )

    for gate in (
        "unit",
        "integration",
        "cli",
        "restart",
        "scoped-runtime-controller",
        "scoped-qualification-worker",
        "zero-recovery-actions",
        "qualification-identity-digest",
    ):
        command = [argument for argument in commands[gate] if argument != "-q"]
        command.extend(("--collect-only", "-q"))
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert completed.returncode == 0, (
            f"{gate} collection failed:\n{completed.stdout}\n{completed.stderr}"
        )
        assert "collected 0 items" not in completed.stdout


def test_unknown_or_missing_paradigm_uses_existing_gate_profile() -> None:
    assert eval_local.gate_commands_for({"paradigm": "repository"}) == GATE_COMMANDS
    assert eval_local.gate_commands_for({"paradigm": "unknown"}) == GATE_COMMANDS
    assert eval_local.gate_commands_for({}) == GATE_COMMANDS


def test_main_selects_gates_from_run_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(json.dumps({"paradigm": "living-doc-contract"}))
    executed: list[list[str]] = []

    def runner(command: list[str]) -> GateResult:
        executed.append(command)
        return GateResult(True, 0, "ok")

    monkeypatch.setattr(eval_local, "run_command", runner)
    monkeypatch.setattr(sys, "argv", ["eval_local.py", str(run_dir)])

    assert eval_local.main() == 0
    assert executed == list(
        eval_local.gate_commands_for({"paradigm": "living-doc-contract"}).values()
    )
    payload = json.loads((run_dir / "local-eval.json").read_text())
    assert payload["total"] == 100.0


def test_main_without_manifest_preserves_legacy_direct_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "legacy-run"
    run_dir.mkdir()
    executed: list[list[str]] = []

    def runner(command: list[str]) -> GateResult:
        executed.append(command)
        return GateResult(True, 0, "ok")

    monkeypatch.setattr(eval_local, "run_command", runner)

    assert eval_local.main([str(run_dir)]) == 0
    assert executed == list(GATE_COMMANDS.values())
    assert (run_dir / "local-eval.json").is_file()


def test_main_reports_malformed_manifest_without_running_gates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run_dir = tmp_path / "bad-run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{not json")
    monkeypatch.setattr(
        eval_local,
        "run_command",
        lambda command: pytest.fail(f"must not run gate: {command}"),
    )

    assert eval_local.main([str(run_dir)]) == 2
    assert "invalid climb manifest" in capsys.readouterr().err
    assert not (run_dir / "local-eval.json").exists()


def test_score_is_mean_of_five_binary_gates() -> None:
    results = {
        "planning": GateResult(True, 0, "ok"),
        "unit": GateResult(True, 0, "ok"),
        "integration": GateResult(False, 1, "failed"),
        "cli": GateResult(True, 0, "ok"),
        "restart": GateResult(False, 1, "failed"),
    }

    payload = build_score(results)

    assert payload["total"] == 60.0
    assert payload["subscores"] == {
        "planning": 100.0,
        "unit": 100.0,
        "integration": 0.0,
        "cli": 100.0,
        "restart": 0.0,
    }
    assert payload["disaster_pattern"] is True


def test_all_green_score_is_100_without_disaster() -> None:
    results = {
        name: GateResult(True, 0, "ok")
        for name in ("planning", "unit", "integration", "cli", "restart")
    }

    payload = build_score(results)

    assert payload["total"] == 100.0
    assert payload["disaster_pattern"] is False


def test_evaluate_gates_records_bounded_command_evidence(tmp_path: Path) -> None:
    commands = {
        "planning": ["fake", "planning"],
        "unit": ["fake", "unit"],
    }

    def runner(command: list[str]) -> GateResult:
        return GateResult(
            passed=command[-1] == "planning",
            returncode=0 if command[-1] == "planning" else 1,
            output="x" * 20_000,
        )

    output_path = tmp_path / "local-eval.json"
    payload = evaluate_gates(commands, runner=runner, output_path=output_path)

    assert payload["subscores"] == {"planning": 100.0, "unit": 0.0}
    assert payload["total"] == 50.0
    assert len(payload["commands"]["planning"]["output"]) == 8_000
    assert json.loads(output_path.read_text()) == payload


def test_run_command_has_no_competing_outer_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def run(*args, **kwargs):
        captured.update(kwargs)
        return subprocess.CompletedProcess(args[0], 0, stdout="passed")

    monkeypatch.setattr(eval_local.subprocess, "run", run)

    result = eval_local.run_command(["pytest", "slow-test"])

    assert result == GateResult(passed=True, returncode=0, output="passed")
    assert "timeout" not in captured


def test_evaluate_gates_checkpoints_and_resumes_exact_run_identity(tmp_path: Path) -> None:
    commands = {
        "planning": ["fake", "planning"],
        "unit": ["fake", "unit"],
    }
    output_path = tmp_path / "local-eval.json"
    checkpoint_path = tmp_path / "local-eval.progress.json"
    first_calls: list[list[str]] = []

    def interrupted_runner(command: list[str]) -> GateResult:
        first_calls.append(command)
        if command[-1] == "unit":
            raise KeyboardInterrupt
        return GateResult(True, 0, "planning passed")

    with pytest.raises(KeyboardInterrupt):
        evaluate_gates(
            commands,
            runner=interrupted_runner,
            output_path=output_path,
            checkpoint_path=checkpoint_path,
            run_identity="commit-a",
        )

    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["status"] == "running"
    assert checkpoint["run_identity"] == "commit-a"
    assert tuple(checkpoint["results"]) == ("planning",)
    assert not output_path.exists()

    resumed_calls: list[list[str]] = []

    def resumed_runner(command: list[str]) -> GateResult:
        resumed_calls.append(command)
        return GateResult(True, 0, "unit passed")

    payload = evaluate_gates(
        commands,
        runner=resumed_runner,
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        run_identity="commit-a",
    )

    assert first_calls == [["fake", "planning"], ["fake", "unit"]]
    assert resumed_calls == [["fake", "unit"]]
    assert payload["total"] == 100.0
    assert not checkpoint_path.exists()


def test_evaluate_gates_rejects_malformed_progress_and_does_not_reuse_stale_identity(
    tmp_path: Path,
) -> None:
    commands = {"planning": ["fake", "planning"]}
    output_path = tmp_path / "local-eval.json"
    checkpoint_path = tmp_path / "local-eval.progress.json"
    checkpoint_path.write_text("{not-json")

    with pytest.raises(ValueError, match="invalid local-eval checkpoint"):
        evaluate_gates(
            commands,
            runner=lambda _command: GateResult(True, 0, "passed"),
            output_path=output_path,
            checkpoint_path=checkpoint_path,
            run_identity="commit-a",
        )

    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "running",
                "run_identity": "stale-commit",
                "commands": commands,
                "results": {"planning": {"passed": True, "returncode": 0, "output": "stale"}},
            }
        )
    )
    calls: list[list[str]] = []

    payload = evaluate_gates(
        commands,
        runner=lambda command: calls.append(command) or GateResult(True, 0, "fresh"),
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        run_identity="commit-a",
    )

    assert calls == [["fake", "planning"]]
    assert payload["commands"]["planning"]["output"] == "fresh"


def test_train_script_is_compatible_with_system_bash(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["CLIMB_ARTIFACT_DIR"] = str(tmp_path)

    completed = subprocess.run(
        ["bash", "tools/climb/train.sh", "H-001"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    run_dir = Path(completed.stdout.strip())
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["hypothesis_id"] == "H-001"
    assert manifest["status"] == "ready-for-eval"
