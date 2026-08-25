from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GateResult:
    passed: bool
    returncode: int
    output: str


def build_score(results: Mapping[str, GateResult]) -> dict:
    if not results:
        raise ValueError("at least one gate is required")
    subscores = {name: 100.0 if result.passed else 0.0 for name, result in results.items()}
    total = sum(subscores.values()) / len(subscores)
    return {
        "total": total,
        "subscores": subscores,
        "disaster_pattern": any(score == 0.0 for score in subscores.values()),
    }


def evaluate_gates(
    commands: Mapping[str, list[str]],
    *,
    runner: Callable[[list[str]], GateResult],
    output_path: Path,
) -> dict:
    results = {name: runner(command) for name, command in commands.items()}
    payload = build_score(results)
    payload["commands"] = {
        name: {
            "argv": commands[name],
            "returncode": result.returncode,
            "output": result.output[-8_000:],
        }
        for name, result in results.items()
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


ROOT = Path(__file__).resolve().parents[2]
GATE_TIMEOUT_S = 120
GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_position_repository.py",
        "tests/routing/test_position_tracker.py",
        "-q",
    ],
    "integration": ["uv", "run", "pytest", "tests/execution", "-q"],
    "cli": ["uv", "run", "pytest", "tests/cli", "-q"],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/cli/test_arbitrage_cli_process.py",
        "-q",
    ],
}
LIVING_DOC_CONTRACT_GATE_COMMANDS = {
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
OPPORTUNITY_FEED_CHAIN_TRUTH_GATE_COMMANDS = {
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
OPPORTUNITY_FEED_CADENCE_SLA_GATE_COMMANDS = {
    "planning": ["make", "-n", "collect-neg-risk-quotes"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_neg_risk_quote_store.py",
        "tests/routing/test_neg_risk_quote_collector.py",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_opportunity_scanner.py",
        "tests/m1-perception/test_arbitrage_opportunities_http.py",
        "-q",
    ],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/cli/test_arbitrage_cli_process.py",
        "-k",
        "collect_neg_risk_quotes or scan_quotes",
        "-q",
    ],
    "restart": ["make", "-n", "scan-arb-quotes"],
}
L3_PREREQUISITE_CHAIN_TRUTH_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/alembic/test_006.py",
        "tests/storage/test_supabase_mirror.py",
        "tests/observation/test_l2_temp_db.py",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/observation/test_l2_candidate_refresh.py",
        "tests/m1-perception/test_l3_promoter.py",
        "tests/m1-perception/test_l2_health_l3_subchecks.py",
        "-q",
    ],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_l3_promote_dry_run.py",
        "tests/test_makefile.py",
        "-k",
        "dry_run or l3_seed",
        "-q",
    ],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_candidate_refresh_l3_protection.py",
        "-q",
    ],
}
CHECKPOINTED_STRUCTURE_RECOVERY_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_structure_generation_publication.py",
        "-k",
        "expired_read_budget or preserves_prior_checkpoint or certification_rejects",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_postgres.py",
        "-q",
    ],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_shadow.py",
        "-q",
    ],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_structure_generation_publication.py",
        "-k",
        "expired_read_budget or preserves_prior_checkpoint or certification_rejects",
        "-q",
    ],
}
TRANSACTIONAL_PRODUCTION_PROMOTION_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_postgres.py",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_rollout.py",
        "tests/m1-perception/test_control_plane_shadow.py",
        "-q",
    ],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_cli.py",
        "-k",
        "render_rollout or preflight",
        "-q",
    ],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_structure_generation_publication.py",
        "-k",
        "expired_read_budget or preserves_prior_checkpoint",
        "-q",
    ],
}
BUDGETED_TRANSACTIONAL_CLOUD_INPUT_GATE_COMMANDS = {
    "planning": ["make", "control-plane-egress-preflight"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_transactional_structure_source_worker.py",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_postgres.py",
        "-q",
    ],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_cli.py",
        "tests/m1-perception/test_control_plane_watchdog.py",
        "-q",
    ],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_control_plane_watchdog.py",
        "-q",
    ],
}
EVENT_DRIVEN_RUNTIME_SELF_HEALING_GATE_COMMANDS = {
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
FENCED_DEADLINE_RECONCILER_GATE_COMMANDS = {
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
        "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_once_store_conflicts_fail_loud",
        "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_serve_store_conflicts_exit_current_turn",
        "tests/m1-perception/test_control_plane_cli.py::test_runtime_reconcile_serve_stops_cleanly_on_signal_and_is_sequential",
        "tests/m1-perception/test_makefile_contract.py::test_make_runtime_controller_targets_are_wired",
        "tests/m1-perception/test_makefile_contract.py::test_make_runtime_mutation_target_has_enable_guard",
        "tests/m1-perception/test_makefile_contract.py::test_make_runtime_status_is_read_only_dry_run",
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
        "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_claim_reclaims_expired_worker_lease_and_unwedges_active_index",
        "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_old_worker_cannot_mutate_after_action_lease_reclaim",
        "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_atomic_rollback_keeps_business_and_action_running",
        "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_schedule_is_idempotent_and_conflicting_replay_fails_closed",
        "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_active_target_race_persists_one_stale_noop",
        "tests/m1-perception/test_control_plane_postgres.py::test_recovery_action_concurrent_exact_schedule_replay_is_atomic",
        "-q",
    ],
}
ROLLING_QUALIFICATION_CERTIFICATES_GATE_COMMANDS = {
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


def gate_commands_for(manifest: Mapping[str, object]) -> Mapping[str, list[str]]:
    if manifest.get("paradigm") == "living-doc-contract":
        commands = LIVING_DOC_CONTRACT_GATE_COMMANDS
    elif manifest.get("paradigm") == "opportunity-feed-chain-truth":
        commands = OPPORTUNITY_FEED_CHAIN_TRUTH_GATE_COMMANDS
    elif manifest.get("paradigm") == "opportunity-feed-cadence-sla":
        commands = OPPORTUNITY_FEED_CADENCE_SLA_GATE_COMMANDS
    elif manifest.get("paradigm") == "l3-prerequisite-chain-truth":
        commands = L3_PREREQUISITE_CHAIN_TRUTH_GATE_COMMANDS
    elif manifest.get("paradigm") == "checkpointed-structure-recovery":
        commands = CHECKPOINTED_STRUCTURE_RECOVERY_GATE_COMMANDS
    elif manifest.get("paradigm") == "transactional-production-promotion":
        commands = TRANSACTIONAL_PRODUCTION_PROMOTION_GATE_COMMANDS
    elif manifest.get("paradigm") == "budgeted-transactional-cloud-input":
        commands = BUDGETED_TRANSACTIONAL_CLOUD_INPUT_GATE_COMMANDS
    elif manifest.get("paradigm") == "event-driven-runtime-self-healing":
        commands = EVENT_DRIVEN_RUNTIME_SELF_HEALING_GATE_COMMANDS
    elif manifest.get("paradigm") == "fenced-deadline-reconciler":
        commands = FENCED_DEADLINE_RECONCILER_GATE_COMMANDS
    elif manifest.get("paradigm") == "rolling-qualification-certificates":
        commands = ROLLING_QUALIFICATION_CERTIFICATES_GATE_COMMANDS
    else:
        commands = GATE_COMMANDS
    return {name: list(command) for name, command in commands.items()}


def run_command(command: list[str]) -> GateResult:
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=GATE_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as error:
        output = error.output or ""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        return GateResult(
            passed=False,
            returncode=124,
            output=f"gate timed out after {GATE_TIMEOUT_S}s\n{output}",
        )
    return GateResult(
        passed=completed.returncode == 0,
        returncode=completed.returncode,
        output=completed.stdout,
    )


def load_manifest(run_dir: Path) -> Mapping[str, object]:
    path = run_dir / "manifest.json"
    if not path.exists():
        # Backward compatibility: old/direct evaluator invocations predate
        # manifests and always used the repository gate profile.
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid climb manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid climb manifest {path}: expected a JSON object")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.run_dir)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    output_path = args.run_dir / "local-eval.json"
    payload = evaluate_gates(
        gate_commands_for(manifest),
        runner=run_command,
        output_path=output_path,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not payload["disaster_pattern"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
