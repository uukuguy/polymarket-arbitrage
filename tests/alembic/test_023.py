"""Contracts for the additive runtime recovery action migration."""

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/023_m1_runtime_recovery.py")


def test_023_chains_after_022_and_declares_recovery_tables() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "023"' in text
    assert 'down_revision = "022"' in text
    assert '"m1_runtime_controller_leases"' in text
    assert '"m1_recovery_actions"' in text
    assert '"m1_incidents.incident_key"' in text
    assert '"m1_job_attempts.attempt_id"' in text


def test_023_recovery_actions_are_fenced_bounded_and_single_active_per_target() -> None:
    text = MIGRATION_PATH.read_text()

    assert "expected_controller_epoch" in text
    assert "expected_attempt_id" in text
    assert "expected_lease_epoch" in text
    assert "idempotency_key" in text
    assert "pg_column_size(detail) <= 4096" in text
    assert "ck_m1_recovery_actions_state" in text
    assert "ck_m1_recovery_actions_result_code" in text
    assert "CREATE UNIQUE INDEX uq_m1_recovery_action_active_target" in text
    assert "ON m1_recovery_actions(target_type, target_id)" in text
    assert "WHERE state IN ('pending', 'running')" in text


def test_023_controller_lease_epoch_is_singleton_and_monotonic() -> None:
    text = MIGRATION_PATH.read_text()

    assert '"controller_id"' in text
    assert '"lease_epoch"' in text
    assert "lease_epoch > 0" in text
    assert "uq_m1_runtime_controller_leases_owner_epoch" in text
