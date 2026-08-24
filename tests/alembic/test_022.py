"""Contracts for the additive runtime evidence migration."""

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/022_m1_job_runtime_evidence.py")


def test_022_chains_after_021_and_declares_runtime_tables() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "022"' in text
    assert 'down_revision = "021"' in text
    assert '"m1_job_runtime_state"' in text
    assert '"m1_job_runtime_events"' in text
    assert '"m1_jobs.job_key"' in text
    assert '"m1_job_attempts.attempt_id"' in text


def test_022_runtime_events_are_append_only_and_bounded() -> None:
    text = MIGRATION_PATH.read_text()

    assert "m1_reject_runtime_event_mutation" in text
    assert "m1_runtime_events_immutable" in text
    assert "BEFORE UPDATE OR DELETE" in text
    assert "event_sequence" in text
    assert "idempotency_key" in text
    assert "pg_column_size(detail) <= 4096" in text
