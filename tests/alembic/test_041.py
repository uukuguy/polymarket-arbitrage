"""Contracts for bounded qualification ingress projection."""

from pathlib import Path


MIGRATION = Path("alembic/versions/041_m1_bounded_qualification_ingress.py")


def test_revision_041_filters_only_noisy_normal_runtime_lifecycle_events() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "041"' in text
    assert 'down_revision = "040"' in text
    assert "m1_project_runtime_qualification_ingress" in text
    assert "structure-fetch" in text
    assert "structure-normalize" in text
    assert "structure-materialize" in text
    assert "quote-batch" in text
    assert "job.stage-changed" in text
    assert "job.retryable-failed" not in text
    assert "job.terminal-failed" not in text
    assert "m1_record_qualification_ingress" in text
