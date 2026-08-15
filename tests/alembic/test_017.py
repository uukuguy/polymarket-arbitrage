"""Contract for cloud-resident immutable transactional soak evidence."""

from __future__ import annotations

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/017_m1_cloud_soak_evidence.py")


def test_cloud_soak_ledger_locks_runs_and_rejects_row_mutation() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "017"' in text
    assert 'down_revision = "016"' in text
    assert '"m1_soak_runs"' in text
    assert '"m1_soak_observations"' in text
    assert '"run_id"' in text
    assert '"machine_ids"' in text
    assert '"baseline_record"' in text
    assert '"snapshot_sha256"' in text
    assert "m1_reject_soak_evidence_mutation" in text
    assert "BEFORE UPDATE OR DELETE" in text
