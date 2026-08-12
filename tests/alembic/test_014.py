from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/014_m1_transactional_circuits.py")


def test_014_adds_job_scoped_circuit_state() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")

    assert 'revision = "014"' in text
    assert 'down_revision = "013"' in text
    assert '"m1_job_circuits"' in text
    for column in ("job_key", "consecutive_failures", "next_probe_at", "opened_at"):
        assert column in text


def test_014_is_reversible() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    _upgrade, downgrade = text.split("def downgrade() -> None:", maxsplit=1)

    assert 'op.drop_table("m1_job_circuits")' in downgrade
