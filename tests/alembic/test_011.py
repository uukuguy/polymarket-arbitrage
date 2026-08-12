from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/011_m1_structure_source_materialization.py")


def test_011_chains_from_010_and_records_source_window_bundle_receipts() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")

    assert 'revision = "011"' in text
    assert 'down_revision = "010"' in text
    assert '"m1_structure_source_window_bundles"' in text
    for column in (
        "window_key",
        "producer_job_key",
        "source_digest",
        "bundle_key",
        "bundle_digest",
    ):
        assert column in text


def test_011_is_reversible() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    _upgrade, downgrade = text.split("def downgrade() -> None:", maxsplit=1)
    assert 'op.drop_table("m1_structure_source_window_bundles")' in downgrade
