from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/012_m1_transactional_quote_admission.py")


def test_012_chains_from_011_and_records_immutable_quote_admission_input() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")

    assert 'revision = "012"' in text
    assert 'down_revision = "011"' in text
    assert '"m1_quote_admission_inputs"' in text
    for column in ("job_key", "generation_key", "bundle_key", "bundle_digest"):
        assert column in text


def test_012_is_reversible() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    _upgrade, downgrade = text.split("def downgrade() -> None:", maxsplit=1)
    assert 'op.drop_table("m1_quote_admission_inputs")' in downgrade
