"""Contract for the staged Quote-input capacity cutover."""

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/019_m1_quote_input_artifacts.py")


def test_quote_input_reference_columns_arrive_before_jsonb_removal() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "019"' in text
    assert 'down_revision = "018"' in text
    assert '"input_artifact_key"' in text
    assert '"input_artifact_digest"' in text
    assert '"leg_count"' in text
    assert 'drop_column("m1_quote_batch_inputs", "token_ids")' not in text
    assert 'drop_column("m1_quote_batch_inputs", "legs")' not in text
