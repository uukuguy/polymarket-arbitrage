"""Contract for compact Quote inputs before legacy JSONB removal."""

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/020_m1_quote_input_null_legacy_payloads.py")


def test_020_allows_compact_quote_inputs_without_jsonb_payloads() -> None:
    text = MIGRATION_PATH.read_text()

    assert '"token_ids"' in text
    assert "nullable=True" in text
    assert "020" in text
