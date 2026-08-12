from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/010_m1_transactional_structure_source.py")


def test_010_chains_from_009_and_creates_source_window_authority() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")

    assert 'revision = "010"' in text
    assert 'down_revision = "009"' in text
    for table in (
        "m1_structure_source_windows",
        "m1_structure_source_page_inputs",
        "m1_structure_source_page_receipts",
    ):
        assert f'"{table}"' in text
    for column in (
        "window_key",
        "stream",
        "ordinal",
        "requested_cursor",
        "next_cursor",
        "artifact_digest",
    ):
        assert column in text


def test_010_is_additive_and_reversible() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    upgrade, downgrade = text.split("def downgrade() -> None:", maxsplit=1)

    assert "m1_structure_source_page_inputs" in upgrade
    assert "m1_structure_source_page_receipts" in upgrade
    assert "m1_structure_source_windows" in upgrade
    for table in (
        "m1_structure_source_page_receipts",
        "m1_structure_source_page_inputs",
        "m1_structure_source_windows",
    ):
        assert f'op.drop_table("{table}")' in downgrade
