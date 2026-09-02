"""Contract for bounded Structure Market Intelligence projection schema."""

from pathlib import Path


MIGRATION = Path("alembic/versions/046_m1_structure_intelligence.py")


def test_revision_046_declares_bounded_structure_intelligence_relations() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "046"' in text
    assert 'down_revision = "045"' in text
    assert "m1_structure_intelligence_events" in text
    assert "m1_structure_intelligence_groups" in text
    assert "m1_structure_intelligence_summaries" in text
    assert "payload_octets" in text
