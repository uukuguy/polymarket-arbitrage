"""Contract for the formal certified opportunity projection migration."""

from pathlib import Path


MIGRATION_PATH = Path("alembic/versions/016_m1_transactional_opportunity_projection.py")


def test_opportunity_projection_is_versioned_and_has_an_atomic_current_pointer() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "016"' in text
    assert 'down_revision = "015"' in text
    assert '"m1_opportunity_projections"' in text
    assert '"m1_opportunity_projection_rows"' in text
    assert '"m1_opportunity_publication_pointers"' in text
    assert '"m1_generation_manifests.generation_key"' in text
    assert '"m1_opportunity_projections.generation_key"' in text
    assert '"gross_edge_bps > 0"' in text
