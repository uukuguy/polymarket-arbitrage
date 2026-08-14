"""Contract for additive event-rooted Structure source inputs."""

from pathlib import Path

from polyarb.control_plane.models import StructureSourcePageSpec


def test_015_adds_authenticated_market_batch_inputs() -> None:
    text = Path("alembic/versions/015_m1_event_rooted_structure_source.py").read_text()

    assert 'revision = "015"' in text
    assert 'down_revision = "014"' in text
    assert "market_ids_json" in text
    assert "market_ids_digest" in text


def test_market_batch_spec_canonicalizes_and_hashes_ids() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window",
        stream="markets",
        ordinal=3,
        requested_cursor=None,
        market_ids=("market-a", "market-b"),
    )

    assert spec.market_ids == ("market-a", "market-b")
    assert spec.market_ids_digest is not None
    assert spec.input_identity.endswith(spec.market_ids_digest)
