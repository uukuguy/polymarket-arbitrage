from __future__ import annotations

from dataclasses import replace

import pytest

from polyarb.control_plane.models import StructureSourcePageSpec
from polyarb.control_plane.structure_artifact import parse_structure_bundle_bytes
from polyarb.control_plane.structure_source import (
    StructureSourceError,
    StructureSourcePageArtifact,
    materialize_structure_source_pages,
)


def _page(
    *,
    stream: str,
    ordinal: int,
    records: tuple[dict[str, object], ...],
    completed: bool,
    next_cursor: str | None,
) -> tuple[StructureSourcePageSpec, StructureSourcePageArtifact]:
    spec = StructureSourcePageSpec(
        window_key="source-window:bundle",
        stream=stream,
        ordinal=ordinal,
        requested_cursor=None if ordinal == 0 else f"{stream}-cursor-{ordinal}",
    )
    return (
        spec,
        StructureSourcePageArtifact.from_page(
            spec=spec,
            records=records,
            next_cursor=next_cursor,
            completed=completed,
            started_at_ms=ordinal * 10,
            finished_at_ms=ordinal * 10 + 1,
        ),
    )


def test_sealed_source_pages_materialize_current_structure_bundle_without_sqlite() -> None:
    event = _page(
        stream="events",
        ordinal=0,
        records=(
            {
                "id": "event-a",
                "slug": "event-a",
                "active": True,
                "closed": False,
                "markets": [
                    {
                        "id": "market-a",
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                ],
            },
        ),
        completed=True,
        next_cursor=None,
    )
    market = _page(
        stream="markets",
        ordinal=0,
        records=(
            {
                "id": "market-a",
                "conditionId": "condition-a",
                "slug": "market-a",
                "question": "Question?",
                "clobTokenIds": '["yes-a", "no-a"]',
                "outcomePrices": '["0.4", "0.6"]',
                "active": True,
                "closed": False,
                "negRisk": False,
            },
        ),
        completed=True,
        next_cursor=None,
    )

    bundle = materialize_structure_source_pages((event, market))

    identity, components = parse_structure_bundle_bytes(
        bundle.payload, expected_sha256=bundle.sha256
    )
    assert identity.source_kind == "gamma-source-window-v1"
    assert identity.window_id == "source-window:bundle"
    assert identity.component_counts == {
        "events": 1,
        "event_tags": 0,
        "memberships": 0,
        "group_truth": 0,
        "markets": 1,
        "issues": 0,
    }
    assert components["markets"][0]["event_id"] == "event-a"


def test_materializer_rejects_missing_or_tampered_source_page_before_bundle_exists() -> None:
    event = _page(
        stream="events",
        ordinal=1,
        records=(),
        completed=True,
        next_cursor=None,
    )
    market = _page(
        stream="markets",
        ordinal=0,
        records=(),
        completed=True,
        next_cursor=None,
    )

    with pytest.raises(StructureSourceError, match="source page ordinal gap"):
        materialize_structure_source_pages((event, market))

    valid_event = _page(
        stream="events",
        ordinal=0,
        records=(),
        completed=True,
        next_cursor=None,
    )
    tampered_market = (market[0], replace(market[1], payload=market[1].payload + b"tampered"))
    with pytest.raises(StructureSourceError, match="digest-mismatch"):
        materialize_structure_source_pages((valid_event, tampered_market))
