from __future__ import annotations

from dataclasses import replace

import pytest

from polyarb.control_plane.models import StructureSourcePageSpec
from polyarb.control_plane.structure_artifact import parse_structure_bundle_bytes
from polyarb.control_plane.structure_source import (
    StructureSourceError,
    StructureSourcePageArtifact,
    materialize_event_page_shards,
    materialize_event_records_components,
    materialize_sharded_source_manifest,
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


def test_event_embedded_market_evidence_materializes_without_a_second_market_stream() -> None:
    event = _page(
        stream="events",
        ordinal=0,
        records=(
            {
                "id": "event-a",
                "slug": "event-a",
                "active": True,
                "closed": False,
                "negRisk": True,
                "enableNegRisk": True,
                "negRiskAugmented": False,
                "negRiskMarketID": "group-a",
                "markets": [
                    {
                        "id": "market-a",
                        "conditionId": "condition-a",
                        "slug": "market-a",
                        "question": "Question?",
                        "clobTokenIds": '["yes-a", "no-a"]',
                        "outcomePrices": '["0.4", "0.6"]',
                        "active": True,
                        "closed": False,
                        "negRisk": True,
                        "negRiskOther": False,
                    }
                ],
            },
        ),
        completed=True,
        next_cursor=None,
    )

    bundle = materialize_structure_source_pages((event,))

    identity, components = parse_structure_bundle_bytes(
        bundle.payload, expected_sha256=bundle.sha256
    )
    assert identity.source_kind == "gamma-source-window-events-v2"
    assert identity.component_counts["markets"] == 1
    assert components["markets"] == (
        {
            "market_id": "market-a",
            "condition_id": "condition-a",
            "slug": "market-a",
            "question": "Question?",
            "yes_token_id": "yes-a",
            "no_token_id": "no-a",
            "mid_price": 0.4,
            "liquidity_usd": None,
            "volume_usd": None,
            "best_bid_price": None,
            "best_bid_size": None,
            "best_ask_price": None,
            "best_ask_size": None,
            "end_time_ms": None,
            "active": True,
            "closed": False,
            "neg_risk": True,
            "neg_risk_market_id": "group-a",
            "fetched_at_ms": None,
            "page_fetched_at_ms": None,
            "incomplete": False,
            "event_id": "event-a",
        },
    )


def test_event_embedded_source_excludes_closed_children_from_active_market_view() -> None:
    """Event pages can retain a closed child after its enclosing event stays open."""
    event = _page(
        stream="events",
        ordinal=0,
        records=(
            {
                "id": "event-a",
                "slug": "event-a",
                "active": True,
                "closed": False,
                "negRisk": False,
                "markets": [
                    {
                        "id": "closed-market",
                        "conditionId": "closed-condition",
                        "clobTokenIds": '[]',
                        "outcomePrices": '[]',
                        "active": True,
                        "closed": True,
                    },
                    {
                        "id": "open-market",
                        "conditionId": "open-condition",
                        "clobTokenIds": '["yes", "no"]',
                        "outcomePrices": '["0.4", "0.6"]',
                        "active": True,
                        "closed": False,
                    },
                ],
            },
        ),
        completed=True,
        next_cursor=None,
    )

    bundle = materialize_structure_source_pages((event,))
    _identity, components = parse_structure_bundle_bytes(
        bundle.payload, expected_sha256=bundle.sha256
    )

    assert [row["market_id"] for row in components["markets"]] == ["open-market"]


def test_one_event_page_normalizes_to_independent_shard_components() -> None:
    components = materialize_event_records_components(
        (
            {
                "id": "event-a",
                "slug": "event-a",
                "active": True,
                "closed": False,
                "negRisk": False,
                "markets": [
                    {
                        "id": "market-a",
                        "conditionId": "condition-a",
                        "clobTokenIds": '["yes-a", "no-a"]',
                        "outcomePrices": '["0.4", "0.6"]',
                        "active": True,
                        "closed": False,
                    }
                ],
            },
        )
    )

    assert components["events"][0]["id"] == "event-a"
    assert components["markets"][0]["market_id"] == "market-a"
    assert components["markets"][0]["neg_risk"] is False


def test_event_page_quarantines_standard_neg_risk_children_without_group_identity() -> None:
    """Do not publish a Gamma neg-risk claim when its parent has no group proof."""
    components = materialize_event_records_components(
        (
            {
                "id": "event-group-less",
                "slug": "event-group-less",
                "active": True,
                "closed": False,
                "negRisk": True,
                "enableNegRisk": True,
                "negRiskAugmented": False,
                "negRiskMarketID": None,
                "markets": [
                    {
                        "id": "market-group-less",
                        "conditionId": "condition-group-less",
                        "clobTokenIds": '["yes", "no"]',
                        "outcomePrices": '["0.4", "0.6"]',
                        "active": True,
                        "closed": False,
                        "negRisk": True,
                        "negRiskMarketID": None,
                    }
                ],
            },
        )
    )

    assert components["markets"] == ()
    assert components["memberships"] == ()
    assert components["group_truth"] == ()


def test_one_sealed_event_page_becomes_component_shards() -> None:
    page = _page(
        stream="events",
        ordinal=0,
        records=(
            {
                "id": "event-a",
                "slug": "event-a",
                "active": True,
                "closed": False,
                "negRisk": False,
                "markets": [
                    {
                        "id": "market-a",
                        "conditionId": "condition-a",
                        "clobTokenIds": '["yes-a", "no-a"]',
                        "outcomePrices": '["0.4", "0.6"]',
                        "active": True,
                        "closed": False,
                    }
                ],
            },
        ),
        completed=True,
        next_cursor=None,
    )

    shards = materialize_event_page_shards(page, source_digest="a" * 64)

    assert [(component, artifact.key) for component, artifact in shards] == [
        ("events", shards[0][1].key),
        ("markets", shards[1][1].key),
    ]


def test_authenticated_batch_receipts_become_a_v3_manifest() -> None:
    from polyarb.control_plane.structure_artifact import (
        StructureShardBatchArtifact,
        StructureShardReceipt,
        canonical_structure_shard_batch_bytes,
    )

    shard = StructureShardReceipt(
        component="events",
        ordinal=0,
        artifact_key="structure-shards/a/rows.ndjson",
        artifact_digest="a" * 64,
        row_count=1,
    )
    batch = StructureShardBatchArtifact.from_bytes(
        canonical_structure_shard_batch_bytes(
            window_key="source-window:bundle",
            source_digest="b" * 64,
            start_ordinal=0,
            end_ordinal=1,
            shards=(shard,),
        )
    )
    identity, manifest, ranges = materialize_sharded_source_manifest(
        window_key="source-window:bundle",
        source_digest="b" * 64,
        expected_page_count=1,
        batches=(("shard-batch:00000000", batch.sha256, batch.key),),
        read_batch=lambda key: batch.payload if key == batch.key else b"",
    )

    assert identity.source_kind == "gamma-source-window-events-v3-sharded"
    assert identity.component_counts["events"] == 1
    assert manifest.key.startswith("structure-bundles/")
    assert ranges == (("events", "shard:00000000", "shard:00000001"),)


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
