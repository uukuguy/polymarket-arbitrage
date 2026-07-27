"""Focused observer-only top-of-book collection contracts."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import pytest

from polyarb.routing.focused_quote_collector import (
    ActiveOpportunity,
    SqliteStructureMembershipReader,
    StructureGroup,
    StructureLeg,
    collect_focused_observation,
)
from polyarb.storage.sqlite_store import SQLiteStore


@dataclass
class FakeReader:
    books: object

    def __post_init__(self) -> None:
        self.requests: list[list[str]] = []
        self.projections: list[str] = []

    async def get_books(self, token_ids: list[str], *, projection: str = "full") -> object:
        self.requests.append(token_ids)
        self.projections.append(projection)
        return self.books


@dataclass
class FakeMembershipReader:
    group: StructureGroup | None

    def current_group(self, event_id: str, group_id: str) -> StructureGroup | None:
        assert (event_id, group_id) == ("event-1", "group-1")
        return self.group


@pytest.fixture
def active_opportunity() -> ActiveOpportunity:
    return ActiveOpportunity(
        id="opportunity-1",
        event_id="event-1",
        group_id="group-1",
        membership_hash="membership-1",
        structure_revision=17,
        quote_run_id=42,
        legs=(
            StructureLeg("market-1", "condition-1", "alpha", "token-1"),
            StructureLeg("market-2", "condition-2", "beta", "token-2"),
        ),
    )


@pytest.fixture
def membership_reader() -> FakeMembershipReader:
    return FakeMembershipReader(
        StructureGroup(
            structure_revision=18,
            event_id="event-1",
            group_id="group-1",
            membership_hash="membership-1",
            legs=(
                StructureLeg("market-1", "condition-1", "alpha", "token-1"),
                StructureLeg("market-2", "condition-2", "beta", "token-2"),
            ),
        )
    )


async def test_focused_collector_rechecks_all_durable_legs(
    active_opportunity: ActiveOpportunity,
    membership_reader: FakeMembershipReader,
) -> None:
    reader = FakeReader(
        [
            {"asset_id": "token-1", "asks": [{"price": "0.45", "size": "50"}]},
            {"asset_id": "token-2", "asks": [{"price": "0.52", "size": "42"}]},
        ]
    )

    result = await collect_focused_observation(
        active_opportunity,
        reader=reader,
        membership_reader=membership_reader,
        now_ms=lambda: 1_800_000_000_000,
    )

    assert result.status == "observe"
    assert result.bundle_cost == 0.97
    assert result.gross_edge_bps == 300.0
    assert result.max_bundle_size == 42.0
    assert result.quote_run_id == 42
    assert reader.requests == [["token-1", "token-2"]]
    assert reader.projections == ["top"]


async def test_membership_change_invalidates_before_clob_fetch(
    active_opportunity: ActiveOpportunity,
) -> None:
    reader = FakeReader([])
    changed_membership_reader = FakeMembershipReader(
        StructureGroup(
            structure_revision=18,
            event_id="event-1",
            group_id="group-1",
            membership_hash="membership-changed",
            legs=(
                StructureLeg("market-1", "condition-1", "alpha", "token-1"),
                StructureLeg("market-2", "condition-2", "beta", "token-2"),
            ),
        )
    )

    result = await collect_focused_observation(
        active_opportunity,
        reader=reader,
        membership_reader=changed_membership_reader,
        now_ms=lambda: 1_800_000_000_000,
    )

    assert result.status == "invalidated"
    assert result.reason == "structure-membership-changed"
    assert reader.requests == []


async def test_missing_book_is_unavailable(
    active_opportunity: ActiveOpportunity,
    membership_reader: FakeMembershipReader,
) -> None:
    reader = FakeReader(
        [{"asset_id": "token-1", "asks": [{"price": "0.45", "size": "50"}]}]
    )

    result = await collect_focused_observation(
        active_opportunity,
        reader=reader,
        membership_reader=membership_reader,
        now_ms=lambda: 1_800_000_000_000,
    )

    assert result.status == "unavailable"
    assert result.reason == "incomplete-quotes"


async def test_valid_below_threshold_closes_as_no_edge(
    active_opportunity: ActiveOpportunity,
    membership_reader: FakeMembershipReader,
) -> None:
    reader = FakeReader(
        [
            {"asset_id": "token-1", "asks": [{"price": "0.51", "size": "50"}]},
            {"asset_id": "token-2", "asks": [{"price": "0.52", "size": "42"}]},
        ]
    )

    result = await collect_focused_observation(
        active_opportunity,
        reader=reader,
        membership_reader=membership_reader,
        now_ms=lambda: 1_800_000_000_000,
    )

    assert result.status == "no-edge"
    assert result.bundle_cost == 1.03
    assert result.gross_edge_bps == -300.0


def test_sqlite_membership_reader_does_not_fall_back_from_newest_structure(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    with sqlite3.connect(db_path) as con:
        for snapshot_id, published, quality in (
            (1, 1, "complete-supported"),
            (2, 1, "incomplete-source"),
        ):
            con.execute(
                "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
                "market_view_published,data_product,is_valid,parquet_path) "
                "VALUES (?,?,?,'subset',2,?,'structure',1,'fixture.parquet')",
                (
                    snapshot_id,
                    1_800_000_000_000 + snapshot_id,
                    1_800_000_000_100 + snapshot_id,
                    published,
                ),
            )
            con.execute(
                "INSERT INTO neg_risk_group_truth(snapshot_id,event_id,neg_risk_market_id,"
                "neg_risk_type,expected_member_count,active_named_count,"
                "membership_hash,quality,reason) "
                "VALUES (?, 'event-1', 'group-1', 'standard', 2, 2, 'membership-1', ?, NULL)",
                (snapshot_id, quality),
            )
            con.execute(
                "INSERT INTO snapshot_source_coverage("
                "snapshot_id,completed,market_items,event_items) "
                "VALUES (?,1,2,1)",
                (snapshot_id,),
            )
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,"
            "member_kind,active,closed) VALUES (1,'event-1','group-1',?,'named',1,0)",
            [("market-1",), ("market-2",)],
        )
        con.executemany(
            "INSERT INTO markets(market_id,condition_id,slug,yes_token_id,active,closed,"
            "neg_risk_market_id,fetched_at_ms,snapshot_id,incomplete,event_id) "
            "VALUES (?,?,'slug',?,1,0,'group-1',1,1,0,'event-1')",
            [("market-1", "condition-1", "token-1"), ("market-2", "condition-2", "token-2")],
        )

    assert SqliteStructureMembershipReader(db_path).current_group("event-1", "group-1") is None
