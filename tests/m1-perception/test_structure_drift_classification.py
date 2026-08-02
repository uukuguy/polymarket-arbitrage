from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from polyarb.perception.structure_drift import (
    FreshMemberEvidence,
    StructuralMemberIdentity,
    classify_structure_member_drift,
)
from polyarb.storage.sqlite_store import SQLiteStore


def _member(market_id: str = "market-1") -> StructuralMemberIdentity:
    return StructuralMemberIdentity(
        event_id="event-1",
        group_id="group-1",
        market_id=market_id,
        member_kind="named",
        active=True,
        closed=False,
        condition_id=f"condition-{market_id}",
        yes_token_id=f"yes-{market_id}",
        no_token_id=f"no-{market_id}",
        neg_risk=True,
        incomplete=False,
    )


def _addition_evidence() -> FreshMemberEvidence:
    return FreshMemberEvidence(
        source_present=True,
        current_active=True,
        current_closed=False,
        projector_matches=True,
        generation_certified=True,
        event_only_quarantine=False,
        market_side_quarantine=False,
        absent_from_event_catalog=False,
        absent_from_market_catalog=False,
    )


def test_shared_structural_identity_is_exact() -> None:
    member = _member()
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(member,),
        evidence={},
    )
    assert result.shared_count == 1
    assert result.fresh_addition_count == 0
    assert result.legacy_removal_counts == {}
    assert result.overlap_conflicts == ()
    assert result.unclassified == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "changed-event"),
        ("group_id", "changed-group"),
        ("member_kind", "other"),
        ("active", False),
        ("closed", True),
        ("condition_id", "changed-condition"),
        ("yes_token_id", "changed-yes"),
        ("no_token_id", "changed-no"),
        ("neg_risk", False),
        ("incomplete", True),
    ],
)
def test_every_shared_structural_mutation_is_an_overlap_conflict(
    field: str,
    value: object,
) -> None:
    legacy = _member()
    generation = replace(legacy, **{field: value})
    result = classify_structure_member_drift(
        legacy=(legacy,),
        generation=(generation,),
        evidence={},
    )
    assert result.shared_count == 0
    assert [row.market_id for row in result.overlap_conflicts] == ["market-1"]
    assert result.authorized is False


@pytest.mark.parametrize(
    "change",
    [
        {"source_present": False},
        {"projector_matches": False},
        {"generation_certified": False},
        {"event_only_quarantine": True},
        {"market_side_quarantine": True},
    ],
)
def test_fresh_addition_requires_every_source_and_certification_fact(
    change: dict[str, bool],
) -> None:
    evidence = replace(_addition_evidence(), **change)
    result = classify_structure_member_drift(
        legacy=(),
        generation=(_member(),),
        evidence={"market-1": evidence},
    )
    assert result.fresh_addition_count == 0
    assert [row.market_id for row in result.unclassified] == ["market-1"]
    assert result.authorized is False


@pytest.mark.parametrize(
    ("reason", "evidence"),
    [
        (
            "current-nontradable",
            replace(_addition_evidence(), current_active=False),
        ),
        (
            "event-only-quarantine",
            replace(
                _addition_evidence(),
                source_present=False,
                projector_matches=False,
                generation_certified=False,
                event_only_quarantine=True,
                absent_from_market_catalog=True,
            ),
        ),
        (
            "market-side-quarantine",
            replace(
                _addition_evidence(),
                projector_matches=False,
                generation_certified=False,
                market_side_quarantine=True,
            ),
        ),
        (
            "fresh-source-absent",
            replace(
                _addition_evidence(),
                source_present=False,
                projector_matches=False,
                generation_certified=False,
                absent_from_event_catalog=True,
                absent_from_market_catalog=True,
            ),
        ),
    ],
)
def test_legacy_removal_requires_one_exact_reason(
    reason: str,
    evidence: FreshMemberEvidence,
) -> None:
    result = classify_structure_member_drift(
        legacy=(_member(),),
        generation=(),
        evidence={"market-1": evidence},
    )
    assert result.legacy_removal_counts == {reason: 1}
    assert result.unclassified == ()
    assert result.authorized is True


def test_ambiguous_legacy_removal_is_unclassified() -> None:
    ambiguous = replace(
        _addition_evidence(),
        current_active=False,
        event_only_quarantine=True,
        absent_from_market_catalog=True,
    )
    result = classify_structure_member_drift(
        legacy=(_member(),),
        generation=(),
        evidence={"market-1": ambiguous},
    )
    assert result.legacy_removal_counts == {}
    assert [row.market_id for row in result.unclassified] == ["market-1"]
    assert result.authorized is False


def test_full_symmetric_difference_cannot_cancel_at_net_zero() -> None:
    removed = _member("removed")
    added = _member("added")
    result = classify_structure_member_drift(
        legacy=(removed,),
        generation=(added,),
        evidence={
            "removed": FreshMemberEvidence(
                source_present=False,
                current_active=False,
                current_closed=True,
                projector_matches=False,
                generation_certified=False,
                event_only_quarantine=False,
                market_side_quarantine=False,
                absent_from_event_catalog=True,
                absent_from_market_catalog=True,
            ),
            "added": _addition_evidence(),
        },
    )
    assert result.fresh_addition_count == 1
    assert result.legacy_removal_counts == {"fresh-source-absent": 1}
    assert result.symmetric_difference_count == 2
    assert result.generation_count - result.legacy_count == 0
    assert result.authorized is True
    assert result.class_digests["fresh-addition"] != result.class_digests[
        "fresh-source-absent"
    ]


@pytest.mark.parametrize("side", ["legacy", "generation"])
def test_duplicate_member_identity_is_unclassified(side: str) -> None:
    member = _member()
    kwargs = {
        "legacy": (member, member) if side == "legacy" else (member,),
        "generation": (member, member) if side == "generation" else (member,),
        "evidence": {},
    }
    result = classify_structure_member_drift(**kwargs)
    assert result.authorized is False
    assert [row.market_id for row in result.unclassified] == ["market-1"]


def test_legacy_and_generation_member_loaders_are_bounded_keysets(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "member-loaders.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (1,1000,1001,'full',501,1,'structure','legacy','ok',1,'')"
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality) "
            "VALUES (1,'event-1','group-1','standard',501,501,?,'complete-supported')",
            ("a" * 64,),
        )
        con.execute(
            "INSERT INTO structure_generation_group_truth SELECT * FROM "
            "neg_risk_group_truth WHERE snapshot_id=1"
        )
        members = []
        markets = []
        for index in range(501):
            market_id = f"market-{index:03d}"
            members.append((1, "event-1", "group-1", market_id))
            markets.append(
                (
                    market_id,
                    f"condition-{index}",
                    f"yes-{index}",
                    f"no-{index}",
                    1,
                )
            )
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed) "
            "VALUES (?,?,?,?,'named',1,0)",
            members,
        )
        con.execute(
            "INSERT INTO structure_generation_memberships SELECT * FROM "
            "event_market_memberships WHERE snapshot_id=1"
        )
        con.executemany(
            "INSERT INTO markets(market_id,condition_id,yes_token_id,no_token_id,"
            "active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,snapshot_id,"
            "incomplete,event_id) VALUES (?,?,?,?,1,0,1,'group-1',1000,?,0,'event-1')",
            markets,
        )
        market_columns = (
            "snapshot_id,market_id,condition_id,slug,question,yes_token_id,no_token_id,"
            "mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,"
            "best_ask_price,best_ask_size,end_time_ms,active,closed,neg_risk,"
            "neg_risk_market_id,fetched_at_ms,page_fetched_at_ms,incomplete,event_id"
        )
        con.execute(
            f"INSERT INTO structure_generation_markets({market_columns}) "
            f"SELECT {market_columns} FROM markets WHERE snapshot_id=1"
        )

    statements: list[str] = []
    generation = store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=True,
        after_market_id=None,
        limit=500,
        trace_callback=statements.append,
    )
    legacy = store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=False,
        after_market_id=None,
        limit=500,
        trace_callback=statements.append,
    )
    assert generation == legacy
    assert len(generation) == 500
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) == 2
    tail = store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=True,
        after_market_id=generation[-1].market_id,
        limit=500,
    )
    assert [row.market_id for row in tail] == ["market-500"]
    overlap = store.fetch_structure_drift_members_by_id(
        snapshot_id=1,
        generation=False,
        market_ids=[row.market_id for row in generation],
    )
    assert overlap == generation
