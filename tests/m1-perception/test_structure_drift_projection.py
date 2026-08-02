from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path

import pytest

from polyarb.perception.market_truth import membership_hash
from polyarb.perception.structure_drift import (
    StructuralMemberIdentity,
    classify_structure_member_drift,
    hash_legacy_compatible_projection,
    project_legacy_compatible_event,
    project_legacy_compatible_market,
)
from polyarb.perception.structure_publication import event_only_member_quarantine_issue
from polyarb.storage.row_chain_sha256 import ROW_CHAIN_DOMAINS, RowChainSHA256
from polyarb.storage.sqlite_store import SQLiteStore


def _raw_event() -> dict[str, object]:
    return {
        "id": "event-1",
        "slug": "event-1",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "group-1",
        "markets": [
            {
                "id": "market-1",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            },
            {
                "id": "event-only-market",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            },
        ],
    }


def _published_source_store(
    tmp_path: Path,
    *,
    event_count: int,
    event_only_members: tuple[tuple[str, bool], ...] = (),
    global_relation_conflict: bool = False,
    duplicate_market_identity: bool = False,
    duplicate_event_only_identity: bool = False,
    null_event_source_ordinal: bool = False,
    certified_event_only_conflict: bool = False,
    raw_market_overrides: dict[str, object] | None = None,
    raw_event_overrides: dict[str, object] | None = None,
    raw_member_overrides: dict[str, object] | None = None,
) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "drift-source.db")
    store.init_schema()
    digest = "a" * 64
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (1,1000,1001,'full',?,1,'structure','legacy','ok',1,'')",
            (event_count,),
        )
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES ('window-1','open',1000,1000)"
        )
        event_rows = []
        relation_rows = []
        market_rows = []
        for index in range(event_count):
            event_id = f"event-{index:03d}"
            market_id = f"market-{index:03d}"
            raw_event = {
                "id": event_id,
                "slug": event_id,
                "active": True,
                "closed": False,
                "negRisk": True,
                "enableNegRisk": True,
                "negRiskAugmented": False,
                "negRiskMarketID": f"group-{index:03d}",
                "markets": [
                    {
                        "id": market_id,
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                ],
            }
            if index == 0 and raw_event_overrides:
                raw_event.update(raw_event_overrides)
            if index == 0 and raw_member_overrides:
                raw_event["markets"][0].update(raw_member_overrides)
            if index == 0:
                if duplicate_market_identity:
                    raw_event["markets"].append(dict(raw_event["markets"][0]))
                if global_relation_conflict:
                    raw_event["markets"].insert(
                        0,
                        {
                            "id": "inactive-a",
                            "active": False,
                            "closed": True,
                            "negRiskOther": False,
                        },
                    )
                raw_event["markets"].extend(
                    {
                        "id": market_id,
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                    for market_id, _certified in event_only_members
                )
                if duplicate_event_only_identity and event_only_members:
                    raw_event["markets"].append(dict(raw_event["markets"][-1]))
            raw_market = {
                "id": market_id,
                "conditionId": f"condition-{index:03d}",
                "clobTokenIds": json.dumps([f"yes-{index}", f"no-{index}"]),
                "active": True,
                "closed": False,
                "negRisk": True,
                "negRiskMarketID": f"group-{index:03d}",
            }
            if index == 0 and raw_market_overrides:
                raw_market.update(raw_market_overrides)
            event_rows.append(
                (
                    "window-1",
                    event_id,
                    json.dumps(raw_event),
                    None if index == 0 and null_event_source_ordinal else index + 1,
                )
            )
            relation_rows.append(("window-1", market_id, event_id, index + 1))
            if index == 0:
                relation_rows.extend(
                    ("window-1", market_id, event_id, index + 1)
                    for market_id, _certified in event_only_members
                )
            if index == 1 and global_relation_conflict:
                relation_rows.append(("window-1", "market-000", event_id, index + 1))
            if index == 1 and certified_event_only_conflict:
                relation_rows.append(
                    ("window-1", "event-only-certified", event_id, index + 1)
                )
            market_rows.append(
                ("window-1", market_id, json.dumps(raw_market), index + 1)
            )
        con.executemany(
            "INSERT INTO structure_sync_event_staging("
            "window_id,event_id,payload_json,source_ordinal) VALUES (?,?,?,?)",
            event_rows,
        )
        con.executemany(
            "INSERT INTO structure_sync_event_market_staging("
            "window_id,market_id,event_id,source_ordinal) VALUES (?,?,?,?)",
            relation_rows,
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='events_complete' WHERE id='window-1'"
        )
        con.executemany(
            "INSERT INTO structure_sync_market_staging("
            "window_id,market_id,payload_json,source_ordinal) VALUES (?,?,?,?)",
            market_rows,
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='published',"
            "published_snapshot_id=1 WHERE id='window-1'"
        )
        con.execute(
            "INSERT INTO structure_generation_issues("
            "snapshot_id,issue_index,layer,category,market_id,detail,raw_payload) "
            "VALUES (1,1,1,'api_jitter','forged','forged','forged')"
        )
        for issue_index, (market_id, certified) in enumerate(
            event_only_members, start=100
        ):
            if not certified:
                continue
            raw_event = json.loads(str(event_rows[0][2]))
            issue = event_only_member_quarantine_issue(
                raw_event,
                event_source_ordinal=1,
                market_id=market_id,
            )
            assert issue is not None
            con.execute(
                "INSERT INTO structure_generation_issues(snapshot_id,issue_index,"
                "layer,category,market_id,detail,raw_payload) VALUES (1,?,?,?,?,?,?)",
                (
                    issue_index,
                    issue["layer"],
                    issue["category"],
                    market_id,
                    issue["detail"],
                    issue["raw_payload"],
                ),
            )
        con.execute(
            "INSERT INTO structure_publications("
            "publication_id,window_id,snapshot_id,status,normalization_contract_version,"
            "expected_counts_json,committed_counts_json,validation_hash,"
            "certification_component,certification_hash,created_at_ms,checkpoint_at_ms) "
            "VALUES ('publication-1','window-1',1,'published','contract-v1','{}','{}',"
            "?,'bounded-complete',?,1000,1001)",
            (digest, digest),
        )
    return store


def test_projection_union_excludes_only_certified_event_only_member(
    tmp_path: Path,
) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        event_only_members=(
            ("event-only-certified", True),
            ("event-only-uncertified", False),
        ),
    )

    statements: list[str] = []
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
        trace_callback=statements.append,
    )

    assert chunk.members == (
        StructuralMemberIdentity(
            event_id="event-000",
            group_id="group-000",
            market_id="market-000",
            member_kind="named",
            active=True,
            closed=False,
            condition_id="condition-000",
            yes_token_id="yes-0",
            no_token_id="no-0",
            neg_risk=True,
            incomplete=False,
        ),
    )
    assert [item.code for item in chunk.diagnostics] == [
        "uncertified-event-only-member"
    ]
    assert chunk.diagnostics[0].envelope.market_id == "event-only-uncertified"
    assert chunk.candidates_processed == 3
    assert chunk.cursor is None
    selects = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]
    assert len(selects) <= 8
    assert not any(" WHERE relation.market_id='" in sql for sql in selects)
    with sqlite3.connect(store.db_path) as con:
        plans = "\n".join(
            str(row[3])
            for sql in selects
            for row in con.execute("EXPLAIN QUERY PLAN " + sql)
        )
    assert "USE TEMP B-TREE FOR ORDER BY" not in plans


def test_projection_preserves_exact_eleven_field_identity(tmp_path: Path) -> None:
    store = _published_source_store(tmp_path, event_count=1)
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
    )
    member = chunk.members[0]
    assert (
        member.event_id,
        member.group_id,
        member.market_id,
        member.member_kind,
        member.active,
        member.closed,
        member.condition_id,
        member.yes_token_id,
        member.no_token_id,
        member.neg_risk,
        member.incomplete,
    ) == (
        "event-000",
        "group-000",
        "market-000",
        "named",
        True,
        False,
        "condition-000",
        "yes-0",
        "no-0",
        True,
        False,
    )


def test_projection_duplicate_market_identity_fails_closed(tmp_path: Path) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        duplicate_market_identity=True,
    )
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
    )
    assert chunk.members == ()
    assert chunk.diagnostics[0].code == "duplicate-market-identity"


@pytest.mark.parametrize("limit", [1, 17, 500])
def test_event_only_keyset_is_complete_with_adversarial_order_and_null_ordinal(
    tmp_path: Path,
    limit: int,
) -> None:
    event_only = tuple(
        (f"event-only-{(index * 283) % 503:04d}", False)
        for index in range(503)
    )
    case_path = tmp_path / str(limit)
    case_path.mkdir()
    store = _published_source_store(
        case_path,
        event_count=1,
        event_only_members=event_only,
        null_event_source_ordinal=True,
    )

    cursor = None
    seen: list[str] = []
    traced_selects: list[str] = []
    event_member_cursors: list[int] = []
    for _ in range(600):
        statements: list[str] = []
        chunk = store.fetch_structure_drift_fresh_projection_chunk(
            publication_id="publication-1",
            generation_snapshot_id=1,
            cursor=cursor,
            limit=limit,
            trace_callback=statements.append if limit == 500 else None,
        )
        selects = [
            sql for sql in statements if sql.lstrip().upper().startswith("SELECT")
        ]
        assert len(selects) <= 10
        traced_selects.extend(selects)
        seen.extend(item.envelope.market_id for item in chunk.diagnostics)
        cursor = chunk.cursor
        if cursor is None:
            break
        if cursor.stream == "event-only":
            assert cursor.source_ordinal == 1
            assert cursor.member_ordinal is not None
            event_member_cursors.append(cursor.member_ordinal)
    else:
        pytest.fail("fresh projection cursor did not terminate")

    assert len(seen) == 503
    assert len(set(seen)) == 503
    assert set(seen) == {market_id for market_id, _certified in event_only}
    assert event_member_cursors == sorted(set(event_member_cursors))
    if traced_selects:
        assert not any(" WHERE relation.market_id='" in sql for sql in traced_selects)
        with sqlite3.connect(store.db_path) as con:
            plans = "\n".join(
                str(row[3])
                for sql in traced_selects
                for row in con.execute("EXPLAIN QUERY PLAN " + sql)
            )
        assert "USE TEMP B-TREE FOR ORDER BY" not in plans


def test_event_only_exact_full_page_returns_terminal_cursor(tmp_path: Path) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        event_only_members=tuple(
            (f"event-only-{index:04d}", False) for index in range(499)
        ),
        null_event_source_ordinal=True,
    )
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
    )
    assert chunk.candidates_processed == 500
    assert chunk.cursor is None


def test_certified_event_only_global_conflict_wins_before_quarantine(
    tmp_path: Path,
) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=2,
        event_only_members=(("event-only-certified", True),),
        certified_event_only_conflict=True,
    )
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
    )
    matching = [
        item
        for item in chunk.diagnostics
        if item.envelope.market_id == "event-only-certified"
    ]
    assert [item.code for item in matching] == ["conflicting-event-membership"]


def test_event_only_duplicate_precedes_uncertified_quarantine(tmp_path: Path) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        event_only_members=(("event-only-duplicate", False),),
        duplicate_event_only_identity=True,
    )
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
    )
    matching = [
        item
        for item in chunk.diagnostics
        if item.envelope.market_id == "event-only-duplicate"
    ]
    assert [item.code for item in matching] == ["duplicate-market-identity"]


@pytest.mark.parametrize(
    ("market_overrides", "event_overrides", "member_overrides", "code"),
    [
        ({"conditionId": None}, None, None, "active-open-projection-missing"),
        (
            {"clobTokenIds": json.dumps([None, "no-0"])},
            None,
            None,
            "active-open-projection-missing",
        ),
        (
            {"clobTokenIds": json.dumps(["yes-0", None])},
            None,
            None,
            "active-open-projection-missing",
        ),
        (None, {"id": None}, None, "invalid-event-membership"),
        (None, {"negRiskMarketID": None}, None, "invalid-event-membership"),
        (None, None, {"active": None}, "invalid-event-membership"),
        (None, None, {"closed": None}, "invalid-event-membership"),
    ],
)
def test_projection_missing_identity_fields_fail_closed_with_nullable_envelope(
    tmp_path: Path,
    market_overrides: dict[str, object] | None,
    event_overrides: dict[str, object] | None,
    member_overrides: dict[str, object] | None,
    code: str,
) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        raw_market_overrides=market_overrides,
        raw_event_overrides=event_overrides,
        raw_member_overrides=member_overrides,
    )
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
    )
    assert chunk.members == ()
    assert chunk.diagnostics[0].code == code
    envelope = chunk.diagnostics[0].envelope
    if market_overrides and "conditionId" in market_overrides:
        assert envelope.condition_id is None
    if market_overrides and "clobTokenIds" in market_overrides:
        assert envelope.yes_token_id is None or envelope.no_token_id is None
    if event_overrides and "id" in event_overrides:
        assert envelope.event_id is None
    if event_overrides and "negRiskMarketID" in event_overrides:
        assert envelope.group_id is None
    if member_overrides and "active" in member_overrides:
        assert envelope.active is None
    if member_overrides and "closed" in member_overrides:
        assert envelope.closed is None


@pytest.mark.parametrize("limit", [1, 17, 500])
def test_global_relation_conflict_wins_across_projection_chunks(
    tmp_path: Path,
    limit: int,
) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=2,
        global_relation_conflict=True,
    )

    cursor = None
    diagnostics = []
    members = []
    while True:
        chunk = store.fetch_structure_drift_fresh_projection_chunk(
            publication_id="publication-1",
            generation_snapshot_id=1,
            cursor=cursor,
            limit=limit,
        )
        diagnostics.extend(chunk.diagnostics)
        members.extend(chunk.members)
        if chunk.cursor is None:
            break
        cursor = chunk.cursor

    assert diagnostics[0].code == "conflicting-event-membership"
    assert {
        item.envelope.market_id
        for item in diagnostics
        if item.code == "conflicting-event-membership"
    } == {"inactive-a", "market-000"}
    assert {item.market_id for item in members} == {"market-001"}


def test_global_relation_projection_root_is_chunk_invariant(tmp_path: Path) -> None:
    results = []
    for limit in (1, 17, 500):
        case_path = tmp_path / str(limit)
        case_path.mkdir()
        store = _published_source_store(
            case_path,
            event_count=2,
            global_relation_conflict=True,
        )
        cursor = None
        projected = []
        diagnostics = []
        while True:
            chunk = store.fetch_structure_drift_fresh_projection_chunk(
                publication_id="publication-1",
                generation_snapshot_id=1,
                cursor=cursor,
                limit=limit,
            )
            projected.extend(chunk.members)
            diagnostics.extend(chunk.diagnostics)
            if chunk.cursor is None:
                break
            cursor = chunk.cursor
        digest = RowChainSHA256.new("projection-member")
        for member in sorted(projected, key=lambda item: item.market_id):
            digest.update(
                (
                    member.event_id,
                    member.group_id,
                    member.market_id,
                    member.member_kind,
                    member.active,
                    member.closed,
                    member.condition_id,
                    member.yes_token_id,
                    member.no_token_id,
                    member.neg_risk,
                    member.incomplete,
                )
            )
        results.append((digest.hexdigest(), tuple(item.code for item in diagnostics)))
    assert all(result == results[0] for result in results)


def test_production_complete_projection_commitment_is_chunk_invariant(
    tmp_path: Path,
) -> None:
    results = []
    for limit in (1, 17, 500):
        case_path = tmp_path / f"commitment-{limit}"
        case_path.mkdir()
        store = _published_source_store(case_path, event_count=30)
        commitment = None
        for _ in range(100):
            commitment = store.advance_structure_drift_fresh_projection_commitment(
                publication_id="publication-1",
                generation_snapshot_id=1,
                commitment=commitment,
                limit=limit,
            )
            if commitment.complete:
                break
        else:
            pytest.fail("fresh projection commitment did not complete")
        assert commitment.member_count == 30
        assert commitment.diagnostic_count == 0
        assert json.loads(commitment.diagnostic_digest_state)["domain"] == (
            "diagnostic/unclassified"
        )
        assert "projection-diagnostic" not in ROW_CHAIN_DOMAINS
        results.append((commitment.member_count, commitment.root))
    assert all(result == results[0] for result in results)


def test_event_projection_removes_only_exact_event_only_evidence_and_rehashes() -> None:
    projection = project_legacy_compatible_event(
        _raw_event(),
        event_source_ordinal=17,
        complete_market_ids=frozenset({"market-1"}),
    )

    assert [member.market_id for member in projection.members] == ["market-1"]
    assert len(projection.truths) == 1
    truth = projection.truths[0]
    assert truth.expected_member_count == 1
    assert truth.active_named_count == 1
    assert truth.membership_hash == membership_hash(
        "event-1", "group-1", projection.members
    )
    assert [issue["market_id"] for issue in projection.issues] == [
        "event-only-market"
    ]
    assert str(projection.issues[0]["raw_payload"]).startswith(
        "active-open-neg-risk-event-member-absent-from-complete-market-catalogue:"
    )


def test_event_projection_keeps_members_present_in_complete_market_catalogue() -> None:
    projection = project_legacy_compatible_event(
        _raw_event(),
        event_source_ordinal=17,
        complete_market_ids=frozenset({"market-1", "event-only-market"}),
    )

    assert [member.market_id for member in projection.members] == [
        "market-1",
        "event-only-market",
    ]
    assert projection.truths[0].expected_member_count == 2
    assert projection.issues == ()


def test_market_projection_uses_pinned_parent_and_exact_market_quarantine() -> None:
    raw = {
        "id": "market-1",
        "conditionId": "condition-1",
        "clobTokenIds": '["yes-1","no-1"]',
        "active": True,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": "group-1",
    }
    projected = project_legacy_compatible_market(
        raw,
        event_ids=("event-1",),
        taken_at_ms=1_000,
    )
    assert projected.row is not None
    assert projected.row["event_id"] == "event-1"
    assert projected.row["yes_token_id"] == "yes-1"
    assert projected.row["fetched_at_ms"] == 1_000
    assert projected.issue is None

    quarantined = project_legacy_compatible_market(
        raw,
        event_ids=(),
        taken_at_ms=1_000,
    )
    assert quarantined.row is None
    assert quarantined.issue is not None
    assert quarantined.issue["market_id"] == "market-1"


def test_published_event_source_chunk_is_bounded_and_issue_independent(
    tmp_path: Path,
) -> None:
    store = _published_source_store(tmp_path, event_count=500)
    statements: list[str] = []

    rows = store.fetch_structure_drift_event_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_event_id=None,
        limit=500,
        trace_callback=statements.append,
    )
    before = [
        project_legacy_compatible_event(
            raw,
            event_source_ordinal=ordinal,
            complete_market_ids=market_ids,
        )
        for ordinal, _event_id, raw, market_ids in rows
    ]
    assert len(rows) == 100
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) <= 5
    assert "structure_generation_issues" not in "\n".join(statements)

    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_generation_issues_frozen_update_v2")
        con.execute(
            "UPDATE structure_generation_issues SET raw_payload='changed' "
            "WHERE snapshot_id=1"
        )
    after_rows = store.fetch_structure_drift_event_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_event_id=None,
        limit=500,
    )
    after = [
        project_legacy_compatible_event(
            raw,
            event_source_ordinal=ordinal,
            complete_market_ids=market_ids,
        )
        for ordinal, _event_id, raw, market_ids in after_rows
    ]
    assert after == before


def test_event_source_chunk_rejects_nonpublished_or_wrong_snapshot(
    tmp_path: Path,
) -> None:
    store = _published_source_store(tmp_path, event_count=1)
    with pytest.raises(ValueError, match="structure-drift-source-identity-mismatch"):
        store.fetch_structure_drift_event_source_chunk(
            publication_id="publication-1",
            generation_snapshot_id=2,
            after_event_id=None,
            limit=1,
        )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET status='complete' WHERE id='window-1'"
        )
    with pytest.raises(ValueError, match="structure-drift-source-identity-mismatch"):
        store.fetch_structure_drift_event_source_chunk(
            publication_id="publication-1",
            generation_snapshot_id=1,
            after_event_id=None,
            limit=1,
        )


def test_published_market_source_chunk_is_bounded_and_issue_independent(
    tmp_path: Path,
) -> None:
    store = _published_source_store(tmp_path, event_count=500)
    statements: list[str] = []
    rows = store.fetch_structure_drift_market_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_market_id=None,
        limit=500,
        trace_callback=statements.append,
    )
    before = [
        project_legacy_compatible_market(
            raw,
            event_ids=event_ids,
            taken_at_ms=taken_at_ms,
        )
        for _market_id, raw, event_ids, taken_at_ms in rows
    ]
    assert len(rows) == 500
    assert all(item.row is not None and item.issue is None for item in before)
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) <= 2
    assert "structure_generation_issues" not in "\n".join(statements)

    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_generation_issues_frozen_delete")
        con.execute("DELETE FROM structure_generation_issues WHERE snapshot_id=1")
    after_rows = store.fetch_structure_drift_market_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_market_id=None,
        limit=500,
    )
    after = [
        project_legacy_compatible_market(
            raw,
            event_ids=event_ids,
            taken_at_ms=taken_at_ms,
        )
        for _market_id, raw, event_ids, taken_at_ms in after_rows
    ]
    assert after == before


def test_compatibility_hash_uses_exact_eligible_universe_and_fresh_group_truth() -> None:
    event_projection = project_legacy_compatible_event(
        _raw_event(),
        event_source_ordinal=17,
        complete_market_ids=frozenset({"market-1"}),
    )
    market_projection = project_legacy_compatible_market(
        {
            "id": "market-1",
            "conditionId": "condition-1",
            "clobTokenIds": '["yes-1","no-1"]',
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskMarketID": "group-1",
        },
        event_ids=("event-1",),
        taken_at_ms=1_000,
    )
    receipt = hash_legacy_compatible_projection(
        (event_projection,),
        (market_projection,),
    )
    truth = event_projection.truths[0]
    expected_row = ("group-1", truth.membership_hash, "market-1", "yes-1")
    expected_universe_hash = hashlib.sha256(
        json.dumps([expected_row], separators=(",", ":")).encode()
    ).hexdigest()
    assert receipt.eligible_market_count == 1
    assert receipt.universe_hash == expected_universe_hash
    assert len(receipt.group_truth_hash) == 64

    changed_market = project_legacy_compatible_market(
        {
            "id": "market-1",
            "conditionId": "condition-1",
            "clobTokenIds": '["changed-yes","no-1"]',
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskMarketID": "group-1",
        },
        event_ids=("event-1",),
        taken_at_ms=1_000,
    )
    changed = hash_legacy_compatible_projection(
        (event_projection,),
        (changed_market,),
    )
    assert changed.universe_hash != receipt.universe_hash
    assert changed.group_truth_hash == receipt.group_truth_hash


def test_source_chunks_resume_on_exact_keyset_boundary(tmp_path: Path) -> None:
    store = _published_source_store(tmp_path, event_count=501)
    first_events = store.fetch_structure_drift_event_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_event_id=None,
        limit=500,
    )
    final_events = store.fetch_structure_drift_event_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_event_id=first_events[-1][1],
        limit=500,
    )
    assert len(first_events) == 100
    assert len(final_events) == 100
    assert first_events[-1][1] == "event-099"
    assert final_events[0][1] == "event-100"

    first_markets = store.fetch_structure_drift_market_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_market_id=None,
        limit=500,
    )
    final_markets = store.fetch_structure_drift_market_source_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        after_market_id=first_markets[-1][0],
        limit=500,
    )
    assert len(first_markets) == 500
    assert [row[0] for row in final_markets] == ["market-500"]


def test_member_resume_uses_ordered_market_id_range_without_nullable_or(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "member-range.db")
    store.init_schema()
    statements: list[str] = []

    store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=True,
        after_market_id="market-050",
        limit=500,
        trace_callback=statements.append,
    )

    member_select = next(
        statement
        for statement in statements
        if "structure_generation_memberships m" in statement
    )
    assert "m.market_id>'market-050'" in member_select
    assert "IS NULL OR m.market_id>" not in member_select


def test_member_scan_indexes_build_and_plan_on_120k_rows(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "member-scan-120k.db")
    store.init_schema()
    row_count = 120_000
    members_per_group = 24
    group_count = row_count // members_per_group
    with sqlite3.connect(store.db_path) as con:
        con.execute("PRAGMA synchronous=OFF")
        for generation in (False, True):
            membership_table = (
                "structure_generation_memberships"
                if generation
                else "event_market_memberships"
            )
            truth_table = (
                "structure_generation_group_truth"
                if generation
                else "neg_risk_group_truth"
            )
            market_table = "structure_generation_markets" if generation else "markets"
            snapshot_id = 2 if generation else 1
            con.executemany(
                f"INSERT INTO {membership_table}(snapshot_id,event_id,"
                "neg_risk_market_id,market_id,member_kind,active,closed) "
                "VALUES (?,?,?,?,'named',1,0)",
                (
                    (
                        snapshot_id,
                        f"event-{index // members_per_group:06d}",
                        f"group-{index // members_per_group:06d}",
                        f"market-{index:06d}",
                    )
                    for index in range(row_count)
                ),
            )
            con.executemany(
                f"INSERT INTO {truth_table}(snapshot_id,event_id,"
                "neg_risk_market_id,neg_risk_type,expected_member_count,"
                "active_named_count,membership_hash,quality) "
                "VALUES (?,?,?,'standard',24,24,?,'complete-supported')",
                (
                    (
                        snapshot_id,
                        f"event-{index:06d}",
                        f"group-{index:06d}",
                        "a" * 64,
                    )
                    for index in range(group_count)
                ),
            )
            con.executemany(
                f"INSERT INTO {market_table}(snapshot_id,market_id,condition_id,"
                "yes_token_id,no_token_id,active,closed,neg_risk,"
                "neg_risk_market_id,fetched_at_ms,incomplete,event_id) "
                "VALUES (?,?,?,?,?,1,0,1,?,1000,0,?)",
                (
                    (
                        snapshot_id,
                        f"market-{index:06d}",
                        f"condition-{index:06d}",
                        f"yes-{index:06d}",
                        f"no-{index:06d}",
                        f"group-{index // members_per_group:06d}",
                        f"event-{index // members_per_group:06d}",
                    )
                    for index in range(row_count)
                ),
            )
        con.execute("DROP INDEX idx_structure_generation_memberships_drift_scan")
        con.execute("DROP INDEX idx_event_market_memberships_drift_scan")

    started = time.monotonic()
    store.init_schema()
    startup_elapsed_s = time.monotonic() - started
    assert startup_elapsed_s < 30.0

    for generation, expected_index in (
        (True, "idx_structure_generation_memberships_drift_scan"),
        (False, "idx_event_market_memberships_drift_scan"),
    ):
        statements: list[str] = []
        rows = store.fetch_structure_drift_member_chunk(
            snapshot_id=2 if generation else 1,
            generation=generation,
            after_market_id="market-059999",
            limit=500,
            trace_callback=statements.append,
        )
        member_select = next(
            statement
            for statement in statements
            if "memberships m" in statement
        )
        with sqlite3.connect(store.db_path) as con:
            plan = "\n".join(
                str(row[3])
                for row in con.execute("EXPLAIN QUERY PLAN " + member_select)
            )
            persisted_count = con.execute(
                "SELECT COUNT(*) FROM "
                + (
                    "structure_generation_memberships"
                    if generation
                    else "event_market_memberships"
                )
            ).fetchone()
        assert len(rows) == 500
        assert persisted_count == (row_count,)
        assert expected_index in plan
        assert "USE TEMP B-TREE FOR ORDER BY" not in plan


def test_fresh_member_evidence_is_bulk_raw_derived_and_issue_independent(
    tmp_path: Path,
) -> None:
    store = _published_source_store(tmp_path, event_count=500)
    members = tuple(
        StructuralMemberIdentity(
            event_id=f"event-{index:03d}",
            group_id=f"group-{index:03d}",
            market_id=f"market-{index:03d}",
            member_kind="named",
            active=True,
            closed=False,
            condition_id=f"condition-{index:03d}",
            yes_token_id=f"yes-{index}",
            no_token_id=f"no-{index}",
            neg_risk=True,
            incomplete=False,
        )
        for index in range(500)
    )
    statements: list[str] = []
    evidence = store.fetch_structure_drift_fresh_evidence(
        publication_id="publication-1",
        generation_snapshot_id=1,
        members=members,
        trace_callback=statements.append,
    )
    assert len(evidence) == 500
    assert all(
        item.source_present
        and item.projector_matches
        and item.generation_certified
        and not item.event_only_quarantine
        and not item.market_side_quarantine
        for item in evidence.values()
    )
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) <= 3
    assert "structure_generation_issues" not in "\n".join(statements)
    classified = classify_structure_member_drift(
        legacy=(),
        generation=members,
        evidence=evidence,
    )
    assert classified.fresh_addition_count == 500
    assert classified.authorized is True
