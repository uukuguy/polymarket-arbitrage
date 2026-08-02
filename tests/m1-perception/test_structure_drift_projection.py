from __future__ import annotations

import hashlib
import json
import sqlite3
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


def _published_source_store(tmp_path: Path, *, event_count: int) -> SQLiteStore:
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
            raw_market = {
                "id": market_id,
                "conditionId": f"condition-{index:03d}",
                "clobTokenIds": json.dumps([f"yes-{index}", f"no-{index}"]),
                "active": True,
                "closed": False,
                "negRisk": True,
                "negRiskMarketID": f"group-{index:03d}",
            }
            event_rows.append(
                ("window-1", event_id, json.dumps(raw_event), index + 1)
            )
            relation_rows.append(("window-1", market_id, event_id, index + 1))
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
