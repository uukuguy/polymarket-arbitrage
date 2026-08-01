"""Executable contracts for invisible, atomic Structure generations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import polyarb.perception.structure_publication as structure_publication_module
from polyarb.perception.market_truth import EventMember, membership_hash
from polyarb.perception.structure_publication import (
    StructurePublicationCheckpoint,
    normalize_structure_component_chunk,
    run_structure_publication_step,
)
from polyarb.storage.sqlite_store import SQLiteStore, StructurePublicationCursorError

COMPONENT_COUNTS = {
    "events": 1,
    "event_tags": 0,
    "memberships": 1,
    "group_truth": 1,
    "markets": 1,
    "issues": 0,
}


def test_normalization_chunk_never_fetches_more_than_raw_row_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window_id = _complete_window(store, "market-1", now_ms=100)
    publication = store.begin_structure_publication(
        window_id=window_id,
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": 100,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": {key: 0 for key in COMPONENT_COUNTS},
        },
        now_ms=103,
    )
    observed: list[int] = []
    original = store.fetch_structure_staging_chunk

    def instrumented(*args, **kwargs):
        observed.append(kwargs["limit"])
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "fetch_structure_staging_chunk", instrumented)
    chunk = normalize_structure_component_chunk(
        store, publication, "events", None, 1
    )

    assert chunk.source_rows == 1
    assert observed == [1]
    assert max(observed) <= 1


def test_ready_publication_requires_a_later_invocation_to_switch(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window_id = _complete_window(store, "market-1", now_ms=100)

    result: StructurePublicationCheckpoint | object
    for _ in range(40):
        result = run_structure_publication_step(
            settings_for_test, window_id, max_rows=1, max_elapsed_s=60
        )
        if isinstance(result, StructurePublicationCheckpoint) and result.stage == "ready":
            break
    else:
        raise AssertionError("publication never reached ready")

    assert store.current_structure_generation() is None
    published = run_structure_publication_step(
        settings_for_test, window_id, max_rows=1, max_elapsed_s=60
    )
    assert not isinstance(published, StructurePublicationCheckpoint)
    assert store.current_structure_generation()["snapshot_id"] == published.snapshot_id


def test_elapsed_budget_checkpoints_before_starting_a_chunk(
    settings_for_test, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window_id = _complete_window(store, "market-1", now_ms=100)
    clock = iter((0.0, 2.0))
    monkeypatch.setattr(structure_publication_module.time, "monotonic", lambda: next(clock))

    checkpoint = run_structure_publication_step(
        settings_for_test, window_id, max_rows=1, max_elapsed_s=1
    )

    assert isinstance(checkpoint, StructurePublicationCheckpoint)
    assert checkpoint.stage == "normalizing"
    assert checkpoint.rows_processed == 0
    progress = store.get_structure_publication_progress(window_id)
    assert progress is not None and progress.cursor is None


def _event(snapshot_id: int) -> dict[str, object]:
    return {
        "id": "event-1",
        "slug": "event-1",
        "title": "Generation publication event",
        "active": 1,
        "closed": 0,
        "fetched_at_ms": snapshot_id * 1_000,
        "snapshot_id": snapshot_id,
    }


def _membership(snapshot_id: int, market_id: str) -> dict[str, object]:
    return {
        "snapshot_id": snapshot_id,
        "event_id": "event-1",
        "neg_risk_market_id": "group-1",
        "market_id": market_id,
        "member_kind": "named",
        "active": 1,
        "closed": 0,
    }


def _group_truth(
    snapshot_id: int,
    *,
    market_id: str = "new-market",
    expected_member_count: int = 1,
    stored_membership_hash: str | None = None,
) -> dict[str, object]:
    durable_hash = membership_hash(
        "event-1",
        "group-1",
        [EventMember("event-1", "group-1", market_id, "named", True, False)],
    )
    return {
        "snapshot_id": snapshot_id,
        "event_id": "event-1",
        "neg_risk_market_id": "group-1",
        "neg_risk_type": "standard",
        "expected_member_count": expected_member_count,
        "active_named_count": 1,
        "membership_hash": stored_membership_hash or durable_hash,
        "quality": "complete-supported",
        "reason": None,
    }


def _market(market_id: str, snapshot_id: int) -> dict[str, object]:
    return {
        "market_id": market_id,
        "condition_id": f"condition-{market_id}",
        "slug": market_id,
        "question": f"Will {market_id} publish?",
        "yes_token_id": f"yes-{market_id}",
        "no_token_id": f"no-{market_id}",
        "active": 1,
        "closed": 0,
        "neg_risk": 1,
        "neg_risk_market_id": "group-1",
        "fetched_at_ms": snapshot_id * 1_000,
        "snapshot_id": snapshot_id,
        "incomplete": 0,
        "event_id": "event-1",
    }


def _complete_window(store: SQLiteStore, market_id: str, *, now_ms: int) -> str:
    window = store.begin_or_resume_structure_sync(started_at_ms=now_ms)
    window_id = str(window["id"])
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[
            {
                "id": "event-1",
                "slug": "event-1",
                "title": "Generation publication event",
                "active": True,
                "closed": False,
                "negRisk": True,
                "enableNegRisk": True,
                "negRiskAugmented": False,
                "negRiskMarketID": "group-1",
                "markets": [
                    {
                        "id": market_id,
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                ],
            }
        ],
        finished_at_ms=now_ms + 1,
    )
    store.commit_structure_market_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[
            {
                "id": market_id,
                "conditionId": f"condition-{market_id}",
                "slug": market_id,
                "question": f"Will {market_id} publish?",
                "clobTokenIds": f'["yes-{market_id}","no-{market_id}"]',
                "event_id": "event-1",
                "negRisk": True,
                "negRiskMarketID": "group-1",
                "active": True,
                "closed": False,
            }
        ],
        finished_at_ms=now_ms + 2,
    )
    return window_id


def _begin_generation(
    store: SQLiteStore,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
):
    window_id = _complete_window(store, market_id, now_ms=now_ms)
    return store.begin_structure_publication(
        window_id=window_id,
        snapshot_metadata={
            "snapshot_id": snapshot_id,
            "taken_at_ms": now_ms,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=now_ms + 3,
    )


def _append_market(
    store: SQLiteStore,
    publication,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
) -> None:
    store.append_structure_publication_chunk(
        publication_id=publication.publication_id,
        component="markets",
        rows=(_market(market_id, snapshot_id),),
        expected_prior_cursor=None,
        next_cursor=market_id,
        now_ms=now_ms,
    )


def _append_generation_truth(
    store: SQLiteStore,
    publication,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
    expected_member_count: int = 1,
) -> None:
    chunks = (
        ("events", (_event(snapshot_id),), "event-1"),
        (
            "memberships",
            (_membership(snapshot_id, market_id),),
            f"event-1:{market_id}",
        ),
        (
            "group_truth",
            (
                _group_truth(
                    snapshot_id,
                    market_id=market_id,
                    expected_member_count=expected_member_count,
                ),
            ),
            "group-1",
        ),
        ("markets", (_market(market_id, snapshot_id),), market_id),
    )
    expected_prior_cursor = None
    for offset, (component, rows, next_cursor) in enumerate(chunks):
        store.append_structure_publication_chunk(
            publication_id=publication.publication_id,
            component=component,
            rows=rows,
            expected_prior_cursor=expected_prior_cursor,
            next_cursor=next_cursor,
            now_ms=now_ms + offset,
        )
        expected_prior_cursor = next_cursor


def _certify(
    store: SQLiteStore,
    publication,
    *,
    now_ms: int,
    coverage_completed: bool = True,
) -> None:
    store.certify_structure_generation(
        publication_id=publication.publication_id,
        receipt={
            "component_counts": COMPONENT_COUNTS,
            "source_coverage": {
                "completed": coverage_completed,
                "event_items": 1,
                "market_items": 1,
            },
            "membership_validation": {
                "valid": True,
                "expected_member_count": 1,
                "actual_member_count": 1,
            },
            "validation_hash": "a" * 64,
            "certified_at_ms": now_ms,
        },
    )


def _publish_generation(
    store: SQLiteStore,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
):
    publication = _begin_generation(
        store,
        snapshot_id=snapshot_id,
        market_id=market_id,
        now_ms=now_ms,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=snapshot_id,
        market_id=market_id,
        now_ms=now_ms + 4,
    )
    _certify(store, publication, now_ms=now_ms + 8)
    assert (
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=now_ms + 9,
        )
        == snapshot_id
    )
    return publication


def test_bounded_certification_resumes_every_primary_key_checkpoint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=1,
        market_id="market-1",
        now_ms=1_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=1,
        market_id="market-1",
        now_ms=1_004,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-1",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)

    checkpoints: list[tuple[str | None, str | None, int]] = []
    for offset in range(30):
        restarted = SQLiteStore(db_path)
        chunk = restarted.advance_structure_certification_chunk(
            publication.publication_id,
            max_rows=1,
            now_ms=1_011 + offset,
        )
        checkpoints.append((chunk.component, chunk.cursor, chunk.rows_processed))
        if chunk.ready:
            break
    else:
        raise AssertionError("bounded certification never reached ready")

    assert any(component == "memberships" and cursor for component, cursor, _ in checkpoints)
    assert any(component == "group_truth" and cursor for component, cursor, _ in checkpoints)
    assert any(component == "source_events" for component, _, _ in checkpoints)
    assert any(component == "source_markets" for component, _, _ in checkpoints)
    assert store.current_structure_generation() is None
    with sqlite3.connect(db_path) as con:
        status, validation_hash, certification_component = con.execute(
            "SELECT status,validation_hash,certification_component "
            "FROM structure_publications WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone()
    assert status == "ready"
    assert len(validation_hash) == 64
    assert certification_component == "bounded-complete"


def test_certification_start_freezes_every_generation_mutation(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-1", now_ms=1_000
    )
    _append_generation_truth(
        store, publication, snapshot_id=1, market_id="market-1", now_ms=1_004
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-1",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)

    statements = (
        "INSERT INTO structure_generation_issues(snapshot_id,issue_index,layer,category) "
        "VALUES (1,1,1,'schema')",
        "UPDATE structure_generation_events SET title='tampered' WHERE snapshot_id=1",
        "DELETE FROM structure_generation_markets WHERE snapshot_id=1",
    )
    for statement in statements:
        with pytest.raises(sqlite3.IntegrityError, match="structure-generation-frozen"):
            with sqlite3.connect(store.db_path) as con:
                con.execute(statement)


def test_frozen_generation_rejects_snapshot_id_moves_from_every_component(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-1", now_ms=1_000
    )
    _append_generation_truth(
        store, publication, snapshot_id=1, market_id="market-1", now_ms=1_004
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-1",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (2,1,2,'full',1,0,'structure','local','building',0,'')"
        )
        con.execute(
            "INSERT INTO structure_generation_events(snapshot_id,id,slug,fetched_at_ms) "
            "VALUES (2,'u-event','u-event',2)"
        )
        con.execute(
            "INSERT INTO structure_generation_event_tags(snapshot_id,event_id,tag_id,"
            "tag_label,tag_slug) VALUES (2,'u-event','u-tag','U','u')"
        )
        con.execute(
            "INSERT INTO structure_generation_memberships(snapshot_id,event_id,"
            "neg_risk_market_id,market_id,member_kind,active,closed) "
            "VALUES (2,'u-event','u-group','u-market','named',1,0)"
        )
        con.execute(
            "INSERT INTO structure_generation_group_truth(snapshot_id,event_id,"
            "neg_risk_market_id,neg_risk_type,expected_member_count,active_named_count,"
            "membership_hash,quality) VALUES "
            "(2,'u-event','u-group','standard',1,1,'u-hash','complete-supported')"
        )
        con.execute(
            "INSERT INTO structure_generation_markets(snapshot_id,market_id,condition_id,"
            "fetched_at_ms) VALUES (2,'u-market','u-condition',2)"
        )
        con.execute(
            "INSERT INTO structure_generation_issues(snapshot_id,issue_index,layer,"
            "category) VALUES (2,1,1,'schema')"
        )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)

    tables = (
        "events",
        "event_tags",
        "memberships",
        "group_truth",
        "markets",
        "issues",
    )
    with sqlite3.connect(store.db_path) as con:
        frozen_hash = store._generation_hash(con, 1)
        unfrozen_hash = store._generation_hash(con, 2)
        before_counts = {
            table: tuple(
                con.execute(
                    f"SELECT snapshot_id,COUNT(*) FROM structure_generation_{table} "
                    "GROUP BY snapshot_id ORDER BY snapshot_id"
                ).fetchall()
            )
            for table in tables
        }
        pointer = con.execute("SELECT * FROM current_structure_generation").fetchall()

    for table in tables:
        with sqlite3.connect(store.db_path) as con:
            con.execute(
                f"UPDATE structure_generation_{table} SET snapshot_id=2 "
                "WHERE snapshot_id=2"
            )
        with pytest.raises(sqlite3.IntegrityError, match="structure-generation-frozen"):
            with sqlite3.connect(store.db_path) as con:
                con.execute(
                    f"UPDATE structure_generation_{table} SET snapshot_id=1 "
                    "WHERE snapshot_id=2"
                )

    with sqlite3.connect(store.db_path) as con:
        assert store._generation_hash(con, 1) == frozen_hash
        assert store._generation_hash(con, 2) == unfrozen_hash
        assert {
            table: tuple(
                con.execute(
                    f"SELECT snapshot_id,COUNT(*) FROM structure_generation_{table} "
                    "GROUP BY snapshot_id ORDER BY snapshot_id"
                ).fetchall()
            )
            for table in tables
        } == before_counts
        assert con.execute("SELECT * FROM current_structure_generation").fetchall() == pointer


def test_bounded_certification_rejects_generation_source_drift(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-1", now_ms=1_000
    )
    _append_generation_truth(
        store, publication, snapshot_id=1, market_id="market-1", now_ms=1_004
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-1",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_events SET title='source drift' "
            "WHERE snapshot_id=1"
        )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)

    with pytest.raises(ValueError, match="source-truth-invalid"):
        for offset in range(30):
            store.advance_structure_certification_chunk(
                publication.publication_id, max_rows=1, now_ms=1_011 + offset
            )


def test_duplicate_market_uses_first_source_parent_and_never_publishes(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    window_id = str(window["id"])
    events = []
    for event_id, group_id in (("z-first", "group-z"), ("a-second", "group-a")):
        events.append(
            {
                "id": event_id,
                "active": True,
                "closed": False,
                "negRisk": True,
                "enableNegRisk": True,
                "negRiskAugmented": False,
                "negRiskMarketID": group_id,
                "markets": [
                    {
                        "id": "shared-market",
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                ],
            }
        )
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=events,
        finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[
            {
                "id": "shared-market",
                "negRisk": True,
                "negRiskMarketID": "group-z",
                "active": True,
                "closed": False,
            }
        ],
        finished_at_ms=102,
    )

    publication_id = None
    with pytest.raises(ValueError, match="generation-validation-issues|membership-invalid"):
        for _ in range(60):
            checkpoint = run_structure_publication_step(
                settings_for_test, window_id, max_rows=1, max_elapsed_s=60
            )
            if isinstance(checkpoint, StructurePublicationCheckpoint):
                publication_id = checkpoint.publication_id
    assert publication_id is not None
    assert store.structure_event_id_for_market(publication_id, "shared-market") == "z-first"
    assert store.current_structure_generation() is None


def test_worker_entry_migrates_pre_task3_structure_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as con:
        con.executescript(
            "CREATE TABLE snapshots(id INTEGER PRIMARY KEY);"
            "CREATE TABLE structure_sync_windows("
            "id TEXT PRIMARY KEY,status TEXT,event_cursor TEXT,market_cursor TEXT,"
            "started_at_ms INTEGER,checkpoint_at_ms INTEGER,event_pages INTEGER,"
            "market_pages INTEGER,published_snapshot_id INTEGER,failure_reason TEXT);"
            "CREATE TABLE structure_sync_event_staging("
            "window_id TEXT,event_id TEXT,payload_json TEXT,source_cursor TEXT,"
            "PRIMARY KEY(window_id,event_id));"
            "CREATE TABLE structure_sync_market_staging("
            "window_id TEXT,market_id TEXT,payload_json TEXT,source_cursor TEXT,"
            "PRIMARY KEY(window_id,market_id));"
            "CREATE TABLE structure_publications("
            "publication_id TEXT PRIMARY KEY,window_id TEXT,snapshot_id INTEGER,"
            "status TEXT,normalization_component TEXT,normalization_source_cursor TEXT,"
            "write_component TEXT,write_row_cursor TEXT,expected_counts_json TEXT,"
            "committed_counts_json TEXT,validation_hash TEXT,created_at_ms INTEGER,"
            "checkpoint_at_ms INTEGER,certified_at_ms INTEGER,published_at_ms INTEGER,"
            "failure_reason TEXT);"
        )

    store = SQLiteStore(db_path)
    store.init_structure_sync_schema()

    with sqlite3.connect(db_path) as con:
        publication_columns = {
            str(row[1]) for row in con.execute("PRAGMA table_info(structure_publications)")
        }
        event_columns = {
            str(row[1])
            for row in con.execute("PRAGMA table_info(structure_sync_event_staging)")
        }
        event_market_table = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='structure_sync_event_market_staging'"
        ).fetchone()
    assert {
        "write_prior_cursor",
        "certification_component",
        "certification_row_cursor",
        "certification_hash",
        "certification_counts_json",
    } <= publication_columns
    assert "source_ordinal" in event_columns
    assert event_market_table == (1,)


def test_bounded_certification_rejects_stale_membership_hash(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=1,
        market_id="market-1",
        now_ms=1_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=1,
        market_id="market-1",
        now_ms=1_004,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-1",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_group_truth SET membership_hash=? "
            "WHERE snapshot_id=1",
            ("b" * 64,),
        )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)

    with pytest.raises(ValueError, match="membership-invalid"):
        for offset in range(20):
            store.advance_structure_certification_chunk(
                publication.publication_id,
                max_rows=1,
                now_ms=1_011 + offset,
            )


def test_generation_publication_attempt_is_invisible_until_pointer_switch(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )

    _append_market(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_switches_all_reads_after_terminal_receipt(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    _certify(store, publication, now_ms=11_008)

    assert (
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )
        == 11
    )

    assert store.current_structure_generation()["snapshot_id"] == 11
    assert store.current_generation_market_ids() == ("new-market",)


def test_generation_publication_attempt_rolls_back_pointer_switch_exception(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    _certify(store, publication, now_ms=11_008)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "CREATE TRIGGER reject_structure_pointer_switch "
            "BEFORE UPDATE OF snapshot_id ON current_structure_generation "
            "BEGIN SELECT RAISE(ABORT, 'injected-pointer-switch-failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected-pointer-switch-failure"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_rejects_incomplete_source_coverage(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )

    with pytest.raises(ValueError, match="source-coverage-incomplete"):
        _certify(
            store,
            publication,
            now_ms=11_008,
            coverage_completed=False,
        )
    with pytest.raises(ValueError, match="not-ready"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_recomputes_durable_completeness(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_market(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )

    # The default receipt falsely claims complete 1/1 event, membership,
    # group-truth, and market components plus completed source coverage. Only
    # the market component exists durably, so certification must recompute and
    # reject the generation instead of trusting the terminal receipt.
    with pytest.raises(ValueError, match="generation-incomplete"):
        _certify(store, publication, now_ms=11_005)
    with pytest.raises(ValueError, match="not-ready"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_006,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_rejects_invalid_membership_truth(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
        expected_member_count=2,
    )

    # The receipt claims 1/1 valid membership. Certification must re-read the
    # stored group truth (expected=2) instead of trusting caller-supplied counts.
    with pytest.raises(ValueError, match="membership-invalid"):
        _certify(
            store,
            publication,
            now_ms=11_008,
        )
    with pytest.raises(ValueError, match="not-ready"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_rejects_stale_durable_membership_hash(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=11, market_id="new-market", now_ms=11_000
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_group_truth SET membership_hash=? "
            "WHERE snapshot_id=11",
            ("f" * 64,),
        )

    with pytest.raises(ValueError, match="membership-invalid"):
        _certify(store, publication, now_ms=11_008)


def test_generation_chunk_cursor_cas_rejects_stale_and_skipped_callers(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=11, market_id="market-a", now_ms=11_000
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "markets",
        (_market("market-a", 11),),
        expected_prior_cursor=None,
        next_cursor="cursor-1",
        now_ms=11_004,
    )

    for stale_prior, next_cursor in ((None, "cursor-2"), ("cursor-2", "cursor-3")):
        with pytest.raises(StructurePublicationCursorError):
            store.append_structure_publication_chunk(
                publication.publication_id,
                "markets",
                (_market("market-b", 11),),
                expected_prior_cursor=stale_prior,
                next_cursor=next_cursor,
                now_ms=11_005,
            )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT market_id FROM structure_generation_markets WHERE snapshot_id=11"
        ).fetchall() == [("market-a",)]
        assert con.execute(
            "SELECT write_row_cursor FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == ("cursor-1",)


def test_generation_chunk_replay_authenticates_composite_keys_and_full_rows(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=11, market_id="market-a", now_ms=11_000
    )
    tags = (
        {"event_id": "event-1", "tag_id": "shared", "tag_label": "A", "tag_slug": "a"},
        {"event_id": "event-2", "tag_id": "shared", "tag_label": "B", "tag_slug": "b"},
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "event_tags",
        tags,
        expected_prior_cursor=None,
        next_cursor="tags-2",
        now_ms=11_004,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "event_tags",
        tags,
        expected_prior_cursor=None,
        next_cursor="tags-2",
        now_ms=11_005,
    )
    with sqlite3.connect(store.db_path) as con:
        progress_before = con.execute(
            "SELECT write_component,write_prior_cursor,write_row_cursor,"
            "committed_counts_json,checkpoint_at_ms FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone()
    wrong = (dict(tags[0], tag_label="tampered"), tags[1])
    with pytest.raises(StructurePublicationCursorError):
        store.append_structure_publication_chunk(
            publication.publication_id,
            "event_tags",
            wrong,
            expected_prior_cursor=None,
            next_cursor="tags-2",
            now_ms=11_006,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT write_component,write_prior_cursor,write_row_cursor,"
            "committed_counts_json,checkpoint_at_ms FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == progress_before
        assert con.execute(
            "SELECT event_id,tag_id,tag_label FROM structure_generation_event_tags "
            "WHERE snapshot_id=11 ORDER BY event_id"
        ).fetchall() == [
            ("event-1", "shared", "A"),
            ("event-2", "shared", "B"),
        ]


def test_generation_issue_later_chunk_replay_uses_committed_offsets(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=11, market_id="market-a", now_ms=11_000
    )
    issue_1 = {"layer": 1, "category": "schema", "detail": "first"}
    issue_2 = {"layer": 2, "category": "semantic", "detail": "second"}
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (issue_1,),
        expected_prior_cursor=None,
        next_cursor="issue-1",
        now_ms=11_004,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (issue_2,),
        expected_prior_cursor="issue-1",
        next_cursor="issue-2",
        now_ms=11_005,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (issue_2,),
        expected_prior_cursor="issue-1",
        next_cursor="issue-2",
        now_ms=11_006,
    )
    with sqlite3.connect(store.db_path) as con:
        progress_before = con.execute(
            "SELECT write_component,write_prior_cursor,write_row_cursor,"
            "committed_counts_json,checkpoint_at_ms FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone()
    with pytest.raises(StructurePublicationCursorError):
        store.append_structure_publication_chunk(
            publication.publication_id,
            "issues",
            (dict(issue_2, detail="tampered"),),
            expected_prior_cursor="issue-1",
            next_cursor="issue-2",
            now_ms=11_007,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT write_component,write_prior_cursor,write_row_cursor,"
            "committed_counts_json,checkpoint_at_ms FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == progress_before
        assert con.execute(
            "SELECT issue_index,detail FROM structure_generation_issues "
            "WHERE snapshot_id=11 ORDER BY issue_index"
        ).fetchall() == [(1, "first"), (2, "second")]
        assert con.execute(
            "SELECT committed_counts_json,write_row_cursor FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone()[1] == "issue-2"


def test_generation_chunk_cursor_mismatch_rolls_back_rows_and_progress(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="market-a",
        now_ms=11_000,
    )
    _append_market(
        store,
        publication,
        snapshot_id=11,
        market_id="market-a",
        now_ms=11_004,
    )

    with pytest.raises(StructurePublicationCursorError):
        store.append_structure_publication_chunk(
            publication_id=publication.publication_id,
            component="markets",
            rows=(_market("market-b", 11),),
            expected_prior_cursor=None,
            next_cursor="market-a",
            now_ms=11_005,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT market_id FROM structure_generation_markets "
            "WHERE snapshot_id=11 ORDER BY market_id"
        ).fetchall() == [("market-a",)]
        counts_json, cursor = con.execute(
            "SELECT committed_counts_json,write_row_cursor "
            "FROM structure_publications WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone()
    assert '"markets":1' in counts_json
    assert cursor == "market-a"


def test_backfill_current_generation_resumes_bounded_and_switches_last(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "legacy.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (7,1,2,'full',2,1,'structure','legacy','ok',1,'')"
        )
        for market_id in ("market-a", "market-b"):
            con.execute(
                "INSERT INTO markets(market_id,condition_id,fetched_at_ms,snapshot_id,"
                "incomplete) VALUES (?,?,?,?,0)",
                (market_id, f"condition-{market_id}", 2, 7),
            )

    first = store.backfill_current_structure_generation(max_rows=1)
    assert first.complete is False
    assert first.copied_rows == 1
    assert store.current_structure_generation() is None

    second = store.backfill_current_structure_generation(max_rows=1)
    assert second.complete is True
    assert second.copied_rows == 1
    assert store.current_generation_market_ids() == ("market-a", "market-b")
    replay = store.backfill_current_structure_generation(max_rows=1)
    assert replay.complete is True
    assert replay.copied_rows == 0
    assert store.current_generation_market_ids() == ("market-a", "market-b")


def test_backfill_freezes_before_hash_and_resumes_after_hash_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStore(tmp_path / "legacy-freeze.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (7,1,2,'full',1,1,'structure','legacy','ok',1,'')"
        )
        con.execute(
            "INSERT INTO markets(market_id,condition_id,fetched_at_ms,snapshot_id,"
            "incomplete) VALUES ('market-a','condition-a',2,7,0)"
        )

    original_hash = SQLiteStore._legacy_generation_hash
    observed: dict[str, object] = {}

    def crash_before_source_hash(_con: sqlite3.Connection, _snapshot_id: int) -> str:
        with sqlite3.connect(store.db_path) as probe:
            observed["marker"] = probe.execute(
                "SELECT certification_component FROM structure_publications "
                "WHERE publication_id='backfill:7'"
            ).fetchone()
        raise RuntimeError("crash-before-backfill-hash")

    monkeypatch.setattr(
        SQLiteStore, "_legacy_generation_hash", staticmethod(crash_before_source_hash)
    )
    with pytest.raises(RuntimeError, match="crash-before-backfill-hash"):
        store.backfill_current_structure_generation(max_rows=1)

    assert observed["marker"] == ("backfill-frozen",)
    assert store.current_structure_generation() is None

    def source_hash_with_mutation_attempt(
        con: sqlite3.Connection, snapshot_id: int
    ) -> str:
        source_hash = original_hash(con, snapshot_id)
        with sqlite3.connect(store.db_path) as probe:
            try:
                probe.execute(
                    "UPDATE structure_generation_markets SET question='tampered' "
                    "WHERE snapshot_id=7"
                )
            except sqlite3.IntegrityError as exc:
                observed["mutation"] = str(exc)
        observed["source_hash"] = source_hash
        return source_hash

    monkeypatch.setattr(
        SQLiteStore,
        "_legacy_generation_hash",
        staticmethod(source_hash_with_mutation_attempt),
    )
    resumed = SQLiteStore(store.db_path).backfill_current_structure_generation(max_rows=1)
    assert resumed.complete is True
    assert resumed.copied_rows == 0
    assert store.current_generation_market_ids() == ("market-a",)
    assert observed["mutation"] == "structure-generation-frozen"
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT question FROM structure_generation_markets WHERE snapshot_id=7"
        ).fetchone() == (None,)
        assert con.execute(
            "SELECT certification_component FROM structure_publications "
            "WHERE publication_id='backfill:7'"
        ).fetchone() == ("backfill-authenticated",)
    replay = store.backfill_current_structure_generation(max_rows=1)
    assert replay.complete is True
    assert replay.copied_rows == 0


def test_backfill_bounds_every_component_and_advances_deterministically(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "legacy-scale.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (7,1,2,'full',2,1,'structure','legacy','ok',1,'')"
        )
        for index in (1, 2):
            event_id = f"event-{index}"
            market_id = f"market-{index}"
            group_id = f"group-{index}"
            con.execute(
                "INSERT INTO events(id,slug,fetched_at_ms,snapshot_id) VALUES (?,?,2,7)",
                (event_id, event_id),
            )
            con.execute(
                "INSERT INTO event_tags(event_id,tag_id,tag_label,tag_slug,snapshot_id) "
                "VALUES (?,?,?,?,7)",
                (event_id, "shared", f"Tag {index}", f"tag-{index}"),
            )
            con.execute(
                "INSERT INTO event_market_memberships(snapshot_id,event_id,"
                "neg_risk_market_id,market_id,member_kind,active,closed) "
                "VALUES (7,?,?,?,'named',1,0)",
                (event_id, group_id, market_id),
            )
            con.execute(
                "INSERT INTO neg_risk_group_truth(snapshot_id,event_id,"
                "neg_risk_market_id,neg_risk_type,expected_member_count,"
                "active_named_count,membership_hash,quality) "
                "VALUES (7,?,?,'standard',1,1,?,'complete-supported')",
                (event_id, group_id, f"hash-{index}"),
            )
            con.execute(
                "INSERT INTO markets(market_id,condition_id,fetched_at_ms,snapshot_id,"
                "incomplete) VALUES (?,?,?,?,0)",
                (market_id, f"condition-{index}", 2, 7),
            )
            con.execute(
                "INSERT INTO validation_issues(snapshot_id,layer,category,detail) "
                "VALUES (7,1,'schema',?)",
                (f"issue-{index}",),
            )

    destination_tables = (
        "structure_generation_events",
        "structure_generation_event_tags",
        "structure_generation_memberships",
        "structure_generation_group_truth",
        "structure_generation_markets",
        "structure_generation_issues",
    )
    previous_total = 0
    observed_components: list[str] = []
    for _ in range(12):
        checkpoint = store.backfill_current_structure_generation(max_rows=1)
        with sqlite3.connect(store.db_path) as con:
            total = sum(
                int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in destination_tables
            )
            publication = con.execute(
                "SELECT write_component FROM structure_publications "
                "WHERE publication_id='backfill:7'"
            ).fetchone()
        assert total - previous_total <= 1
        previous_total = total
        if publication is not None:
            observed_components.append(str(publication[0]))
        if checkpoint.complete:
            break

    assert checkpoint.complete is True
    assert previous_total == 12
    component_order = {name: index for index, name in enumerate(COMPONENT_COUNTS)}
    assert [component_order[name] for name in observed_components] == sorted(
        component_order[name] for name in observed_components
    )


def test_backfill_resumes_ready_publication_after_pre_switch_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SQLiteStore(tmp_path / "legacy-ready.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (7,1,2,'full',0,1,'structure','legacy','ok',1,'')"
        )

    def _crash_before_switch(publication_id: str, now_ms: int) -> int:
        raise RuntimeError(f"crash:{publication_id}:{now_ms}")

    monkeypatch.setattr(store, "publish_structure_generation", _crash_before_switch)
    with pytest.raises(RuntimeError, match="crash:backfill:7"):
        store.backfill_current_structure_generation(max_rows=1)
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status FROM structure_publications "
            "WHERE publication_id='backfill:7'"
        ).fetchone() == ("ready",)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_windows WHERE id='backfill:7'"
        ).fetchone() == (1,)

    resumed = SQLiteStore(store.db_path).backfill_current_structure_generation(max_rows=1)
    assert resumed.complete is True
    assert resumed.copied_rows == 0
    assert store.current_structure_generation()["snapshot_id"] == 7
