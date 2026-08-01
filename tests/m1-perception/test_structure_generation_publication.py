"""Executable contracts for invisible, atomic Structure generations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from polyarb.perception.market_truth import EventMember, membership_hash
from polyarb.storage.sqlite_store import SQLiteStore, StructurePublicationCursorError

COMPONENT_COUNTS = {
    "events": 1,
    "event_tags": 0,
    "memberships": 1,
    "group_truth": 1,
    "markets": 1,
    "issues": 0,
}


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
        events=[{"id": "event-1", "active": True, "closed": False}],
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
