"""Executable contracts for invisible, atomic Structure generations."""

from __future__ import annotations

import json
import random
import sqlite3
import time
from pathlib import Path

import pytest

import polyarb.perception.structure_publication as structure_publication_module
from polyarb.perception.market_truth import EventMember, membership_hash
from polyarb.perception.structure_publication import (
    StructurePublicationCheckpoint,
    normalize_structure_component_chunk,
    run_structure_publication_slice,
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

PRODUCTION_CROSS_STREAM_MATRIX = {
    "global_active_open_neg_risk": 48_102,
    "event_active_open_membership": 47_983,
    "both_present": 47_921,
    "global_only": 181,
    "event_only": 62,
    "both_field_mismatch": 0,
    "multi_parent": 0,
}


def test_production_cross_stream_matrix_is_closed_and_mutually_exclusive() -> None:
    matrix = PRODUCTION_CROSS_STREAM_MATRIX
    assert matrix["global_active_open_neg_risk"] == (
        matrix["both_present"] + matrix["global_only"]
    )
    assert matrix["event_active_open_membership"] == (
        matrix["both_present"] + matrix["event_only"]
    )
    assert matrix["global_only"] == 137 + 44
    assert matrix["both_field_mismatch"] == matrix["multi_parent"] == 0


def test_publication_slice_advances_multiple_durable_chunks_before_returning(
    settings_for_test, monkeypatch
) -> None:
    checkpoints = iter(
        (
            StructurePublicationCheckpoint(
                "normalizing", "events", 500, "event-500", "publication-1"
            ),
            StructurePublicationCheckpoint(
                "normalizing", "events", 500, "event-1000", "publication-1"
            ),
            StructurePublicationCheckpoint(
                "certifying", "legacy-universe", 17, "legacy-17", "publication-1"
            ),
        )
    )
    calls: list[tuple[int, float, object]] = []

    def advance(_settings, _window_id, max_rows, remaining_s, *, store=None):
        calls.append((max_rows, remaining_s, store))
        return next(checkpoints)

    ticks = iter((100.0, 100.0, 101.0, 101.0, 102.0, 102.0, 145.0))
    monkeypatch.setattr(structure_publication_module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(
        structure_publication_module, "run_structure_publication_step", advance
    )
    store = object()

    result = run_structure_publication_slice(
        settings_for_test,
        "window-1",
        max_rows=500,
        max_elapsed_s=45.0,
        max_chunks=100,
        store=store,
    )

    assert result == StructurePublicationCheckpoint(
        stage="certifying",
        component="legacy-universe",
        rows_processed=1_017,
        cursor="legacy-17",
        publication_id="publication-1",
        chunks_processed=3,
        elapsed_ms=45_000,
    )
    assert [call[0] for call in calls] == [500, 500, 500]
    assert [call[1] for call in calls] == [45.0, 44.0, 43.0]
    assert all(call[2] is store for call in calls)


def test_publication_slice_stops_before_starting_chunk_at_elapsed_deadline(
    settings_for_test, monkeypatch
) -> None:
    calls = 0

    def advance(_settings, _window_id, _max_rows, _remaining_s, *, store=None):
        nonlocal calls
        calls += 1
        return StructurePublicationCheckpoint(
            "normalizing", "events", 500, f"event-{calls * 500}", "publication-1"
        )

    ticks = iter((100.0, 100.0, 145.0))
    monkeypatch.setattr(structure_publication_module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(
        structure_publication_module, "run_structure_publication_step", advance
    )

    result = run_structure_publication_slice(
        settings_for_test,
        "window-1",
        max_rows=500,
        max_elapsed_s=45.0,
        max_chunks=100,
        store=object(),
    )

    assert calls == 1
    assert result.chunks_processed == 1
    assert result.elapsed_ms == 45_000


def test_publication_slice_rejects_mixed_publication_identity(
    settings_for_test, monkeypatch
) -> None:
    checkpoints = iter(
        (
            StructurePublicationCheckpoint(
                "normalizing", "events", 500, "event-500", "publication-1"
            ),
            StructurePublicationCheckpoint(
                "normalizing", "events", 500, "event-1000", "publication-2"
            ),
        )
    )
    ticks = iter((100.0, 100.0, 101.0, 101.0))
    monkeypatch.setattr(structure_publication_module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(
        structure_publication_module,
        "run_structure_publication_step",
        lambda *_args, **_kwargs: next(checkpoints),
    )

    with pytest.raises(ValueError, match="structure-publication-identity-changed"):
        run_structure_publication_slice(
            settings_for_test,
            "window-1",
            max_rows=500,
            max_elapsed_s=45.0,
            max_chunks=100,
            store=object(),
        )


def test_publication_slice_rejects_more_than_500_source_rows(
    settings_for_test,
) -> None:
    with pytest.raises(ValueError, match="invalid-structure-publication-slice-budget"):
        run_structure_publication_slice(
            settings_for_test,
            "window-1",
            max_rows=501,
            max_elapsed_s=45.0,
        )

    with pytest.raises(ValueError, match="invalid-structure-publication-budget"):
        run_structure_publication_step(
            settings_for_test,
            "window-1",
            501,
            45.0,
        )


def test_publication_slice_emits_committed_progress_markers(
    settings_for_test, monkeypatch, capsys
) -> None:
    checkpoints = iter(
        (
            StructurePublicationCheckpoint(
                "normalizing", "events", 500, "event-500", "publication-1"
            ),
            StructurePublicationCheckpoint(
                "ready", None, 0, None, "publication-1"
            ),
        )
    )
    monkeypatch.setattr(
        structure_publication_module,
        "run_structure_publication_step",
        lambda *_args, **_kwargs: next(checkpoints),
    )

    result = run_structure_publication_slice(
        settings_for_test,
        "window-1",
        max_rows=500,
        max_elapsed_s=45.0,
        store=object(),
    )

    assert result.chunks_processed == 2
    assert capsys.readouterr().err.splitlines() == [
        "snapshot-stage stage=persist state=start elapsed_ms=0",
        "structure-publication-progress stage=normalizing component=events "
        "chunks=1 rows=500",
        "structure-publication-progress stage=ready component=none chunks=2 rows=500",
    ]


def test_group_truth_500_event_chunk_uses_bounded_bulk_duplicate_lookup(
    tmp_path: Path, monkeypatch
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events = []
    markets = []
    for index in range(500):
        market_id = "market-shared" if index < 2 else f"market-{index:03d}"
        events.append(
            {
                "id": f"event-{index:03d}",
                "negRisk": True,
                "enableNegRisk": True,
                "negRiskMarketID": f"group-{index:03d}",
                "markets": [{"id": market_id, "active": True, "closed": False}],
            }
        )
        if index != 1:
            markets.append({"id": market_id, "active": True, "closed": False})
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=markets, finished_at_ms=300,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=400
    )["completed"] is True
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": 500,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=500,
    )
    event_ids = [f"event-{index:03d}" for index in range(500)] + ["missing-event"]
    expected = {
        event_id
        for event_id in event_ids
        if store.structure_event_has_duplicate_market(publication.publication_id, event_id)
    }

    real_connect = sqlite3.connect
    connect_count = 0

    def counted_connect(*args, **kwargs):
        nonlocal connect_count
        connect_count += 1
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", counted_connect)
    started = time.monotonic()
    actual = store.structure_events_with_duplicate_markets(
        publication.publication_id, event_ids[:500]
    )
    elapsed_s = time.monotonic() - started

    assert actual == expected == {"event-000", "event-001"}
    assert connect_count == 1
    assert elapsed_s < 10.0


def test_publication_slice_with_insufficient_remaining_time_is_zero_chunk(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=1,
        market_id="market-1",
        now_ms=100,
    )
    before = store.get_structure_publication_progress(publication.window_id)

    result = run_structure_publication_slice(
        type("Settings", (), {"db_path": store.db_path})(),
        publication.window_id,
        max_rows=500,
        max_elapsed_s=9.0,
        store=store,
    )
    after = store.get_structure_publication_progress(publication.window_id)

    assert result.chunks_processed == 0
    assert result.rows_processed == 0
    assert result.stage == "normalizing"
    assert result.component == "events"
    assert after == before


def test_market_500_row_chunk_bulk_resolves_parents_with_o1_connections(
    tmp_path: Path, monkeypatch
) -> None:
    store, publication = _begin_large_generation(store_path=tmp_path / "state.db")
    market_ids = [f"market-{index:03d}" for index in range(500)]
    expected = {
        market_id: event_id
        for market_id in market_ids
        if (event_id := store.structure_event_id_for_market(
            publication.publication_id, market_id
        )) is not None
    }
    assert expected["market-000"] == "event-000"

    actual = store.structure_event_ids_for_markets(
        publication.publication_id, market_ids
    )
    assert actual == expected

    for component in ("events", "event_tags", "memberships", "group_truth"):
        _normalize_component_to_done(store, publication, component)
    real_connect = sqlite3.connect
    connect_count = 0

    def counted_connect(*args, **kwargs):
        nonlocal connect_count
        connect_count += 1
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", counted_connect)
    started = time.monotonic()
    chunk = normalize_structure_component_chunk(
        store, publication, "markets", None, 500
    )
    elapsed_s = time.monotonic() - started

    assert chunk.source_rows == 500
    assert connect_count <= 6
    assert elapsed_s < 10.0


def test_non_market_normalizers_keep_500_row_chunks_o1_in_sqlite_connections(
    tmp_path: Path, monkeypatch
) -> None:
    store, publication = _begin_large_generation(store_path=tmp_path / "state.db")
    real_connect = sqlite3.connect

    for component in ("events", "event_tags", "memberships", "group_truth"):
        connect_count = 0

        def counted_connect(*args, **kwargs):
            nonlocal connect_count
            connect_count += 1
            return real_connect(*args, **kwargs)

        with monkeypatch.context() as scoped:
            scoped.setattr(sqlite3, "connect", counted_connect)
            started = time.monotonic()
            first = normalize_structure_component_chunk(
                store, publication, component, None, 500
            )
            elapsed_s = time.monotonic() - started
        assert first.source_rows == 500
        assert connect_count <= 6
        assert elapsed_s < 10.0
        completed = normalize_structure_component_chunk(
            store, publication, component, first.cursor, 500
        )
        assert completed.completed is True


def test_issues_checkpoint_advances_500_source_keys_when_no_duplicates(
    tmp_path: Path, monkeypatch,
) -> None:
    store, publication = _begin_large_generation(
        store_path=tmp_path / "state.db", event_count=501
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets"
    ):
        _normalize_component_to_done(store, publication, component)

    real_connect = sqlite3.connect
    connect_count = 0

    def counted_connect(*args, **kwargs):
        nonlocal connect_count
        connect_count += 1
        return real_connect(*args, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(sqlite3, "connect", counted_connect)
        started = time.monotonic()
        first = normalize_structure_component_chunk(
            store, publication, "issues", None, 500
        )
        elapsed_s = time.monotonic() - started
    reopened = SQLiteStore(store.db_path)
    second = normalize_structure_component_chunk(
        reopened, publication, "issues", first.cursor, 500
    )

    assert first.source_rows == 500
    assert first.canonical_rows == 0
    assert first.completed is False
    assert first.cursor == "market-499"
    assert connect_count <= 5
    assert elapsed_s < 10.0
    assert second.source_rows == 1
    assert second.canonical_rows == 0
    assert second.completed is True
    assert second.cursor == "market-500"


def test_issues_duplicates_cross_500_key_boundary_without_restart_loss(
    tmp_path: Path,
) -> None:
    store, publication = _begin_large_generation(
        store_path=tmp_path / "state.db",
        event_count=501,
        duplicate_market_indexes=(499, 500),
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets"
    ):
        _normalize_component_to_done(store, publication, component)

    first = normalize_structure_component_chunk(
        store, publication, "issues", None, 500
    )
    reopened = SQLiteStore(store.db_path)
    second = normalize_structure_component_chunk(
        reopened, publication, "issues", first.cursor, 500
    )
    with sqlite3.connect(store.db_path) as con:
        issue_markets = con.execute(
            "SELECT market_id FROM structure_generation_issues "
            "WHERE snapshot_id=? ORDER BY issue_index",
            (publication.snapshot_id,),
        ).fetchall()

    assert (first.source_rows, first.canonical_rows, first.cursor, first.completed) == (
        500, 1, "market-499", False
    )
    assert (second.source_rows, second.canonical_rows, second.cursor, second.completed) == (
        1, 1, "market-500", True
    )
    assert issue_markets == [("market-499",), ("market-500",)]


@pytest.mark.parametrize(
    ("raw_market", "raw_events", "expected_market_count", "reason_prefix"),
    (
        pytest.param(
            {
                "id": "orphan-neg-risk",
                "active": True,
                "closed": False,
                "negRisk": True,
                "negRiskMarketID": "inactive-parent-group",
            },
            [],
            0,
            "active-open-neg-risk-market-parent-absent-from-active-event-catalogue",
            id="inactive-parent-not-visible",
        ),
        pytest.param(
            {
                "id": "missing-group",
                "active": True,
                "closed": False,
                "negRisk": True,
                "negRiskMarketID": None,
            },
            [{"id": "event-1", "markets": [{"id": "missing-group"}]}],
            0,
            "active-open-neg-risk-market-missing-group-identity",
            id="missing-group-identity",
        ),
        pytest.param(
            {
                "id": "ordinary-orphan",
                "active": True,
                "closed": False,
                "negRisk": False,
                "negRiskMarketID": None,
            },
            [],
            1,
            None,
            id="ordinary-orphan-remains-visible",
        ),
        pytest.param(
            {
                "id": "recovered-market",
                "active": True,
                "closed": False,
                "negRisk": True,
                "negRiskMarketID": "recovered-group",
            },
            [
                {
                    "id": "recovered-event",
                    "negRisk": True,
                    "enableNegRisk": True,
                    "negRiskAugmented": False,
                    "negRiskMarketID": "recovered-group",
                    "markets": [
                        {
                            "id": "recovered-market",
                            "active": True,
                            "closed": False,
                            "negRiskOther": False,
                        }
                    ],
                }
            ],
            1,
            None,
            id="parent-restored-next-generation",
        ),
    ),
)
def test_structure_quarantine_is_exact_and_source_authenticated(
    tmp_path: Path,
    raw_market: dict[str, object],
    raw_events: list[dict[str, object]],
    expected_market_count: int,
    reason_prefix: str | None,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=raw_events,
        finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[raw_market],
        finished_at_ms=102,
    )
    while not store.advance_structure_event_market_backfill(
        window_id=window["id"],
        max_events=500,
        max_relationships=500,
        now_ms=103,
    )["completed"]:
        pass
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": 1_000,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events",
        "event_tags",
        "memberships",
        "group_truth",
        "markets",
        "issues",
    ):
        _normalize_component_to_done(store, publication, component)

    with sqlite3.connect(store.db_path) as con:
        markets = con.execute(
            "SELECT market_id FROM structure_generation_markets WHERE snapshot_id=1"
        ).fetchall()
        issues = con.execute(
            "SELECT market_id,detail,raw_payload FROM structure_generation_issues "
            "WHERE snapshot_id=1"
        ).fetchall()
    assert len(markets) == expected_market_count
    if reason_prefix is None:
        assert issues == []
    else:
        assert len(issues) == 1
        assert issues[0][0] == raw_market["id"]
        assert str(issues[0][2]).startswith(f"{reason_prefix}:")
        assert len(str(issues[0][2]).rsplit(":", 1)[1]) == 64

    store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
    observed_source_market = False
    for offset in range(30):
        chunk = SQLiteStore(store.db_path).advance_structure_certification_chunk(
            publication.publication_id,
            max_rows=500,
            now_ms=201 + offset,
        )
        if chunk.component == "source_markets" and chunk.rows_processed == 1:
            observed_source_market = True
            break
    assert observed_source_market is True


def test_structure_quarantine_exactly_equals_production_shaped_184_row_difference(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    orphan_markets = [
        {
            "id": f"orphan-{index:03d}",
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskMarketID": f"inactive-parent-{index:03d}",
        }
        for index in range(140)
    ]
    missing_group_markets = [
        {
            "id": f"missing-group-{index:03d}",
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskMarketID": None,
        }
        for index in range(44)
    ]
    events = [
        {
            "id": f"event-{index:03d}",
            "markets": [{"id": f"missing-group-{index:03d}"}],
        }
        for index in range(44)
    ]
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[*orphan_markets, *missing_group_markets],
        finished_at_ms=102,
    )
    while not store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"]:
        pass
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": 1_000,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)

    with sqlite3.connect(store.db_path) as con:
        source_minus_generation = {
            str(row[0])
            for row in con.execute(
                "SELECT market_id FROM structure_sync_market_staging WHERE window_id=? "
                "EXCEPT SELECT market_id FROM structure_generation_markets "
                "WHERE snapshot_id=1",
                (window["id"],),
            ).fetchall()
        }
        issues = con.execute(
            "SELECT market_id,raw_payload FROM structure_generation_issues "
            "WHERE snapshot_id=1 ORDER BY market_id"
        ).fetchall()
    issue_ids = {str(row[0]) for row in issues}
    assert len(source_minus_generation) == 184
    assert issue_ids == source_minus_generation
    assert sum("parent-absent" in str(row[1]) for row in issues) == 140
    assert sum("missing-group-identity" in str(row[1]) for row in issues) == 44


def test_event_only_active_member_is_quarantined_with_recomputed_group_truth(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    raw_event = {
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
                "id": "both-present",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            },
            {
                "id": "event-only",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            },
        ],
    }
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[raw_event], finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        markets=[{
            "id": "both-present",
            "conditionId": "condition-both",
            "slug": "both-present",
            "question": "Both present?",
            "clobTokenIds": '["yes-both","no-both"]',
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskMarketID": "group-1",
        }],
        finished_at_ms=102,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"] is True
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": 1_000,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)

    with sqlite3.connect(store.db_path) as con:
        memberships = con.execute(
            "SELECT market_id FROM structure_generation_memberships "
            "WHERE snapshot_id=1 ORDER BY market_id"
        ).fetchall()
        truth = con.execute(
            "SELECT expected_member_count,active_named_count,membership_hash "
            "FROM structure_generation_group_truth WHERE snapshot_id=1"
        ).fetchone()
        issue = con.execute(
            "SELECT market_id,raw_payload FROM structure_generation_issues "
            "WHERE snapshot_id=1"
        ).fetchone()
    expected_member = EventMember(
        "event-1", "group-1", "both-present", "named", True, False
    )
    assert memberships == [("both-present",)]
    assert truth == (
        1,
        1,
        membership_hash("event-1", "group-1", [expected_member]),
    )
    assert issue is not None
    assert issue[0] == "event-only"
    assert str(issue[1]).startswith(
        "active-open-neg-risk-event-member-absent-from-complete-market-catalogue:"
    )
    assert len(str(issue[1]).rsplit(":", 1)[1]) == 64

    store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
    observed_source_event = False
    for offset in range(30):
        chunk = store.advance_structure_certification_chunk(
            publication.publication_id, max_rows=500, now_ms=201 + offset
        )
        if chunk.component == "source_events" and chunk.rows_processed == 1:
            observed_source_event = True
            break
    assert observed_source_event is True


def test_event_only_quarantine_evidence_is_exact_and_forgery_fails() -> None:
    raw_event = {
        "id": "event-1",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskMarketID": "group-1",
        "markets": [{
            "id": "event-only", "active": True, "closed": False,
            "negRiskOther": False,
        }],
    }
    issue = structure_publication_module.event_only_member_quarantine_issue(
        raw_event, event_source_ordinal=17, market_id="event-only"
    )
    assert issue is not None
    baseline = issue["raw_payload"]
    for changed in (
        ({**raw_event, "title": "payload drift"}, 17),
        (raw_event, 18),
        ({**raw_event, "negRiskMarketID": "other-group"}, 17),
        ({**raw_event, "markets": [{
            **raw_event["markets"][0], "closed": True,
        }]}, 17),
    ):
        forged = structure_publication_module.event_only_member_quarantine_issue(
            changed[0], event_source_ordinal=changed[1], market_id="event-only"
        )
        assert forged is None or forged["raw_payload"] != baseline


def test_event_projection_does_not_filter_non_active_open_anti_join_candidate() -> None:
    raw_event = {
        "id": "event-1",
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "group-1",
        "markets": [{
            "id": "closed-event-only", "active": True, "closed": True,
            "negRiskOther": False,
        }],
    }
    members, truths = structure_publication_module.project_event_structure(
        raw_event, frozenset({"closed-event-only"})
    )
    assert [member.market_id for member in members] == ["closed-event-only"]
    assert truths[0].expected_member_count == 1


def test_duplicate_parent_event_only_candidate_remains_membership_invalid(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events = [
        {
            "id": f"event-{index}", "slug": f"event-{index}",
            "negRisk": True, "enableNegRisk": True,
            "negRiskAugmented": False, "negRiskMarketID": f"group-{index}",
            "markets": [{
                "id": "shared-market", "active": True, "closed": False,
                "negRiskOther": False,
            }],
        }
        for index in range(2)
    ]
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=102,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"] is True
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1, "taken_at_ms": 1_000, "mode": "full",
            "data_product": "structure", "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)
    store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
    with pytest.raises(ValueError, match="membership-invalid"):
        for offset in range(20):
            store.advance_structure_certification_chunk(
                publication.publication_id, max_rows=500, now_ms=201 + offset
            )


@pytest.mark.parametrize(
    ("event_active", "event_closed", "market_active", "market_closed"),
    ((True, False, False, True), (False, True, True, False)),
)
@pytest.mark.parametrize("certification_path", ("bounded", "legacy"))
def test_both_present_status_mismatch_remains_membership_invalid(
    tmp_path: Path,
    event_active: bool,
    event_closed: bool,
    market_active: bool,
    market_closed: bool,
    certification_path: str,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[{
            "id": "event-1", "slug": "event-1", "negRisk": True,
            "enableNegRisk": True, "negRiskAugmented": False,
            "negRiskMarketID": "group-1", "markets": [{
                "id": "both-present", "active": event_active,
                "closed": event_closed, "negRiskOther": False,
            }],
        }], finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[{
            "id": "both-present", "conditionId": "condition-1",
            "slug": "both-present", "question": "Both present?",
            "clobTokenIds": '["yes","no"]', "active": market_active,
            "closed": market_closed, "negRisk": True,
            "negRiskMarketID": "group-1",
        }], finished_at_ms=102,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"] is True
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1, "taken_at_ms": 1_000, "mode": "full",
            "data_product": "structure", "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)
    if certification_path == "bounded":
        store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
        with pytest.raises(ValueError, match="membership-invalid"):
            for offset in range(20):
                store.advance_structure_certification_chunk(
                    publication.publication_id, max_rows=500, now_ms=201 + offset
                )
    else:
        with pytest.raises(ValueError, match="membership-invalid"):
            store.certify_structure_generation(
                publication_id=publication.publication_id,
                receipt={
                    "source_coverage": {
                        "completed": True, "event_items": 1, "market_items": 1,
                    },
                    "validation_hash": "a" * 64,
                    "certified_at_ms": 200,
                },
            )


def test_event_only_issue_source_keyset_is_bounded_at_500(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events = [
        {
            "id": f"event-{index:03d}", "negRisk": True, "enableNegRisk": True,
            "negRiskAugmented": False, "negRiskMarketID": f"group-{index:03d}",
            "markets": [{
                "id": f"event-only-{index:03d}", "active": True,
                "closed": False, "negRiskOther": False,
            }],
        }
        for index in range(501)
    ]
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=102,
    )
    while not store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"]:
        pass
    first = store.fetch_structure_issue_source_chunk(
        window_id=window["id"], after_market_id=None, limit=500
    )
    second = store.fetch_structure_issue_source_chunk(
        window_id=window["id"], after_market_id=first[-1][0], limit=500
    )
    assert len(first) == 500
    assert len(second) == 1
    assert first[-1][0] < second[0][0]
    assert {row[1]["source_kind"] for row in [*first, *second]} == {"event_only"}


def test_production_shaped_62_event_only_members_across_14_groups_certify(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events: list[dict[str, object]] = []
    markets: list[dict[str, object]] = []
    event_only_total = 0
    for event_index in range(14):
        event_id = f"event-{event_index:02d}"
        group_id = f"group-{event_index:02d}"
        both_id = f"both-{event_index:02d}"
        absent_count = 5 if event_index < 6 else 4
        embedded = [{
            "id": both_id, "active": True, "closed": False,
            "negRiskOther": False,
        }]
        for member_index in range(absent_count):
            embedded.append({
                "id": f"event-only-{event_index:02d}-{member_index}",
                "active": True,
                "closed": False,
                "negRiskOther": False,
            })
            event_only_total += 1
        events.append({
            "id": event_id, "slug": event_id, "active": True, "closed": False,
            "negRisk": True, "enableNegRisk": True, "negRiskAugmented": False,
            "negRiskMarketID": group_id, "markets": embedded,
        })
        markets.append({
            "id": both_id, "conditionId": f"condition-{both_id}", "slug": both_id,
            "question": f"Will {both_id}?",
            "clobTokenIds": f'["yes-{both_id}","no-{both_id}"]',
            "active": True, "closed": False, "negRisk": True,
            "negRiskMarketID": group_id,
        })
    assert event_only_total == 62
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=markets, finished_at_ms=102,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"] is True
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1, "taken_at_ms": 1_000, "mode": "full",
            "data_product": "structure", "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_memberships WHERE snapshot_id=1"
        ).fetchone() == (14,)
        assert con.execute(
            "SELECT COUNT(*),SUM(expected_member_count),SUM(active_named_count) "
            "FROM structure_generation_group_truth WHERE snapshot_id=1"
        ).fetchone() == (14, 14, 14)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_issues WHERE snapshot_id=1 "
            "AND raw_payload LIKE ?",
            (f"{structure_publication_module.EVENT_ONLY_NEG_RISK_QUARANTINE_REASON}:%",),
        ).fetchone() == (62,)
    store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
    observed = 0
    for offset in range(30):
        chunk = store.advance_structure_certification_chunk(
            publication.publication_id, max_rows=500, now_ms=201 + offset
        )
        if chunk.component == "source_events":
            observed += chunk.rows_processed
        if observed == 14:
            break
    assert observed == 14


def test_forged_quarantine_issue_remains_fatal(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-active", now_ms=1_000
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=1,
        market_id="market-active",
        now_ms=1_004,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (
            {
                "layer": 1,
                "category": "api_jitter",
                "market_id": "market-active",
                "detail": "forged quarantine",
                "raw_payload": (
                    "active-open-neg-risk-market-missing-group-identity:" + "0" * 64
                ),
            },
        ),
        expected_prior_cursor="market-active",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    with sqlite3.connect(store.db_path) as con:
        counts = json.loads(
            con.execute(
                "SELECT committed_counts_json FROM structure_publications "
                "WHERE publication_id=?",
                (publication.publication_id,),
            ).fetchone()[0]
        )
        con.execute(
            "UPDATE structure_publications SET expected_counts_json=? "
            "WHERE publication_id=?",
            (json.dumps(counts, sort_keys=True), publication.publication_id),
        )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)

    with pytest.raises(ValueError, match="generation-validation-issues"):
        for offset in range(20):
            store.advance_structure_certification_chunk(
                publication.publication_id, max_rows=500, now_ms=1_011 + offset
            )


def test_source_market_difference_without_exact_quarantine_evidence_fails_closed(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[], finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        markets=[{
            "id": "orphan", "active": True, "closed": False,
            "negRisk": True, "negRiskMarketID": "group-1",
        }],
        finished_at_ms=102,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"] is True
    zero_counts = {component: 0 for component in COMPONENT_COUNTS}
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1, "taken_at_ms": 1_000, "mode": "full",
            "data_product": "structure", "expected_counts": zero_counts,
        },
        now_ms=104,
    )
    cursor = None
    for offset, component in enumerate(COMPONENT_COUNTS):
        next_cursor = f"{component}|done"
        store.append_structure_publication_chunk(
            publication.publication_id,
            component,
            (),
            expected_prior_cursor=cursor,
            next_cursor=next_cursor,
            now_ms=105 + offset,
        )
        cursor = next_cursor
    store.seal_structure_publication_counts(publication.publication_id, now_ms=120)

    with pytest.raises(ValueError, match="source-truth-invalid"):
        for offset in range(20):
            store.advance_structure_certification_chunk(
                publication.publication_id, max_rows=500, now_ms=121 + offset
            )


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


@pytest.mark.asyncio
async def test_actual_comparison_transition_checkpoint_is_accepted_by_parent(
    settings_for_test,
) -> None:
    from polyarb.daemon.scheduler import (
        IsolatedStructurePublicationCheckpoint,
        run_snapshot_in_subprocess,
    )

    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window_id = _complete_window(store, "market-1", now_ms=100)
    for _ in range(40):
        checkpoint = run_structure_publication_step(
            settings_for_test, window_id, max_rows=1, max_elapsed_s=60
        )
        if (
            isinstance(checkpoint, StructurePublicationCheckpoint)
            and checkpoint.stage == "certifying"
            and checkpoint.component == "legacy-universe"
        ):
            break
    else:
        raise AssertionError("actual comparison transition checkpoint not emitted")

    class Process:
        returncode = 0
        stderr = b""

        async def communicate(self):
            return json.dumps(
                {"checkpointed": True, **checkpoint.__dict__}
            ).encode(), self.stderr

    async def spawn(*_args, **_kwargs):
        return Process()

    parsed = await run_snapshot_in_subprocess(spawn=spawn)
    assert isinstance(parsed, IsolatedStructurePublicationCheckpoint)
    assert parsed.component == "legacy-universe"


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
    bootstrap = store.advance_structure_event_market_backfill(
        window_id=window_id,
        max_events=10,
        max_relationships=10,
        now_ms=now_ms + 3,
    )
    assert bootstrap["completed"] is True
    return window_id


def _begin_large_generation(
    *,
    store_path: Path,
    event_count: int = 500,
    duplicate_market_indexes: tuple[int, ...] = (),
):
    store = SQLiteStore(store_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events = [
        {
            "id": f"event-{index:03d}",
            "negRisk": True,
            "enableNegRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": f"group-{index:03d}",
            "markets": [
                {
                    "id": f"market-{index:03d}",
                    "active": True,
                    "closed": False,
                    "negRiskOther": False,
                }
            ],
        }
        for index in range(event_count)
    ]
    events.extend(
        {
            "id": f"event-duplicate-{index:03d}",
            "negRisk": True,
            "enableNegRisk": True,
            "negRiskMarketID": f"group-duplicate-{index:03d}",
            "markets": [
                {
                    "id": f"market-{index:03d}",
                    "active": True,
                    "closed": False,
                }
            ],
        }
        for index in duplicate_market_indexes
    )
    markets = [
        {
            "id": f"market-{index:03d}",
            "active": True,
            "closed": False,
            "negRisk": True,
            "negRiskMarketID": f"group-{index:03d}",
        }
        for index in range(event_count)
    ]
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=markets, finished_at_ms=300,
    )
    now_ms = 400
    while True:
        bootstrap = store.advance_structure_event_market_backfill(
            window_id=window["id"],
            max_events=500,
            max_relationships=500,
            now_ms=now_ms,
        )
        now_ms += 1
        if bootstrap["completed"]:
            break
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": now_ms,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=now_ms,
    )
    return store, publication


def _normalize_component_to_done(
    store: SQLiteStore,
    publication,
    component: str,
) -> None:
    cursor = None
    while True:
        chunk = normalize_structure_component_chunk(
            store, publication, component, cursor, 500
        )
        if chunk.completed:
            return
        cursor = chunk.cursor


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
    for offset in range(20):
        chunk = store.advance_structure_certification_chunk(
            publication.publication_id,
            max_rows=1,
            now_ms=now_ms + offset + 1,
        )
        if chunk.ready:
            return
    raise AssertionError("comparison certification never reached ready")


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


@pytest.mark.parametrize("stored_contract", (None, "legacy-contract-v1"))
def test_incompatible_publication_contract_is_atomically_superseded_and_idempotent(
    tmp_path: Path,
    stored_contract: str | None,
) -> None:
    """Production-shaped 846 must retire without touching serving truth or rows."""
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=845,
        market_id="serving-market",
        now_ms=1_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=846,
        market_id="candidate-market",
        now_ms=2_000,
    )
    for component in structure_publication_module.STRUCTURE_COMPONENTS:
        _normalize_component_to_done(store, publication, component)
    store.seal_structure_publication_counts(publication.publication_id, now_ms=2_100)

    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=? "
            "WHERE publication_id=?",
            (stored_contract, publication.publication_id),
        )
        generation_before = {
            component: con.execute(
                f"SELECT * FROM structure_generation_{component} "
                "WHERE snapshot_id=846 ORDER BY rowid"
            ).fetchall()
            for component in structure_publication_module.STRUCTURE_COMPONENTS
        }

    first = store.reconcile_structure_publication_contract(
        window_id=publication.window_id,
        current_version=STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
        now_ms=2_200,
    )
    second = store.reconcile_structure_publication_contract(
        window_id=publication.window_id,
        current_version=STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
        now_ms=2_201,
    )

    assert first == second
    assert first.publication_id == publication.publication_id
    assert first.compatible is False
    assert first.superseded is True
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status,failure_reason FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == ("failed", "publication-contract-superseded")
        assert con.execute(
            "SELECT snapshot_status,is_valid,market_view_published FROM snapshots "
            "WHERE id=846"
        ).fetchone() == ("failed", 0, 0)
        assert con.execute(
            "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
            (publication.window_id,),
        ).fetchone() == ("failed", "publication-contract-superseded")
        assert con.execute(
            "SELECT snapshot_id FROM current_structure_generation WHERE id=1"
        ).fetchone() == (845,)
        generation_after = {
            component: con.execute(
                f"SELECT * FROM structure_generation_{component} "
                "WHERE snapshot_id=846 ORDER BY rowid"
            ).fetchall()
            for component in structure_publication_module.STRUCTURE_COMPONENTS
        }
    assert generation_after == generation_before


def test_matching_publication_contract_resumes_without_mutation(tmp_path: Path) -> None:
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=1,
        market_id="candidate-market",
        now_ms=1_000,
    )

    result = store.reconcile_structure_publication_contract(
        window_id=publication.window_id,
        current_version=STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
        now_ms=1_100,
    )

    assert result.publication_id == publication.publication_id
    assert result.compatible is True
    assert result.superseded is False
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status,normalization_contract_version,checkpoint_at_ms "
            "FROM structure_publications WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == (
            "writing",
            STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
            1_003,
        )


def test_publication_step_returns_controlled_contract_supersession_checkpoint(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=1,
        market_id="candidate-market",
        now_ms=1_000,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=NULL "
            "WHERE publication_id=?",
            (publication.publication_id,),
        )

    result = run_structure_publication_step(
        settings_for_test,
        publication.window_id,
        max_rows=1,
        max_elapsed_s=60,
        store=store,
    )

    assert result == StructurePublicationCheckpoint(
        stage="superseded",
        component=None,
        rows_processed=0,
        cursor=None,
        publication_id=publication.publication_id,
    )


@pytest.mark.parametrize(
    ("candidate_snapshot_id", "successor_snapshot_id"),
    ((846, 847), (847, 848)),
)
def test_fresh_source_window_reserves_next_id_only_after_contract_supersession(
    settings_for_test,
    candidate_snapshot_id: int,
    successor_snapshot_id: int,
) -> None:
    """N retires the old contract; N+1 collects fresh truth before reservation."""
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=845,
        market_id="serving-market",
        now_ms=1_000,
    )
    stale = _begin_generation(
        store,
        snapshot_id=candidate_snapshot_id,
        market_id="stale-market",
        now_ms=2_000,
    )
    store.upsert_scheduler_state(state="RECOVERING", failure_counter=261)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=NULL "
            "WHERE publication_id=?",
            (stale.publication_id,),
        )

    retired = run_structure_publication_step(
        settings_for_test,
        stale.window_id,
        max_rows=1,
        max_elapsed_s=60,
        store=store,
    )
    assert isinstance(retired, StructurePublicationCheckpoint)
    assert retired.stage == "superseded"

    successor = store.begin_or_resume_structure_sync(started_at_ms=3_000)
    assert successor["id"] != stale.window_id
    assert successor["status"] == "open"
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT 1 FROM snapshots WHERE id=?", (successor_snapshot_id,)
        ).fetchone() is None
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
            (successor["id"],),
        ).fetchone() == (0,)

    fresh_window_id = str(successor["id"])
    store.commit_structure_event_page(
        window_id=fresh_window_id,
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
                        "id": "serving-market",
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                ],
            }
        ],
        finished_at_ms=3_100,
    )
    store.commit_structure_market_page(
        window_id=fresh_window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[
            {
                "id": "serving-market",
                "conditionId": "condition-serving-market",
                "slug": "serving-market",
                "question": "Will serving-market publish?",
                "clobTokenIds": '["yes-serving-market","no-serving-market"]',
                "event_id": "event-1",
                "negRisk": True,
                "negRiskMarketID": "group-1",
                "active": True,
                "closed": False,
            }
        ],
        finished_at_ms=3_200,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=fresh_window_id,
        max_events=10,
        max_relationships=10,
        now_ms=3_300,
    )["completed"] is True
    assert store.current_structure_generation()["snapshot_id"] == 845
    assert store.get_scheduler_state()["failure_counter"] == 261

    terminal = None
    for _ in range(80):
        terminal = run_structure_publication_step(
            settings_for_test,
            fresh_window_id,
            max_rows=1,
            max_elapsed_s=60,
            store=store,
        )
        if not isinstance(terminal, StructurePublicationCheckpoint):
            break
    else:
        raise AssertionError(f"fresh {successor_snapshot_id} never published")

    assert terminal is not None
    assert terminal.snapshot_id == successor_snapshot_id
    assert store.current_structure_generation()["snapshot_id"] == successor_snapshot_id
    assert store.current_generation_market_ids() == ("serving-market",)
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT window_id,normalization_contract_version "
            "FROM structure_publications WHERE snapshot_id=?",
            (successor_snapshot_id,),
        ).fetchone() == (
            fresh_window_id,
            STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
        )
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_markets "
            "WHERE snapshot_id=? AND market_id='stale-market'",
            (successor_snapshot_id,),
        ).fetchone() == (0,)


@pytest.mark.parametrize("publication_status", ("writing", "ready"))
def test_active_snapshot_status_backfill_does_not_fail_building_generation(
    tmp_path: Path,
    publication_status: str,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=846,
        market_id="candidate-market",
        now_ms=2_000,
    )
    if publication_status == "ready":
        with sqlite3.connect(store.db_path) as con:
            con.execute(
                "UPDATE structure_publications SET status='ready' WHERE publication_id=?",
                (publication.publication_id,),
            )

    store.init_schema()

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT snapshot_status,is_valid,market_view_published FROM snapshots "
            "WHERE id=846"
        ).fetchone() == ("building", 0, 0)
        assert con.execute(
            "SELECT status FROM structure_publications WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == (publication_status,)


def test_existing_split_contract_state_repairs_without_rewriting_snapshot_evidence(
    settings_for_test,
) -> None:
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=845,
        market_id="serving-market",
        now_ms=1_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=846,
        market_id="candidate-market",
        now_ms=2_000,
    )
    _normalize_component_to_done(store, publication, "events")
    historical_finished_at_ms = 1_754_000_066_000
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE snapshots SET snapshot_status='failed',finished_at_ms=? WHERE id=846",
            (historical_finished_at_ms,),
        )
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=NULL "
            "WHERE publication_id=?",
            (publication.publication_id,),
        )
        generation_before = con.execute(
            "SELECT * FROM structure_generation_events WHERE snapshot_id=846"
        ).fetchall()

    result = run_structure_publication_step(
        settings_for_test,
        publication.window_id,
        max_rows=1,
        max_elapsed_s=60,
        store=store,
    )

    assert isinstance(result, StructurePublicationCheckpoint)
    assert result.stage == "superseded"
    replay = run_structure_publication_step(
        settings_for_test,
        publication.window_id,
        max_rows=1,
        max_elapsed_s=60,
        store=store,
    )
    assert replay == result
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status,failure_reason FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == ("failed", "publication-contract-superseded")
        assert con.execute(
            "SELECT snapshot_status,finished_at_ms,is_valid,market_view_published "
            "FROM snapshots WHERE id=846"
        ).fetchone() == ("failed", historical_finished_at_ms, 0, 0)
        assert con.execute(
            "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
            (publication.window_id,),
        ).fetchone() == ("failed", "publication-contract-superseded")
        assert con.execute(
            "SELECT snapshot_id FROM current_structure_generation WHERE id=1"
        ).fetchone() == (845,)
        assert con.execute(
            "SELECT * FROM structure_generation_events WHERE snapshot_id=846"
        ).fetchall() == generation_before

    successor = store.begin_or_resume_structure_sync(started_at_ms=1_754_012_000_001)
    assert successor["id"] != publication.window_id
    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT 1 FROM snapshots WHERE id=847").fetchone() is None
    fresh_window_id = _complete_window(
        store,
        "serving-market",
        now_ms=1_754_012_000_002,
    )
    assert fresh_window_id == successor["id"]
    run_structure_publication_step(
        settings_for_test,
        fresh_window_id,
        max_rows=1,
        max_elapsed_s=60,
        store=store,
    )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT snapshot_id,normalization_contract_version "
            "FROM structure_publications WHERE window_id=?",
            (fresh_window_id,),
        ).fetchone() == (847, STRUCTURE_NORMALIZATION_CONTRACT_VERSION)
        assert con.execute(
            "SELECT snapshot_id FROM current_structure_generation WHERE id=1"
        ).fetchone() == (845,)


def test_contract_supersession_rolls_back_every_row_on_late_window_failure(
    tmp_path: Path,
) -> None:
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store,
        snapshot_id=846,
        market_id="candidate-market",
        now_ms=2_000,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=NULL "
            "WHERE publication_id=?",
            (publication.publication_id,),
        )
        con.execute(
            "CREATE TRIGGER reject_supersession_window_update "
            "BEFORE UPDATE OF status ON structure_sync_windows "
            "WHEN NEW.status='failed' BEGIN "
            "SELECT RAISE(ABORT,'injected-late-window-failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected-late-window-failure"):
        store.reconcile_structure_publication_contract(
            publication.window_id,
            STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
            now_ms=2_100,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status,failure_reason FROM structure_publications "
            "WHERE publication_id=?",
            (publication.publication_id,),
        ).fetchone() == ("writing", None)
        assert con.execute(
            "SELECT snapshot_status,finished_at_ms FROM snapshots WHERE id=846"
        ).fetchone() == ("building", 2_003)
        assert con.execute(
            "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
            (publication.window_id,),
        ).fetchone() == ("complete", None)


@pytest.mark.parametrize(
    "unsafe_case",
    (
        "snapshot-valid",
        "snapshot-published",
        "pointer-candidate",
        "window-not-complete",
        "current-contract",
        "unexpected-failure-reason",
    ),
)
def test_other_partial_contract_states_remain_fail_closed(
    tmp_path: Path,
    unsafe_case: str,
) -> None:
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=845,
        market_id="serving-market",
        now_ms=1_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=846,
        market_id="candidate-market",
        now_ms=2_000,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE snapshots SET snapshot_status='failed' WHERE id=846"
        )
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=NULL "
            "WHERE publication_id=?",
            (publication.publication_id,),
        )
        if unsafe_case == "snapshot-valid":
            con.execute("UPDATE snapshots SET is_valid=1 WHERE id=846")
        elif unsafe_case == "snapshot-published":
            con.execute("UPDATE snapshots SET market_view_published=1 WHERE id=846")
        elif unsafe_case == "pointer-candidate":
            con.execute(
                "UPDATE current_structure_generation SET snapshot_id=?,publication_id=? "
                "WHERE id=1",
                (846, publication.publication_id),
            )
        elif unsafe_case == "window-not-complete":
            con.execute(
                "UPDATE structure_sync_windows SET status='failed',"
                "failure_reason='different-failure' WHERE id=?",
                (publication.window_id,),
            )
        elif unsafe_case == "current-contract":
            con.execute(
                "UPDATE structure_publications SET normalization_contract_version=? "
                "WHERE publication_id=?",
                (STRUCTURE_NORMALIZATION_CONTRACT_VERSION, publication.publication_id),
            )
        elif unsafe_case == "unexpected-failure-reason":
            con.execute(
                "UPDATE structure_publications SET status='failed',"
                "failure_reason='different-failure' WHERE publication_id=?",
                (publication.publication_id,),
            )
        before = {
            "publication": con.execute(
                "SELECT status,failure_reason,normalization_contract_version "
                "FROM structure_publications WHERE publication_id=?",
                (publication.publication_id,),
            ).fetchone(),
            "snapshot": con.execute(
                "SELECT snapshot_status,is_valid,market_view_published,finished_at_ms "
                "FROM snapshots WHERE id=846"
            ).fetchone(),
            "window": con.execute(
                "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
                (publication.window_id,),
            ).fetchone(),
            "pointer": con.execute(
                "SELECT snapshot_id,publication_id FROM current_structure_generation "
                "WHERE id=1"
            ).fetchone(),
        }

    with pytest.raises(
        ValueError,
        match=(
            "structure-publication-supersession-unsafe|"
            "structure-publication-contract-not-reconcilable"
        ),
    ):
        store.reconcile_structure_publication_contract(
            publication.window_id,
            STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
            now_ms=2_100,
        )

    with sqlite3.connect(store.db_path) as con:
        after = {
            "publication": con.execute(
                "SELECT status,failure_reason,normalization_contract_version "
                "FROM structure_publications WHERE publication_id=?",
                (publication.publication_id,),
            ).fetchone(),
            "snapshot": con.execute(
                "SELECT snapshot_status,is_valid,market_view_published,finished_at_ms "
                "FROM snapshots WHERE id=846"
            ).fetchone(),
            "window": con.execute(
                "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
                (publication.window_id,),
            ).fetchone(),
            "pointer": con.execute(
                "SELECT snapshot_id,publication_id FROM current_structure_generation "
                "WHERE id=1"
            ).fetchone(),
        }
    assert after == before


@pytest.mark.parametrize(
    "corruption",
    (
        "current-contract",
        "wrong-product",
        "snapshot-valid",
        "snapshot-published",
        "pointer-candidate",
    ),
)
def test_idempotent_supersession_rejects_corrupt_terminal_postcondition(
    tmp_path: Path,
    corruption: str,
) -> None:
    from polyarb.perception.structure_contract import (
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    )

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=845,
        market_id="serving-market",
        now_ms=1_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=846,
        market_id="candidate-market",
        now_ms=2_000,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version=NULL "
            "WHERE publication_id=?",
            (publication.publication_id,),
        )
    assert store.reconcile_structure_publication_contract(
        publication.window_id,
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
        now_ms=2_100,
    ).superseded is True

    with sqlite3.connect(store.db_path) as con:
        if corruption == "current-contract":
            con.execute(
                "UPDATE structure_publications SET normalization_contract_version=? "
                "WHERE publication_id=?",
                (STRUCTURE_NORMALIZATION_CONTRACT_VERSION, publication.publication_id),
            )
        elif corruption == "wrong-product":
            con.execute(
                "UPDATE snapshots SET data_product='legacy_combined' WHERE id=846"
            )
        elif corruption == "snapshot-valid":
            con.execute("UPDATE snapshots SET is_valid=1 WHERE id=846")
        elif corruption == "snapshot-published":
            con.execute("UPDATE snapshots SET market_view_published=1 WHERE id=846")
        elif corruption == "pointer-candidate":
            con.execute(
                "UPDATE current_structure_generation SET snapshot_id=?,publication_id=? "
                "WHERE id=1",
                (846, publication.publication_id),
            )
        before = {
            "publication": con.execute(
                "SELECT * FROM structure_publications WHERE publication_id=?",
                (publication.publication_id,),
            ).fetchone(),
            "snapshot": con.execute(
                "SELECT * FROM snapshots WHERE id=846"
            ).fetchone(),
            "window": con.execute(
                "SELECT * FROM structure_sync_windows WHERE id=?",
                (publication.window_id,),
            ).fetchone(),
            "pointer": con.execute(
                "SELECT * FROM current_structure_generation WHERE id=1"
            ).fetchone(),
        }

    with pytest.raises(
        ValueError,
        match="structure-publication-supersession-incomplete",
    ):
        store.reconcile_structure_publication_contract(
            publication.window_id,
            STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
            now_ms=2_200,
        )

    with sqlite3.connect(store.db_path) as con:
        after = {
            "publication": con.execute(
                "SELECT * FROM structure_publications WHERE publication_id=?",
                (publication.publication_id,),
            ).fetchone(),
            "snapshot": con.execute(
                "SELECT * FROM snapshots WHERE id=846"
            ).fetchone(),
            "window": con.execute(
                "SELECT * FROM structure_sync_windows WHERE id=?",
                (publication.window_id,),
            ).fetchone(),
            "pointer": con.execute(
                "SELECT * FROM current_structure_generation WHERE id=1"
            ).fetchone(),
        }
    assert after == before


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
    assert all(rows_processed <= 1 for _, _, rows_processed in checkpoints)
    assert any(component == "group_truth" and cursor for component, cursor, _ in checkpoints)
    assert any(component == "source_events" for component, _, _ in checkpoints)
    assert any(component == "source_markets" for component, _, _ in checkpoints)
    assert any(component == "generation-universe" for component, _, _ in checkpoints)
    assert any(
        component == "generation-universe" and cursor
        for component, cursor, _ in checkpoints
    )
    assert any(component == "legacy-rejections" for component, _, _ in checkpoints)
    assert any(component == "generation-rejections" for component, _, _ in checkpoints)
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
    with sqlite3.connect(db_path) as con:
        receipt = con.execute(
            "SELECT legacy_universe_hash,generation_universe_hash,receipt_digest "
            "FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        ).fetchone()
    assert receipt is not None and len(receipt[2]) == 64
    assert receipt[0] != receipt[1]

    statements: list[str] = []
    store.publish_structure_generation(
        publication.publication_id,
        now_ms=2_000,
        trace_callback=statements.append,
    )
    pointer_sql = "\n".join(statements).lower()
    assert "count(" not in pointer_sql
    assert "from structure_generation_markets" not in pointer_sql
    assert "from event_market_memberships" not in pointer_sql


def test_membership_certification_restarts_across_500_row_boundary(
    tmp_path: Path,
) -> None:
    store, publication = _begin_large_generation(
        store_path=tmp_path / "state.db", event_count=501
    )
    for component in (
        "events",
        "event_tags",
        "memberships",
        "group_truth",
        "markets",
        "issues",
    ):
        _normalize_component_to_done(store, publication, component)
    counts = {
        "events": 501,
        "event_tags": 0,
        "memberships": 501,
        "group_truth": 501,
        "markets": 501,
        "issues": 0,
    }
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_publications SET expected_counts_json=? "
            "WHERE publication_id=?",
            (json.dumps(counts, sort_keys=True), publication.publication_id),
        )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_000)

    observed = []
    for offset in range(10):
        chunk = SQLiteStore(store.db_path).advance_structure_certification_chunk(
            publication.publication_id,
            max_rows=500,
            now_ms=1_001 + offset,
        )
        if chunk.component == "memberships" and chunk.rows_processed:
            observed.append(chunk)
        if chunk.component == "group_truth":
            break

    assert [(item.rows_processed, item.cursor) for item in observed] == [
        (500, '["event-499","market-499"]'),
        (1, '["event-500","market-500"]'),
    ]
    assert chunk.component == "group_truth"


def test_source_certification_prefetches_each_500_row_chunk_in_o1_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, publication = _begin_large_generation(
        store_path=tmp_path / "state.db", event_count=500
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)
    store.seal_structure_publication_counts(publication.publication_id, now_ms=1_000)
    now_ms = 1_001
    while store.structure_certification_checkpoint(publication.publication_id)[0] != (
        "source_events"
    ):
        store.advance_structure_certification_chunk(
            publication.publication_id, max_rows=500, now_ms=now_ms
        )
        now_ms += 1

    statements: list[str] = []
    real_connect = sqlite3.connect
    with real_connect(store.db_path) as limit_con:
        assert limit_con.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) >= 504

    def traced_connect(*args, **kwargs):
        con = real_connect(*args, **kwargs)
        con.set_trace_callback(statements.append)
        return con

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    started = time.monotonic()
    event_chunk = store.advance_structure_certification_chunk(
        publication.publication_id, max_rows=500, now_ms=now_ms
    )
    event_elapsed = time.monotonic() - started
    event_selects = [
        statement for statement in statements if statement.lstrip().upper().startswith("SELECT")
    ]
    assert event_chunk.rows_processed == 500
    assert event_elapsed < 10.0
    assert len(event_selects) <= 10

    statements.clear()
    now_ms += 1
    store.advance_structure_certification_chunk(
        publication.publication_id, max_rows=500, now_ms=now_ms
    )
    statements.clear()
    now_ms += 1
    started = time.monotonic()
    market_chunk = store.advance_structure_certification_chunk(
        publication.publication_id, max_rows=500, now_ms=now_ms
    )
    market_elapsed = time.monotonic() - started
    market_selects = [
        statement for statement in statements if statement.lstrip().upper().startswith("SELECT")
    ]
    assert market_chunk.rows_processed == 500
    assert market_elapsed < 10.0
    assert len(market_selects) <= 8


def test_bulk_source_certification_matches_per_event_reference_projection(
    tmp_path: Path,
) -> None:
    rng = random.Random(848)
    store = SQLiteStore(tmp_path / "differential.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events: list[dict[str, object]] = []
    markets: list[dict[str, object]] = []
    for event_index in range(100):
        event_id = f"event-{event_index:03d}"
        group_id = f"group-{event_index:03d}"
        embedded: list[dict[str, object]] = []
        for member_index in range(rng.randint(1, 6)):
            market_id = f"market-{event_index:03d}-{member_index}"
            shape = rng.choice(("both", "event-only", "closed-event-only"))
            active, closed = (True, False) if shape != "closed-event-only" else (True, True)
            embedded.append({
                "id": market_id,
                "active": active,
                "closed": closed,
                "negRiskOther": False,
            })
            if shape == "both":
                markets.append({
                    "id": market_id,
                    "conditionId": f"condition-{market_id}",
                    "slug": market_id,
                    "question": f"Will {market_id}?",
                    "clobTokenIds": f'["yes-{market_id}","no-{market_id}"]',
                    "active": active,
                    "closed": closed,
                    "negRisk": True,
                    "negRiskMarketID": group_id,
                })
        events.append({
            "id": event_id,
            "slug": event_id,
            "negRisk": True,
            "enableNegRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": group_id,
            "markets": embedded,
        })
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=markets, finished_at_ms=102,
    )
    while not store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"]:
        pass
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1, "taken_at_ms": 1_000, "mode": "full",
            "data_product": "structure", "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)

    with sqlite3.connect(store.db_path) as con:
        for raw_event in events:
            event_id = str(raw_event["id"])
            anti_join = frozenset(
                str(row[0])
                for row in con.execute(
                    "SELECT relation.market_id FROM "
                    "structure_sync_event_market_staging relation LEFT JOIN "
                    "structure_sync_market_staging market ON "
                    "market.window_id=relation.window_id AND "
                    "market.market_id=relation.market_id WHERE "
                    "relation.window_id=? AND relation.event_id=? "
                    "AND market.market_id IS NULL ORDER BY relation.market_id",
                    (window["id"], event_id),
                ).fetchall()
            )
            expected_members, expected_truths = (
                structure_publication_module.project_event_structure(raw_event, anti_join)
            )
            actual_members = con.execute(
                "SELECT market_id FROM structure_generation_memberships "
                "WHERE snapshot_id=1 AND event_id=? ORDER BY market_id",
                (event_id,),
            ).fetchall()
            actual_truths = con.execute(
                "SELECT expected_member_count,active_named_count,membership_hash "
                "FROM structure_generation_group_truth WHERE snapshot_id=1 "
                "AND event_id=? ORDER BY neg_risk_market_id",
                (event_id,),
            ).fetchall()
            assert actual_members == [(member.market_id,) for member in expected_members]
            assert actual_truths == [
                (
                    truth.expected_member_count,
                    truth.active_named_count,
                    truth.membership_hash,
                )
                for truth in expected_truths
            ]

    store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
    for offset in range(30):
        chunk = store.advance_structure_certification_chunk(
            publication.publication_id, max_rows=500, now_ms=201 + offset
        )
        if chunk.component == "comparison":
            break
    else:
        raise AssertionError("bulk source certification did not reach comparison")


@pytest.mark.parametrize("drift_status", ("failed", "published"))
def test_bulk_event_only_evidence_rejects_post_seal_window_status_drift(
    tmp_path: Path,
    drift_status: str,
) -> None:
    store = SQLiteStore(tmp_path / "window-drift.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[{
            "id": "event-1", "slug": "event-1", "negRisk": True,
            "enableNegRisk": True, "negRiskAugmented": False,
            "negRiskMarketID": "group-1", "markets": [{
                "id": "event-only", "active": True, "closed": False,
                "negRiskOther": False,
            }],
        }], finished_at_ms=101,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=102,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=103
    )["completed"] is True
    publication = store.begin_structure_publication(
        window_id=window["id"],
        snapshot_metadata={
            "snapshot_id": 1, "taken_at_ms": 1_000, "mode": "full",
            "data_product": "structure", "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=104,
    )
    for component in (
        "events", "event_tags", "memberships", "group_truth", "markets", "issues"
    ):
        _normalize_component_to_done(store, publication, component)
    store.seal_structure_publication_counts(publication.publication_id, now_ms=200)
    now_ms = 201
    while store.structure_certification_checkpoint(publication.publication_id)[0] != (
        "source_events"
    ):
        store.advance_structure_certification_chunk(
            publication.publication_id, max_rows=500, now_ms=now_ms
        )
        now_ms += 1
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET status=?,failure_reason='drift' "
            "WHERE id=?",
            (drift_status, window["id"]),
        )
    with pytest.raises(ValueError, match="source-truth-invalid"):
        store.advance_structure_certification_chunk(
            publication.publication_id, max_rows=500, now_ms=now_ms
        )


def test_certification_keysets_legacy_null_source_ordinals(tmp_path: Path) -> None:
    """Legacy NULL ordinals must not make certification skip the remaining source."""
    store = SQLiteStore(tmp_path / "null-ordinal.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-1", now_ms=1_000
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
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_event_staging_update_guard")
        con.execute("DROP TRIGGER trg_structure_event_staging_insert_guard")
        con.execute(
            "UPDATE structure_sync_event_staging SET source_ordinal=NULL "
            "WHERE window_id=?",
            (publication.window_id,),
        )
        con.execute(
            "INSERT INTO structure_sync_event_staging("
            "window_id,event_id,payload_json,source_cursor,source_ordinal) "
            "VALUES (?, 'event-2', ?, NULL, NULL)",
            (publication.window_id, json.dumps({"id": "event-2", "markets": []})),
        )
    store.init_structure_sync_schema()

    with pytest.raises(ValueError, match="source-truth-invalid"):
        for offset in range(20):
            store.advance_structure_certification_chunk(
                publication.publication_id,
                max_rows=1,
                now_ms=1_011 + offset,
            )


def test_comparison_certification_rejects_pinned_legacy_identity_drift(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "legacy-drift.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (50,1,2,'full',0,1,'structure','legacy','ok',1,'')"
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage(snapshot_id,completed,market_items,"
            "event_items) VALUES (50,1,0,0)"
        )
    publication = _begin_generation(
        store,
        snapshot_id=51,
        market_id="market-51",
        now_ms=51_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=51,
        market_id="market-51",
        now_ms=51_004,
    )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-51",
        next_cursor="issues|done",
        now_ms=51_009,
    )
    store.seal_structure_publication_counts(publication.publication_id, now_ms=51_010)
    for offset in range(30):
        SQLiteStore(store.db_path).advance_structure_certification_chunk(
            publication.publication_id,
            max_rows=1,
            now_ms=51_011 + offset,
        )
        checkpoint = store.structure_certification_checkpoint(
            publication.publication_id
        )
        if checkpoint is not None and checkpoint[0] == "comparison":
            break
    else:
        raise AssertionError("comparison phase did not start")

    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (52,3,4,'full',0,1,'structure','legacy','ok',1,'')"
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage(snapshot_id,completed,market_items,"
            "event_items) VALUES (52,1,0,0)"
        )

    with pytest.raises(ValueError, match="structure-comparison-legacy-drift"):
        SQLiteStore(store.db_path).advance_structure_certification_chunk(
            publication.publication_id,
            max_rows=1,
            now_ms=52_000,
        )


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
    assert store.advance_structure_event_market_backfill(
        window_id=window_id, max_events=10, max_relationships=10, now_ms=103
    )["completed"] is True

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
            "CREATE TABLE structure_sync_event_market_backfill_progress("
            "window_id TEXT PRIMARY KEY,after_rowid INTEGER NOT NULL DEFAULT 0,"
            "events_processed INTEGER NOT NULL DEFAULT 0,checkpoint_at_ms INTEGER,"
            "completed_at_ms INTEGER,blocked_reason TEXT);"
            "CREATE TABLE structure_publications("
            "publication_id TEXT PRIMARY KEY,window_id TEXT,snapshot_id INTEGER,"
            "status TEXT,normalization_component TEXT,normalization_source_cursor TEXT,"
            "write_component TEXT,write_row_cursor TEXT,expected_counts_json TEXT,"
            "committed_counts_json TEXT,validation_hash TEXT,created_at_ms INTEGER,"
            "checkpoint_at_ms INTEGER,certified_at_ms INTEGER,published_at_ms INTEGER,"
            "failure_reason TEXT);"
        )
        con.execute(
            "INSERT INTO structure_sync_windows VALUES "
            "('legacy-window','complete',NULL,NULL,100,300,1,1,NULL,NULL)"
        )
        con.execute(
            "INSERT INTO structure_sync_event_staging VALUES (?,?,?,?)",
            (
                "legacy-window",
                "z-event",
                json.dumps({"id": "z-event", "markets": [{"id": "market-z"}]}),
                None,
            ),
        )
        con.execute(
            "INSERT INTO structure_sync_event_staging VALUES (?,?,?,?)",
            (
                "legacy-window",
                "a-event",
                json.dumps({"id": "a-event", "markets": [{"id": "market-a"}]}),
                None,
            ),
        )
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress "
            "VALUES ('legacy-window',1,1,200,NULL,NULL)"
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
        progress_columns = {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_sync_event_market_backfill_progress)"
            )
        }
    assert {
        "write_prior_cursor",
        "normalization_contract_version",
        "certification_component",
        "certification_row_cursor",
        "certification_hash",
        "certification_counts_json",
    } <= publication_columns
    assert "source_ordinal" in event_columns
    assert event_market_table == (1,)
    assert {
        "window_checkpoint_at_ms",
        "event_cursor",
        "member_offset",
        "relationships_processed",
        "migration_reason",
    } <= progress_columns
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT event_cursor,member_offset,events_processed,completed_at_ms,"
            "migration_reason FROM structure_sync_event_market_backfill_progress "
            "WHERE window_id='legacy-window'"
        ).fetchone() == ("", 0, 0, None, "legacy-after-rowid-rewound")

    first = store.advance_structure_event_market_backfill(
        window_id="legacy-window", max_events=1, max_relationships=1, now_ms=400
    )
    second = store.advance_structure_event_market_backfill(
        window_id="legacy-window", max_events=1, max_relationships=1, now_ms=500
    )
    assert first["event_cursor"] == "a-event"
    assert second["completed"] is True
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT event_id,market_id FROM structure_sync_event_market_staging "
            "WHERE window_id='legacy-window' ORDER BY event_id"
        ).fetchall() == [("a-event", "market-a"), ("z-event", "market-z")]


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


@pytest.mark.parametrize("certification_path", ("bounded", "legacy"))
@pytest.mark.parametrize(
    ("member_kind", "member_active", "member_closed"),
    (
        pytest.param("inactive-reserved", False, False, id="inactive-open"),
        pytest.param("named", True, True, id="active-closed"),
    ),
)
def test_membership_certification_allows_non_open_member_absent_from_active_stream(
    tmp_path: Path,
    certification_path: str,
    member_kind: str,
    member_active: bool,
    member_closed: bool,
) -> None:
    """Event structure retains inactive members that /markets intentionally omits."""
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window_id = _complete_window(store, "market-active", now_ms=1_000)
    active = EventMember(
        "event-1", "group-1", "market-active", "named", True, False
    )
    reserved = EventMember(
        "event-1",
        "group-1",
        "market-reserved",
        member_kind,
        member_active,
        member_closed,
    )
    counts = {**COMPONENT_COUNTS, "memberships": 2}
    publication = store.begin_structure_publication(
        window_id=window_id,
        snapshot_metadata={
            "snapshot_id": 1,
            "taken_at_ms": 1_000,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": counts,
        },
        now_ms=1_003,
    )
    chunks = (
        ("events", (_event(1),), "event-1"),
        (
            "memberships",
            (
                _membership(1, "market-active"),
                {
                    **_membership(1, "market-reserved"),
                    "member_kind": member_kind,
                    "active": int(member_active),
                    "closed": int(member_closed),
                },
            ),
            "event-1:market-reserved",
        ),
        (
            "group_truth",
            (
                {
                        **_group_truth(1),
                        "expected_member_count": 2,
                        "active_named_count": 1 + int(
                            member_kind == "named" and member_active
                        ),
                    "membership_hash": membership_hash(
                        "event-1", "group-1", [active, reserved]
                    ),
                    "quality": "complete-unsupported",
                    "reason": "standard-neg-risk-has-non-tradable-members",
                },
            ),
            "group-1",
        ),
        ("markets", (_market("market-active", 1),), "market-active"),
        ("issues", (), "issues|done"),
    )
    cursor = None
    for offset, (component, rows, next_cursor) in enumerate(chunks):
        store.append_structure_publication_chunk(
            publication.publication_id,
            component,
            rows,
            expected_prior_cursor=cursor,
            next_cursor=next_cursor,
            now_ms=1_004 + offset,
        )
        cursor = next_cursor
    if certification_path == "legacy":
        store.certify_structure_generation(
            publication_id=publication.publication_id,
            receipt={
                "source_coverage": {
                    "completed": True,
                    "event_items": 1,
                    "market_items": 1,
                },
                "validation_hash": "a" * 64,
                "certified_at_ms": 1_010,
            },
        )
        assert store.structure_certification_checkpoint(publication.publication_id)[0] == (
            "comparison"
        )
    else:
        store.seal_structure_publication_counts(
            publication.publication_id, now_ms=1_010
        )
        for offset in range(10):
            chunk = store.advance_structure_certification_chunk(
                publication.publication_id, max_rows=500, now_ms=1_011 + offset
            )
            if chunk.component == "group_truth":
                break

        assert chunk.component == "group_truth"


@pytest.mark.parametrize("certification_path", ("bounded", "legacy"))
def test_membership_certification_rejects_active_open_member_without_market(
    tmp_path: Path, certification_path: str,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-active", now_ms=1_000
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=1,
        market_id="market-active",
        now_ms=1_004,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_markets SET market_id='different-market' "
            "WHERE snapshot_id=1"
        )

    if certification_path == "bounded":
        store.append_structure_publication_chunk(
            publication.publication_id,
            "issues",
            (),
            expected_prior_cursor="market-active",
            next_cursor="issues|done",
            now_ms=1_009,
        )
        store.seal_structure_publication_counts(publication.publication_id, now_ms=1_010)
        with pytest.raises(ValueError, match="membership-invalid"):
            for offset in range(10):
                store.advance_structure_certification_chunk(
                    publication.publication_id,
                    max_rows=500,
                    now_ms=1_011 + offset,
                )
    else:
        with pytest.raises(ValueError, match="membership-invalid"):
            store.certify_structure_generation(
                publication_id=publication.publication_id,
                receipt={
                    "source_coverage": {
                        "completed": True,
                        "event_items": 1,
                        "market_items": 1,
                    },
                    "validation_hash": "a" * 64,
                    "certified_at_ms": 1_010,
                },
            )


@pytest.mark.parametrize("certification_path", ("bounded", "legacy"))
def test_membership_certification_rejects_existing_market_identity_mismatch(
    tmp_path: Path, certification_path: str,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    publication = _begin_generation(
        store, snapshot_id=1, market_id="market-active", now_ms=1_000
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=1,
        market_id="market-active",
        now_ms=1_004,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_markets SET neg_risk_market_id='wrong-group' "
            "WHERE snapshot_id=1"
        )
    store.append_structure_publication_chunk(
        publication.publication_id,
        "issues",
        (),
        expected_prior_cursor="market-active",
        next_cursor="issues|done",
        now_ms=1_009,
    )
    if certification_path == "bounded":
        store.seal_structure_publication_counts(
            publication.publication_id, now_ms=1_010
        )
        with pytest.raises(ValueError, match="membership-invalid"):
            for offset in range(10):
                SQLiteStore(store.db_path).advance_structure_certification_chunk(
                    publication.publication_id,
                    max_rows=500,
                    now_ms=1_011 + offset,
                )
    else:
        with pytest.raises(ValueError, match="membership-invalid"):
            store.certify_structure_generation(
                publication_id=publication.publication_id,
                receipt={
                    "source_coverage": {
                        "completed": True,
                        "event_items": 1,
                        "market_items": 1,
                    },
                    "validation_hash": "a" * 64,
                    "certified_at_ms": 1_010,
                },
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
    assert second.complete is False
    assert second.copied_rows == 1
    for _ in range(40):
        second = SQLiteStore(store.db_path).backfill_current_structure_generation(
            max_rows=1
        )
        if second.complete:
            break
    assert second.complete is True
    assert store.current_generation_market_ids() == ("market-a", "market-b")
    replay = store.backfill_current_structure_generation(max_rows=1)
    assert replay.complete is True
    assert replay.copied_rows == 0
    assert store.current_generation_market_ids() == ("market-a", "market-b")


def test_backfill_certification_never_calls_one_shot_hash_helpers(
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

    def forbidden_hash(*_args, **_kwargs) -> str:
        raise AssertionError("backfill invoked one-shot hash helper")

    monkeypatch.setattr(
        SQLiteStore, "_legacy_generation_hash", staticmethod(forbidden_hash)
    )
    monkeypatch.setattr(
        SQLiteStore,
        "_generation_hash",
        staticmethod(forbidden_hash),
    )
    for _ in range(40):
        resumed = SQLiteStore(store.db_path).backfill_current_structure_generation(
            max_rows=1
        )
        if resumed.complete:
            break
    assert resumed.complete is True
    assert store.current_generation_market_ids() == ("market-a",)
    with sqlite3.connect(store.db_path) as con:
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
    for _ in range(80):
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
        for _ in range(40):
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
