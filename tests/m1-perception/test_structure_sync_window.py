from __future__ import annotations

from polyarb.storage.sqlite_store import SQLiteStore


def test_structure_window_commits_page_and_resumes_exact_successor_cursor(tmp_path) -> None:
    """A restart observes only a fully committed page and its opaque cursor."""
    db_path = tmp_path / "state.db"
    first = SQLiteStore(db_path)
    first.init_schema()

    window = first.begin_or_resume_structure_sync(started_at_ms=100)
    assert window["status"] == "open"
    assert window["event_cursor"] is None

    first.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor="opaque-event-2",
        completed=False,
        events=[{"id": "event-1", "active": True, "closed": False}],
        finished_at_ms=200,
    )

    restarted = SQLiteStore(db_path)
    resumed = restarted.begin_or_resume_structure_sync(started_at_ms=300)

    assert resumed["id"] == window["id"]
    assert resumed["event_cursor"] == "opaque-event-2"
    assert resumed["event_pages"] == 1
    assert restarted.list_staged_structure_events(window["id"]) == [
        {"id": "event-1", "active": True, "closed": False}
    ]


def test_structure_window_stages_markets_only_after_event_coverage_completes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[],
        finished_at_ms=200,
    )

    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor="opaque-market-2",
        completed=False,
        markets=[{"id": "market-1", "active": True, "closed": False}],
        finished_at_ms=300,
    )

    resumed = SQLiteStore(tmp_path / "state.db").begin_or_resume_structure_sync(
        started_at_ms=400
    )
    assert resumed["market_cursor"] == "opaque-market-2"
    assert resumed["market_pages"] == 1
