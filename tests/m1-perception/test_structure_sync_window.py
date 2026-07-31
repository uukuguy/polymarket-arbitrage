from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from polyarb.clients.gamma_client import EventPage, MarketPage
from polyarb.perception import structure_sync as structure_sync_module
from polyarb.perception.structure_sync import (
    StagedGammaSource,
    StructureSyncWorker,
    finalize_structure_window,
    run_structure_sync_until_published,
)
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


async def test_structure_worker_advances_one_event_then_one_market_page(tmp_path) -> None:
    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            assert (cursor, limit) == (None, 100)
            return EventPage(({"id": "event-1"},), None, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            assert (cursor, limit) == (None, 100)
            return MarketPage(({"id": "market-1"},), None, None, True, 30, 40)

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    worker = StructureSyncWorker(gamma=Gamma(), store=store)

    assert (await worker.run_batch()).stage == "events"
    assert store.get_latest_structure_sync()["started_at_ms"] > 0
    assert (await worker.run_batch()).stage == "markets"
    assert store.get_latest_structure_sync()["status"] == "complete"


async def test_structure_worker_emits_scheduler_stage_before_remote_page_fetch(
    tmp_path, capsys
) -> None:
    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), cursor, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            raise AssertionError("market page is not part of this batch")

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()

    await StructureSyncWorker(gamma=Gamma(), store=store).run_batch()

    stderr = capsys.readouterr().err
    assert "snapshot-stage stage=gamma-events state=start elapsed_ms=0" in stderr
    assert "snapshot-stage stage=gamma-events state=complete elapsed_ms=" in stderr


async def test_staged_source_releases_raw_rows_as_stream_consumes_them() -> None:
    events = [{"id": "event-1"}, {"id": "event-2"}]
    markets = [{"id": "market-1"}, {"id": "market-2"}]
    source = StagedGammaSource(events, markets)

    assert events == []
    assert markets == []
    assert len(source._events) == 2
    assert len(source._markets) == 2

    event_stream = source.iter_active_events(SimpleNamespace(result=None))
    market_stream = source.iter_active_markets(SimpleNamespace(result=None))
    assert await anext(event_stream) == {"id": "event-1"}
    assert await anext(market_stream) == {"id": "market-1"}
    assert len(source._events) == 1
    assert len(source._markets) == 1


async def test_completed_window_can_stream_rows_directly_from_sqlite(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "event-1"}, {"id": "event-2"}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": "market-1"}, {"id": "market-2"}],
        finished_at_ms=300,
    )
    source_type = getattr(structure_sync_module, "SQLiteStagedGammaSource", None)
    assert source_type is not None, "SQLite-backed staged source is missing"
    source = source_type(store, window["id"])
    event_coverage = SimpleNamespace(result=None)
    market_coverage = SimpleNamespace(result=None)

    events = [row async for row in source.iter_active_events(event_coverage)]
    markets = [row async for row in source.iter_active_markets(market_coverage)]

    assert events == [{"id": "event-1"}, {"id": "event-2"}]
    assert markets == [{"id": "market-1"}, {"id": "market-2"}]
    assert event_coverage.result.items_yielded == 2
    assert market_coverage.result.items_yielded == 2


def test_incomplete_structure_window_cannot_be_read_for_publication(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=1)
    with pytest.raises(ValueError, match="not-complete"):
        store.read_complete_structure_sync(window["id"])


async def test_structure_retry_skips_full_database_schema_migration(
    settings_for_test,
) -> None:
    """A scheduler retry must not rescan the whole production database."""
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), None, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            return MarketPage((), None, None, True, 30, 40)

    result = SimpleNamespace(is_valid=False)
    with (
        patch.object(
            SQLiteStore,
            "init_schema",
            side_effect=AssertionError("full schema migration on retry"),
        ),
        patch(
            "polyarb.perception.structure_sync.GammaClient",
            return_value=Gamma(),
        ),
        patch(
            "polyarb.perception.structure_sync.finalize_structure_window",
            new=AsyncMock(return_value=result),
        ),
    ):
        assert await run_structure_sync_until_published(settings_for_test) is result


async def test_structure_finalizer_reuses_daemon_initialized_schema(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
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
        next_cursor=None,
        completed=True,
        markets=[],
        finished_at_ms=300,
    )
    result = SimpleNamespace(is_valid=False)
    run_snapshot = AsyncMock(return_value=result)

    with (
        patch.object(
            SQLiteStore,
            "init_schema",
            side_effect=AssertionError("full schema migration in finalizer"),
        ),
        patch.object(
            SQLiteStore,
            "read_complete_structure_sync",
            side_effect=AssertionError("completed window materialized in memory"),
        ),
        patch("polyarb.snapshot.orchestrator.run_snapshot", new=run_snapshot),
    ):
        assert (
            await finalize_structure_window(
                settings_for_test,
                window["id"],
                now_ms=400,
            )
            is result
        )

    assert run_snapshot.await_args.kwargs["schema_ready"] is True
