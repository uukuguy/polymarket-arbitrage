from __future__ import annotations

import asyncio
import inspect
import tomllib
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from polyarb.config import Settings
from polyarb.http.app import create_app
from polyarb.storage.sqlite_store import SQLiteStore


def test_create_app_exposes_quote_worker_runtime(tmp_path) -> None:
    settings = Settings(db_path=tmp_path / "state.db")
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    runtime = object()

    app = create_app(
        scheduler=MagicMock(),
        sqlite_store=store,
        settings=settings,
        quote_worker_runtime=runtime,
    )

    assert app.state.quote_worker_runtime is runtime


async def test_l1_start_helper_runs_quote_worker_and_task_is_cancellable() -> None:
    from polyarb.daemon.main import _start_quote_worker

    entered = asyncio.Event()

    async def run(_stop_event: asyncio.Event) -> None:
        entered.set()
        await asyncio.Event().wait()

    worker = MagicMock()
    worker.run = AsyncMock(side_effect=run)
    stop_event = asyncio.Event()

    task = _start_quote_worker(worker, stop_event)
    assert task is not None
    await asyncio.wait_for(entered.wait(), timeout=1)

    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    worker.run.assert_awaited_once_with(stop_event)


def test_l1_main_owns_quote_worker_shutdown() -> None:
    from polyarb.daemon import main

    source = inspect.getsource(main.main)
    assert "build_production_quote_worker(settings)" in source
    assert "_start_quote_worker(quote_worker, stop_event)" in source
    assert "quote_worker_task.cancel()" in source
    assert "quote_worker_task" in source.partition("asyncio.gather(")[2]


def test_fly_enables_worker_at_120_seconds() -> None:
    config = tomllib.loads(Path("fly.toml").read_text())
    env = config["env"]

    assert env["POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED"] == "true"
    assert env["POLYARB_NEG_RISK_QUOTE_INTERVAL_S"] == "120"


def test_fly_refreshes_structure_within_the_quote_freshness_window() -> None:
    """Production must not silently fall back to the one-hour scheduler default."""
    config = tomllib.loads(Path("fly.toml").read_text())

    assert config["env"]["POLYARB_SCHEDULER_INTERVAL_S"] == "300"


@pytest.mark.asyncio
async def test_quote_subprocess_classifies_replaced_structure_revision(tmp_path) -> None:
    """A safe rejection of an old Structure revision is retryable, not opaque."""
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSourceSupersededError,
        collect_quotes_in_subprocess,
    )

    class Process:
        returncode = 2

        async def communicate(self):
            return (
                b"",
                b"quote collection failed: verified universe snapshot is no longer "
                b"the latest published truth\n",
            )

    async def spawn(*_args, **_kwargs):
        return Process()

    with pytest.raises(QuoteCollectionSourceSupersededError):
        await collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
        )


@pytest.mark.asyncio
async def test_quote_worker_immediately_retries_superseded_structure_revision() -> None:
    """A Structure publish during CLOB collection does not create a two-minute gap."""
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSourceSupersededError,
        QuoteWorker,
        QuoteWorkerRuntime,
    )
    from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult

    stop_event = asyncio.Event()
    waits: list[float] = []
    outcomes: list[object] = [
        QuoteCollectionSourceSupersededError(),
        QuoteCollectionResult(
            run_id=11,
            status="complete",
            universe_snapshot_id=22,
            requested_token_count=4,
            successful_response_count=4,
            quote_taken_at_ms=1_000,
            elapsed_ms=20,
            universe_hash="a" * 64,
        ),
    ]

    async def collect_once() -> QuoteCollectionResult:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, QuoteCollectionResult)
        return outcome

    async def wait_for_stop(_stop_event: asyncio.Event, delay_s: float) -> bool:
        waits.append(delay_s)
        if len(waits) == 2:
            stop_event.set()
            return True
        return False

    runtime = QuoteWorkerRuntime()
    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        runtime=runtime,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(stop_event)

    assert outcomes == []
    assert waits[0] == 0
    assert runtime.failure_count == 0
    assert runtime.success_count == 1


@pytest.mark.asyncio
async def test_quote_worker_restores_certified_feed_before_first_collection() -> None:
    """A daemon restart can serve the durable certified feed before fresh CLOB I/O."""
    from polyarb.daemon.quote_worker import (
        CertifiedQuoteFeed,
        CertifiedQuoteMetadata,
        QuoteWorker,
        QuoteWorkerRuntime,
    )
    from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult

    restored = CertifiedQuoteFeed(
        projection=CertifiedQuoteMetadata(
            run_id=11,
            universe_snapshot_id=22,
            universe_taken_at_ms=900,
            quoted_at_ms=1_000,
            requested_token_count=4,
            successful_response_count=4,
            universe_hash="a" * 64,
            source_truth_hash="b" * 64,
        ),
        opportunity_scan=None,
    )
    stop_event = asyncio.Event()
    runtime = QuoteWorkerRuntime()

    async def restore_feed() -> CertifiedQuoteFeed | None:
        return restored

    async def collect_once() -> QuoteCollectionResult:
        assert runtime.certified_feed() is restored
        return QuoteCollectionResult(
            run_id=12,
            status="complete",
            universe_snapshot_id=22,
            requested_token_count=4,
            successful_response_count=4,
            quote_taken_at_ms=1_010,
            elapsed_ms=20,
            universe_hash="a" * 64,
        )

    async def wait_for_stop(_stop_event: asyncio.Event, _delay_s: float) -> bool:
        stop_event.set()
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        runtime=runtime,
        restore_feed=restore_feed,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(stop_event)

    assert runtime.certified_feed() is restored


@pytest.mark.asyncio
async def test_production_quote_worker_can_restore_an_empty_durable_store(tmp_path) -> None:
    """Cold production storage stays a normal no-feed state, not a startup error."""
    from polyarb.daemon.quote_worker import build_production_quote_worker

    settings = Settings(
        db_path=tmp_path / "state.db",
        neg_risk_quote_worker_enabled=True,
    )
    SQLiteStore(settings.db_path).init_schema()

    worker = build_production_quote_worker(settings)

    assert worker is not None
    assert worker._restore_feed is not None
    assert await worker._restore_feed() is None


@pytest.mark.asyncio
async def test_quote_worker_cancellation_releases_its_durable_lease() -> None:
    """A replacement daemon must not wait for the stopped worker's quote run lease."""
    from polyarb.daemon.quote_worker import QuoteWorker

    entered = asyncio.Event()
    cleaned = AsyncMock(return_value=1)
    stop_event = asyncio.Event()

    async def collect_once():
        entered.set()
        await asyncio.Event().wait()

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        cleanup_collecting_runs=cleaned,
    )
    task = asyncio.create_task(worker.run(stop_event))
    await asyncio.wait_for(entered.wait(), timeout=1)

    task.cancel()
    result = await asyncio.gather(task, return_exceptions=True)

    assert isinstance(result[0], asyncio.CancelledError)
    cleaned.assert_awaited_once_with()
