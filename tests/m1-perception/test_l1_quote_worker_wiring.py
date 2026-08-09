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


def test_quote_runtime_snapshot_exposes_pipeline_activity() -> None:
    from polyarb.daemon.quote_worker import QuoteWorkerRuntime

    runtime = QuoteWorkerRuntime()

    assert runtime.pipeline_active() is False
    assert runtime.snapshot().pipeline_active is False


def test_generation_cleanup_settings_are_bounded_and_enabled_by_default() -> None:
    settings = Settings()

    assert settings.structure_generation_cleanup_enabled is True
    assert settings.structure_generation_cleanup_max_rows == 500
    assert settings.structure_generation_cleanup_active_interval_s == 0.05
    assert settings.structure_generation_cleanup_idle_interval_s == 30.0
    assert settings.structure_generation_cleanup_writer_busy_interval_s == 5.0
    assert settings.structure_generation_cleanup_retry_initial_s == 1.0
    assert settings.structure_generation_cleanup_retry_max_s == 30.0
    assert settings.structure_generation_cleanup_failure_threshold == 3


def test_generation_cleanup_retry_range_rejects_inverted_bounds() -> None:
    with pytest.raises(ValueError, match="cleanup_retry_initial"):
        Settings(
            structure_generation_cleanup_retry_initial_s=31,
            structure_generation_cleanup_retry_max_s=30,
        )


def test_create_app_exposes_candidate_watcher_runtime(tmp_path) -> None:
    settings = Settings(db_path=tmp_path / "state.db")
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    runtime = object()

    app = create_app(
        scheduler=MagicMock(),
        sqlite_store=store,
        settings=settings,
        candidate_watcher_runtime=runtime,
    )

    assert app.state.candidate_watcher_runtime is runtime


async def test_candidate_start_helper_is_disabled_by_default_and_cancellable() -> None:
    from polyarb.daemon.main import _start_candidate_watcher

    stop_event = asyncio.Event()
    assert _start_candidate_watcher(None, stop_event) is None

    entered = asyncio.Event()

    async def run(_stop_event: asyncio.Event) -> None:
        entered.set()
        await asyncio.Event().wait()

    scheduler = MagicMock()
    scheduler.run = AsyncMock(side_effect=run)
    task = _start_candidate_watcher(scheduler, stop_event)
    assert task is not None
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    result = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(result[0], asyncio.CancelledError)


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


async def test_isolated_parent_hydrates_durable_quote_feed_without_collecting() -> None:
    from polyarb.daemon.main import _hydrate_durable_quote_feed

    runtime = MagicMock()
    feed = object()
    loader = MagicMock(return_value=feed)

    hydrated = await _hydrate_durable_quote_feed(runtime, loader)

    assert hydrated is True
    loader.assert_called_once_with()
    runtime.restore_certified_feed.assert_called_once_with(feed)


async def test_generation_cleanup_start_helper_is_optional_and_cancellable() -> None:
    from polyarb.daemon.main import _start_generation_cleanup_worker

    stop_event = asyncio.Event()
    assert _start_generation_cleanup_worker(None, stop_event) is None

    entered = asyncio.Event()

    async def run(_stop_event: asyncio.Event) -> None:
        entered.set()
        await asyncio.Event().wait()

    worker = MagicMock()
    worker.run = AsyncMock(side_effect=run)
    task = _start_generation_cleanup_worker(worker, stop_event)
    assert task is not None
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    worker.run.assert_awaited_once_with(stop_event)


def test_generation_cleanup_owner_runs_in_isolated_topology_but_not_without_sync() -> None:
    from polyarb.daemon.main import _build_generation_cleanup_worker

    settings = Settings()
    store = MagicMock()
    lock = asyncio.Lock()
    runtime = MagicMock()

    # Isolated snapshot producers do not own this resident cleanup loop. The
    # parent daemon must still own it, otherwise enabled cleanup has no
    # heartbeat or executor in production.
    assert _build_generation_cleanup_worker(
        settings,
        store,
        lock,
        runtime,
        isolated_producers=True,
        structure_sync_enabled=True,
    ) is not None
    assert (
        _build_generation_cleanup_worker(
            settings,
            store,
            lock,
            runtime,
            isolated_producers=False,
            structure_sync_enabled=False,
        )
        is None
    )
    assert _build_generation_cleanup_worker(
        settings,
        store,
        lock,
        runtime,
        isolated_producers=False,
        structure_sync_enabled=True,
    ) is not None


def test_l1_main_owns_quote_worker_shutdown() -> None:
    from polyarb.daemon import main

    source = inspect.getsource(main.main)
    assert "build_production_quote_worker(" in source
    assert "opportunity_watcher=focused_watcher" in source
    assert "None if isolated_producers else quote_worker" in source
    assert "_start_durable_quote_feed_hydrator(" in source
    assert "quote_worker_task.cancel()" in source
    assert "quote_worker_task" in source.partition("asyncio.gather(")[2]


def test_l1_main_owns_generation_cleanup_worker_shutdown() -> None:
    from polyarb.daemon import main

    source = inspect.getsource(main.main)
    assert "_build_generation_cleanup_worker(" in source
    assert "_start_generation_cleanup_worker(cleanup_worker, stop_event)" in source
    assert "cleanup_worker_task.cancel()" in source
    assert "cleanup_worker_task" in source.partition("asyncio.gather(")[2]


def test_l1_main_feature_flags_candidate_watcher_as_sibling_task() -> None:
    from polyarb.daemon import main

    builder_source = inspect.getsource(main._build_daemon_perception_workers)
    lifecycle_source = inspect.getsource(main.main)
    assert "_build_daemon_perception_workers(" in lifecycle_source
    assert "build_production_candidate_watcher(" in builder_source
    assert "candidate_group_ids=candidate_group_ids" in builder_source
    assert 'fault_runtime=component_fault_runtimes["candidate"]' in builder_source
    assert (
        "if settings.opportunity_first_watcher_enabled and not isolated_producers"
        in builder_source
    )
    assert "_start_candidate_watcher(candidate_watcher, stop_event)" in lifecycle_source
    assert "candidate_watcher_task.cancel()" in lifecycle_source
    assert "candidate_watcher_runtime=" in lifecycle_source


def test_candidate_watcher_controller_settings_are_explicit_and_off_by_default() -> None:
    settings = Settings()

    assert settings.opportunity_first_watcher_enabled is False
    assert settings.candidate_high_interval_s == 15
    assert settings.candidate_normal_interval_s == 60
    assert settings.candidate_explore_interval_s == 300
    assert settings.candidate_quote_hard_stale_s == 90
    assert settings.candidate_cycle_max_groups == 12
    assert settings.candidate_reserved_non_high_slots == 3
    assert settings.candidate_group_timeout_s == 30
    assert settings.candidate_high_burst_groups == 1
    assert settings.candidate_lower_lane_max_wait_s == 120
    assert settings.candidate_supervisor_retry_s == 1
    assert settings.candidate_scheduler_poll_s == 1
    assert settings.candidate_high_clob_workers == 2
    assert settings.candidate_lower_clob_workers == 1
    assert settings.discovery_candidate_max_wait_s == 60
    assert settings.discovery_effective_admission_capacity == 1
    assert settings.candidate_selection_budget_s == 6
    assert settings.candidate_source_max_groups == 500
    assert settings.candidate_terminal_write_budget_s == 5
    assert settings.candidate_attempt_start_write_budget_s == 5
    assert settings.discovery_effective_start_bound_ms == 47_000


def test_discovery_capacity_charges_every_attempt_start_write() -> None:
    settings = Settings(
        candidate_scheduler_poll_s=1,
        candidate_selection_budget_s=1,
        candidate_group_timeout_s=10,
        candidate_terminal_write_budget_s=5,
        candidate_attempt_start_write_budget_s=5,
        candidate_high_burst_groups=1,
        candidate_reserved_non_high_slots=3,
        discovery_candidate_max_wait_s=60,
    )

    assert settings.discovery_effective_admission_capacity == 2
    assert settings.discovery_effective_start_bound_ms == 42_000


@pytest.mark.parametrize(
    "kwargs",
    [
        {"candidate_high_interval_s": float("inf")},
        {"candidate_group_timeout_s": float("nan")},
        {
            "candidate_high_interval_s": 91,
            "candidate_quote_hard_stale_s": 90,
        },
        {
            "candidate_cycle_max_groups": 2,
            "candidate_reserved_non_high_slots": 2,
        },
        {
            "candidate_cycle_max_groups": 12,
            "candidate_reserved_non_high_slots": 2,
        },
        {
            "candidate_high_burst_groups": 3,
            "candidate_high_clob_workers": 3,
            "candidate_group_timeout_s": 40,
            "candidate_lower_lane_max_wait_s": 120,
        },
        {"candidate_lower_lane_max_wait_s": 120.001},
        {"discovery_candidate_max_wait_s": 60.001},
        {"candidate_selection_budget_s": float("inf")},
        {"candidate_source_max_groups": 0},
        {"candidate_terminal_write_budget_s": 4.999},
        {"candidate_attempt_start_write_budget_s": 4.999},
        {
            "candidate_group_timeout_s": 30,
            "candidate_high_burst_groups": 2,
            "candidate_high_clob_workers": 2,
            "candidate_scheduler_poll_s": 1,
            "discovery_candidate_max_wait_s": 60,
        },
        {
            "candidate_high_burst_groups": 3,
            "candidate_high_clob_workers": 2,
        },
    ],
)
def test_candidate_controller_settings_reject_invalid_relationships(kwargs) -> None:
    with pytest.raises(ValueError):
        Settings(**kwargs)


def test_candidate_controller_accepts_strictly_sub_boundary_high_burst() -> None:
    settings = Settings(
        candidate_high_burst_groups=3,
        candidate_high_clob_workers=3,
        candidate_group_timeout_s=10,
        candidate_lower_lane_max_wait_s=120,
    )

    assert (
        settings.candidate_high_burst_groups
        * settings.candidate_group_timeout_s
        < settings.candidate_lower_lane_max_wait_s
    )


def test_fly_readonly_quote_release_enables_quote_and_keeps_cleanup_bounded() -> None:
    config = tomllib.loads(Path("fly.toml").read_text())
    env = config["env"]

    assert env["POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED"] == "true"
    assert env["POLYARB_NEG_RISK_QUOTE_INTERVAL_S"] == "60"
    assert env["POLYARB_STRUCTURE_GENERATION_READ_MODE"] == "generation"
    assert env["POLYARB_STRUCTURE_GENERATION_CLEANUP_ENABLED"] == "true"
    assert env["POLYARB_STRUCTURE_GENERATION_CLEANUP_MAX_ROWS"] == "500"
    assert env["POLYARB_STRUCTURE_GENERATION_CLEANUP_ACTIVE_INTERVAL_S"] == "0.05"
    assert env["POLYARB_STRUCTURE_GENERATION_CLEANUP_IDLE_INTERVAL_S"] == "30"
    assert env["POLYARB_STRUCTURE_GENERATION_CLEANUP_WRITER_BUSY_INTERVAL_S"] == "5"
    assert env["POLYARB_CAPACITY_CONTROLLER_ENABLED"] == "true"


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
async def test_quote_subprocess_cancellation_keeps_one_reap_task_until_child_exit(
    tmp_path,
) -> None:
    """Cancellation must reap one child, not abandon its pipe reader and lease."""
    from polyarb.daemon.quote_worker import collect_quotes_in_subprocess

    class Process:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.calls = 0
            self.started = asyncio.Event()
            self.released = asyncio.Event()
            self.terminated = False
            self.killed = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.calls += 1
            if self.calls != 1:
                raise AssertionError("communicate must not be re-entered after cancellation")
            self.started.set()
            await self.released.wait()
            return b"", b""

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9
            self.released.set()

    process = Process()

    async def spawn(*_args, **_kwargs):
        return process

    task = asyncio.create_task(
        collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )
    )
    await asyncio.wait_for(process.started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated is True
    assert process.killed is True
    assert process.calls == 1


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
async def test_quote_pipeline_stays_active_through_post_publication_and_release() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker, QuoteWorkerRuntime
    from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult

    runtime = QuoteWorkerRuntime()
    stop_event = asyncio.Event()
    result = QuoteCollectionResult(
        run_id=11,
        status="complete",
        universe_snapshot_id=22,
        requested_token_count=4,
        successful_response_count=4,
        quote_taken_at_ms=1_000,
        elapsed_ms=20,
        universe_hash="a" * 64,
    )
    projection = MagicMock(
        run_id=11,
        universe_snapshot_id=22,
        universe_taken_at_ms=900,
        quoted_at_ms=1_000,
        requested_token_count=4,
        successful_response_count=4,
        universe_hash="a" * 64,
        source_truth_hash="b" * 64,
    )

    async def collect_once():
        assert runtime.pipeline_active() is True
        return result

    async def certify_projection(_result):
        assert runtime.pipeline_active() is True
        return projection

    async def cleanup_old_runs() -> int:
        assert runtime.pipeline_active() is True
        return 0

    async def reconcile_global_projection(_projection) -> None:
        assert runtime.pipeline_active() is True

    def release_projection_memory() -> None:
        assert runtime.pipeline_active() is True

    async def wait_for_stop(_stop_event: asyncio.Event, _delay_s: float) -> bool:
        assert runtime.pipeline_active() is False
        stop_event.set()
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        cleanup_old_runs=cleanup_old_runs,
        reconcile_global_projection=reconcile_global_projection,
        interval_s=120,
        runtime=runtime,
        wait_for_stop=wait_for_stop,
        release_projection_memory=release_projection_memory,
    )

    await worker.run(stop_event)

    assert runtime.pipeline_active() is False
    assert runtime.snapshot().pipeline_active is False


@pytest.mark.asyncio
async def test_quote_pipeline_activity_clears_when_worker_is_cancelled() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker, QuoteWorkerRuntime

    runtime = QuoteWorkerRuntime()
    entered = asyncio.Event()

    async def collect_once():
        entered.set()
        await asyncio.Event().wait()

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        runtime=runtime,
    )
    task = asyncio.create_task(worker.run(asyncio.Event()))
    await asyncio.wait_for(entered.wait(), timeout=1)
    assert runtime.pipeline_active() is True

    task.cancel()
    result = await asyncio.gather(task, return_exceptions=True)

    assert isinstance(result[0], asyncio.CancelledError)
    assert runtime.pipeline_active() is False
    assert runtime.snapshot().pipeline_active is False


@pytest.mark.asyncio
async def test_quote_pipeline_activity_clears_after_failed_attempt() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker, QuoteWorkerRuntime

    runtime = QuoteWorkerRuntime()
    stop_event = asyncio.Event()

    async def collect_once():
        assert runtime.pipeline_active() is True
        raise RuntimeError("upstream unavailable")

    async def wait_for_stop(_stop_event: asyncio.Event, _delay_s: float) -> bool:
        assert runtime.pipeline_active() is False
        stop_event.set()
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        runtime=runtime,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(stop_event)

    assert runtime.failure_count == 1
    assert runtime.pipeline_active() is False


def test_daemon_passes_exact_quote_runtime_to_structure_scheduler() -> None:
    from polyarb.daemon import main as main_module

    source = inspect.getsource(main_module.main)

    assert "quote_worker_runtime=(" in source
    assert "quote_worker.runtime if quote_worker is not None else None" in source


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
