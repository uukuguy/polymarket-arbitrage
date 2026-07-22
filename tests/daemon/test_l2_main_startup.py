"""Tests for polyarb.daemon.l2_main — init order + P9 server-started gate.

Phase 03 Plan 03 — L2 daemon entry. Mirrors src/polyarb/daemon/main.py (L1)
but uses Sentry tag service=polyarb-l2 and Mock-shaped ws_consumer placeholder.

Phase 02 LEARNINGS cited:
- L5: 100-iter / 0.1s server-started polling gate is MANDATORY
- L9: patch at IMPORT SITE (polyarb.daemon.l2_main.*) not definition site
- F-04: CancelledError MUST propagate (not be swallowed)

Test fixtures use loguru StringIO sink (Phase 02.1 L4) for log capture.
"""
from __future__ import annotations

import asyncio
import io
import os
import signal
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from loguru import logger

# F-3 SECURITY ESCAPE HATCH: pytest tmp_path lives outside project root by design
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


# ─────────────────────────────────────────────────────────────────────────────
# Loguru capture fixture (Phase 02.1 L4)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def loguru_sink():
    """Capture loguru output into a StringIO buffer for assertion."""
    sink = io.StringIO()
    sink_id = logger.add(sink, format="{message}", level="INFO")
    yield sink
    logger.remove(sink_id)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a fake settings MagicMock with all fields tests need
# ─────────────────────────────────────────────────────────────────────────────

def _make_fake_settings():
    """Phase 02.1 L3 — explicitly populate all fields so .env渗透 cannot bleed in."""
    s = MagicMock()
    s.db_path = ":memory:"
    s.http_port = 8080
    s.version = "0.0.0"
    s.release_id = "test"
    s.daemon_variant = "l2"
    s.supabase_url = ""
    s.supabase_mirror_enabled = False
    s.l2_mirror_enabled = False
    s.event_reconcile_poll_seconds = 60
    s.bootstrap_asset_ids = ""
    s.l3_evidence_sample_interval_s = 30
    s.l3_evidence_max_sample_gap_s = 75
    s.l3_promote_interval_s = 300
    s.l3_promote_max_start_gap_s = 360
    s.l3_evidence_retention_days = 30
    s.l3_market_book_fresh_s = 120
    s.l3_market_ohlc_fresh_s = 120
    s.scan_shared_secret = MagicMock(get_secret_value=lambda: "")
    s.supabase_db_dsn = MagicMock(get_secret_value=lambda: "")
    s.l2_runtime_db_dsn = MagicMock(get_secret_value=lambda: "")
    s.supabase_service_key = MagicMock(get_secret_value=lambda: "")
    return s


def test_l3_dependency_builder_uses_only_dedicated_runtime_dsn(monkeypatch):
    """Owner/migration DSN is poison: daemon construction must not read it."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.supabase_db_dsn.get_secret_value = MagicMock(
        side_effect=AssertionError("migration owner DSN must remain unread by l2_main")
    )
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    started_at = datetime(2026, 7, 23, 2, 0, tzinfo=UTC)
    store = MagicMock()
    monkeypatch.setenv("FLY_MACHINE_ID", "machine-1")
    monkeypatch.setenv("FLY_MACHINE_VERSION", "version-2")
    monkeypatch.setenv("FLY_IMAGE_REF", "registry/image@sha256:abc")

    with patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store) as store_type:
        dependencies = l2_main._build_l3_evidence_dependencies(
            settings=settings,
            recipe_yaml_path=l2_main._L3_RECIPE_PATH,
            started_at=started_at,
        )

    store_type.assert_called_once_with("runtime-dsn")
    settings.supabase_db_dsn.get_secret_value.assert_not_called()
    assert "runtime-dsn" not in repr(dependencies)
    assert dependencies.store is store
    assert dependencies.runtime.snapshot().started_at == started_at
    assert dependencies.boot.started_at == started_at
    assert dependencies.boot.boot_id == dependencies.runtime.snapshot().boot_id
    assert (
        dependencies.acceptance_config.digest()
        == dependencies.identity.acceptance_config_hash
        == dependencies.boot.acceptance_config_hash
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, RuntimeError("role rejected")])
async def test_boot_append_failure_marks_runtime_failed_without_raising(outcome):
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    dependencies = l2_main._build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=l2_main._L3_RECIPE_PATH,
        started_at=datetime(2026, 7, 23, 2, 0, tzinfo=UTC),
    )
    if isinstance(outcome, Exception):
        dependencies.store.append_boot = AsyncMock(side_effect=outcome)
    else:
        dependencies.store.append_boot = AsyncMock(return_value=outcome)

    assert await l2_main._append_l3_boot(dependencies) is False
    status = dependencies.runtime.snapshot()
    assert status.writer_ok is False
    assert status.status.value == "fail"
    assert status.reason_code == "evidence_writer_failed"
    assert status.writer_reason_code == "boot_append_failed"
    dependencies.store.append_boot.assert_awaited_once_with(dependencies.boot)


@pytest.mark.asyncio
async def test_missing_runtime_dsn_fails_closed_without_store_or_owner_fallback():
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.supabase_db_dsn.get_secret_value = MagicMock(
        side_effect=AssertionError("owner fallback is forbidden")
    )
    dependencies = l2_main._build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=l2_main._L3_RECIPE_PATH,
    )
    dependencies.store.append_boot = AsyncMock(return_value=True)

    assert await l2_main._append_l3_boot(dependencies) is False
    dependencies.store.append_boot.assert_not_awaited()
    settings.supabase_db_dsn.get_secret_value.assert_not_called()
    assert dependencies.runtime.snapshot().status.value == "fail"


@pytest.mark.asyncio
async def test_main_binds_http_before_rejected_boot_and_never_starts_promoter():
    """A rejected runtime role keeps healthz serving without counterfeit evidence."""
    from polyarb.daemon import l2_main

    calls: list[str] = []
    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    settings.supabase_db_dsn.get_secret_value = MagicMock(
        side_effect=AssertionError("owner DSN must never be read")
    )
    store = MagicMock()

    async def _append_boot(_record):
        calls.append("boot")
        return False

    store.append_boot = AsyncMock(side_effect=_append_boot)
    server = MagicMock(started=False, should_exit=False)

    async def _serve():
        calls.append("server_started")
        server.started = True
        await asyncio.sleep(0)

    server.serve = _serve
    stop_event = asyncio.Event()
    stop_event.set()
    promoter = MagicMock(name="run_periodic")

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=stop_event),
        patch("polyarb.observation.l3_promote.run_periodic", promoter),
    ):
        assert await asyncio.wait_for(l2_main.main(), timeout=3.0) == 0

    assert calls == ["server_started", "boot"]
    settings.supabase_db_dsn.get_secret_value.assert_not_called()
    store.append_boot.assert_awaited_once()
    promoter.assert_not_called()
    assert server.should_exit is True


@pytest.mark.asyncio
async def test_main_shares_exact_runtime_store_and_dsn_after_successful_boot():
    """HTTP, boot, and every named evidence task share one dependency graph."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    settings.supabase_db_dsn.get_secret_value = MagicMock(
        side_effect=AssertionError("owner DSN must never be read")
    )
    lifecycle: list[str] = []
    store = MagicMock()

    async def _append_boot(_record):
        lifecycle.append("boot")
        return True

    store.append_boot = AsyncMock(side_effect=_append_boot)
    server = _make_mock_server()

    def _serve():
        lifecycle.append("http_bind")

        async def _bound_server():
            while not server.should_exit:
                await asyncio.sleep(0)

        return _bound_server()

    server.serve = _serve
    stop_event = asyncio.Event()
    consumer = MagicMock()

    async def _idle(_stop_event):
        await asyncio.sleep(0)

    consumer.run = _idle
    consumer.run_quiet_refresh = _idle
    event_writer_kwargs: dict[str, object] = {}
    promoter_kwargs: dict[str, object] = {}
    sampler_kwargs: dict[str, object] = {}
    task_names: list[str | None] = []
    real_create_task = asyncio.create_task

    def _create_task(coro, *, name=None, context=None):
        task_names.append(name)
        kwargs = {"name": name}
        if context is not None:
            kwargs["context"] = context
        return real_create_task(coro, **kwargs)

    def _run_event_writer(_stop_event, **kwargs):
        lifecycle.append("event_writer")
        event_writer_kwargs.update(kwargs)

        async def _idle_writer():
            await asyncio.sleep(0)

        return _idle_writer()

    def _run_periodic(**kwargs):
        lifecycle.append("promoter")
        promoter_kwargs.update(kwargs)

        async def _idle_promoter():
            await asyncio.sleep(0)

        return _idle_promoter()

    def _run_sampler(_stop_event, **kwargs):
        lifecycle.append("sampler")
        sampler_kwargs.update(kwargs)
        stop_event.set()

        async def _idle_sampler():
            await asyncio.sleep(0)

        return _idle_sampler()

    with patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store):
        dependencies = l2_main._build_l3_evidence_dependencies(
            settings=settings,
            recipe_yaml_path=l2_main._L3_RECIPE_PATH,
        )

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch(
            "polyarb.daemon.l2_main._build_l3_evidence_dependencies",
            return_value=dependencies,
        ),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer) as consumer_type,
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=MagicMock()) as cursor,
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()) as create_app,
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=stop_event),
        patch("polyarb.daemon.l2_main.asyncio.create_task", side_effect=_create_task),
        patch("polyarb.observation.l3_sampler.run_event_writer", new=_run_event_writer),
        patch("polyarb.observation.l3_promote.run_periodic", new=_run_periodic),
        patch("polyarb.observation.l3_sampler.run_sampler", new=_run_sampler),
    ):
        assert await asyncio.wait_for(l2_main.main(), timeout=3.0) == 0

    store.append_boot.assert_awaited_once()
    cursor.assert_called_once_with(dsn="runtime-dsn")
    settings.supabase_db_dsn.get_secret_value.assert_not_called()
    observer = consumer_type.call_args.kwargs["membership_observer"]
    runtime = promoter_kwargs["evidence_runtime"]
    assert observer.__self__ is runtime
    assert create_app.call_args.kwargs["evidence_runtime"] is runtime
    assert event_writer_kwargs["runtime"] is runtime
    assert event_writer_kwargs["store"] is store
    assert promoter_kwargs["evidence_store"] is store
    assert promoter_kwargs["acceptance_config"] is dependencies.acceptance_config
    assert promoter_kwargs["settings"] is settings
    assert sampler_kwargs["runtime"] is runtime
    assert sampler_kwargs["store"] is store
    assert sampler_kwargs["settings"] is settings
    assert sampler_kwargs["ws_consumer"] is consumer
    assert (
        promoter_kwargs["acceptance_config"].digest()
        == dependencies.boot.acceptance_config_hash
    )
    assert store.append_boot.await_args.args[0].boot_id == runtime.snapshot().boot_id
    assert lifecycle == ["http_bind", "boot", "event_writer", "promoter", "sampler"]
    assert task_names == [
        "l2-http-server",
        "l2-stop-wait",
        "l3-boot-append",
        "ws-watchdog",
        "ws-consumer",
        "ws-quiet-refresh",
        "reconciliation-pump",
        "snapshot-listener",
        "l3-event-writer",
        "l3-promoter",
        "l3-evidence-sampler",
    ]
    assert runtime.snapshot().writer_ok is True
    assert runtime.snapshot().status.value == "warn"
    assert runtime.snapshot().reason_code == "cold_start"


@pytest.mark.asyncio
async def test_main_signal_durably_drains_shutdown_event_within_five_seconds():
    """The real signal callback feeds the real writer before stop becomes visible."""
    from polyarb.daemon import l2_main
    from polyarb.observation.l3_evidence import RuntimeEventKind

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    store.append_boot = AsyncMock(return_value=True)
    durable_events = []

    async def _append_event(event):
        durable_events.append(event)
        return True

    store.append_event = AsyncMock(side_effect=_append_event)
    dependencies = l2_main._build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=l2_main._L3_RECIPE_PATH,
    )
    object.__setattr__(dependencies, "store", store)
    stop_event = asyncio.Event()
    stop_states_at_record: list[bool] = []
    original_record_event = dependencies.runtime.record_event

    def _record_before_stop(*args, **kwargs):
        stop_states_at_record.append(stop_event.is_set())
        return original_record_event(*args, **kwargs)

    dependencies.runtime.record_event = _record_before_stop  # type: ignore[method-assign]
    server = _make_mock_server()
    handlers: dict[object, tuple[object, tuple[object, ...]]] = {}
    loop = asyncio.get_running_loop()

    def _capture_handler(sig, callback, *args):
        handlers[sig] = (callback, args)

    async def _idle(local_stop_event):
        await local_stop_event.wait()

    consumer = MagicMock()
    consumer.run = _idle
    consumer.run_quiet_refresh = _idle
    watchdog = MagicMock()
    watchdog.watch = _idle
    pump = MagicMock()
    pump.run = _idle

    async def _listener(**kwargs):
        await kwargs["stop_event"].wait()

    async def _promoter(**kwargs):
        await kwargs["stop_event"].wait()

    async def _sampler(local_stop_event, **_kwargs):
        while signal.SIGTERM not in handlers:
            await asyncio.sleep(0)
        callback, args = handlers[signal.SIGTERM]
        callback(*args)
        await local_stop_event.wait()

    started = loop.time()
    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch(
            "polyarb.daemon.l2_main._build_l3_evidence_dependencies",
            return_value=dependencies,
        ),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer),
        patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog),
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.ReconciliationPump", return_value=pump),
        patch("polyarb.daemon.l2_main.listen_snapshot_complete", new=_listener),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=stop_event),
        patch.object(loop, "add_signal_handler", side_effect=_capture_handler),
        patch("polyarb.observation.l3_promote.run_periodic", new=_promoter),
        patch("polyarb.observation.l3_sampler.run_sampler", new=_sampler),
    ):
        assert await asyncio.wait_for(l2_main.main(), timeout=5.0) == 0

    assert loop.time() - started < 5.0
    assert [event.kind for event in durable_events] == [
        RuntimeEventKind.SHUTDOWN_SIGNAL
    ]
    assert stop_states_at_record == [False]
    assert durable_events[0].detail == {"signal": "SIGTERM"}
    source = Path(l2_main.__file__).read_text()
    assert "UPDATE l3_runtime_boots" not in source


@pytest.mark.asyncio
async def test_cancellation_during_shutdown_propagates_after_reaping_writer():
    """Cancellation while the durable writer drains must not be swallowed."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    store.append_boot = AsyncMock(return_value=True)
    dependencies = l2_main._build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=l2_main._L3_RECIPE_PATH,
    )
    object.__setattr__(dependencies, "store", store)
    stop_event = asyncio.Event()
    writer_draining = asyncio.Event()
    writer_reaped = asyncio.Event()
    writer_blocker = asyncio.Event()
    handlers: dict[object, tuple[object, tuple[object, ...]]] = {}
    loop = asyncio.get_running_loop()

    def _capture_handler(sig, callback, *args):
        handlers[sig] = (callback, args)

    async def _idle(local_stop_event):
        await local_stop_event.wait()

    async def _listener(**kwargs):
        await kwargs["stop_event"].wait()

    async def _promoter(**kwargs):
        await kwargs["stop_event"].wait()

    async def _writer(local_stop_event, **_kwargs):
        try:
            await local_stop_event.wait()
            writer_draining.set()
            await writer_blocker.wait()
        finally:
            writer_reaped.set()

    async def _sampler(local_stop_event, **_kwargs):
        while signal.SIGTERM not in handlers:
            await asyncio.sleep(0)
        callback, args = handlers[signal.SIGTERM]
        callback(*args)
        await local_stop_event.wait()

    consumer = MagicMock()
    consumer.run = _idle
    consumer.run_quiet_refresh = _idle
    watchdog = MagicMock()
    watchdog.watch = _idle
    pump = MagicMock()
    pump.run = _idle

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch(
            "polyarb.daemon.l2_main._build_l3_evidence_dependencies",
            return_value=dependencies,
        ),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer),
        patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog),
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.ReconciliationPump", return_value=pump),
        patch("polyarb.daemon.l2_main.listen_snapshot_complete", new=_listener),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=_make_mock_server()),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=stop_event),
        patch.object(loop, "add_signal_handler", side_effect=_capture_handler),
        patch("polyarb.observation.l3_promote.run_periodic", new=_promoter),
        patch("polyarb.observation.l3_sampler.run_event_writer", new=_writer),
        patch("polyarb.observation.l3_sampler.run_sampler", new=_sampler),
    ):
        main_task = asyncio.create_task(l2_main.main())
        await asyncio.wait_for(writer_draining.wait(), timeout=1.0)
        main_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(main_task, timeout=1.0)

    assert writer_reaped.is_set()


@pytest.mark.asyncio
async def test_shutdown_deadline_force_cancels_and_reaps_writer_once():
    """A stuck writer consumes one shared bound and is not left pending."""
    from polyarb.daemon import l2_main

    writer_started = asyncio.Event()
    writer_reaped = asyncio.Event()

    async def _server():
        await asyncio.sleep(0)

    async def _writer():
        writer_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            # Model bounded async cleanup that needs more than one loop turn.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            writer_reaped.set()

    server_task = asyncio.create_task(_server(), name="l2-http-server")
    writer_task = asyncio.create_task(_writer(), name="l3-event-writer")
    await writer_started.wait()
    started = asyncio.get_running_loop().time()

    await l2_main._drain_daemon_tasks(
        server_task=server_task,
        event_writer_task=writer_task,
        peer_tasks=(),
        normal_shutdown=True,
        timeout_s=0.05,
    )

    assert asyncio.get_running_loop().time() - started < 0.075
    assert writer_task.done()
    assert writer_task.cancelling() == 1
    assert writer_reaped.is_set()


@pytest.mark.asyncio
async def test_shutdown_force_cancel_interrupts_stuck_cancellation_cleanup():
    """Force phase sends a second cancel to a task stuck after graceful cancel."""
    from polyarb.daemon import l2_main

    entered_cleanup = asyncio.Event()
    cleanup_blocker = asyncio.Event()

    async def _server():
        await asyncio.sleep(0)

    async def _stuck_peer():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            entered_cleanup.set()
            await cleanup_blocker.wait()
            raise

    server_task = asyncio.create_task(_server(), name="l2-http-server")
    peer_task = asyncio.create_task(_stuck_peer(), name="stuck-peer")
    await asyncio.sleep(0)
    try:
        await l2_main._drain_daemon_tasks(
            server_task=server_task,
            event_writer_task=None,
            peer_tasks=(peer_task,),
            normal_shutdown=True,
            timeout_s=0.05,
        )

        assert entered_cleanup.is_set()
        assert peer_task.done()
        assert peer_task.cancelling() == 2
    finally:
        if not peer_task.done():
            peer_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await peer_task


@pytest.mark.asyncio
async def test_shutdown_never_returns_clean_with_pathological_pending_task():
    """A task ignoring both cancel phases makes shutdown fail closed."""
    from polyarb.daemon import l2_main

    release = asyncio.Event()

    async def _server():
        await asyncio.sleep(0)

    async def _pathological_peer():
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    server_task = asyncio.create_task(_server(), name="l2-http-server")
    peer_task = asyncio.create_task(_pathological_peer(), name="pathological-peer")
    await asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="pathological-peer"):
            await l2_main._drain_daemon_tasks(
                server_task=server_task,
                event_writer_task=None,
                peer_tasks=(peer_task,),
                normal_shutdown=True,
                timeout_s=0.05,
            )
    finally:
        release.set()
        await asyncio.wait_for(peer_task, timeout=0.2)


@pytest.mark.asyncio
async def test_bound_server_failure_during_boot_cancels_and_reaps_boot_task():
    """A bound HTTP server dying during boot preflight aborts startup."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    boot_started = asyncio.Event()
    boot_reaped = asyncio.Event()
    boot_blocker = asyncio.Event()
    server_exit = asyncio.Event()

    async def _append_boot(_record):
        boot_started.set()
        try:
            await boot_blocker.wait()
        finally:
            boot_reaped.set()

    store.append_boot = AsyncMock(side_effect=_append_boot)
    server = MagicMock(started=True, should_exit=False)

    async def _serve():
        await server_exit.wait()
        raise RuntimeError("serve failed after bind")

    server.serve = _serve
    consumer = MagicMock()
    consumer.run = AsyncMock()
    consumer.run_quiet_refresh = AsyncMock()
    watchdog = MagicMock()
    watchdog.watch = AsyncMock()
    tracked: list[asyncio.Task[object]] = []
    real_create = l2_main._create_daemon_task

    def _track(awaitable, *, name):
        task = real_create(awaitable, name=name)
        tracked.append(task)
        return task

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer),
        patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main._create_daemon_task", side_effect=_track),
    ):
        main_task = asyncio.create_task(l2_main.main())
        await asyncio.wait_for(boot_started.wait(), timeout=0.5)
        server_exit.set()
        with pytest.raises(RuntimeError, match="serve failed after bind"):
            await asyncio.wait_for(main_task, timeout=0.5)

    assert boot_reaped.is_set()
    assert consumer.run.await_count == 0
    assert consumer.run_quiet_refresh.await_count == 0
    assert watchdog.watch.await_count == 0
    assert tracked and all(task.done() for task in tracked)


@pytest.mark.asyncio
async def test_boot_failure_same_tick_as_signal_prefers_clean_stop_and_reaps_warning():
    """Signal wins a same-tick boot failure; drain still retrieves the error."""
    from polyarb.daemon import l2_main

    release = asyncio.Event()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    exception_contexts: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: exception_contexts.append(context))

    async def _boot():
        await release.wait()
        raise RuntimeError("boot failed at signal boundary")

    async def _signal():
        await release.wait()
        stop_event.set()

    async def _server():
        await stop_event.wait()

    boot_task = asyncio.create_task(_boot(), name="l3-boot-append")
    stop_wait_task = asyncio.create_task(stop_event.wait(), name="l2-stop-wait")
    server_task = asyncio.create_task(_server(), name="l2-http-server")
    signal_task = asyncio.create_task(_signal())
    release.set()
    await signal_task
    while not (boot_task.done() and stop_wait_task.done() and server_task.done()):
        await asyncio.sleep(0)

    try:
        assert await l2_main._await_boot_or_daemon_exit(
            boot_task=boot_task,
            server_task=server_task,
            stop_wait_task=stop_wait_task,
            stop_event=stop_event,
        ) == (False, True)
    finally:
        await l2_main._drain_daemon_tasks(
            server_task=server_task,
            event_writer_task=None,
            peer_tasks=(boot_task, stop_wait_task),
            normal_shutdown=True,
            timeout_s=0.05,
        )
        await asyncio.sleep(0)
        loop.set_exception_handler(previous_handler)

    assert not [
        context
        for context in exception_contexts
        if "exception was never retrieved" in str(context.get("message", "")).lower()
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("server_outcome", ["clean", "exception"])
async def test_bound_server_steady_exit_aborts_and_reaps_all_tasks(server_outcome):
    """A server exit before stop supervises every WS/PG/evidence peer."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    store.append_boot = AsyncMock(return_value=True)
    dependencies = l2_main._build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=l2_main._L3_RECIPE_PATH,
    )
    object.__setattr__(dependencies, "store", store)
    server_exit = asyncio.Event()
    all_started = asyncio.Event()
    started: set[str] = set()
    reaped: set[str] = set()

    async def _owned(name: str):
        started.add(name)
        if len(started) == 8:
            all_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            reaped.add(name)

    server = MagicMock(started=True, should_exit=False)

    async def _serve():
        await server_exit.wait()
        if server_outcome == "exception":
            raise RuntimeError("steady server failure")

    server.serve = _serve
    consumer = MagicMock()
    consumer.run = lambda _stop: _owned("ws-consumer")
    consumer.run_quiet_refresh = lambda _stop: _owned("ws-quiet-refresh")
    watchdog = MagicMock()
    watchdog.watch = lambda _stop: _owned("ws-watchdog")
    pump = MagicMock()
    pump.run = lambda _stop: _owned("reconciliation-pump")

    async def _listener(**_kwargs):
        await _owned("snapshot-listener")

    async def _writer(_stop, **_kwargs):
        await _owned("l3-event-writer")

    async def _promoter(**_kwargs):
        await _owned("l3-promoter")

    async def _sampler(_stop, **_kwargs):
        await _owned("l3-evidence-sampler")

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch(
            "polyarb.daemon.l2_main._build_l3_evidence_dependencies",
            return_value=dependencies,
        ),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer),
        patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog),
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.ReconciliationPump", return_value=pump),
        patch("polyarb.daemon.l2_main.listen_snapshot_complete", new=_listener),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.observation.l3_promote.run_periodic", new=_promoter),
        patch("polyarb.observation.l3_sampler.run_event_writer", new=_writer),
        patch("polyarb.observation.l3_sampler.run_sampler", new=_sampler),
    ):
        main_task = asyncio.create_task(l2_main.main())
        await asyncio.wait_for(all_started.wait(), timeout=0.5)
        server_exit.set()
        expected = "steady server failure" if server_outcome == "exception" else "exited"
        with pytest.raises(RuntimeError, match=expected):
            await asyncio.wait_for(main_task, timeout=0.5)

    assert started == reaped


@pytest.mark.asyncio
async def test_cancellation_during_steady_state_reaps_every_named_task_once():
    """Steady-state cancellation owns server, WS, PG, and evidence tasks."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    store.append_boot = AsyncMock(return_value=True)
    dependencies = l2_main._build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=l2_main._L3_RECIPE_PATH,
    )
    object.__setattr__(dependencies, "store", store)
    started: set[str] = set()
    reaped: set[str] = set()
    all_started = asyncio.Event()
    tracked: list[asyncio.Task[object]] = []
    real_create_daemon_task = l2_main._create_daemon_task

    async def _owned(name: str, _stop_event=None):
        started.add(name)
        if len(started) == 9:
            all_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            reaped.add(name)

    def _track(awaitable, *, name):
        task = real_create_daemon_task(awaitable, name=name)
        tracked.append(task)
        return task

    server = MagicMock(started=True, should_exit=False)
    server.serve = lambda: _owned("l2-http-server")
    consumer = MagicMock()
    consumer.run = lambda stop: _owned("ws-consumer", stop)
    consumer.run_quiet_refresh = lambda stop: _owned("ws-quiet-refresh", stop)
    watchdog = MagicMock()
    watchdog.watch = lambda stop: _owned("ws-watchdog", stop)
    pump = MagicMock()
    pump.run = lambda stop: _owned("reconciliation-pump", stop)

    async def _listener(**kwargs):
        await _owned("snapshot-listener", kwargs["stop_event"])

    async def _event_writer(stop, **_kwargs):
        await _owned("l3-event-writer", stop)

    async def _promoter(**kwargs):
        await _owned("l3-promoter", kwargs["stop_event"])

    async def _sampler(stop, **_kwargs):
        await _owned("l3-evidence-sampler", stop)

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch(
            "polyarb.daemon.l2_main._build_l3_evidence_dependencies",
            return_value=dependencies,
        ),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer),
        patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog),
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.ReconciliationPump", return_value=pump),
        patch("polyarb.daemon.l2_main.listen_snapshot_complete", new=_listener),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main._create_daemon_task", side_effect=_track),
        patch("polyarb.observation.l3_promote.run_periodic", new=_promoter),
        patch("polyarb.observation.l3_sampler.run_event_writer", new=_event_writer),
        patch("polyarb.observation.l3_sampler.run_sampler", new=_sampler),
    ):
        main_task = asyncio.create_task(l2_main.main())
        await asyncio.wait_for(all_started.wait(), timeout=1.0)
        main_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(main_task, timeout=1.0)

    expected = {
        "l2-http-server",
        "ws-watchdog",
        "ws-consumer",
        "ws-quiet-refresh",
        "reconciliation-pump",
        "snapshot-listener",
        "l3-event-writer",
        "l3-promoter",
        "l3-evidence-sampler",
    }
    assert started == reaped == expected
    assert all(task.done() for task in tracked)
    assert all(
        task.cancelling() == (0 if task.get_name() == "l3-boot-append" else 1)
        for task in tracked
    )


def _patch_minimal_l2_main(l2_main, *, settings, server, store):
    """Return the common side-effect-free daemon patch stack inputs."""
    return (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
    )


@pytest.mark.asyncio
async def test_server_gate_timeout_cleans_server_and_never_starts_boot_or_runtime_tasks():
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    server = MagicMock(started=False, should_exit=False)
    serve_stopped = asyncio.Event()
    serve_blocker = asyncio.Event()

    async def _serve():
        try:
            await serve_blocker.wait()
        finally:
            serve_stopped.set()

    server.serve = _serve
    append_boot = AsyncMock(return_value=True)
    real_sleep = asyncio.sleep

    async def _yield_only(_delay):
        await real_sleep(0)

    common = _patch_minimal_l2_main(
        l2_main, settings=settings, server=server, store=store
    )
    with ExitStack() as stack:
        for context in common:
            stack.enter_context(context)
        stack.enter_context(patch("polyarb.daemon.l2_main._append_l3_boot", append_boot))
        stack.enter_context(
            patch("polyarb.daemon.l2_main.asyncio.sleep", side_effect=_yield_only)
        )
        with pytest.raises(TimeoutError, match="server.*start"):
            await asyncio.wait_for(l2_main.main(), timeout=1.0)

    append_boot.assert_not_awaited()
    assert serve_stopped.is_set()
    assert server.should_exit is True


@pytest.mark.asyncio
async def test_server_gate_propagates_serve_failure_before_boot_or_runtime_tasks():
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    server = MagicMock(started=False, should_exit=False)

    async def _serve():
        raise RuntimeError("bind failed")

    server.serve = _serve
    append_boot = AsyncMock(return_value=True)
    common = _patch_minimal_l2_main(
        l2_main, settings=settings, server=server, store=store
    )
    with ExitStack() as stack:
        for context in common:
            stack.enter_context(context)
        stack.enter_context(patch("polyarb.daemon.l2_main._append_l3_boot", append_boot))
        with pytest.raises(RuntimeError, match="bind failed"):
            await asyncio.wait_for(l2_main.main(), timeout=1.0)

    append_boot.assert_not_awaited()
    assert server.should_exit is True


@pytest.mark.asyncio
async def test_cancellation_during_boot_cleans_server_without_starting_runtime_tasks():
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    store = MagicMock()
    server = MagicMock(started=True, should_exit=False)
    serve_stopped = asyncio.Event()
    serve_blocker = asyncio.Event()
    boot_entered = asyncio.Event()
    boot_blocker = asyncio.Event()

    async def _serve():
        try:
            await serve_blocker.wait()
        finally:
            serve_stopped.set()

    async def _append_boot(_dependencies):
        boot_entered.set()
        await boot_blocker.wait()
        return True

    server.serve = _serve
    consumer = MagicMock()
    consumer.run = AsyncMock()
    consumer.run_quiet_refresh = AsyncMock()
    watchdog = MagicMock()
    watchdog.watch = AsyncMock()
    listener = AsyncMock()
    pump = MagicMock()
    pump.run = AsyncMock()
    common = _patch_minimal_l2_main(
        l2_main, settings=settings, server=server, store=store
    )
    with ExitStack() as stack:
        for context in common:
            stack.enter_context(context)
        stack.enter_context(
            patch("polyarb.daemon.l2_main._append_l3_boot", side_effect=_append_boot)
        )
        stack.enter_context(patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer))
        stack.enter_context(patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog))
        stack.enter_context(
            patch("polyarb.daemon.l2_main.ReconciliationPump", return_value=pump)
        )
        stack.enter_context(patch("polyarb.daemon.l2_main.listen_snapshot_complete", listener))
        main_task = asyncio.create_task(l2_main.main())
        await asyncio.wait_for(boot_entered.wait(), timeout=1.0)
        main_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(main_task, timeout=1.0)

    assert serve_stopped.is_set()
    assert server.should_exit is True
    consumer.run.assert_not_awaited()
    consumer.run_quiet_refresh.assert_not_awaited()
    watchdog.watch.assert_not_awaited()
    pump.run.assert_not_awaited()
    listener.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authorization_case", "runtime_dsn"),
    [
        ("missing", ""),
        ("rejected", "runtime-dsn"),
        ("service", "runtime-dsn"),
        ("superuser", "runtime-dsn"),
        ("retention", "runtime-dsn"),
    ],
)
async def test_failed_runtime_authorization_gates_all_direct_postgres_tasks(
    authorization_case, runtime_dsn
):
    """Every role-preflight failure keeps HTTP/WS fail-soft but starts no direct PG I/O."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value=runtime_dsn)
    settings.supabase_db_dsn.get_secret_value = MagicMock(
        side_effect=AssertionError("owner fallback is forbidden")
    )
    stop_event = asyncio.Event()
    store = MagicMock()
    store.append_boot = AsyncMock(return_value=False)
    cursor_store = MagicMock()
    cursor_store.read_position = AsyncMock()
    cursor_store.commit = AsyncMock()
    pump = MagicMock()
    pump.run = AsyncMock()
    listener = AsyncMock()
    consumer = MagicMock()

    async def _ws_run(_stop_event):
        await asyncio.sleep(0)
        stop_event.set()

    async def _idle(_stop_event):
        await asyncio.sleep(0)

    consumer.run = AsyncMock(side_effect=_ws_run)
    consumer.run_quiet_refresh = AsyncMock(side_effect=_idle)
    watchdog = MagicMock()
    watchdog.watch = AsyncMock(side_effect=_idle)
    server = _make_mock_server()
    create_app = MagicMock(return_value=MagicMock())
    promoter = MagicMock(name="run_periodic")
    sampler = MagicMock(name="run_sampler")
    event_writer = MagicMock(name="run_event_writer")

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer),
        patch("polyarb.daemon.l2_main.WsWatchdog", return_value=watchdog),
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=cursor_store),
        patch("polyarb.daemon.l2_main.ReconciliationPump", return_value=pump),
        patch("polyarb.daemon.l2_main.listen_snapshot_complete", listener),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", create_app),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=stop_event),
        patch("polyarb.observation.l3_promote.run_periodic", promoter),
        patch("polyarb.observation.l3_sampler.run_sampler", sampler),
        patch("polyarb.observation.l3_sampler.run_event_writer", event_writer),
    ):
        assert await asyncio.wait_for(l2_main.main(), timeout=2.0) == 0

    assert authorization_case
    create_app.assert_called_once()
    consumer.run.assert_awaited_once()
    consumer.run_quiet_refresh.assert_awaited_once()
    settings.supabase_db_dsn.get_secret_value.assert_not_called()
    cursor_store.read_position.assert_not_awaited()
    cursor_store.commit.assert_not_awaited()
    pump.run.assert_not_awaited()
    listener.assert_not_awaited()
    promoter.assert_not_called()
    sampler.assert_not_called()
    event_writer.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a mocked uvicorn.Server that "starts" immediately so the P9
# polling loop exits on the first iteration. .serve() remains alive until
# main sets should_exit, matching uvicorn's supervised lifetime contract.
# ─────────────────────────────────────────────────────────────────────────────

def _make_mock_server():
    instance = MagicMock()
    instance.started = True
    instance.should_exit = False

    async def _serve():
        while not instance.should_exit:
            await asyncio.sleep(0)

    instance.serve = _serve
    return instance


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — init order: logging → settings → sentry → sqlite
# ─────────────────────────────────────────────────────────────────────────────

async def test_init_order():
    """init_logging → load_settings → init_sentry → SQLiteStore (Phase 02 P9 invariant)."""
    calls: list[str] = []

    def _make_recorder(name: str, ret=None):
        def _inner(*a, **kw):
            calls.append(name)
            return ret if ret is not None else MagicMock()
        return _inner

    fake_settings = _make_fake_settings()
    mock_server = _make_mock_server()

    # Pre-set event so main() exits after the gate (skip awaiting stop_event)
    real_event = asyncio.Event()
    real_event.set()

    with (
        patch("polyarb.daemon.l2_main.init_logging", side_effect=_make_recorder("logging")),
        patch(
            "polyarb.daemon.l2_main.load_settings",
            side_effect=_make_recorder("settings", ret=fake_settings),
        ),
        patch("polyarb.daemon.l2_main.init_sentry", side_effect=_make_recorder("sentry")),
        patch(
            "polyarb.daemon.l2_main.SQLiteStore",
            side_effect=_make_recorder("sqlite", ret=MagicMock()),
        ),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=mock_server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=real_event),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
    ):
        from polyarb.daemon import l2_main
        try:
            await asyncio.wait_for(l2_main.main(), timeout=3.0)
        except (TimeoutError, SystemExit):
            pass

    # Order invariant — all 4 must have been recorded
    assert "logging" in calls, f"init_logging not invoked: calls={calls}"
    assert "settings" in calls, f"load_settings not invoked: calls={calls}"
    assert "sentry" in calls, f"init_sentry not invoked: calls={calls}"
    assert "sqlite" in calls, f"SQLiteStore not invoked: calls={calls}"
    assert calls.index("logging") < calls.index("settings"), f"order broken: {calls}"
    assert calls.index("settings") < calls.index("sentry"), f"order broken: {calls}"
    assert calls.index("sentry") < calls.index("sqlite"), f"order broken: {calls}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — server-started polling loop (P9 invariant)
# ─────────────────────────────────────────────────────────────────────────────

def test_server_started_gate_polled():
    """Grep-style assertion: l2_main.py source contains range(100) + sleep(0.1) gate."""
    src_path = Path(__file__).resolve().parents[2] / "src" / "polyarb" / "daemon" / "l2_main.py"
    assert src_path.exists(), f"l2_main.py not found at {src_path}"
    text = src_path.read_text()
    assert "range(100)" in text, "P9 invariant missing: range(100) polling loop"
    assert "server.started" in text, "P9 invariant missing: server.started gate"
    assert "asyncio.sleep(0.1)" in text, "P9 invariant missing: 0.1s sleep granularity"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — Sentry service tag set to polyarb-l2
# ─────────────────────────────────────────────────────────────────────────────

async def test_sentry_service_tag_polyarb_l2():
    """sentry_sdk.set_tag('service', 'polyarb-l2') called during main()."""
    tag_calls: list[tuple[str, str]] = []

    def _set_tag(key, val):
        tag_calls.append((key, val))

    fake_settings = _make_fake_settings()
    mock_server = _make_mock_server()
    real_event = asyncio.Event()
    real_event.set()

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=fake_settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag", side_effect=_set_tag),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=mock_server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=real_event),
    ):
        from polyarb.daemon import l2_main
        try:
            await asyncio.wait_for(l2_main.main(), timeout=3.0)
        except (TimeoutError, SystemExit):
            pass

    assert ("service", "polyarb-l2") in tag_calls, (
        f"expected service=polyarb-l2 tag, got {tag_calls}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — no L1 cross-pollination (T-03-03-03 mitigation)
# ─────────────────────────────────────────────────────────────────────────────

def test_no_import_from_l1_main():
    """l2_main.py must NOT import from polyarb.daemon.main (T-03-03-03)."""
    src_path = Path(__file__).resolve().parents[2] / "src" / "polyarb" / "daemon" / "l2_main.py"
    assert src_path.exists(), f"l2_main.py not found at {src_path}"
    text = src_path.read_text()
    assert "from polyarb.daemon.main import" not in text, "cross-pollination: imports L1 symbols"
    # Substring check excluding our own module
    forbidden_substr = "import polyarb.daemon.main"
    assert forbidden_substr not in text, f"cross-pollination: {forbidden_substr!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — first log line contains "polyarb-l2"
# ─────────────────────────────────────────────────────────────────────────────

async def test_logger_first_line_is_polyarb_l2(loguru_sink):
    """First INFO log line emitted by main() must contain 'polyarb-l2'."""
    fake_settings = _make_fake_settings()
    mock_server = _make_mock_server()
    real_event = asyncio.Event()
    real_event.set()

    with (
        patch("polyarb.daemon.l2_main.init_logging"),  # do NOT reset our test sink
        patch("polyarb.daemon.l2_main.load_settings", return_value=fake_settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=mock_server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=real_event),
    ):
        from polyarb.daemon import l2_main
        try:
            await asyncio.wait_for(l2_main.main(), timeout=3.0)
        except (TimeoutError, SystemExit):
            pass

    output = loguru_sink.getvalue()
    assert "polyarb-l2" in output, f"expected 'polyarb-l2' in first log lines, got: {output!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — shutdown signal propagates (F-04 contract — CancelledError NOT swallowed)
# ─────────────────────────────────────────────────────────────────────────────

async def test_shutdown_uses_one_five_second_drain_deadline():
    """Server, writer, and peers share one bounded shutdown deadline (F-04)."""
    src_path = Path(__file__).resolve().parents[2] / "src" / "polyarb" / "daemon" / "l2_main.py"
    assert src_path.exists()
    text = src_path.read_text()
    assert "async def _drain_daemon_tasks(" in text
    assert "timeout_s: float = 5.0" in text
    assert "await asyncio.wait(owned, timeout=drain_timeout_s)" in text
    assert "await _drain_daemon_tasks(" in text
    assert "server.should_exit = True" in text
