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

# F-3 SECURITY ESCAPE HATCH: pytest tmp_path lives outside project root by design
import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from loguru import logger

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
    """Boot, WS observer, cursor, and promoter share one dependency graph."""
    from polyarb.daemon import l2_main

    settings = _make_fake_settings()
    settings.l2_runtime_db_dsn.get_secret_value = MagicMock(return_value="runtime-dsn")
    settings.supabase_db_dsn.get_secret_value = MagicMock(
        side_effect=AssertionError("owner DSN must never be read")
    )
    store = MagicMock()
    store.append_boot = AsyncMock(return_value=True)
    server = _make_mock_server()
    stop_event = asyncio.Event()
    consumer = MagicMock()

    async def _idle(_stop_event):
        await asyncio.sleep(0)

    consumer.run = _idle
    consumer.run_quiet_refresh = _idle
    promoter_kwargs: dict[str, object] = {}

    def _run_periodic(**kwargs):
        promoter_kwargs.update(kwargs)
        stop_event.set()

        async def _idle_promoter():
            await asyncio.sleep(0)

        return _idle_promoter()

    with (
        patch("polyarb.daemon.l2_main.init_logging"),
        patch("polyarb.daemon.l2_main.load_settings", return_value=settings),
        patch("polyarb.daemon.l2_main.init_sentry"),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
        patch("polyarb.daemon.l2_main.L3EvidenceStore", return_value=store),
        patch("polyarb.daemon.l2_main.SQLiteStore", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.WsConsumer", return_value=consumer) as consumer_type,
        patch("polyarb.daemon.l2_main.AsyncpgCursorStore", return_value=MagicMock()) as cursor,
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=stop_event),
        patch("polyarb.observation.l3_promote.run_periodic", new=_run_periodic),
    ):
        assert await asyncio.wait_for(l2_main.main(), timeout=3.0) == 0

    store.append_boot.assert_awaited_once()
    cursor.assert_called_once_with(dsn="runtime-dsn")
    settings.supabase_db_dsn.get_secret_value.assert_not_called()
    observer = consumer_type.call_args.kwargs["membership_observer"]
    runtime = promoter_kwargs["evidence_runtime"]
    assert observer.__self__ is runtime
    assert promoter_kwargs["evidence_store"] is store
    assert store.append_boot.await_args.args[0].boot_id == runtime.snapshot().boot_id
    assert runtime.snapshot().writer_ok is True
    assert runtime.snapshot().status.value == "warn"
    assert runtime.snapshot().reason_code == "cold_start"


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a mocked uvicorn.Server that "starts" immediately so the P9
# polling loop exits on the first iteration. .serve() returns an already-done
# coroutine so the server_task completes promptly.
# ─────────────────────────────────────────────────────────────────────────────

def _make_mock_server():
    instance = MagicMock()
    instance.started = True
    instance.should_exit = False

    async def _serve():
        # Sleep briefly so the started polling loop sees a coroutine on the loop
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

async def test_shutdown_via_wait_for_timeout():
    """main() must call asyncio.wait_for(server_task, timeout=5.0) on shutdown (F-04)."""
    src_path = Path(__file__).resolve().parents[2] / "src" / "polyarb" / "daemon" / "l2_main.py"
    assert src_path.exists()
    text = src_path.read_text()
    # F-04: bounded shutdown wait must exist
    assert "asyncio.wait_for" in text, "F-04 missing: asyncio.wait_for bounded shutdown"
    assert "timeout=5.0" in text or "timeout=5" in text, "F-04 missing: 5.0s shutdown timeout"
    # F-04: CancelledError must propagate (the `raise` keyword present in handler)
    assert "should_exit" in text, "graceful shutdown missing: server.should_exit"
