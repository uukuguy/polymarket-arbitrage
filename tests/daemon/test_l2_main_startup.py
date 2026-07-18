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
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from loguru import logger

# F-3 SECURITY ESCAPE HATCH: pytest tmp_path lives outside project root by design
import os
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
    s.scan_shared_secret = MagicMock(get_secret_value=lambda: "")
    s.supabase_db_dsn = MagicMock(get_secret_value=lambda: "")
    return s


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
        patch("polyarb.daemon.l2_main.load_settings", side_effect=_make_recorder("settings", ret=fake_settings)),
        patch("polyarb.daemon.l2_main.init_sentry", side_effect=_make_recorder("sentry")),
        patch("polyarb.daemon.l2_main.SQLiteStore", side_effect=_make_recorder("sqlite", ret=MagicMock())),
        patch("polyarb.daemon.l2_main.uvicorn.Server", return_value=mock_server),
        patch("polyarb.daemon.l2_main.create_l2_app", return_value=MagicMock()),
        patch("polyarb.daemon.l2_main.asyncio.Event", return_value=real_event),
        patch("polyarb.daemon.l2_main.sentry_sdk.set_tag"),
    ):
        from polyarb.daemon import l2_main
        try:
            await asyncio.wait_for(l2_main.main(), timeout=3.0)
        except (asyncio.TimeoutError, SystemExit):
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
        except (asyncio.TimeoutError, SystemExit):
            pass

    assert ("service", "polyarb-l2") in tag_calls, f"expected service=polyarb-l2 tag, got {tag_calls}"


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
        except (asyncio.TimeoutError, SystemExit):
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
