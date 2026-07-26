"""RED tests for polyarb.daemon.ws_consumer.WsConsumer state surfacing.

Plan 04 Wave 0. Drives WsConsumer implementation:
- Initial state == "DISCONNECTED" before run() starts
- current_state visible via app.state to /health endpoint
- last_event_at_s updates on each event
- subscribed_assets tracked from constructor
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

# F-3 SECURITY ESCAPE HATCH (Phase 02.1 — pytest tmp_path lives outside project root by design)
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def _make_settings(tmp_path: Path) -> Any:
    """Build a Settings instance with minimal viable fields for create_l2_app."""
    from polyarb.config import Settings

    return Settings(
        db_path=tmp_path / "l2.db",
        parquet_root=tmp_path / "parquet",
        cache_root=tmp_path / "cache",
        _env_file=None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — initial state is DISCONNECTED
# ─────────────────────────────────────────────────────────────────────────────


def test_consumer_initial_state_disconnected() -> None:
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=[],
    )
    assert consumer.current_state == "DISCONNECTED", (
        f"expected DISCONNECTED before run(), got {consumer.current_state!r}"
    )


def test_membership_mutation_synchronously_updates_runtime_snapshot() -> None:
    """Mutation -> callback -> runtime snapshot is one synchronous chain."""
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog
    from polyarb.observation.l3_evidence import L3EvidenceRuntime, RuntimeIdentity

    identity = RuntimeIdentity(
        machine_id="machine",
        machine_version="version",
        image_ref="image",
        release_id="release",
        code_version="code",
        recipe_sha256="a" * 64,
        acceptance_config_hash="b" * 64,
    )
    runtime = L3EvidenceRuntime(
        identity,
        started_at=datetime(2026, 7, 23, tzinfo=UTC),
    )
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30.0),
        on_event=lambda ev: None,
        initial_assets=[],
        membership_observer=runtime.update_membership,
        event_recorder=lambda *args, **kwargs: None,
    )

    consumer.set_l3_desired(["yes", "no"])

    status = runtime.snapshot()
    assert status.ws_generation == 0
    assert status.desired == frozenset({"yes", "no"})
    assert status.committed == frozenset()
    assert status.evidenced == frozenset()


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — current_state visible to /health endpoint via app.state.ws_consumer
# ─────────────────────────────────────────────────────────────────────────────


def test_state_visible_to_health_endpoint(tmp_path: Path) -> None:
    """Real WsConsumer wired into create_l2_app → /health reads .current_state."""
    from starlette.testclient import TestClient

    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog
    from polyarb.http.l2_app import create_l2_app
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = _make_settings(tmp_path)
    wd = WsWatchdog(stale_s=30.0)
    wd.touch()  # → state=WAITING_FOR_EVENT
    consumer = WsConsumer(
        settings=settings,
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=["0xabc"],
    )
    # current_state should delegate to watchdog when wired
    state = consumer.current_state
    assert state in ("WAITING_FOR_EVENT", "CONNECTED", "DISCONNECTED"), state

    store = SQLiteStore(settings.db_path)
    store.init_schema()
    app = create_l2_app(
        sqlite_store=store,
        settings=settings,
        ws_consumer=consumer,
        event_listener=None,
    )
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code in (200, 503)
    body = resp.json()
    # ws_connection_state sub-check renders consumer.current_state
    ws_check = body["checks"].get("ws:connection_state", [{}])[0]
    assert ws_check.get("observedValue") == state, (
        f"health body did not surface consumer.current_state={state!r}; got: {ws_check}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — last_event_at_s updates on event
# ─────────────────────────────────────────────────────────────────────────────


def test_last_event_at_s_initialized() -> None:
    """last_event_at_s must be an epoch float at construction time."""
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    now = time.time()
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=[],
    )
    # last_event_at_s should be epoch seconds float near construction time
    assert isinstance(consumer.last_event_at_s, float)
    # within ±5s of construction
    assert abs(consumer.last_event_at_s - now) < 5.0, (
        f"last_event_at_s={consumer.last_event_at_s}, now={now}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — subscribed_assets tracked from constructor
# ─────────────────────────────────────────────────────────────────────────────


def test_subscribed_assets_tracked() -> None:
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=["0xabc", "0xdef"],
    )
    assert consumer.subscribed_assets == ["0xabc", "0xdef"]
    # The returned list must be a copy (defensive — Plan 05 candidate refresh
    # MUST NOT be able to mutate the consumer's internal list via the property)
    snapshot = consumer.subscribed_assets
    snapshot.append("0xZZZ")
    assert consumer.subscribed_assets == ["0xabc", "0xdef"], (
        "subscribed_assets returned the internal list (not a copy)"
    )


def test_l2_main_owns_quiet_refresh_task_lifecycle() -> None:
    """The refresh loop is named, cancelled, and bounded with daemon peers."""
    root = Path(__file__).resolve().parents[2]
    source = (root / "src/polyarb/daemon/l2_main.py").read_text()

    assert "quiet_refresh_task = _create_daemon_task(" in source
    assert "ws_consumer.run_quiet_refresh(stop_event)" in source
    assert 'name="ws-quiet-refresh"' in source
    assert "await _drain_daemon_tasks(" in source
    assert "quiet_refresh_task," in source
