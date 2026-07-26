"""Phase 05.1 startup wiring: the durable pump replaces replay and sentinel prime."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

_L2_MAIN_PATH = Path(__file__).parents[2] / "src" / "polyarb" / "daemon" / "l2_main.py"


def _source() -> str:
    return _L2_MAIN_PATH.read_text()


def test_settings_expose_positive_reconciliation_windows(monkeypatch):
    from pydantic import ValidationError

    from polyarb.config import Settings

    monkeypatch.delenv("POLYARB_EVENT_RECONCILE_POLL_SECONDS", raising=False)
    monkeypatch.delenv("POLYARB_EVENT_RECONCILE_STALE_SECONDS", raising=False)
    settings = Settings()
    assert settings.event_reconcile_poll_seconds == 60
    assert settings.event_reconcile_stale_seconds == 180

    with pytest.raises(ValidationError):
        Settings(event_reconcile_poll_seconds=0)
    with pytest.raises(ValidationError):
        Settings(event_reconcile_stale_seconds=-1)


def test_l2_main_uses_one_reconciliation_pump_as_cursor_owner():
    src = _source()
    assert "ReconciliationPump(" in src
    assert "AsyncpgCursorStore(" in src
    assert "pump_task = _create_daemon_task" in src
    assert "pump.run(stop_event)" in src
    assert 'name="reconciliation-pump"' in src


def test_notify_callback_only_wakes_pump_without_task_fanout():
    src = _source()
    assert "reconciliation_pump.notify(payload)" in src
    assert "asyncio.create_task(\n                on_snapshot_complete" not in src
    assert "def _dispatch_on_snapshot" not in src


def test_startup_replay_and_sentinel_prime_are_removed():
    src = _source()
    assert "catchup_from_cursor" not in src
    assert '"snapshot_id": -1' not in src
    assert "_startup_prime" not in src
    assert "INSERT INTO l2_event_cursor" not in src


def test_listener_and_pump_share_runtime_state_and_stop_independently():
    src = _source()
    assert "ReconciliationState()" in src
    assert "state=reconciliation_state" in src
    assert "await _drain_daemon_tasks(" in src
    assert "pump_task," in src
