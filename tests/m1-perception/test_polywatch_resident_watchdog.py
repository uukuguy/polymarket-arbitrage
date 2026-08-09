"""Contracts for the external watchdog of the resident Polywatch machine."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
WATCHDOG_PATH = PROJECT_ROOT / "scripts" / "polywatch" / "resident_watchdog.py"


def _load_watchdog() -> ModuleType:
    spec = importlib.util.spec_from_file_location("polywatch_resident_watchdog", WATCHDOG_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


WATCHDOG = _load_watchdog()


def _machine(machine_id: str, state: str, process_group: str) -> dict[str, Any]:
    return {
        "id": machine_id,
        "state": state,
        "config": {"metadata": {"fly_process_group": process_group}},
    }


def test_inspect_requires_exactly_one_started_cron_machine() -> None:
    healthy = [_machine("app-1", "started", "app"), _machine("cron-1", "started", "cron")]
    stopped = [_machine("app-1", "started", "app"), _machine("cron-1", "stopped", "cron")]

    assert WATCHDOG.inspect_cron_machines(healthy).healthy is True
    assessment = WATCHDOG.inspect_cron_machines(stopped)
    assert assessment.healthy is False
    assert assessment.repair_ids == ("cron-1",)
    assert "stopped" in assessment.reason


def test_missing_or_duplicate_cron_is_an_incident_not_a_healthy_zero() -> None:
    missing = WATCHDOG.inspect_cron_machines([_machine("app-1", "started", "app")])
    duplicate = WATCHDOG.inspect_cron_machines([
        _machine("cron-1", "started", "cron"),
        _machine("cron-2", "started", "cron"),
    ])

    assert missing.healthy is False
    assert missing.repair_ids == ()
    assert "expected exactly one" in missing.reason
    assert duplicate.healthy is False
    assert duplicate.repair_ids == ()
    assert "expected exactly one" in duplicate.reason


def test_main_repairs_stopped_cron_and_alerts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    alerts: list[str] = []

    def fake_run(argv: list[str], **_kwargs: object) -> str:
        calls.append(argv)
        if argv[1:3] == ["machines", "list"]:
            list_calls = [call for call in calls if call[1:3] == ["machines", "list"]]
            state = "stopped" if len(list_calls) == 1 else "started"
            return json.dumps([_machine("cron-1", state, "cron")])
        assert argv[1:3] == ["machines", "start"]
        return "started"

    monkeypatch.setattr(WATCHDOG, "_run_flyctl", fake_run)
    monkeypatch.setattr(WATCHDOG, "_send_telegram", lambda text: alerts.append(text) or True)

    assert WATCHDOG.main(["--app", "polyarb-l1", "--repair"]) == 0
    assert calls[1][:4] == ["flyctl", "machines", "start", "cron-1"]
    assert calls[2][1:3] == ["machines", "list"]
    assert len(alerts) == 1
    assert "stopped" in alerts[0]
    assert "start issued=ok" in alerts[0]


def test_main_fails_loudly_when_repair_cannot_run(monkeypatch: pytest.MonkeyPatch) -> None:
    alerts: list[str] = []

    def fake_run(argv: list[str], **_kwargs: object) -> str:
        if argv[1:3] == ["machines", "list"]:
            return json.dumps([_machine("cron-1", "stopped", "cron")])
        raise WATCHDOG.FlyctlError("permission denied")

    monkeypatch.setattr(WATCHDOG, "_run_flyctl", fake_run)
    monkeypatch.setattr(WATCHDOG, "_send_telegram", lambda text: alerts.append(text) or True)

    assert WATCHDOG.main(["--repair"]) == 1
    assert alerts and "start issued=failed" in alerts[0]
