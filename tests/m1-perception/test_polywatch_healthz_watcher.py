"""Regression tests for the unified M1 production watcher.

The watcher is loaded from ``scripts/`` so these tests exercise the exact file
used by GitHub Actions without importing the application package or touching
the network.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
WATCHER_PATH = PROJECT_ROOT / "scripts" / "polywatch" / "healthz_watcher.py"


def _load_watcher() -> ModuleType:
    spec = importlib.util.spec_from_file_location("polywatch_healthz_watcher", WATCHER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


WATCHER = _load_watcher()


def _decision(name: str) -> Callable[..., tuple[str, str]]:
    function = getattr(WATCHER, name, None)
    assert callable(function), f"operational decision {name} is not implemented"
    return function


def _health(*, status: str = "pass", checks: dict[str, Any]) -> dict[str, Any]:
    return {"status": status, "checks": checks}


def _check(value: Any, *, status: str = "pass") -> list[dict[str, Any]]:
    return [{"componentId": "test", "observedValue": value, "status": status}]


def _healthy_l2_checks() -> dict[str, Any]:
    return {
        "ws:connection_state": _check("WAITING_FOR_EVENT", status="warn"),
        "ws:last_event_age_seconds": _check(5.0),
        "l3:active_count": _check(10),
        "l3:evidence_sample_age_seconds": _check(15.0),
        "l3:promoter_ledger_age_seconds": _check(60.0),
        "l3:membership_convergence": _check({"desired": 10, "committed": 10, "evidenced": 10}),
        "l3:worst_market_freshness": _check(30.0),
    }


def test_l1_quote_age_failure_pushes() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(301.0, status="fail"),
            "quote_feed:collector_state": _check("running"),
        }
    )

    action, reason = WATCHER.decide_l1(health)

    assert action == "push"
    assert "quote" in reason.lower()


@pytest.mark.parametrize("collector_state", ["error", "stopped"])
def test_l1_bad_collector_state_pushes(collector_state: str) -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check(collector_state),
        }
    )

    action, reason = WATCHER.decide_l1(health)

    assert action == "push"
    assert collector_state in reason


def test_polywatch_alerts_on_market_truth_coverage_failure() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "market_truth:coverage": _check("incomplete-source", status="fail"),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check("running"),
        }
    )

    assert WATCHER.decide_l1(health) == (
        "push",
        "L1 market truth coverage failed",
    )


def test_polywatch_alerts_when_market_truth_coverage_is_missing() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check("running"),
        }
    )

    assert WATCHER.decide_l1(health) == (
        "push",
        "L1 market truth coverage failed",
    )


def test_empty_opportunity_list_is_healthy() -> None:
    action, reason = _decision("decide_opportunity")(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "verified-standard-neg-risk",
            "count": 0,
            "opportunities": [],
        }
    )

    assert action == "noop"
    assert "count=0" in reason


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {
            "strategy": "wrong",
            "profit_basis": "gross-before-fees",
            "coverage": "verified-standard-neg-risk",
            "opportunities": [],
        },
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "net-after-fees",
            "coverage": "verified-standard-neg-risk",
            "opportunities": [],
        },
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "verified-standard-neg-risk",
        },
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "legacy-snapshot",
            "opportunities": [],
        },
    ],
)
def test_invalid_opportunity_contract_pushes(payload: dict[str, Any]) -> None:
    action, _ = _decision("decide_opportunity")(payload)
    assert action == "push"


def test_unreachable_opportunity_endpoint_pushes() -> None:
    action, reason = _decision("decide_opportunity")(None)
    assert action == "push"
    assert "unreachable" in reason.lower()


def test_l3_underfill_pushes() -> None:
    checks = _healthy_l2_checks()
    checks["l3:active_count"] = _check(8, status="warn")

    action, reason = WATCHER.decide_l2(_health(status="warn", checks=checks))

    assert action == "push"
    assert "active_count" in reason


@pytest.mark.parametrize(
    "failed_key",
    [
        "l3:evidence_sample_age_seconds",
        "l3:promoter_ledger_age_seconds",
        "l3:membership_convergence",
        "l3:worst_market_freshness",
    ],
)
def test_failed_strict_l3_check_pushes(failed_key: str) -> None:
    checks = _healthy_l2_checks()
    checks[failed_key][0]["status"] = "fail"

    action, reason = WATCHER.decide_l2(_health(status="warn", checks=checks))

    assert action == "push"
    assert failed_key in reason


def test_waiting_for_event_with_fresh_data_is_quiet() -> None:
    action, reason = WATCHER.decide_l2(_health(status="warn", checks=_healthy_l2_checks()))

    assert action == "noop"
    assert "ok" in reason.lower()


@pytest.mark.parametrize("status", [200, 302, 307])
def test_dashboard_live_or_sso_response_is_healthy(status: int) -> None:
    action, _ = _decision("decide_dashboard")(status, {}, None)
    assert action == "noop"


def test_dashboard_missing_deployment_pushes() -> None:
    action, reason = _decision("decide_dashboard")(
        404, {"x-vercel-error": "DEPLOYMENT_NOT_FOUND"}, None
    )
    assert action == "push"
    assert "DEPLOYMENT_NOT_FOUND" in reason


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (503, None),
        (None, "timed out"),
    ],
)
def test_dashboard_server_or_transport_failure_pushes(
    status: int | None, error: str | None
) -> None:
    action, _ = _decision("decide_dashboard")(status, {}, error)
    assert action == "push"


def test_resident_watcher_alerts_immediately_on_first_failure() -> None:
    decision = _decision("notification_decision")

    assert decision(("l1",), {}, now_s=1000.0, reminder_s=1800) == "alert"


def test_resident_watcher_suppresses_duplicate_alert_until_reminder() -> None:
    decision = _decision("notification_decision")
    state = {"active_keys": ["l1"], "last_alert_at_s": 1000.0}

    assert decision(("l1",), state, now_s=1100.0, reminder_s=1800) == "suppress"
    assert decision(("l1",), state, now_s=2800.0, reminder_s=1800) == "alert"


def test_resident_watcher_alerts_when_failure_set_changes() -> None:
    decision = _decision("notification_decision")
    state = {"active_keys": ["l1"], "last_alert_at_s": 1000.0}

    assert (
        decision(("l1", "dashboard"), state, now_s=1100.0, reminder_s=1800)
        == "alert"
    )


def test_resident_watcher_sends_one_recovery_transition() -> None:
    decision = _decision("notification_decision")

    assert (
        decision((), {"active_keys": ["l2"], "last_alert_at_s": 1000.0},
                 now_s=1100.0, reminder_s=1800)
        == "recovery"
    )
    assert decision((), {}, now_s=1100.0, reminder_s=1800) == "noop"


def test_failed_recovery_delivery_preserves_state_for_retry() -> None:
    update_state = getattr(WATCHER, "updated_notification_state", None)
    assert callable(update_state)
    state = {
        "active_keys": ["l2"],
        "last_alert_at_s": 1000.0,
        "last_seen_at_s": 1050.0,
    }

    updated = update_state(
        (),
        state,
        notification="recovery",
        now_s=1100.0,
        delivery_ok=False,
    )

    assert updated["active_keys"] == ["l2"]
    assert updated["last_alert_at_s"] == 1000.0


def test_successful_recovery_clears_resident_state() -> None:
    update_state = getattr(WATCHER, "updated_notification_state", None)
    assert callable(update_state)

    updated = update_state(
        (),
        {"active_keys": ["dashboard"], "last_alert_at_s": 1000.0},
        notification="recovery",
        now_s=1100.0,
        delivery_ok=True,
    )

    assert updated == {
        "active_keys": [],
        "last_seen_at_s": 1100.0,
        "last_alert_at_s": 0.0,
    }


def test_cron_machine_runs_polywatch_every_two_minutes() -> None:
    crontab = (PROJECT_ROOT / "crontab").read_text()

    assert "*/2 * * * *" in crontab
    assert "POLYWATCH_STATE_FILE=/tmp/polywatch-healthz-state.json" in crontab
    assert "python /app/scripts/polywatch/healthz_watcher.py" in crontab


def test_runtime_image_contains_polywatch_script() -> None:
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text()

    assert (
        "COPY --chown=polyarb:polyarb scripts/polywatch/healthz_watcher.py "
        "/app/scripts/polywatch/healthz_watcher.py"
    ) in dockerfile


def test_makefile_exposes_resident_polywatch_status() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()

    assert "polywatch-resident-status:" in makefile
    assert "flyctl status -a polyarb-l1 --json" in makefile
    assert "flyctl logs -a polyarb-l1 --machine" in makefile
    assert "--no-tail --json" in makefile
    assert 'contains("polywatch")' in makefile
    assert "/tmp/polywatch-healthz-state.json" in makefile
