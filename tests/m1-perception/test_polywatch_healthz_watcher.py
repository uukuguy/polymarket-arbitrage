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
        "l3:membership_convergence": _check(
            {"desired": 10, "committed": 10, "evidenced": 10}
        ),
        "l3:worst_market_freshness": _check(30.0),
    }


def test_l1_quote_age_failure_pushes() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
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
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check(collector_state),
        }
    )

    action, reason = WATCHER.decide_l1(health)

    assert action == "push"
    assert collector_state in reason


def test_empty_opportunity_list_is_healthy() -> None:
    action, reason = _decision("decide_opportunity")(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
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
            "opportunities": [],
        },
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "net-after-fees",
            "opportunities": [],
        },
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
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
    action, reason = WATCHER.decide_l2(
        _health(status="warn", checks=_healthy_l2_checks())
    )

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
