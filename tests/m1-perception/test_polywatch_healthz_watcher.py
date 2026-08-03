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


def _check(
    value: Any,
    *,
    status: str = "pass",
    output: str | None = None,
) -> list[dict[str, Any]]:
    return [
        {
            "componentId": "test",
            "observedValue": value,
            "status": status,
            "output": output,
        }
    ]


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


@pytest.mark.parametrize("reason", [
    "structure-event-member-checkpoint-invalid",
    "structure-event-source-receipt-invalid",
    "structure-event-member-receipt-invalid",
])
def test_event_member_failure_preempts_generic_snapshot_cancellation(reason) -> None:
    health = _health(status="fail", checks={
        "snapshot:structure_event_members": _check(
            "invalid", status="fail",
            output=reason,
        ),
        "snapshot:last_success_age_seconds": _check(100000.0, status="fail"),
        "snapshot:latest_attempt": _check("cancelled", status="fail"),
    })
    assert WATCHER.decide_l1(health) == (
        "push", "L1 Structure event-member sidecar failed "
        f"({reason})",
    )


def test_event_member_waiting_natural_window_does_not_alert() -> None:
    health = _health(status="pass", checks={
        "snapshot:structure_event_members": _check(
            "waiting-natural-window",
            status="pass",
            output=("authenticated=true reason="
                    "structure-event-source-receipt-unavailable"),
        ),
        "snapshot:last_success_age_seconds": _check(60.0),
        "market_truth:coverage": _check("complete"),
        "quote_feed:last_complete_age_seconds": _check(60.0),
        "quote_feed:collector_state": _check("running"),
    })
    action, reason = WATCHER.decide_l1(health)
    assert action == "noop"
    assert "sidecar failed" not in reason


def test_event_member_component_first_suppress_reminder_recovery_lifecycle() -> None:
    health = _health(status="fail", checks={
        "snapshot:structure_event_members": _check(
            "invalid", status="fail",
            output="structure-event-member-checkpoint-invalid",
        ),
    })
    assert WATCHER.decide_l1(health)[0] == "push"
    state: dict[str, Any] = {}
    decisions = WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=1_000.0, reminder_s=1_800,
    )
    assert decisions == {"l1": "alert"}
    state = WATCHER.updated_component_notification_state(
        {"l1": True}, state, decisions, now_s=1_000.0,
        delivery_ok_by_component={"l1": True},
    )
    assert WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=1_100.0, reminder_s=1_800,
    ) == {"l1": "suppress"}
    assert WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=2_800.0, reminder_s=1_800,
    ) == {"l1": "alert"}
    assert WATCHER.component_notification_decisions(
        {"l1": False}, state, now_s=1_100.0, reminder_s=1_800,
    ) == {"l1": "recovery"}


def _terminal_drift_check(*, status: str = "fail") -> list[dict[str, Any]]:
    return _check(
        "terminal-stale" if status == "fail" else "drift-safe-sealed",
        status=status,
        output=(
            "contract=structure-drift-classifier-v2 "
            "comparison=comparison-v2-terminal reason=drift-unclassified "
            'diagnostic_counts={"other-zero-removal-reason": 2}'
            if status == "fail"
            else "contract=structure-drift-classifier-v2 "
            "comparison=comparison-v2-sealed reason=None"
        ),
    )


def test_authenticated_drift_terminal_preempts_generic_snapshot_failures() -> None:
    health = _health(status="fail", checks={
        "snapshot:structure_generation_drift": _terminal_drift_check(),
        "snapshot:last_success_age_seconds": _check(100_000.0, status="fail"),
        "snapshot:latest_attempt": _check("cancelled", status="fail"),
    })

    action, reason = WATCHER.decide_l1(health)

    assert action == "push"
    assert reason == (
        "L1 Structure drift terminal "
        "(contract=structure-drift-classifier-v2, "
        "comparison=comparison-v2-terminal, reason=drift-unclassified, "
        'diagnostics={"other-zero-removal-reason": 2})'
    )


@pytest.mark.parametrize(
    "defer_reason",
    ("structure-drift-identity-stale", "structure-drift-status-unavailable"),
)
def test_durable_drift_defer_preempts_generic_snapshot_failure(
    defer_reason: str,
) -> None:
    health = _health(status="fail", checks={
        "snapshot:structure_generation_drift": _check(
            "none", status="warn", output="reason=structure-drift-incomplete"
        ),
        "snapshot:producer_defer": _check(defer_reason, status="warn"),
        "snapshot:last_success_age_seconds": _check(100_000.0, status="fail"),
        "snapshot:latest_attempt": _check("cancelled", status="fail"),
    })

    assert WATCHER.decide_l1(health) == (
        "push",
        f"L1 Structure drift admission deferred (reason={defer_reason})",
    )


def test_drift_incident_lifecycle_recovers_only_after_healthy_drift_status() -> None:
    stale = _health(status="fail", checks={
        "snapshot:structure_generation_drift": _terminal_drift_check(),
    })
    sealed = _health(status="pass", checks={
        "snapshot:structure_generation_drift": _terminal_drift_check(status="pass"),
        "snapshot:last_success_age_seconds": _check(60.0),
        "market_truth:coverage": _check("complete"),
        "quote_feed:last_complete_age_seconds": _check(20.0),
        "quote_feed:collector_state": _check("running"),
    })
    pending = _health(status="warn", checks={
        "snapshot:structure_generation_drift": _check(
            "none",
            status="warn",
            output=(
                "enabled=true phase=generation-members "
                "contract=structure-drift-classifier-v2 "
                "comparison=comparison-v2-next "
                "reason=structure-drift-incomplete"
            ),
        ),
        "snapshot:last_success_age_seconds": _check(60.0),
        "market_truth:coverage": _check("complete"),
        "quote_feed:last_complete_age_seconds": _check(20.0),
        "quote_feed:collector_state": _check("running"),
    })
    state: dict[str, Any] = {}

    assert WATCHER.decide_l1(stale)[0] == "push"
    decisions = WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=1_000.0, reminder_s=1_800,
    )
    assert decisions == {"l1": "alert"}
    state = WATCHER.updated_component_notification_state(
        {"l1": True}, state, decisions, now_s=1_000.0,
        delivery_ok_by_component={"l1": True},
    )
    assert WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=1_100.0, reminder_s=1_800,
    ) == {"l1": "suppress"}
    assert WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=2_800.0, reminder_s=1_800,
    ) == {"l1": "alert"}

    assert WATCHER.decide_l1(pending)[0] == "push"
    assert WATCHER.component_notification_decisions(
        {"l1": True}, state, now_s=2_900.0, reminder_s=1_800,
    ) == {"l1": "alert"}

    assert WATCHER.decide_l1(sealed)[0] == "noop"
    assert WATCHER.component_notification_decisions(
        {"l1": False}, state, now_s=2_900.0, reminder_s=1_800,
    ) == {"l1": "recovery"}


def test_authenticated_drift_terminal_reaches_telegram_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_l1 = [_health(status="fail", checks={
        "snapshot:structure_generation_drift": _terminal_drift_check(),
    })]
    opportunity = {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "coverage": "verified-standard-neg-risk",
        "refreshing": False,
        "latest_structure_snapshot_id": 10,
        "source_snapshot_id": 10,
        "count": 0,
        "opportunities": [],
    }

    def fetch(url: str) -> dict[str, Any]:
        if url == WATCHER.L1_HEALTHZ:
            return current_l1[0]
        if url == WATCHER.OPPORTUNITY_URL:
            return opportunity
        return _health(status="warn", checks=_healthy_l2_checks())

    messages: list[str] = []
    now_s = [1_000.0]
    monkeypatch.setattr(WATCHER, "_fetch_json", fetch)
    monkeypatch.setattr(WATCHER, "_probe_dashboard", lambda _url: (200, {}, None))
    monkeypatch.setattr(
        WATCHER,
        "_send_telegram",
        lambda message: messages.append(message) or True,
    )
    monkeypatch.setattr(WATCHER, "STATE_FILE", str(tmp_path / "polywatch-state.json"))
    monkeypatch.setattr(WATCHER.time, "time", lambda: now_s[0])

    assert WATCHER.main() == 0
    assert len(messages) == 1
    assert "structure-drift-classifier-v2" in messages[0]
    assert "comparison-v2-terminal" in messages[0]
    assert "drift-unclassified" in messages[0]
    assert "other-zero-removal-reason" in messages[0]

    now_s[0] = 1_100.0
    assert WATCHER.main() == 0
    assert len(messages) == 1

    now_s[0] = 2_800.0
    assert WATCHER.main() == 0
    assert len(messages) == 2
    assert "comparison-v2-terminal" in messages[1]

    current_l1[0] = _health(status="pass", checks={
        "snapshot:structure_generation_drift": _terminal_drift_check(status="pass"),
        "snapshot:last_success_age_seconds": _check(60.0),
        "market_truth:coverage": _check("complete"),
        "quote_feed:last_complete_age_seconds": _check(20.0),
        "quote_feed:collector_state": _check("running"),
    })
    now_s[0] = 2_900.0
    assert WATCHER.main() == 0
    assert len(messages) == 3
    assert "polywatch recovered" in messages[2]
    assert "resolved: l1" in messages[2]


def test_l1_quote_refresh_transition_does_not_alert() -> None:
    health = _health(
        status="warn",
        checks={
            "snapshot:last_success_age_seconds": _check(20.0),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(
                10.0,
                status="warn",
                output="source-snapshot-refreshing-serving-previous",
            ),
            "quote_feed:collector_state": _check("collecting"),
        },
    )

    assert WATCHER.decide_l1(health) == (
        "noop",
        "L1 quote refresh in progress for current Structure",
    )


def test_opportunity_endpoint_failure_during_refresh_alerts() -> None:
    health = _health(
        status="warn",
        checks={
            "quote_feed:last_complete_age_seconds": _check(
                10.0,
                status="warn",
                output="source-snapshot-refreshing-serving-previous",
            ),
            "quote_feed:collector_state": _check("collecting"),
        },
    )

    action, reason = WATCHER.decide_opportunity(None, l1_health=health)

    assert action == "push"
    assert "unreachable" in reason.lower()


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


def test_l1_quote_retention_failure_pushes() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check("pass"),
            "quote_feed:retention": _check(1, status="warn", output="OperationalError"),
        }
    )

    assert WATCHER.decide_l1(health) == (
        "push",
        "L1 quote retention warn (consecutive=1, error=OperationalError)",
    )


def test_l1_volume_headroom_warning_pushes() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check("pass"),
            "quote_feed:retention": _check(0),
            "storage:volume_free_percent": _check(
                19.0,
                status="warn",
                output="free_bytes=19 total_bytes=100",
            ),
        }
    )

    assert WATCHER.decide_l1(health) == (
        "push",
        "L1 volume headroom warn (free=19.0%)",
    )


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


def test_failed_snapshot_attempt_pushes_even_when_last_success_is_fresh() -> None:
    health = _health(
        checks={
            "snapshot:last_success_age_seconds": _check(60.0),
            "snapshot:latest_attempt": _check("failed", status="warn"),
            "snapshot:failure_counter": _check(1, status="warn"),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check("running"),
        }
    )

    action, reason = WATCHER.decide_l1(health)

    assert action == "push"
    assert "latest snapshot attempt failed" in reason.lower()


def test_stale_snapshot_in_recovering_mode_never_blindly_unpauses() -> None:
    health = _health(
        status="fail",
        checks={
            "snapshot:last_success_age_seconds": _check(3_601.0, status="fail"),
            "snapshot:latest_attempt": _check("running"),
            "snapshot:failure_counter": _check(5, status="fail"),
            "snapshot:structure_sync": _check(
                "events_complete",
                status="warn",
                output="stage=markets event_pages=161 market_pages=1106",
            ),
            "market_truth:coverage": _check("complete"),
            "quote_feed:last_complete_age_seconds": _check(20.0),
            "quote_feed:collector_state": _check("running"),
        },
    )

    action, reason = WATCHER.decide_l1(health)

    assert action == "push"
    assert "RECOVERING" in reason
    assert "unpause" not in reason.lower()


def test_empty_opportunity_list_is_healthy() -> None:
    action, reason = _decision("decide_opportunity")(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "verified-standard-neg-risk",
            "refreshing": False,
            "latest_structure_snapshot_id": 10,
            "source_snapshot_id": 10,
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


@pytest.mark.parametrize(
    "version_state",
    (
        {
            "refreshing": True,
            "latest_structure_snapshot_id": 10,
            "source_snapshot_id": 10,
        },
        {
            "refreshing": False,
            "latest_structure_snapshot_id": 11,
            "source_snapshot_id": 10,
        },
    ),
)
def test_opportunity_rejects_incoherent_version_state(
    version_state: dict[str, Any],
) -> None:
    payload = {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "coverage": "verified-standard-neg-risk",
        "opportunities": [],
        **version_state,
    }

    action, reason = WATCHER.decide_opportunity(payload)

    assert action == "push"
    assert "version" in reason.lower()


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


def test_l1_recovery_is_sent_while_l2_remains_active() -> None:
    decisions = WATCHER.component_notification_decisions(
        {"l1": False, "l2": True},
        {
            "incidents": {
                "l1": {"active": True, "last_alert_at_s": 1_000.0},
                "l2": {"active": True, "last_alert_at_s": 1_000.0},
            }
        },
        now_s=1_100.0,
        reminder_s=1_800,
    )

    assert decisions == {"l1": "recovery", "l2": "suppress"}


def test_legacy_active_keys_become_independent_incidents() -> None:
    normalized = WATCHER.normalize_notification_state(
        {"active_keys": ["l1", "l2"], "last_alert_at_s": 1_000.0}
    )

    assert normalized["incidents"] == {
        "l1": {"active": True, "last_alert_at_s": 1_000.0},
        "l2": {"active": True, "last_alert_at_s": 1_000.0},
    }


def test_failed_l1_recovery_delivery_keeps_only_l1_incident() -> None:
    updated = WATCHER.updated_component_notification_state(
        {"l1": False, "l2": True},
        {
            "incidents": {
                "l1": {"active": True, "last_alert_at_s": 1_000.0},
                "l2": {"active": True, "last_alert_at_s": 1_000.0},
            }
        },
        {"l1": "recovery", "l2": "suppress"},
        now_s=1_100.0,
        delivery_ok_by_component={"l1": False, "l2": True},
    )

    assert updated["incidents"] == {
        "l1": {"active": True, "last_alert_at_s": 1_000.0},
        "l2": {"active": True, "last_alert_at_s": 1_000.0},
    }


def test_cron_machine_runs_only_polywatch_every_two_minutes() -> None:
    crontab = (PROJECT_ROOT / "crontab").read_text()

    assert "*/2 * * * *" in crontab
    assert "POLYWATCH_STATE_FILE=/tmp/polywatch-healthz-state.json" in crontab
    assert "python /app/scripts/polywatch/healthz_watcher.py" in crontab
    assert "polyarb.snapshot" not in crontab
    assert "snapshots-purge" not in crontab


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
