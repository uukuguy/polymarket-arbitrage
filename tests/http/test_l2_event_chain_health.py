"""Phase 05.1 live event-chain health contracts."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from pydantic import SecretStr


def _settings(*, dsn: str = "postgresql://user:secret@db.example/prod"):
    from polyarb.config import Settings

    return Settings(
        supabase_db_dsn=SecretStr(""),
        l2_runtime_db_dsn=SecretStr(dsn),
        event_reconcile_poll_seconds=60,
        event_reconcile_stale_seconds=180,
    )


def _checks(state, *, now: float = 1_000.0, dsn: str = "postgresql://x"):
    from polyarb.http.l2_health import _build_l2_health_checks

    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None
    return _build_l2_health_checks(
        store,
        _settings(dsn=dsn),
        ws_consumer=None,
        event_listener=state,
        now_s=now,
    )


def _live_state(**overrides):
    from polyarb.events.reconciliation import ReconciliationState

    state = ReconciliationState(
        is_connected=True,
        reconnect_count=2,
        last_notification_s=100.0,
        last_reconciliation_success_s=995.0,
        latest_snapshot_id=12,
        committed_cursor=12,
        cursor_lag=0,
    )
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


def test_connected_caught_up_chain_exposes_live_facts() -> None:
    checks, _overall = _checks(_live_state())

    assert checks["event_bus:connection_state"][0]["status"] == "pass"
    assert checks["event_bus:connection_state"][0]["observedValue"] == "listening"
    assert checks["event_bus:last_notification_age_seconds"][0]["status"] == "pass"
    assert checks["event_bus:last_reconciliation_age_seconds"][0]["status"] == "pass"
    assert checks["event_bus:cursor_lag"][0]["status"] == "pass"
    reconnect = checks["event_bus:reconnect_count"][0]
    assert reconnect["observedValue"] == 2
    notification_at = checks["event_bus:last_notification_at"][0]
    assert notification_at["observedValue"] == 100.0
    assert notification_at["status"] == "pass"


def test_quiet_notifications_do_not_fail_caught_up_chain() -> None:
    checks, overall = _checks(_live_state(last_notification_s=1.0))

    notification = checks["event_bus:last_notification_age_seconds"][0]
    assert notification["observedValue"] == 999.0
    assert notification["status"] == "pass"
    assert checks["event_bus:last_reconciliation_age_seconds"][0]["status"] == "pass"
    assert checks["event_bus:cursor_lag"][0]["status"] == "pass"
    assert overall != "fail"


def test_stale_reconciliation_fails_strict_health() -> None:
    checks, overall = _checks(_live_state(last_reconciliation_success_s=700.0), now=1_000.0)

    assert checks["event_bus:last_reconciliation_age_seconds"][0]["status"] == "fail"
    assert overall == "fail"


def test_positive_cursor_lag_warns_in_grace_then_fails() -> None:
    recent = _live_state(cursor_lag=3, latest_snapshot_id=15)
    recent.cursor_lag_since_s = 950.0
    checks, _ = _checks(recent)
    assert checks["event_bus:cursor_lag"][0]["status"] == "warn"

    old = _live_state(cursor_lag=3, latest_snapshot_id=15)
    old.cursor_lag_since_s = 700.0
    checks, overall = _checks(old)
    assert checks["event_bus:cursor_lag"][0]["status"] == "fail"
    assert overall == "fail"


def test_disconnected_and_missing_dsn_are_explicit_without_secret_leak() -> None:
    disconnected = _live_state(is_connected=False)
    checks, _ = _checks(disconnected, dsn="")

    connection = checks["event_bus:connection_state"][0]
    assert connection["status"] == "warn"
    assert connection["observedValue"] == "not_configured"
    body = json.dumps(checks)
    assert "postgresql://" not in body
    assert "secret" not in body


def test_owner_migration_dsn_does_not_configure_runtime_listener() -> None:
    from polyarb.config import Settings
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = Settings(
        supabase_db_dsn=SecretStr("postgresql://owner:secret@db.example/prod"),
        l2_runtime_db_dsn=SecretStr(""),
    )
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None

    checks, _overall = _build_l2_health_checks(
        store,
        settings,
        ws_consumer=None,
        event_listener=_live_state(),
        now_s=1_000.0,
    )

    connection = checks["event_bus:connection_state"][0]
    assert connection["status"] == "warn"
    assert connection["observedValue"] == "not_configured"
