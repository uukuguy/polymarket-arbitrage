"""HTTP contract for the durable M1 operator control plane."""

from __future__ import annotations

import threading
from time import monotonic

import pytest


class _AvailableControlPlane:
    def operational_snapshot(self, *, sample_limit: int) -> dict[str, object]:
        assert sample_limit == 20
        return {
            "job_counts": {"retryable": 1},
            "oldest_runnable_age_seconds": 12.5,
            "expired_leases": 0,
            "quote": {
                "admission_job_states": {"runnable": 1},
                "oldest_retryable_admission_age_seconds": None,
                "batch_job_states": {"retryable": 1},
                "certifier_job_states": {},
                "oldest_retryable_batch_age_seconds": 12.5,
                "current_pointer": None,
            },
            "structure": {
                "source_fetch_job_states": {"leased": 1},
                "oldest_retryable_source_age_seconds": None,
                "source_materializer_job_states": {},
                "range_job_states": {},
                "certifier_job_states": {},
                "oldest_retryable_range_age_seconds": None,
                "latest_manifest": None,
                "shadow_pointer": None,
            },
            "recent_attempts": [],
            "open_incidents": [],
            "runtime_controller": {
                "status": "healthy",
                "controller_id": "m1-runtime-reconciler",
                "owner_id": "runtime-controller",
                "epoch": 4,
                "claimed_at": "2026-08-25T11:59:00+00:00",
                "last_tick_at": "2026-08-25T11:59:30+00:00",
                "lease_expires_at": "2026-08-25T12:00:30+00:00",
                "lease_active": True,
                "lease_age_seconds": 30.0,
                "lease_overdue_seconds": 0.0,
            },
            "active_tasks": {"items": [], "total": 0},
            "runtime_incidents": {"items": [], "total": 0},
            "recovery_actions": {"items": [], "total": 0},
            "qualification": {
                "state": "accumulating",
                "epoch_id": "qualification-api",
                "started_at": "2026-08-25T00:00:00+00:00",
                "eligible_seconds": 3600,
                "required_seconds": 86400,
                "max_gap_seconds": 900,
                "last_fact_at": "2026-08-25T11:59:00+00:00",
                "last_fact_age_seconds": 60.0,
                "last_breaker": None,
                "policy_version": "m1-rolling-qualification-v1",
                "release_id": "release-a",
                "config_id": "config-a",
                "role_identity": ["m1", "structure"],
                "certificate": None,
            },
            "pending_alert_outbox": [],
        }


class _MalformedControlPlane:
    def operational_snapshot(self, *, sample_limit: int) -> dict[str, object]:
        raise ValueError("secret DSN postgres://user:pass@example/control-plane")


def test_control_plane_route_returns_durable_operator_snapshot(http_test_client) -> None:
    http_test_client.app.state.control_plane = _AvailableControlPlane()

    response = http_test_client.get("/perception/control-plane")

    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
        "job_counts": {"retryable": 1},
        "oldest_runnable_age_seconds": 12.5,
        "expired_leases": 0,
        "quote": {
            "admission_job_states": {"runnable": 1},
            "oldest_retryable_admission_age_seconds": None,
            "batch_job_states": {"retryable": 1},
            "certifier_job_states": {},
            "oldest_retryable_batch_age_seconds": 12.5,
            "current_pointer": None,
        },
        "structure": {
            "source_fetch_job_states": {"leased": 1},
            "oldest_retryable_source_age_seconds": None,
            "source_materializer_job_states": {},
            "range_job_states": {},
            "certifier_job_states": {},
            "oldest_retryable_range_age_seconds": None,
            "latest_manifest": None,
            "shadow_pointer": None,
        },
        "recent_attempts": [],
        "open_incidents": [],
        "runtime_controller": {
            "status": "healthy",
            "controller_id": "m1-runtime-reconciler",
            "owner_id": "runtime-controller",
            "epoch": 4,
            "claimed_at": "2026-08-25T11:59:00+00:00",
            "last_tick_at": "2026-08-25T11:59:30+00:00",
            "lease_expires_at": "2026-08-25T12:00:30+00:00",
            "lease_active": True,
            "lease_age_seconds": 30.0,
            "lease_overdue_seconds": 0.0,
        },
        "active_tasks": {"items": [], "total": 0},
        "runtime_incidents": {"items": [], "total": 0},
        "recovery_actions": {"items": [], "total": 0},
        "qualification": {
            "state": "accumulating",
            "epoch_id": "qualification-api",
            "started_at": "2026-08-25T00:00:00+00:00",
            "eligible_seconds": 3600,
            "required_seconds": 86400,
            "max_gap_seconds": 900,
            "last_fact_at": "2026-08-25T11:59:00+00:00",
            "last_fact_age_seconds": 60.0,
            "last_breaker": None,
            "policy_version": "m1-rolling-qualification-v1",
            "release_id": "release-a",
            "config_id": "config-a",
            "role_identity": ["m1", "structure"],
            "certificate": None,
        },
        "pending_alert_outbox": [],
    }


def test_control_plane_route_never_reports_missing_dependency_as_empty(http_test_client) -> None:
    http_test_client.app.state.control_plane = None

    response = http_test_client.get("/perception/control-plane")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "reason": "control-plane-read-unavailable",
    }


def test_control_plane_route_redacts_malformed_read_model_failures(http_test_client) -> None:
    http_test_client.app.state.control_plane = _MalformedControlPlane()

    response = http_test_client.get("/perception/control-plane")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "reason": "control-plane-read-unavailable",
    }


def test_control_plane_route_timeout_does_not_join_a_stalled_database_thread(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.http import control_plane as control_plane_http

    started = threading.Event()
    release = threading.Event()

    class Policy:
        request_timeout_seconds = 0.05

    class StalledControlPlane:
        def operational_snapshot(self, **_kwargs: object) -> dict[str, object]:
            started.set()
            release.wait()
            return {}

    monkeypatch.setattr(control_plane_http, "CONTROL_PLANE_DB_POLICY", Policy())
    http_test_client.app.state.control_plane = StalledControlPlane()
    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        response = http_test_client.get("/perception/control-plane")
        assert response.status_code == 503
        assert started.is_set()
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()
