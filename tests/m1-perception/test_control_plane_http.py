"""HTTP contract for the durable M1 operator control plane."""

from __future__ import annotations

from datetime import UTC, datetime


class _AvailableControlPlane:
    def operational_snapshot(self, *, now: datetime, sample_limit: int) -> dict[str, object]:
        assert now.tzinfo is UTC
        assert sample_limit == 20
        return {
            "job_counts": {"retryable": 1},
            "oldest_runnable_age_seconds": 12.5,
            "expired_leases": 0,
            "recent_attempts": [],
            "open_incidents": [],
            "pending_alert_outbox": [],
        }


def test_control_plane_route_returns_durable_operator_snapshot(http_test_client) -> None:
    http_test_client.app.state.control_plane = _AvailableControlPlane()

    response = http_test_client.get("/perception/control-plane")

    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
        "job_counts": {"retryable": 1},
        "oldest_runnable_age_seconds": 12.5,
        "expired_leases": 0,
        "recent_attempts": [],
        "open_incidents": [],
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
