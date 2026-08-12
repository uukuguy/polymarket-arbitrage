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
