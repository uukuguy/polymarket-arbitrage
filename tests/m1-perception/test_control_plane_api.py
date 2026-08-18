"""Independent HTTP service contract for the transactional control plane."""

from __future__ import annotations

from starlette.testclient import TestClient


class _AvailableControlPlane:
    def operational_snapshot(self, *, now, sample_limit: int) -> dict[str, object]:
        assert sample_limit in {1, 20}
        return {
            "job_counts": {"runnable": 1},
            "open_incidents": [],
            "runtime_watchdog": {
                "current": None,
                "recent_events": [
                    {
                        "kind": "recovered",
                        "occurred_at": "2026-08-18T15:00:00+00:00",
                        "detail": {"failures": []},
                    }
                ],
            },
        }

    def current_opportunities(self, *, limit: int, after_group_id: str) -> dict[str, object]:
        assert limit == 1
        assert after_group_id == ""
        return {
            "status": "available",
            "current_opportunity_count": 1,
            "items": [{"group_id": "g-1", "gross_edge_bps": 120.0}],
            "limit": 1,
            "next_after_group_id": None,
        }


def test_standalone_control_api_is_readable_without_legacy_daemon_dependencies() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    with TestClient(create_control_plane_app(control_plane=_AvailableControlPlane())) as client:
        health = client.get("/healthz")
        operator = client.get("/perception/control-plane")
        opportunities = client.get("/perception/opportunities?limit=1")

    assert health.status_code == 200
    assert health.json() == {"status": "ok", "control_plane": "available"}
    assert operator.status_code == 200
    assert operator.json() == {
        "status": "available",
        "job_counts": {"runnable": 1},
        "open_incidents": [],
        "runtime_watchdog": {
            "current": None,
            "recent_events": [
                {
                    "kind": "recovered",
                    "occurred_at": "2026-08-18T15:00:00+00:00",
                    "detail": {"failures": []},
                }
            ],
        },
    }
    assert opportunities.status_code == 200
    assert opportunities.json()["items"] == [{"group_id": "g-1", "gross_edge_bps": 120.0}]


def test_standalone_control_api_fails_readiness_when_postgres_is_unavailable() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    with TestClient(create_control_plane_app(control_plane=None)) as client:
        health = client.get("/healthz")
        operator = client.get("/perception/control-plane")
        opportunities = client.get("/perception/opportunities")

    assert health.status_code == 503
    assert health.json() == {"status": "unavailable", "reason": "control-plane-read-unavailable"}
    assert operator.status_code == 503
    assert opportunities.status_code == 503
