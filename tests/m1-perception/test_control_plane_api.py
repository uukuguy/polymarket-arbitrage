"""Independent HTTP service contract for the transactional control plane."""

from __future__ import annotations

import threading
from time import monotonic

import pytest
from starlette.testclient import TestClient


class _AvailableControlPlane:
    def readiness(self) -> bool:
        return True

    def operational_snapshot(self, *, sample_limit: int) -> dict[str, object]:
        assert sample_limit in {1, 20}
        return {
            "job_counts": {"runnable": 1},
            "open_incidents": [],
            "runtime_watchdog": {
                "current": {
                    "incident_key": "runtime-watchdog-a",
                    "severity": "critical",
                    "summary": "runtime target unavailable",
                    "opened_at": "2026-08-18T15:00:00+00:00",
                    "source": "independent-runtime-watchdog",
                    "failures": ["machine:worker-a:stopped"],
                },
                "recent_events": [
                    {
                        "incident_key": "runtime-watchdog-a",
                        "severity": "critical",
                        "summary": "runtime target unavailable",
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


def test_standalone_control_api_health_uses_only_the_minimal_readiness_probe() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    calls: list[str] = []

    class HealthFocusedControlPlane:
        def readiness(self) -> bool:
            calls.append("readiness")
            return True

        def operational_snapshot(self, *, sample_limit: int) -> dict[str, object]:
            raise AssertionError(f"health must not build the operator snapshot: {sample_limit}")

    with TestClient(create_control_plane_app(control_plane=HealthFocusedControlPlane())) as client:
        platform = client.get("/healthz")
        health = client.get("/health")

    assert platform.status_code == 200
    assert platform.json() == {"status": "ok", "service": "control-plane-api"}
    assert health.status_code == 200
    assert health.json() == {"status": "ok", "control_plane": "available"}
    assert calls == ["readiness"]


def test_standalone_control_api_is_readable_without_legacy_daemon_dependencies() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    with TestClient(create_control_plane_app(control_plane=_AvailableControlPlane())) as client:
        platform = client.get("/healthz")
        health = client.get("/health")
        operator = client.get("/perception/control-plane")
        opportunities = client.get("/perception/opportunities?limit=1")

    assert platform.status_code == 200
    assert platform.json() == {"status": "ok", "service": "control-plane-api"}
    assert health.status_code == 200
    assert health.json() == {"status": "ok", "control_plane": "available"}
    assert operator.status_code == 200
    assert operator.json() == {
        "status": "available",
        "job_counts": {"runnable": 1},
        "open_incidents": [],
        "runtime_watchdog": {
            "current": {
                "incident_key": "runtime-watchdog-a",
                "severity": "critical",
                "summary": "runtime target unavailable",
                "opened_at": "2026-08-18T15:00:00+00:00",
                "source": "independent-runtime-watchdog",
                "failures": ["machine:worker-a:stopped"],
            },
            "recent_events": [
                {
                    "incident_key": "runtime-watchdog-a",
                    "severity": "critical",
                    "summary": "runtime target unavailable",
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
        platform = client.get("/healthz")
        health = client.get("/health")
        operator = client.get("/perception/control-plane")
        opportunities = client.get("/perception/opportunities")

    assert platform.status_code == 200
    assert platform.json() == {"status": "ok", "service": "control-plane-api"}
    assert health.status_code == 503
    assert health.json() == {"status": "unavailable", "reason": "control-plane-read-unavailable"}
    assert operator.status_code == 503
    assert opportunities.status_code == 503


def test_standalone_control_api_health_detaches_a_stalled_readiness_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane import api

    started = threading.Event()
    release = threading.Event()

    class Policy:
        request_timeout_seconds = 0.05

    class StalledControlPlane:
        def readiness(self) -> bool:
            started.set()
            release.wait()
            return True

    monkeypatch.setattr(api, "CONTROL_PLANE_HEALTH_DB_POLICY", Policy())
    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        app = api.create_control_plane_app(control_plane=StalledControlPlane())
        with TestClient(app) as client:
            health = client.get("/health")
            platform = client.get("/healthz")
        assert health.status_code == 503
        assert health.json() == {
            "status": "unavailable",
            "reason": "control-plane-read-unavailable",
        }
        assert platform.status_code == 200
        assert platform.json() == {"status": "ok", "service": "control-plane-api"}
        assert started.is_set()
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()


def test_control_api_connection_factory_bounds_postgres_connect_time(monkeypatch) -> None:
    from polyarb.control_plane import api
    from polyarb.control_plane.db_deadlines import CONTROL_PLANE_HEALTH_DB_POLICY

    calls: list[tuple[str, dict[str, object]]] = []

    class Connection:
        autocommit = False

        def execute(self, query, params):
            calls.append((query, {"params": params}))
            return self

        def fetchone(self):
            if calls[0][1]["connect_timeout"] == 1:
                return ("pg_catalog,public", "1s", "250ms", ["pg_catalog", "public"])
            return ("pg_catalog,public", "5s", "1s", ["pg_catalog", "public"])

        def cancel_safe(self, *, timeout):
            calls.append(("cancel", {"timeout": timeout}))

        def close(self):
            calls.append(("close", {}))

    sentinel = Connection()

    def connect(dsn: str, **kwargs: object) -> object:
        calls.append((dsn, kwargs))
        return sentinel

    monkeypatch.setattr("polyarb.control_plane.db_role_contract.psycopg.connect", connect)
    control_plane = api._build_control_plane("postgresql://control-plane")

    assert control_plane._connection_factory() is sentinel
    assert calls[0] == (
        "postgresql://control-plane",
        {
            "connect_timeout": 5,
            "options": (
                "-csearch_path=pg_catalog,public -cstatement_timeout=5000ms -clock_timeout=1000ms"
            ),
        },
    )
    assert len(calls) == 2
    assert "set_config('search_path'" in calls[1][0]
    assert calls[1][1] == {"params": ("pg_catalog,public", "5000ms", "1000ms")}

    calls.clear()
    assert control_plane._readiness_connection_factory() is sentinel
    assert calls[0] == (
        "postgresql://control-plane",
        {
            "connect_timeout": 1,
            "options": (
                "-csearch_path=pg_catalog,public "
                "-cstatement_timeout=1000ms -clock_timeout=250ms"
            ),
        },
    )
    assert len(calls) == 2
    assert calls[1][1] == {"params": ("pg_catalog,public", "1000ms", "250ms")}
    assert CONTROL_PLANE_HEALTH_DB_POLICY.request_timeout_seconds == 3.5
