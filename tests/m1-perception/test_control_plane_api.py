"""Independent HTTP service contract for the transactional control plane."""

from __future__ import annotations

import threading
from time import monotonic
from typing import cast

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


def test_control_plane_status_keeps_business_snapshot_when_capacity_probe_fails() -> None:
    """A secondary capacity probe must not turn operator truth into a 503."""
    from polyarb.control_plane.api import create_control_plane_app

    class CapacityProbeFails(_AvailableControlPlane):
        def database_capacity(self) -> dict[str, object]:
            raise RuntimeError("capacity probe timed out")

    with TestClient(create_control_plane_app(control_plane=CapacityProbeFails())) as client:
        response = client.get("/perception/control-plane")

    assert response.status_code == 200
    assert response.json()["database_capacity"] == {
        "state": "unavailable",
        "reason_code": "database-size-observation-unavailable",
    }


def test_business_overview_route_transports_one_authoritative_snapshot() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    expected = {
        "schema_version": "m1.business-overview.v1",
        "status": "available",
        "observed_at": "2026-08-31T05:00:00+00:00",
        "eligibility": {"state": "paused", "reason_code": "freshness.structure"},
        "structure": {"status": "stale"},
        "quote": {"status": "lagging"},
        "analysis": {"status": "not-published", "reason_code": "not-yet-projected"},
        "opportunities": {"status": "not-published", "reason_code": "quote-lineage-lagging"},
        "blockers": [],
    }

    class BusinessFocusedControlPlane:
        def business_overview(self) -> dict[str, object]:
            return expected

    with TestClient(create_control_plane_app(control_plane=BusinessFocusedControlPlane())) as client:
        response = client.get("/perception/business-overview")

    assert response.status_code == 200
    assert response.json() == expected


def test_business_overview_route_fails_closed_when_authority_fails() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    class FailingBusinessControlPlane:
        def business_overview(self) -> dict[str, object]:
            raise RuntimeError("database read failed")

    with TestClient(create_control_plane_app(control_plane=FailingBusinessControlPlane())) as client:
        response = client.get("/perception/business-overview")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "reason": "business-overview-unavailable",
    }


def test_business_research_page_routes_transport_generation_bound_rows() -> None:
    from polyarb.control_plane.api import create_control_plane_app

    class ResearchFocusedControlPlane:
        def business_structure_page(
            self, *, generation_key: str | None, limit: int, after: str
        ) -> dict[str, object]:
            assert generation_key == "structure:current"
            assert limit == 2
            assert after == "market:001"
            return {"schema_version": "m1.business-research-page.v1", "product": "structure", "status": "available", "items": [{"entity_id": "market:002"}], "limit": 2, "next_after": None}

        def business_quote_page(
            self, *, generation_key: str | None, limit: int, after: str
        ) -> dict[str, object]:
            assert generation_key is None
            assert limit == 1
            assert after == ""
            return {"schema_version": "m1.business-research-page.v1", "product": "quote", "status": "available", "items": [{"market_id": "market:001"}], "limit": 1, "next_after": "market:001"}

    with TestClient(create_control_plane_app(control_plane=ResearchFocusedControlPlane())) as client:
        structure = client.get("/perception/business/structure?generation_key=structure%3Acurrent&limit=2&after=market%3A001")
        quote = client.get("/perception/business/quotes?limit=1")

    assert structure.status_code == 200
    assert structure.json()["items"] == [{"entity_id": "market:002"}]
    assert quote.status_code == 200
    assert quote.json()["next_after"] == "market:001"


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

    pool_calls: list[tuple[str, dict[str, object]]] = []
    bootstrap_calls: list[tuple[str, object]] = []

    class Connection:
        def __init__(self, settings: tuple[str, str, str, list[str]]) -> None:
            self.autocommit = False
            self._settings = settings

        def execute(self, query, params):
            bootstrap_calls.append((query, params))
            return self

        def fetchone(self):
            return self._settings

        def cancel_safe(self, *, timeout):
            bootstrap_calls.append(("cancel", timeout))

        def close(self):
            bootstrap_calls.append(("close", ()))

    class FakePool:
        def __init__(self, dsn: str, **kwargs: object) -> None:
            pool_calls.append((dsn, kwargs))
            self._kwargs = kwargs

        def connection(self) -> Connection:
            connection_kwargs = cast(dict[str, object], self._kwargs["kwargs"])
            health = connection_kwargs["connect_timeout"] == 1
            settings = (
                "pg_catalog,public",
                "1s" if health else "5s",
                "250ms" if health else "1s",
                ["pg_catalog", "public"],
            )
            connection = Connection(settings)
            configure = self._kwargs["configure"]
            assert callable(configure)
            configure(connection)
            return connection

        def close(self) -> None:
            pass

        def get_stats(self) -> dict[str, int]:
            return {}

    monkeypatch.setattr("polyarb.control_plane.db_role_contract.ConnectionPool", FakePool)
    control_plane = api._build_control_plane("postgresql://control-plane")

    assert control_plane._connection_factory() is not None
    assert pool_calls[0][0] == "postgresql://control-plane"
    assert pool_calls[0][1]["kwargs"] == {
        "connect_timeout": 5,
        "options": (
            "-csearch_path=pg_catalog,public -cstatement_timeout=5000ms -clock_timeout=1000ms"
        ),
    }
    assert pool_calls[1][1]["kwargs"] == {
        "connect_timeout": CONTROL_PLANE_HEALTH_DB_POLICY.connect_timeout_seconds,
        "options": (
            "-csearch_path=pg_catalog,public -cstatement_timeout=1000ms -clock_timeout=250ms"
        ),
    }
    assert sum(cast(int, call[1]["max_size"]) for call in pool_calls) == 2
    assert pool_calls[1][1]["max_size"] == 1
    assert bootstrap_calls[-1] == (
        bootstrap_calls[-1][0],
        ("pg_catalog,public", "5000ms", "1000ms"),
    )
    assert "set_config('search_path'" in bootstrap_calls[-1][0]

    bootstrap_calls.clear()
    assert control_plane._readiness_connection_factory() is not None
    assert len(bootstrap_calls) == 1
    assert bootstrap_calls[0][1] == ("pg_catalog,public", "1000ms", "250ms")
    assert CONTROL_PLANE_HEALTH_DB_POLICY.request_timeout_seconds == 3.5
