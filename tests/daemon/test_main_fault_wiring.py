from __future__ import annotations

import asyncio
from types import SimpleNamespace

from polyarb.perception.fault_runtime import FaultRuntime
from polyarb.perception.store import OpportunityPerceptionStore


def test_nonisolated_daemon_builders_receive_distinct_exact_fault_runtimes(
    tmp_path,
    monkeypatch,
) -> None:
    import polyarb.daemon.main as daemon_main
    from polyarb.perception.store import OpportunityPerceptionStore

    assert hasattr(daemon_main, "_build_daemon_perception_workers")
    path = tmp_path / "daemon.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    settings = SimpleNamespace(
        db_path=path,
        release_id="a" * 40,
        upstream_fault_control_enabled=True,
        opportunity_producer_supervisor_enabled=False,
        opportunity_first_watcher_enabled=True,
        opportunity_discovery_enabled=True,
        opportunity_reconciliation_enabled=True,
    )
    monkeypatch.setenv("FLY_MACHINE_ID", "daemon-machine")
    captured: dict[str, object] = {}
    candidate_calls: list[tuple[object, object]] = []

    def focused_builder(_settings, *, fault_runtime):
        captured["notification"] = fault_runtime
        return SimpleNamespace(candidate_group_ids=lambda: ())

    def candidate_builder(_settings, *, candidate_group_ids, fault_runtime):
        captured["candidate"] = fault_runtime
        candidate_calls.append((candidate_group_ids, fault_runtime))
        return "candidate-worker"

    def discovery_builder(_settings, *, candidate_freshness, fault_runtime):
        captured["discovery"] = fault_runtime
        return "discovery-worker"

    def reconciliation_builder(_settings, *, fault_runtime):
        captured["reconciliation"] = fault_runtime
        return "reconciliation-worker"

    monkeypatch.setattr(daemon_main, "build_focused_opportunity_watcher", focused_builder)
    monkeypatch.setattr(daemon_main, "build_production_candidate_watcher", candidate_builder)
    monkeypatch.setattr(daemon_main, "build_production_discovery", discovery_builder)
    monkeypatch.setattr(
        daemon_main,
        "build_production_reconciliation",
        reconciliation_builder,
    )
    monkeypatch.setattr(
        daemon_main,
        "compose_candidate_group_ids",
        lambda legacy, _store: legacy,
    )

    workers = daemon_main._build_daemon_perception_workers(settings, store)

    assert workers[1:] == (
        "candidate-worker",
        "discovery-worker",
        "reconciliation-worker",
    )
    assert set(captured) == {
        "candidate",
        "discovery",
        "reconciliation",
        "notification",
    }
    runtimes = tuple(captured.values())
    assert all(isinstance(runtime, FaultRuntime) for runtime in runtimes)
    identities = [runtime.identity for runtime in runtimes]
    assert {identity.component for identity in identities} == set(captured)
    assert {identity.release_id for identity in identities} == {"a" * 40}
    assert {identity.machine_id for identity in identities} == {"daemon-machine"}
    assert len({identity.boot_id for identity in identities}) == 4
    assert all(identity.boot_id.version == 4 for identity in identities)
    assert len(candidate_calls) == 1
    assert callable(candidate_calls[0][0])
    assert candidate_calls[0][1] is captured["candidate"]

    settings.opportunity_first_watcher_enabled = False
    disabled_workers = daemon_main._build_daemon_perception_workers(settings, store)

    assert disabled_workers[1] is None
    assert len(candidate_calls) == 1


def test_capacity_worker_builds_without_opportunity_supervisor(tmp_path) -> None:
    import polyarb.daemon.main as daemon_main
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "daemon.db")
    store.init_schema()
    perception_store = OpportunityPerceptionStore(tmp_path / "daemon.db")
    perception_store.init_schema()
    settings = SimpleNamespace(
        capacity_controller_enabled=True,
        capacity_pressure_free_percent=20.0,
        capacity_critical_free_percent=12.0,
        capacity_exhaustion_free_percent=6.0,
        capacity_recovery_hold_s=30.0,
        capacity_interval_s=5.0,
        capacity_retry_delay_s=5.0,
        opportunity_producer_supervisor_enabled=False,
        neg_risk_quote_interval_s=120.0,
    )

    worker = daemon_main._build_capacity_worker(
        settings,
        store,
        perception_store,
        asyncio.Lock(),
        quote_worker_runtime=None,
    )

    assert worker is not None


def test_daemon_control_plane_connection_has_bounded_connect_timeout(monkeypatch) -> None:
    import polyarb.daemon.main as daemon_main

    calls: list[tuple[str, dict[str, object]]] = []

    class Connection:
        def execute(self, query, params):
            calls.append((query, {"params": params}))

        def commit(self):
            calls.append(("commit", {}))

        def close(self):
            calls.append(("close", {}))

    sentinel = Connection()

    def connect(dsn: str, **kwargs: object) -> object:
        calls.append((dsn, kwargs))
        return sentinel

    monkeypatch.setattr("polyarb.control_plane.db_role_contract.psycopg.connect", connect)
    control_plane = daemon_main._build_daemon_control_plane("postgresql://control-plane")

    assert control_plane is not None
    assert control_plane._connection_factory() is sentinel
    assert calls == [
        (
            "postgresql://control-plane",
            {"connect_timeout": 5, "options": "-csearch_path=pg_catalog,public"},
        ),
        (
            "SELECT pg_catalog.set_config('search_path', %s, false), "
            "pg_catalog.set_config('statement_timeout', %s, false), "
            "pg_catalog.set_config('lock_timeout', %s, false)",
            {"params": ("pg_catalog,public", "5000ms", "1000ms")},
        ),
        ("commit", {}),
    ]
    assert daemon_main._build_daemon_control_plane("   ") is None
