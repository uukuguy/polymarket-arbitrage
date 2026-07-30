from __future__ import annotations

from types import SimpleNamespace

from polyarb.perception.fault_runtime import FaultRuntime
from polyarb.perception.store import OpportunityPerceptionStore


def test_nonisolated_daemon_builders_receive_distinct_exact_fault_runtimes(
    tmp_path,
    monkeypatch,
) -> None:
    import polyarb.daemon.main as daemon_main

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

    def focused_builder(_settings, *, fault_runtime):
        captured["notification"] = fault_runtime
        return SimpleNamespace(candidate_group_ids=lambda: ())

    def candidate_builder(_settings, *, candidate_group_ids, fault_runtime):
        captured["candidate"] = fault_runtime
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
