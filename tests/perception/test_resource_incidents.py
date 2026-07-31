from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from polyarb.daemon.main import _run_resource_controller
from polyarb.perception import resource_controller as resource_module
from polyarb.perception.incidents import (
    IncidentManager,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.resource_controller import ResourceController, ResourceSample
from polyarb.perception.resource_incidents import ResourcePressureIncidents
from polyarb.perception.store import OpportunityPerceptionStore


def sample(*, observed_at_ms: int, disk: int, load: float) -> ResourceSample:
    return ResourceSample(
        candidate_count=2,
        candidate_quote_p95_ms=5_000,
        candidate_missing_quote_count=0,
        candidate_worker_ok=True,
        discovery_worker_ok=True,
        reconciliation_running=True,
        previous_discovery_batch_limit=50,
        observed_at_ms=observed_at_ms,
        disk_free_bytes=disk,
        load_per_cpu=load,
    )


def test_resource_pressure_incident_requires_new_healthy_decision(tmp_path) -> None:
    now = [2_000]
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    controller = ResourceController(
        store,
        clock_ms=lambda: now[0],
        cooldown_ms=0,
        min_disk_free_bytes=1_000,
        max_load_per_cpu=2.0,
        _verify_store_authority=False,
    )
    tracker = ResourcePressureIncidents(store, clock_ms=lambda: now[0])
    pressured = controller.decide(
        sample(observed_at_ms=now[0], disk=999, load=0.5)
    )
    tracker.observe(pressured, decision_id=store.latest_resource_decision_id())

    incident = store.open_incidents()[0]
    assert incident.scope == "resource"
    assert incident.kind == "resource-disk-pressure"
    assert incident.state == "recovering"

    now[0] = 2_500
    still_pressured = controller.decide(
        sample(observed_at_ms=now[0], disk=998, load=0.5)
    )
    pressured_id = store.latest_resource_decision_id()
    with pytest.raises(RecoveryEvidenceRequiredError):
        IncidentManager(store, clock_ms=lambda: now[0]).transition(
            incident.id,
            "verified",
            {"decision_id": pressured_id},
        )
    assert still_pressured.mode == "protect-hot-path"

    now[0] = 3_000
    healthy = controller.decide(
        sample(observed_at_ms=now[0], disk=2_000, load=0.5)
    )
    tracker.observe(healthy, decision_id=store.latest_resource_decision_id())

    assert healthy.mode == "normal"
    assert store.open_incidents() == ()


@pytest.mark.asyncio
async def test_resource_runner_persists_disk_pressure_from_host_sensor(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    monkeypatch.setattr(
        resource_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=1),
    )
    monkeypatch.setattr(
        resource_module.os,
        "getloadavg",
        lambda: (0.0, 0.0, 0.0),
    )
    settings = SimpleNamespace(
        resource_hot_quote_age_s=20,
        resource_cooldown_s=0,
        resource_decision_ttl_s=15,
        resource_min_disk_free_mb=128,
        resource_max_load_per_cpu=2.0,
        discovery_page_limit=50,
        opportunity_reconciliation_enabled=False,
        resource_sample_interval_s=0.01,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(
        _run_resource_controller(settings, store, stop)
    )
    for _ in range(100):
        if store.open_incidents():
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    incident = store.open_incidents()[0]
    assert incident.kind == "resource-disk-pressure"
    assert incident.state == "recovering"
