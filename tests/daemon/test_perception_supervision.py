from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from polyarb.daemon import main


@pytest.mark.asyncio
async def test_only_reconciliation_receives_early_stall_detection(
    monkeypatch,
    tmp_path,
) -> None:
    specs = []

    class Supervisor:
        def __init__(self, **_kwargs) -> None:
            pass

        async def run(self, spec, stop_event) -> None:
            specs.append(spec)
            if len(specs) == 3:
                stop_event.set()

    monkeypatch.setattr(main, "ProducerSupervisor", Supervisor)
    settings = SimpleNamespace(
        opportunity_producer_supervisor_enabled=True,
        neg_risk_quote_supervisor_enabled=False,
        neg_risk_quote_worker_enabled=False,
        neg_risk_quote_supervisor_timeout_s=210,
        opportunity_first_watcher_enabled=True,
        opportunity_discovery_enabled=True,
        opportunity_reconciliation_enabled=True,
        producer_stall_detection_s=30,
        producer_stall_timeout_s=180,
        producer_terminate_grace_s=5,
        producer_max_restarts=3,
        producer_backoff_initial_s=1,
        producer_backoff_max_s=30,
    )

    tasks = main._start_supervised_producers(
        settings,
        SimpleNamespace(db_path=tmp_path / "state.db"),
        asyncio.Event(),
    )
    await asyncio.gather(*tasks)

    by_component = {spec.component: spec for spec in specs}
    assert by_component["candidate"].stall_detection_s is None
    assert by_component["discovery"].stall_detection_s is None
    assert by_component["reconciliation"].stall_detection_s == 30


@pytest.mark.asyncio
async def test_quote_supervisor_reserves_time_for_certification_after_child_collection(
    monkeypatch,
    tmp_path,
) -> None:
    """The outer watchdog must not kill a completed Quote child mid-certification."""
    specs = []

    class Supervisor:
        def __init__(self, **_kwargs) -> None:
            pass

        async def run(self, spec, stop_event) -> None:
            specs.append(spec)
            stop_event.set()

    monkeypatch.setattr(main, "ProducerSupervisor", Supervisor)
    settings = SimpleNamespace(
        opportunity_producer_supervisor_enabled=False,
        neg_risk_quote_supervisor_enabled=True,
        neg_risk_quote_worker_enabled=True,
        neg_risk_quote_interval_s=60,
        neg_risk_quote_supervisor_timeout_s=210,
        opportunity_first_watcher_enabled=False,
        opportunity_discovery_enabled=False,
        opportunity_reconciliation_enabled=False,
        producer_stall_detection_s=30,
        producer_stall_timeout_s=180,
        producer_terminate_grace_s=5,
        producer_max_restarts=3,
        producer_backoff_initial_s=1,
        producer_backoff_max_s=30,
    )

    tasks = main._start_supervised_producers(
        settings,
        SimpleNamespace(db_path=tmp_path / "state.db"),
        asyncio.Event(),
    )
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

    assert len(specs) == 1
    assert specs[0].component == "quote"
    assert specs[0].timeout_s == 210
