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

        async def run(self, spec, _stop_event) -> None:
            specs.append(spec)

    monkeypatch.setattr(main, "ProducerSupervisor", Supervisor)
    settings = SimpleNamespace(
        opportunity_producer_supervisor_enabled=True,
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
