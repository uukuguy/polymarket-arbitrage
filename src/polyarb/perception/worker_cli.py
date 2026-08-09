"""Single-component entry point used only by the producer supervisor."""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from uuid import UUID

from polyarb.config import load_settings
from polyarb.daemon.opportunity_watcher import build_focused_opportunity_watcher
from polyarb.daemon.quote_worker import build_production_quote_worker
from polyarb.perception.candidate_watcher import build_production_candidate_watcher
from polyarb.perception.discovery import (
    CandidateFreshness,
    build_production_discovery,
    compose_candidate_group_ids,
)
from polyarb.perception.fault_control import FaultRuntimeIdentity
from polyarb.perception.fault_runtime import (
    PassThroughFaultRuntime,
    build_fault_runtime,
)
from polyarb.perception.reconciliation import build_production_reconciliation
from polyarb.perception.store import OpportunityPerceptionStore

_FLAG_BY_COMPONENT = {
    "candidate": "opportunity_first_watcher_enabled",
    "discovery": "opportunity_discovery_enabled",
    "reconciliation": "opportunity_reconciliation_enabled",
    "quote": "neg_risk_quote_worker_enabled",
}
_QUOTE_SUPERVISED_TIMEOUT_LIMIT = 1


def _build_child_fault_runtime(component: str, settings):
    enabled = bool(getattr(settings, "upstream_fault_control_enabled", False))
    try:
        identity = FaultRuntimeIdentity(
            component=component,
            release_id=settings.release_id,
            machine_id=os.environ.get("FLY_MACHINE_ID", "local"),
            boot_id=UUID(os.environ["POLYARB_PRODUCER_BOOT_ID"]),
        )
        supervisor_run_id = os.environ["POLYARB_PRODUCER_SUPERVISOR_RUN_ID"]
        attempt = int(os.environ["POLYARB_PRODUCER_ATTEMPT"])
    except (KeyError, TypeError, ValueError, AttributeError):
        return PassThroughFaultRuntime(degraded=enabled)
    return build_fault_runtime(
        enabled=enabled,
        db_path=settings.db_path,
        identity=identity,
        supervisor_run_id=supervisor_run_id,
        attempt=attempt,
        started_at_ms=int(time.time() * 1_000),
    )


async def run_component(component: str, settings) -> int:
    if component not in _FLAG_BY_COMPONENT:
        raise ValueError("invalid-producer-component")
    supervisor_enabled = (
        settings.neg_risk_quote_supervisor_enabled
        if component == "quote"
        else settings.opportunity_producer_supervisor_enabled
    )
    if not supervisor_enabled:
        raise RuntimeError("producer-supervisor-disabled")
    if not getattr(settings, _FLAG_BY_COMPONENT[component]):
        raise RuntimeError(f"{component}-producer-disabled")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    store = OpportunityPerceptionStore(settings.db_path)
    store.init_schema()
    store.claim_producer_heartbeat_authority(component)
    if component == "quote":

        async def publish_progress() -> None:
            await asyncio.to_thread(
                store.record_producer_heartbeat,
                "quote",
                observed_at_ms=int(time.time() * 1_000),
            )

        worker = build_production_quote_worker(
            settings,
            perception_store=store,
            stop_after_consecutive_timeouts=_QUOTE_SUPERVISED_TIMEOUT_LIMIT,
            on_cycle_started=publish_progress,
        )
        if worker is None:
            raise RuntimeError("quote-producer-disabled")
        await worker.run(stop_event)
        return 75 if worker.supervisor_recovery_requested else 0

    fault_runtime = _build_child_fault_runtime(component, settings)
    if component == "candidate":
        focused = build_focused_opportunity_watcher(settings)
        source = compose_candidate_group_ids(focused.candidate_group_ids, store)
        worker = build_production_candidate_watcher(
            settings,
            candidate_group_ids=source,
            fault_runtime=fault_runtime,
        )
    elif component == "discovery":

        def freshness() -> CandidateFreshness:
            fact = store.candidate_freshness_snapshot(now_ms=int(time.time() * 1_000))
            return CandidateFreshness(
                candidate_count=fact.candidate_count,
                quote_p95_age_ms=fact.quote_p95_age_ms,
                missing_quote_count=fact.missing_quote_count,
            )

        worker = build_production_discovery(
            settings,
            candidate_freshness=freshness,
            fault_runtime=fault_runtime,
        )
    else:
        worker = build_production_reconciliation(
            settings,
            fault_runtime=fault_runtime,
        )
    await worker.run(stop_event)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one isolated M1 producer")
    parser.add_argument(
        "component",
        choices=tuple(_FLAG_BY_COMPONENT),
    )
    args = parser.parse_args(argv)
    return asyncio.run(run_component(args.component, load_settings()))


if __name__ == "__main__":
    sys.exit(main())
