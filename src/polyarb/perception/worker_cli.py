"""Single-component entry point used only by the producer supervisor."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time

from polyarb.config import load_settings
from polyarb.daemon.opportunity_watcher import build_focused_opportunity_watcher
from polyarb.perception.candidate_watcher import build_production_candidate_watcher
from polyarb.perception.discovery import (
    CandidateFreshness,
    build_production_discovery,
    compose_candidate_group_ids,
)
from polyarb.perception.reconciliation import build_production_reconciliation
from polyarb.perception.store import OpportunityPerceptionStore

_FLAG_BY_COMPONENT = {
    "candidate": "opportunity_first_watcher_enabled",
    "discovery": "opportunity_discovery_enabled",
    "reconciliation": "opportunity_reconciliation_enabled",
}


async def run_component(component: str, settings) -> None:
    if component not in _FLAG_BY_COMPONENT:
        raise ValueError("invalid-producer-component")
    if not settings.opportunity_producer_supervisor_enabled:
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
    if component == "candidate":
        focused = build_focused_opportunity_watcher(settings)
        source = compose_candidate_group_ids(focused.candidate_group_ids, store)
        worker = build_production_candidate_watcher(settings, candidate_group_ids=source)
    elif component == "discovery":

        def freshness() -> CandidateFreshness:
            fact = store.candidate_freshness_snapshot(now_ms=int(time.time() * 1_000))
            return CandidateFreshness(
                candidate_count=fact.candidate_count,
                quote_p95_age_ms=fact.quote_p95_age_ms,
                missing_quote_count=fact.missing_quote_count,
            )

        worker = build_production_discovery(settings, candidate_freshness=freshness)
    else:
        worker = build_production_reconciliation(settings)
    await worker.run(stop_event)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one isolated M1 producer")
    parser.add_argument(
        "component",
        choices=tuple(_FLAG_BY_COMPONENT),
    )
    args = parser.parse_args(argv)
    asyncio.run(run_component(args.component, load_settings()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
