"""Daemon entry-point: HTTP server + snapshot scheduler + observer workers.

Phase 02 Plan 02 — asyncio SIGINT/SIGTERM graceful shutdown.

Run locally:
    POLYARB_ALLOW_EMPTY_SECRET=1 uv run python -m polyarb.daemon.main
    curl http://127.0.0.1:19080/health   # default; override via POLYARB_HTTP_PORT

Architecture (Plan 02):
    1. init_logging() — loguru JSON to stdout (must be FIRST before any logger calls)
    2. Build Settings + SQLiteStore + SnapshotScheduler + optional QuoteWorker
    3. create_app(...) → Starlette app with worker runtime health state
    4. Start uvicorn, scheduler, and quote worker as sibling tasks
    5. SIGINT/SIGTERM → stop_event → cancel producers and stop cleanly

Plan 04 will add Dockerfile + fly.toml [processes] group.
Plan 05 will add init_sentry() before init_logging().

Source: RESEARCH.md §Architecture Patterns Pattern 1 (lines 295-349, verbatim)
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time
import urllib.request

import uvicorn
from loguru import logger

from polyarb.config import load_settings
from polyarb.daemon.opportunity_watcher import (
    OpportunityWatcher,
    build_focused_opportunity_watcher,
)
from polyarb.daemon.quote_worker import (
    QuoteWorker,
    build_production_quote_worker,
)
from polyarb.daemon.scheduler import SnapshotScheduler
from polyarb.http.app import create_app
from polyarb.observability.logging import init_logging
from polyarb.observability.sentry import init_sentry
from polyarb.perception.candidate_watcher import (
    CandidateWatcherScheduler,
    build_production_candidate_watcher,
)
from polyarb.perception.discovery import (
    CandidateFreshness,
    DiscoveryRunner,
    build_production_discovery,
    compose_candidate_group_ids,
)
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.reconciliation import (
    ReconciliationRunner,
    build_production_reconciliation,
)
from polyarb.perception.resource_controller import (
    ResourceController,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.perception.supervisor import ProducerSpec, ProducerSupervisor
from polyarb.storage.sqlite_store import SQLiteStore


def _start_quote_worker(
    quote_worker: QuoteWorker | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if quote_worker is None:
        return None
    return asyncio.create_task(quote_worker.run(stop_event))


def _start_opportunity_watcher(
    watcher: OpportunityWatcher | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if watcher is None:
        return None
    return asyncio.create_task(watcher.run(stop_event))


def _start_candidate_watcher(
    watcher: CandidateWatcherScheduler | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if watcher is None:
        return None
    return asyncio.create_task(watcher.run(stop_event))


def _start_discovery(
    discovery: DiscoveryRunner | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if discovery is None:
        return None
    return asyncio.create_task(discovery.run(stop_event))


def _start_reconciliation(
    reconciliation: ReconciliationRunner | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if reconciliation is None:
        return None
    return asyncio.create_task(reconciliation.run(stop_event))


def _start_supervised_producers(
    settings,
    store: OpportunityPerceptionStore,
    stop_event: asyncio.Event,
) -> list[asyncio.Task[None]]:
    if not settings.opportunity_producer_supervisor_enabled:
        return []
    flags = {
        "candidate": settings.opportunity_first_watcher_enabled,
        "discovery": settings.opportunity_discovery_enabled,
        "reconciliation": settings.opportunity_reconciliation_enabled,
    }
    supervisor = ProducerSupervisor(
        store=store,
        incidents=IncidentManager(store),
    )
    return [
        asyncio.create_task(
            supervisor.run(
                ProducerSpec(
                    component=component,
                    timeout_s=settings.producer_stall_timeout_s,
                    terminate_grace_s=settings.producer_terminate_grace_s,
                    max_restarts=settings.producer_max_restarts,
                    backoff_initial_s=settings.producer_backoff_initial_s,
                    backoff_max_s=settings.producer_backoff_max_s,
                ),
                stop_event,
            )
        )
        for component, enabled in flags.items()
        if enabled
    ]


async def _run_resource_controller(
    settings,
    store: OpportunityPerceptionStore,
    stop_event: asyncio.Event,
) -> None:
    controller = ResourceController(
        store,
        hot_quote_age_ms=int(settings.resource_hot_quote_age_s * 1_000),
        cooldown_ms=int(settings.resource_cooldown_s * 1_000),
    )
    previous_limit = settings.discovery_page_limit
    incident_manager = IncidentManager(store)
    active_incident_id: str | None = None
    while not stop_event.is_set():
        try:
            sample = await asyncio.to_thread(
                controller.capture_sample,
                reconciliation_running=settings.opportunity_reconciliation_enabled,
                previous_discovery_batch_limit=previous_limit,
            )
            decision = await asyncio.to_thread(controller.decide, sample)
            previous_limit = decision.discovery_batch_limit
            if active_incident_id is not None:
                decision_id = await asyncio.to_thread(store.latest_resource_decision_id)
                await asyncio.to_thread(
                    incident_manager.transition,
                    active_incident_id,
                    "verified",
                    {"decision_id": decision_id},
                )
                active_incident_id = None
        except asyncio.CancelledError:
            raise
        except Exception as error:
            incident = await asyncio.to_thread(
                incident_manager.detect,
                "resource",
                "controller-failure",
                {"error_kind": type(error).__name__},
            )
            if incident.state == "detected":
                incident = await asyncio.to_thread(
                    incident_manager.transition,
                    incident.id,
                    "classified",
                    {"class": "control-plane"},
                )
            if incident.state == "classified":
                incident = await asyncio.to_thread(
                    incident_manager.transition,
                    incident.id,
                    "contained",
                    {"policy": "retain-last-decision"},
                )
            if incident.state == "contained":
                incident = await asyncio.to_thread(
                    incident_manager.transition,
                    incident.id,
                    "recovering",
                    {"retry": 1},
                )
            active_incident_id = incident.id
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=settings.resource_sample_interval_s,
            )
        except TimeoutError:
            pass


async def _run_http_recovery_probe(
    settings,
    store: OpportunityPerceptionStore,
    stop_event: asyncio.Event,
) -> None:
    url = f"http://127.0.0.1:{settings.http_port}/healthz"
    manager = IncidentManager(store)
    while not stop_event.is_set():
        started_at_ms = int(time.time() * 1_000)

        def probe() -> bool:
            try:
                with urllib.request.urlopen(url, timeout=2.0) as response:
                    return response.status == 200
            except OSError:
                return False

        responsive = await asyncio.to_thread(probe)
        await asyncio.to_thread(
            store.record_http_probe,
            release_id=settings.release_id,
            started_at_ms=started_at_ms,
            finished_at_ms=int(time.time() * 1_000),
            responsive=responsive,
        )
        if responsive:
            incidents = await asyncio.to_thread(manager.open_incidents)
            for incident in incidents:
                if incident.scope == "http" and incident.state == "recovering":
                    try:
                        await asyncio.to_thread(
                            manager.transition,
                            incident.id,
                            "verified",
                            {"release_id": settings.release_id},
                        )
                    except ValueError:
                        pass
        else:
            incident = await asyncio.to_thread(
                manager.detect,
                "http",
                "unresponsive",
                {"release_id": settings.release_id},
            )
            if incident.state == "detected":
                incident = await asyncio.to_thread(
                    manager.transition,
                    incident.id,
                    "classified",
                    {"class": "http-probe"},
                )
            if incident.state == "classified":
                incident = await asyncio.to_thread(
                    manager.transition,
                    incident.id,
                    "contained",
                    {"probe_timeout_s": 2.0},
                )
            if incident.state == "contained":
                await asyncio.to_thread(
                    manager.transition,
                    incident.id,
                    "recovering",
                    {"release_id": settings.release_id},
                )
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=settings.http_recovery_probe_interval_s,
            )
        except TimeoutError:
            pass


def _start_legacy_structure_scheduler(
    scheduler: SnapshotScheduler,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if not scheduler.legacy_reconciliation_enabled:
        logger.info("legacy Structure reconciliation disabled")
        return None
    return asyncio.create_task(scheduler.run(stop_event))


async def main() -> int:
    # MUST be first — sets up JSON stdout sink + InterceptHandler
    init_logging()

    settings = load_settings()

    # Plan 05: init_sentry runs AFTER init_logging (LoguruIntegration needs
    # the loguru sink already installed) and BEFORE any logger.info that
    # might catch a startup exception we want Sentry to capture.
    init_sentry(settings)

    logger.info("polyarb daemon starting up")

    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()

    scheduler = SnapshotScheduler(settings=settings, sqlite_store=sqlite_store)
    focused_watcher = build_focused_opportunity_watcher(settings)
    perception_store = OpportunityPerceptionStore(settings.db_path)
    perception_store.init_schema()
    candidate_group_ids = compose_candidate_group_ids(
        focused_watcher.candidate_group_ids,
        perception_store,
    )
    isolated_producers = settings.opportunity_producer_supervisor_enabled
    candidate_watcher = (
        build_production_candidate_watcher(
            settings,
            candidate_group_ids=candidate_group_ids,
        )
        if settings.opportunity_first_watcher_enabled and not isolated_producers
        else None
    )

    def _candidate_freshness() -> CandidateFreshness:
        snapshot = perception_store.candidate_freshness_snapshot(now_ms=int(time.time() * 1_000))
        return CandidateFreshness(
            candidate_count=snapshot.candidate_count,
            quote_p95_age_ms=snapshot.quote_p95_age_ms,
            missing_quote_count=snapshot.missing_quote_count,
        )

    discovery = (
        build_production_discovery(
            settings,
            candidate_freshness=_candidate_freshness,
        )
        if settings.opportunity_discovery_enabled and not isolated_producers
        else None
    )
    reconciliation = (
        build_production_reconciliation(settings)
        if settings.opportunity_reconciliation_enabled and not isolated_producers
        else None
    )
    quote_worker = (
        None
        if isolated_producers
        else build_production_quote_worker(
            settings,
            opportunity_watcher=focused_watcher,
        )
    )
    app = create_app(
        scheduler=scheduler,
        sqlite_store=sqlite_store,
        settings=settings,
        quote_worker_runtime=quote_worker.runtime if quote_worker is not None else None,
        quote_worker=quote_worker,
        opportunity_watcher=focused_watcher,
        candidate_watcher_runtime=(
            candidate_watcher.runtime if candidate_watcher is not None else None
        ),
    )

    config = uvicorn.Config(
        app,
        host="0.0.0.0",  # Fly internal network only — fly.toml controls exposure
        port=settings.http_port,
        log_config=None,  # use loguru, not uvicorn's logger
        access_log=False,  # Axiom doesn't need access logs at this volume
    )
    server = uvicorn.Server(config)

    stop_event = asyncio.Event()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info(f"received {sig.name}, initiating graceful shutdown")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    server_task = asyncio.create_task(server.serve())

    # Wait for uvicorn to be ready before starting the scheduler.
    # server.started is set once uvicorn binds its socket and begins
    # accepting connections. Without this gate, the scheduler's first
    # tick can monopolize the event loop for minutes and Fly's health
    # check never sees a live port.
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.1)
    logger.info(f"daemon running: http server on :{settings.http_port}, starting scheduler")

    scheduler_task = (
        None if isolated_producers else _start_legacy_structure_scheduler(scheduler, stop_event)
    )
    quote_worker_task = _start_quote_worker(quote_worker, stop_event)
    focused_watcher_task = _start_opportunity_watcher(
        None if isolated_producers else focused_watcher,
        stop_event,
    )
    candidate_watcher_task = _start_candidate_watcher(candidate_watcher, stop_event)
    discovery_task = _start_discovery(discovery, stop_event)
    reconciliation_task = _start_reconciliation(reconciliation, stop_event)
    supervised_tasks = _start_supervised_producers(settings, perception_store, stop_event)
    resource_task = (
        asyncio.create_task(_run_resource_controller(settings, perception_store, stop_event))
        if settings.opportunity_resource_controller_enabled
        else None
    )
    http_probe_task = (
        asyncio.create_task(_run_http_recovery_probe(settings, perception_store, stop_event))
        if settings.opportunity_producer_supervisor_enabled
        else None
    )

    await stop_event.wait()
    logger.info("stop_event set, shutting down server")
    server.should_exit = True

    # F-04 (Plan 02-08): explicitly cancel the scheduler task so an in-flight
    # tick (e.g. ~minutes-long snapshot waiting on Gamma HTTP) is interrupted
    # within ~1s rather than waiting for the current await to return. The
    # scheduler re-raises CancelledError out of _tick() per F-04 contract.
    if scheduler_task is not None:
        scheduler_task.cancel()
    if focused_watcher_task is not None:
        focused_watcher_task.cancel()
    if candidate_watcher_task is not None:
        candidate_watcher_task.cancel()
    if discovery_task is not None:
        discovery_task.cancel()
    if reconciliation_task is not None:
        reconciliation_task.cancel()
    if quote_worker_task is not None:
        quote_worker_task.cancel()
    for task in supervised_tasks:
        task.cancel()
    if resource_task is not None:
        resource_task.cancel()
    if http_probe_task is not None:
        http_probe_task.cancel()

    # Bounded final wait — even if some task ignores cancellation, the daemon
    # exits within 5s instead of hanging indefinitely.
    try:
        await asyncio.wait_for(
            asyncio.gather(
                server_task,
                *([scheduler_task] if scheduler_task is not None else []),
                *([focused_watcher_task] if focused_watcher_task is not None else []),
                *([quote_worker_task] if quote_worker_task is not None else []),
                *([candidate_watcher_task] if candidate_watcher_task is not None else []),
                *([discovery_task] if discovery_task is not None else []),
                *([reconciliation_task] if reconciliation_task is not None else []),
                *supervised_tasks,
                *([resource_task] if resource_task is not None else []),
                *([http_probe_task] if http_probe_task is not None else []),
                return_exceptions=True,
            ),
            timeout=5.0,
        )
    except TimeoutError:
        logger.warning("graceful shutdown exceeded 5s; daemon exiting anyway")

    logger.info("polyarb daemon stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
