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
import os
import signal
import sys
import time
import uuid
from collections.abc import Callable
from uuid import UUID

import uvicorn
from loguru import logger

from polyarb.config import load_settings
from polyarb.daemon.generation_cleanup_worker import StructureGenerationCleanupWorker
from polyarb.daemon.opportunity_watcher import (
    OpportunityWatcher,
    build_focused_opportunity_watcher,
)
from polyarb.daemon.quote_worker import (
    QuoteWorker,
    QuoteWorkerRuntime,
    build_production_quote_worker,
    load_certified_quote_feed,
)
from polyarb.daemon.scheduler import SnapshotScheduler
from polyarb.http.app import create_app
from polyarb.observability.logging import init_logging
from polyarb.observability.sentry import init_sentry
from polyarb.perception.candidate_watcher import (
    CandidateWatcherScheduler,
    build_production_candidate_watcher,
)
from polyarb.perception.capacity_controller import (
    CapacityController,
    CapacityMaintenanceWorker,
    CapacityPolicy,
)
from polyarb.perception.capacity_incidents import CapacityIncidentLifecycle
from polyarb.perception.discovery import (
    CandidateFreshness,
    DiscoveryRunner,
    build_production_discovery,
    compose_candidate_group_ids,
)
from polyarb.perception.fault_control import FaultRuntimeIdentity
from polyarb.perception.fault_runtime import (
    FaultRuntimeProtocol,
    PassThroughFaultRuntime,
    build_fault_runtime,
)
from polyarb.perception.http_probe import BoundedHttpProbeWriter
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.reconciliation import (
    ReconciliationRunner,
    build_production_reconciliation,
)
from polyarb.perception.resource_controller import (
    ResourceController,
)
from polyarb.perception.resource_incidents import ResourcePressureIncidents
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.perception.supervisor import ProducerSpec, ProducerSupervisor
from polyarb.storage.sqlite_store import SQLiteStore


def _build_daemon_fault_runtime(
    settings,
    *,
    component: str,
    boot_id: UUID,
    supervisor_run_id: str,
) -> FaultRuntimeProtocol:
    enabled = bool(getattr(settings, "upstream_fault_control_enabled", False))
    try:
        identity = FaultRuntimeIdentity(
            component=component,
            release_id=settings.release_id,
            machine_id=os.environ.get("FLY_MACHINE_ID", "local"),
            boot_id=boot_id,
        )
    except (TypeError, ValueError, AttributeError):
        return PassThroughFaultRuntime(degraded=enabled)
    return build_fault_runtime(
        enabled=enabled,
        db_path=settings.db_path,
        identity=identity,
        supervisor_run_id=supervisor_run_id,
        attempt=1,
        started_at_ms=int(time.time() * 1_000),
    )


def _build_daemon_perception_workers(
    settings,
    perception_store: OpportunityPerceptionStore,
) -> tuple[
    OpportunityWatcher,
    CandidateWatcherScheduler | None,
    DiscoveryRunner | None,
    ReconciliationRunner | None,
]:
    """Bind exact in-daemon runtimes to each producer builder."""
    isolated_producers = settings.opportunity_producer_supervisor_enabled
    daemon_boot_id = uuid.uuid4()
    daemon_run_id = uuid.uuid4().hex
    notification_fault_runtime = _build_daemon_fault_runtime(
        settings,
        component="notification",
        boot_id=daemon_boot_id,
        supervisor_run_id=daemon_run_id,
    )
    component_fault_runtimes = (
        {}
        if isolated_producers
        else {
            component: _build_daemon_fault_runtime(
                settings,
                component=component,
                boot_id=uuid.uuid4(),
                supervisor_run_id=daemon_run_id,
            )
            for component in ("candidate", "discovery", "reconciliation")
        }
    )
    focused_watcher = build_focused_opportunity_watcher(
        settings,
        fault_runtime=notification_fault_runtime,
    )
    candidate_group_ids = compose_candidate_group_ids(
        focused_watcher.candidate_group_ids,
        perception_store,
    )
    candidate_watcher = (
        build_production_candidate_watcher(
            settings,
            candidate_group_ids=candidate_group_ids,
            fault_runtime=component_fault_runtimes["candidate"],
        )
        if settings.opportunity_first_watcher_enabled and not isolated_producers
        else None
    )

    def candidate_freshness() -> CandidateFreshness:
        snapshot = perception_store.candidate_freshness_snapshot(
            now_ms=int(time.time() * 1_000)
        )
        return CandidateFreshness(
            candidate_count=snapshot.candidate_count,
            quote_p95_age_ms=snapshot.quote_p95_age_ms,
            missing_quote_count=snapshot.missing_quote_count,
        )

    discovery = (
        build_production_discovery(
            settings,
            candidate_freshness=candidate_freshness,
            fault_runtime=component_fault_runtimes["discovery"],
        )
        if settings.opportunity_discovery_enabled and not isolated_producers
        else None
    )
    reconciliation = (
        build_production_reconciliation(
            settings,
            fault_runtime=component_fault_runtimes["reconciliation"],
        )
        if settings.opportunity_reconciliation_enabled and not isolated_producers
        else None
    )
    return focused_watcher, candidate_watcher, discovery, reconciliation


def _start_quote_worker(
    quote_worker: QuoteWorker | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if quote_worker is None:
        return None
    return asyncio.create_task(quote_worker.run(stop_event))


async def _hydrate_durable_quote_feed(
    runtime: QuoteWorkerRuntime,
    loader: Callable[[], object],
) -> bool:
    """Atomically copy an already-certified child feed into HTTP memory."""
    feed = await asyncio.to_thread(loader)
    if feed is None:
        return False
    runtime.restore_certified_feed(feed)
    return True


async def _run_durable_quote_feed_hydrator(
    runtime: QuoteWorkerRuntime,
    loader: Callable[[], object],
    stop_event: asyncio.Event,
    *,
    interval_s: float,
) -> None:
    """Keep the HTTP cache current when collection runs in a child process."""
    while not stop_event.is_set():
        try:
            await _hydrate_durable_quote_feed(runtime, loader)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # durable retry; endpoint keeps last valid feed
            logger.warning(
                "durable quote feed hydration failed "
                f"kind={type(error).__name__}"
            )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
        except TimeoutError:
            continue


def _start_durable_quote_feed_hydrator(
    runtime: QuoteWorkerRuntime | None,
    loader: Callable[[], object] | None,
    stop_event: asyncio.Event,
    *,
    interval_s: float,
) -> asyncio.Task[None] | None:
    if runtime is None or loader is None:
        return None
    return asyncio.create_task(
        _run_durable_quote_feed_hydrator(
            runtime,
            loader,
            stop_event,
            interval_s=interval_s,
        )
    )


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


async def _wait_for_http_startup(server, server_task, *, timeout_s: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not server.started:
        if server_task.done():
            if server_task.cancelled():
                raise RuntimeError("http-server-startup-failed:cancelled")
            error = server_task.exception()
            detail = "exited" if error is None else type(error).__name__
            raise RuntimeError(f"http-server-startup-failed:{detail}") from error
        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError("http-server-startup-failed:readiness-timeout")
        await asyncio.sleep(0.05)


async def _abort_http_startup(
    server,
    server_task,
    incidents: IncidentManager,
    error: BaseException,
) -> None:
    incident = incidents.detect(
        "http",
        "startup-failure",
        {"error_kind": type(error).__name__},
    )
    if incident.state == "detected":
        incident = incidents.transition(
            incident.id,
            "classified",
            {"class": "http-startup"},
        )
    if incident.state == "classified":
        incident = incidents.transition(
            incident.id,
            "contained",
            {"producers_started": False},
        )
    if incident.state in {"contained", "recovering"}:
        incidents.transition(
            incident.id,
            "escalated",
            {"requires_process_restart": True},
        )
    server.should_exit = True
    if not server_task.done():
        server_task.cancel()
    await asyncio.gather(server_task, return_exceptions=True)


def _start_supervised_producers(
    settings,
    store: OpportunityPerceptionStore,
    stop_event: asyncio.Event,
) -> list[asyncio.Task[None]]:
    all_producers_supervised = settings.opportunity_producer_supervisor_enabled
    quote_supervised = (
        all_producers_supervised or settings.neg_risk_quote_supervisor_enabled
    )
    if not all_producers_supervised and not quote_supervised:
        return []
    flags = {
        "candidate": (
            all_producers_supervised and settings.opportunity_first_watcher_enabled
        ),
        "discovery": all_producers_supervised and settings.opportunity_discovery_enabled,
        "reconciliation": (
            all_producers_supervised and settings.opportunity_reconciliation_enabled
        ),
        "quote": quote_supervised and settings.neg_risk_quote_worker_enabled,
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
                    stall_detection_s=(
                        settings.producer_stall_detection_s
                        if component == "reconciliation"
                        else None
                    ),
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
        decision_ttl_ms=int(settings.resource_decision_ttl_s * 1_000),
        min_disk_free_bytes=settings.resource_min_disk_free_mb * 1024 * 1024,
        max_load_per_cpu=settings.resource_max_load_per_cpu,
    )
    previous_limit = settings.discovery_page_limit
    incident_manager = IncidentManager(store)
    pressure_incidents = ResourcePressureIncidents(store)
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
            decision_id = await asyncio.to_thread(
                store.latest_resource_decision_id
            )
            await asyncio.to_thread(
                pressure_incidents.observe,
                decision,
                decision_id=decision_id,
            )
            if active_incident_id is not None:
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
    writer = BoundedHttpProbeWriter(store, timeout_s=2.0)
    while not stop_event.is_set():
        incidents = await asyncio.to_thread(manager.open_incidents)
        recovering = next(
            (
                incident
                for incident in incidents
                if incident.scope == "http" and incident.state == "recovering"
            ),
            None,
        )
        probe_nonce = (
            recovering.evidence.get("probe_nonce")
            if recovering is not None
            else uuid.uuid4().hex
        )
        if not isinstance(probe_nonce, str) or not probe_nonce:
            probe_nonce = uuid.uuid4().hex
        result = await asyncio.to_thread(
            writer.probe,
            url,
            expected_release_id=settings.release_id,
            probe_nonce=probe_nonce,
        )
        if result.responsive:
            if recovering is not None:
                try:
                    await asyncio.to_thread(
                        manager.transition,
                        recovering.id,
                        "verified",
                        {
                            "release_id": settings.release_id,
                            "probe_nonce": probe_nonce,
                        },
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
                    {
                        "release_id": settings.release_id,
                        "probe_nonce": uuid.uuid4().hex,
                    },
                )
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=settings.http_recovery_probe_interval_s,
            )
        except TimeoutError:
            pass


def _start_structure_scheduler(
    scheduler: SnapshotScheduler,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if not scheduler.structure_sync_enabled:
        logger.info("resumable Structure synchronization disabled")
        return None
    return asyncio.create_task(scheduler.run(stop_event))


def _build_generation_cleanup_worker(
    settings,
    sqlite_store: SQLiteStore,
    producer_lock: asyncio.Lock,
    quote_worker_runtime,
    *,
    isolated_producers: bool,
    structure_sync_enabled: bool,
) -> StructureGenerationCleanupWorker | None:
    """Create the single cleanup owner only beside generation publication."""
    # In isolated topology the scheduler lives in a supervised child, but the
    # parent remains the only resident owner able to drive durable cleanup.
    # Suppressing this worker there leaves an enabled cleanup runtime stale
    # forever while reclaimable generations accumulate.
    if not structure_sync_enabled or not settings.structure_generation_cleanup_enabled:
        return None
    return StructureGenerationCleanupWorker(
        settings=settings,
        sqlite_store=sqlite_store,
        producer_lock=producer_lock,
        quote_worker_runtime=quote_worker_runtime,
    )


def _build_capacity_worker(
    settings,
    sqlite_store: SQLiteStore,
    perception_store: OpportunityPerceptionStore,
    producer_lock: asyncio.Lock,
    *,
    quote_worker_runtime: QuoteWorkerRuntime | None,
) -> CapacityMaintenanceWorker | None:
    """Capacity governance is independent of optional producer supervision."""
    if not getattr(settings, "capacity_controller_enabled", False):
        return None
    policy = CapacityPolicy(
        pressure_free_percent=float(settings.capacity_pressure_free_percent),
        critical_free_percent=float(settings.capacity_critical_free_percent),
        exhaustion_free_percent=float(settings.capacity_exhaustion_free_percent),
        recovery_hold_ms=int(float(settings.capacity_recovery_hold_s) * 1_000),
    )
    return CapacityMaintenanceWorker(
        controller=CapacityController(
            store=sqlite_store,
            policy=policy,
            retry_delay_ms=int(float(settings.capacity_retry_delay_s) * 1_000),
        ),
        producer_lock=producer_lock,
        quote_worker_runtime=quote_worker_runtime,
        quote_interval_s=float(settings.neg_risk_quote_interval_s),
        interval_s=float(settings.capacity_interval_s),
        incident_lifecycle=CapacityIncidentLifecycle(IncidentManager(perception_store)),
    )


def _start_generation_cleanup_worker(
    worker: StructureGenerationCleanupWorker | None,
    stop_event: asyncio.Event,
) -> asyncio.Task[None] | None:
    if worker is None:
        return None
    return asyncio.create_task(worker.run(stop_event))


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

    producer_lock = asyncio.Lock()
    perception_store = OpportunityPerceptionStore(settings.db_path)
    perception_store.init_schema()
    isolated_producers = settings.opportunity_producer_supervisor_enabled
    quote_supervised = (
        isolated_producers or settings.neg_risk_quote_supervisor_enabled
    )
    (
        focused_watcher,
        candidate_watcher,
        discovery,
        reconciliation,
    ) = _build_daemon_perception_workers(
        settings,
        perception_store,
    )
    quote_worker = build_production_quote_worker(
        settings,
        opportunity_watcher=focused_watcher,
        producer_lock=producer_lock,
        perception_store=perception_store,
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=sqlite_store,
        producer_lock=producer_lock,
        on_snapshot_published=(
            quote_worker.request_now if quote_worker is not None else None
        ),
        quote_worker_runtime=(
            quote_worker.runtime if quote_worker is not None else None
        ),
    )
    cleanup_worker = _build_generation_cleanup_worker(
        settings,
        sqlite_store,
        producer_lock,
        quote_worker.runtime if quote_worker is not None else None,
        isolated_producers=isolated_producers,
        structure_sync_enabled=scheduler.structure_sync_enabled,
    )
    capacity_worker = _build_capacity_worker(
        settings,
        sqlite_store,
        perception_store,
        producer_lock,
        quote_worker_runtime=quote_worker.runtime if quote_worker is not None else None,
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
    # The producer supervisor intentionally puts Quote collection in another
    # process. The HTTP parent may therefore hydrate this read-only, already
    # certified feed on demand without taking over collection work.
    if settings.neg_risk_quote_worker_enabled:
        app.state.quote_feed_loader = lambda: load_certified_quote_feed(settings)

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

    # A bound and accepting HTTP socket is the startup commit point. Producer
    # work must not begin if uvicorn exits, fails to bind, or never becomes
    # ready.
    try:
        await _wait_for_http_startup(server, server_task, timeout_s=10.0)
    except RuntimeError as error:
        await _abort_http_startup(
            server,
            server_task,
            IncidentManager(perception_store),
            error,
        )
        logger.error("daemon HTTP startup failed; producers were not started")
        return 1
    logger.info(f"daemon running: http server on :{settings.http_port}, starting scheduler")

    scheduler_task = (
        None if isolated_producers else _start_structure_scheduler(scheduler, stop_event)
    )
    quote_worker_task = _start_quote_worker(
        None if quote_supervised else quote_worker,
        stop_event,
    )
    quote_feed_hydrator_task = _start_durable_quote_feed_hydrator(
        quote_worker.runtime if quote_supervised and quote_worker is not None else None,
        getattr(app.state, "quote_feed_loader", None),
        stop_event,
        interval_s=min(15.0, max(5.0, settings.neg_risk_quote_interval_s / 4)),
    )
    cleanup_worker_task = _start_generation_cleanup_worker(cleanup_worker, stop_event)
    capacity_worker_task = (
        asyncio.create_task(capacity_worker.run(stop_event))
        if capacity_worker is not None
        else None
    )
    focused_watcher_task = _start_opportunity_watcher(
        (
            None
            if isolated_producers or not settings.opportunity_first_watcher_enabled
            else focused_watcher
        ),
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
    if quote_feed_hydrator_task is not None:
        quote_feed_hydrator_task.cancel()
    if cleanup_worker_task is not None:
        cleanup_worker_task.cancel()
    if capacity_worker_task is not None:
        capacity_worker_task.cancel()
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
                *(
                    [quote_feed_hydrator_task]
                    if quote_feed_hydrator_task is not None
                    else []
                ),
                *([cleanup_worker_task] if cleanup_worker_task is not None else []),
                *([capacity_worker_task] if capacity_worker_task is not None else []),
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
