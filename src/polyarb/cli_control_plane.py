"""Safe operator commands for the additive M1 transactional control plane."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

import psycopg

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.clients.gamma_client import GammaClient
from polyarb.config import Settings
from polyarb.control_plane.alert_delivery import TransactionalAlertDeliveryWorker
from polyarb.control_plane.fault_soak import verify_fault_soak
from polyarb.control_plane.faults import IntentionalStagingRetryFault
from polyarb.control_plane.models import JobLease
from polyarb.control_plane.opportunity_worker import TransactionalOpportunityCertifier
from polyarb.control_plane.postgres import PostgresControlPlane
from polyarb.control_plane.quote_admission import TransactionalQuoteAdmitter
from polyarb.control_plane.quote_worker import (
    TransactionalQuoteBatchWorker,
    TransactionalQuoteCertifier,
)
from polyarb.control_plane.rollout import render_rollout_artifacts
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler
from polyarb.control_plane.shadow import project_shadow_sources, read_shadow_sources
from polyarb.control_plane.shadow_parity import verify_shadow_parity
from polyarb.control_plane.soak_evidence import (
    SoakEvidenceError,
    append_record,
    create_record,
    read_records,
    verify_soak,
)
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    canonical_structure_bundle_bytes,
    upload_structure_bundle_artifact,
)
from polyarb.control_plane.structure_shadow import (
    plan_structure_ranges,
    read_legacy_structure_bundle,
)
from polyarb.control_plane.structure_source import (
    TransactionalStructureSourceAdmitter,
    TransactionalStructureSourceMaterializer,
    TransactionalStructureSourcePool,
    TransactionalStructureSourceWorker,
)
from polyarb.control_plane.structure_worker import (
    TransactionalStructureCertifier,
    TransactionalStructureWorker,
)
from polyarb.control_plane.worker_loop import TransactionalWorkerLoop
from polyarb.storage.r2_sync import _build_client

_R2_UPLOAD_FAULT_ACK = "staging-r2-upload-before-receipt"
_RETRY_FAULT_ACK = "staging-retry-before-receipt"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="control-plane")
    subcommands = parser.add_subparsers(dest="command", required=True)
    shadow = subcommands.add_parser(
        "shadow-sync",
        help="project bounded SQLite facts into Postgres without changing pointers",
    )
    shadow.add_argument("--db-path", type=Path, required=True)
    shadow.add_argument("--limit", type=int, default=100)
    shadow.add_argument("--json", action="store_true")
    status = subcommands.add_parser("status", help="read bounded durable operator state")
    status.add_argument("--limit", type=int, default=20)
    status.add_argument("--json", action="store_true")
    preflight = subcommands.add_parser(
        "preflight",
        help="read-only proof that one named database and R2 bucket are ready for shadow work",
    )
    preflight.add_argument("--expected-database", required=True)
    preflight.add_argument("--json", action="store_true")
    quote_once = subcommands.add_parser(
        "quote-once",
        help="explicitly run at most one transactional Quote batch and certification attempt",
    )
    quote_once.add_argument(
        "--enable",
        action="store_true",
        help="required acknowledgement: this command may write to the configured control plane",
    )
    quote_once.add_argument("--worker-id", default="quote-operator-once")
    quote_once.add_argument("--json", action="store_true")
    structure_once = subcommands.add_parser(
        "structure-once",
        help="explicitly run at most one transactional Structure normalization range",
    )
    structure_once.add_argument(
        "--enable",
        action="store_true",
        help="required acknowledgement: this command may write to the configured control plane",
    )
    structure_once.add_argument("--worker-id", default="structure-operator-once")
    structure_once.add_argument("--json", action="store_true")
    structure_source_once = subcommands.add_parser(
        "structure-source-once",
        help=(
            "admit one named Structure source window then fetch at most its first "
            "durable Gamma page"
        ),
    )
    structure_source_once.add_argument("--enable", action="store_true")
    structure_source_once.add_argument("--window-key", required=True)
    structure_source_once.add_argument("--worker-id", default="structure-source-operator-once")
    structure_source_once.add_argument("--json", action="store_true")
    structure_shadow_once = subcommands.add_parser(
        "structure-shadow-once",
        help="export and admit one current legacy Structure publication without pointer changes",
    )
    structure_shadow_once.add_argument("--enable", action="store_true")
    structure_shadow_once.add_argument("--db-path", type=Path, required=True)
    structure_shadow_once.add_argument("--publication-id", required=True)
    structure_shadow_once.add_argument("--range-max-rows", type=int, default=1_000)
    structure_shadow_once.add_argument("--json", action="store_true")
    structure_shadow_publish = subcommands.add_parser(
        "structure-shadow-publish",
        help="explicitly publish one certified Structure generation to the shadow pointer",
    )
    structure_shadow_publish.add_argument("--enable", action="store_true")
    structure_shadow_publish.add_argument("--generation-key", required=True)
    structure_shadow_publish.add_argument("--json", action="store_true")
    tick_once = subcommands.add_parser(
        "tick-once",
        help="explicitly run one bounded transactional control-plane scheduler tick",
    )
    tick_once.add_argument("--enable", action="store_true")
    tick_once.add_argument("--worker-id", default="control-plane-tick-once")
    tick_once.add_argument("--max-turns", type=int, default=4)
    tick_once.add_argument("--structure-materializer-turns", type=int, default=0)
    tick_once.add_argument("--structure-range-turns", type=int, default=0)
    tick_once.add_argument("--fault-crash-after-r2-upload-job-key")
    tick_once.add_argument("--fault-retry-job-key")
    tick_once.add_argument("--fault-retry-attempts", type=int)
    tick_once.add_argument("--acceptance-run-id")
    tick_once.add_argument("--fault-injection-ack")
    tick_once.add_argument("--json", action="store_true")
    serve = subcommands.add_parser(
        "serve",
        help="run bounded transactional ticks until SIGINT or SIGTERM",
    )
    serve.add_argument("--enable", action="store_true")
    serve.add_argument("--worker-id", default="control-plane-service")
    serve.add_argument(
        "--worker-role",
        choices=("all", "coordinator", "structure-range", "quote-batch"),
        default="all",
        help="run all workers, or one independently scalable fenced worker role",
    )
    serve.add_argument("--max-turns", type=int, default=4)
    serve.add_argument("--pool-turns", type=int, default=1)
    serve.add_argument("--structure-high-water", type=int, default=2_000)
    serve.add_argument("--quote-high-water", type=int, default=512)
    serve.add_argument("--structure-materializer-turns", type=int, default=0)
    serve.add_argument("--structure-range-turns", type=int, default=0)
    serve.add_argument("--fault-crash-after-r2-upload-job-key")
    serve.add_argument("--fault-retry-job-key")
    serve.add_argument("--fault-retry-attempts", type=int)
    serve.add_argument("--acceptance-run-id")
    serve.add_argument("--fault-injection-ack")
    serve.add_argument("--interval-seconds", type=float, default=15.0)
    serve.add_argument("--json", action="store_true")
    alert_serve = subcommands.add_parser(
        "alert-serve", help="run the isolated transactional alert-delivery worker"
    )
    alert_serve.add_argument("--enable", action="store_true")
    alert_serve.add_argument("--worker-id", default="control-plane-alert-service")
    alert_serve.add_argument("--interval-seconds", type=float, default=15.0)
    alert_serve.add_argument("--acceptance-run-id")
    alert_serve.add_argument("--json", action="store_true")
    render_rollout = subcommands.add_parser(
        "render-rollout",
        help="render local-only named control-plane rollout artifacts",
    )
    render_rollout.add_argument("--enable", action="store_true")
    render_rollout.add_argument("--api-app", required=True)
    render_rollout.add_argument("--worker-app", required=True)
    render_rollout.add_argument("--alert-app", required=True)
    render_rollout.add_argument("--expected-database", required=True)
    render_rollout.add_argument("--output-dir", type=Path, required=True)
    render_rollout.add_argument("--json", action="store_true")
    verify_parity = subcommands.add_parser(
        "verify-shadow-parity",
        help="verify three local Structure/Quote shadow-run evidence records",
    )
    verify_parity.add_argument("--evidence", type=Path, required=True)
    verify_parity.add_argument("--json", action="store_true")
    verify_fault_soak_command = subcommands.add_parser(
        "verify-fault-soak",
        help="verify local cloud worker-loss and sustained-soak evidence",
    )
    verify_fault_soak_command.add_argument("--evidence", type=Path, required=True)
    verify_fault_soak_command.add_argument("--json", action="store_true")
    for command, help_text in (
        ("soak-start", "record the immutable baseline for a read-only transactional soak window"),
        ("soak-sample", "append one read-only transactional soak observation"),
    ):
        soak = subcommands.add_parser(command, help=help_text)
        soak.add_argument("--output", type=Path, required=True)
        soak.add_argument("--control-api-url", required=True)
        soak.add_argument("--machine-id", action="append", required=True)
        soak.add_argument("--fly-app", default="polyarb-control-worker-staging")
        soak.add_argument("--json", action="store_true")
    soak_verify = subcommands.add_parser(
        "soak-verify", help="verify a local immutable transactional soak evidence file"
    )
    soak_verify.add_argument("--evidence", type=Path, required=True)
    soak_verify.add_argument("--minimum-seconds", type=int, default=86_400)
    soak_verify.add_argument("--max-gap-seconds", type=int, default=900)
    soak_verify.add_argument("--json", action="store_true")
    return parser


def _control_plane_from_env() -> PostgresControlPlane | None:
    dsn = os.environ.get("POLYARB_SUPABASE_DB_DSN", "").strip()
    if not dsn:
        return None
    return PostgresControlPlane(lambda: psycopg.connect(dsn, connect_timeout=5))


def _write(payload: dict[str, object], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}={value}")


def _read_soak_control_snapshot(url: str) -> dict[str, object]:
    """Read the independent control API without importing its database client."""
    with urlopen(url, timeout=10) as response:  # noqa: S310 -- explicit operator URL
        payload = json.loads(response.read())
    if not isinstance(payload, dict):
        raise SoakEvidenceError("control API response must be an object")
    return payload


def _read_fly_machine_states(machine_ids: Sequence[str], *, app: str) -> dict[str, str]:
    """Read exact Fly machine state using its local CLI, with no machine mutation."""
    result = subprocess.run(
        ["flyctl", "machines", "list", "--app", app, "--json"],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    if not isinstance(payload, list):
        raise SoakEvidenceError("Fly machines list must be an array")
    listed = {
        item.get("id"): item.get("state")
        for item in payload
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    states: dict[str, str] = {}
    for machine_id in machine_ids:
        state = listed.get(machine_id)
        if not isinstance(state, str) or not state:
            raise SoakEvidenceError("an exact Fly machine is missing or has no state")
        states[machine_id] = state
    return states


def _record_soak_observation(args: argparse.Namespace, *, exclusive: bool) -> dict[str, object]:
    snapshot = _read_soak_control_snapshot(args.control_api_url)
    record = create_record(
        observed_at=datetime.now(UTC).isoformat(),
        control_api_url=args.control_api_url,
        machine_states=_read_fly_machine_states(args.machine_id, app=args.fly_app),
        control_snapshot=snapshot,
    )
    append_record(args.output, record, exclusive=exclusive)
    return {
        "status": "baseline-recorded" if exclusive else "sample-recorded",
        "evidence": str(args.output),
        "observed_at": record["observed_at"],
        "machine_count": len(args.machine_id),
    }


def _r2_upload_fault_callback(
    *, target_job_key: str | None, acknowledgement: str | None
) -> Callable[[JobLease], None] | None:
    """Create the explicit staging-only crash boundary for takeover acceptance."""
    if target_job_key is None:
        if acknowledgement is not None:
            raise ValueError("fault acknowledgement requires a target job key")
        return None
    if acknowledgement != _R2_UPLOAD_FAULT_ACK:
        raise ValueError("fault injection requires the exact staging acknowledgement")

    def crash_matching_lease(lease: JobLease) -> None:
        if lease.job_key == target_job_key:
            raise KeyboardInterrupt("intentional staging crash after verified R2 upload")

    return crash_matching_lease


def _retry_fault_callback(
    *, target_job_key: str | None, attempts: int | None, acknowledgement: str | None
) -> Callable[[JobLease], None] | None:
    """Create one exact, finite staging retry boundary before durable receipt."""
    if target_job_key is None and attempts is None:
        if acknowledgement == _RETRY_FAULT_ACK:
            raise ValueError("retry fault acknowledgement requires a target job key and attempts")
        return None
    if target_job_key is None or attempts is None or attempts <= 0:
        raise ValueError("retry fault target and attempts must be positive and paired")
    if acknowledgement != _RETRY_FAULT_ACK:
        raise ValueError("retry fault injection requires the exact staging acknowledgement")
    remaining = attempts

    def fail_matching_lease(lease: JobLease) -> None:
        nonlocal remaining
        if lease.job_key == target_job_key and remaining > 0:
            remaining -= 1
            raise IntentionalStagingRetryFault("intentional staging retry before receipt")

    return fail_matching_lease


def _transactional_quote_workers(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    crash_after_r2_upload: Callable[[JobLease], None] | None = None,
    retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
    acceptance_run_id: str | None = None,
) -> tuple[TransactionalQuoteBatchWorker, TransactionalQuoteCertifier]:
    """Build explicitly invoked workers; nothing schedules these by default."""
    settings = Settings()
    if not settings.r2_enabled:
        raise RuntimeError("transactional Quote requires configured R2 credentials")
    object_client = _build_client(
        settings.r2_endpoint,
        settings.r2_access_key_id.get_secret_value(),
        settings.r2_secret_access_key.get_secret_value(),
    )
    return (
        TransactionalQuoteBatchWorker(
            control_plane=control_plane,
            reader=ClobReaderClient(settings),
            object_client=object_client,
            bucket=settings.r2_bucket,
            worker_id=worker_id,
            now=lambda: datetime.now(UTC),
            crash_after_r2_upload=crash_after_r2_upload,
            retry_fault_before_receipt=retry_fault_before_receipt,
            acceptance_run_id=acceptance_run_id,
        ),
        TransactionalQuoteCertifier(
            control_plane=control_plane,
            worker_id=f"{worker_id}:certifier",
            now=lambda: datetime.now(UTC),
        ),
    )


def _transactional_structure_worker(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    crash_after_r2_upload: Callable[[JobLease], None] | None = None,
    retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
    acceptance_run_id: str | None = None,
) -> TransactionalStructureWorker:
    """Build an explicitly invoked worker; it never exports or changes pointers."""
    object_client, bucket = _structure_object_client()
    return TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=object_client,
        bucket=bucket,
        worker_id=worker_id,
        now=lambda: datetime.now(UTC),
        crash_after_r2_upload=crash_after_r2_upload,
        retry_fault_before_receipt=retry_fault_before_receipt,
        acceptance_run_id=acceptance_run_id,
    )


def _transactional_structure_source_worker(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    lane_count: int = 8,
) -> TransactionalStructureSourcePool:
    """Build bounded Gamma lanes; API and range workers never receive them."""
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    object_client, bucket = _structure_object_client()
    settings = Settings()
    return TransactionalStructureSourcePool(
        lanes=tuple(
            TransactionalStructureSourceWorker(
                control_plane=control_plane,
                gamma=GammaClient(settings),
                object_client=object_client,
                bucket=bucket,
                worker_id=f"{worker_id}:{ordinal}",
                now=lambda: datetime.now(UTC),
            )
            for ordinal in range(lane_count)
        )
    )


def _transactional_structure_source_materializer(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
) -> TransactionalStructureSourceMaterializer:
    object_client, bucket = _structure_object_client()
    return TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=object_client,
        bucket=bucket,
        worker_id=worker_id,
        now=lambda: datetime.now(UTC),
        range_max_rows=1_000,
    )


def _transactional_structure_source_admitter(
    control_plane: PostgresControlPlane,
    *,
    structure_high_water: int = 2_000,
    quote_high_water: int = 512,
) -> TransactionalStructureSourceAdmitter:
    """Open cadence windows only inside the transactional worker service."""
    return TransactionalStructureSourceAdmitter(
        control_plane=control_plane,
        cadence_seconds=300,
        structure_high_water=structure_high_water,
        quote_high_water=quote_high_water,
        now=lambda: datetime.now(UTC),
    )


def _transactional_quote_admitter(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
) -> TransactionalQuoteAdmitter:
    """Build the R2-only Structure-to-Quote bridge in the worker service."""
    object_client, bucket = _structure_object_client()
    return TransactionalQuoteAdmitter(
        control_plane=control_plane,
        object_client=object_client,
        bucket=bucket,
        worker_id=worker_id,
        now=lambda: datetime.now(UTC),
        batch_size=Settings().clob_batch_size,
    )


def _transactional_scheduler(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    max_turns: int,
    structure_materializer_turns: int,
    structure_range_turns: int,
    structure_high_water: int = 2_000,
    quote_high_water: int = 512,
    include_structure_range: bool = True,
    include_quote_batch: bool = True,
    crash_after_r2_upload: Callable[[JobLease], None] | None = None,
    retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
    acceptance_run_id: str | None = None,
) -> TransactionalControlPlaneScheduler:
    quote_worker, quote_certifier = _transactional_quote_workers(
        control_plane,
        worker_id=f"{worker_id}:quote",
        crash_after_r2_upload=crash_after_r2_upload,
        retry_fault_before_receipt=retry_fault_before_receipt,
        acceptance_run_id=acceptance_run_id,
    )
    object_client, bucket = _structure_object_client()
    return TransactionalControlPlaneScheduler(
        structure_source_admitter=_transactional_structure_source_admitter(
            control_plane,
            structure_high_water=structure_high_water,
            quote_high_water=quote_high_water,
        ),
        structure_source_worker=_transactional_structure_source_worker(
            control_plane, worker_id=f"{worker_id}:structure-source"
        ),
        structure_source_materializer=_transactional_structure_source_materializer(
            control_plane, worker_id=f"{worker_id}:structure-materializer"
        ),
        structure_worker=TransactionalStructureWorker(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:structure",
            now=lambda: datetime.now(UTC),
            crash_after_r2_upload=crash_after_r2_upload,
            retry_fault_before_receipt=retry_fault_before_receipt,
            acceptance_run_id=acceptance_run_id,
        ),
        structure_certifier=TransactionalStructureCertifier(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:structure-certifier",
            now=lambda: datetime.now(UTC),
        ),
        quote_admitter=_transactional_quote_admitter(
            control_plane, worker_id=f"{worker_id}:quote-admitter"
        ),
        quote_worker=quote_worker,
        quote_certifier=quote_certifier,
        opportunity_certifier=TransactionalOpportunityCertifier(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:opportunity-certifier",
            now=lambda: datetime.now(UTC),
        ),
        max_turns=max_turns,
        structure_materializer_turns=structure_materializer_turns,
        structure_range_turns=structure_range_turns,
        include_structure_range=include_structure_range,
        include_quote_batch=include_quote_batch,
    )


def _structure_object_client() -> tuple[object, str]:
    settings = Settings()
    if not settings.r2_enabled:
        raise RuntimeError("transactional Structure requires configured R2 credentials")
    return (
        _build_client(
            settings.r2_endpoint,
            settings.r2_access_key_id.get_secret_value(),
            settings.r2_secret_access_key.get_secret_value(),
        ),
        settings.r2_bucket,
    )


async def _run_scheduler_service(
    scheduler: TransactionalControlPlaneScheduler | TransactionalWorkerLoop,
    *,
    interval_seconds: float,
    as_json: bool,
) -> dict[str, object]:
    """Own signal delivery while the scheduler owns only bounded worker turns."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        stop_event.set()

    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(stop_signal, request_stop)
        except (NotImplementedError, RuntimeError):
            # The service is still safely stoppable by its hosting runtime on
            # platforms that cannot install asyncio signal handlers.
            pass

    async def emit_tick(outcome: dict[str, object]) -> None:
        _write({"event": "tick", **outcome}, as_json=as_json)

    try:
        return await scheduler.run_until_stopped(
            stop_event=stop_event,
            interval_seconds=interval_seconds,
            on_tick=emit_tick,
        )
    finally:
        await scheduler.aclose()


async def _run_alert_service(
    worker: TransactionalAlertDeliveryWorker, *, interval_seconds: float, as_json: bool
) -> dict[str, object]:
    """Run alert delivery separately from all data-plane process groups."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(stop_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    turns = 0
    while not stop_event.is_set():
        result = await worker.run_once()
        turns += 1
        _write(
            {"event": "alert-delivery", "outbox_id": result.outbox_id, "outcome": result.outcome},
            as_json=as_json,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue
    return {"status": "stopped", "turns": turns}


async def _run_one_structure_source_window(
    control_plane: PostgresControlPlane,
    *,
    window_key: str,
    worker_id: str,
) -> dict[str, object]:
    """Admit one named window and close its Gamma transport in the same loop."""
    control_plane.admit_structure_source_window(window_key=window_key, now=datetime.now(UTC))
    worker = _transactional_structure_source_worker(control_plane, worker_id=worker_id)
    try:
        result = await worker.run_once()
    finally:
        await worker.aclose()
    return {
        "status": "ok",
        "window_key": window_key,
        "page": {"job_key": result.job_key, "outcome": result.outcome},
        "pointer_mutations": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    requires_enable = {
        "quote-once",
        "structure-once",
        "structure-source-once",
        "structure-shadow-once",
        "structure-shadow-publish",
        "tick-once",
        "serve",
        "alert-serve",
        "render-rollout",
    }
    if args.command in requires_enable and not args.enable:
        print(f"--enable is required for {args.command}", file=sys.stderr)
        return 2
    if args.command == "render-rollout":
        try:
            artifacts = render_rollout_artifacts(
                api_app=args.api_app,
                worker_app=args.worker_app,
                alert_app=args.alert_app,
                expected_database=args.expected_database,
                output_dir=args.output_dir,
            )
        except (OSError, ValueError) as error:
            print(
                f"rollout artifact rendering unavailable: {type(error).__name__}",
                file=sys.stderr,
            )
            return 1
        _write({"status": "rendered-local-only", **artifacts}, as_json=args.json)
        return 0
    if args.command == "verify-shadow-parity":
        try:
            evidence = json.loads(args.evidence.read_text())
            if not isinstance(evidence, dict):
                raise ValueError("shadow parity evidence must be an object")
            _write(verify_shadow_parity(evidence), as_json=args.json)
        except (OSError, ValueError) as error:
            print(f"shadow parity unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command == "verify-fault-soak":
        try:
            evidence = json.loads(args.evidence.read_text())
            if not isinstance(evidence, dict):
                raise ValueError("fault/soak evidence must be an object")
            _write(verify_fault_soak(evidence), as_json=args.json)
        except (OSError, ValueError) as error:
            print(f"fault/soak evidence unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command in {"soak-start", "soak-sample"}:
        try:
            _write(
                _record_soak_observation(args, exclusive=args.command == "soak-start"),
                as_json=args.json,
            )
        except (OSError, SoakEvidenceError, subprocess.SubprocessError, ValueError) as error:
            print(f"soak evidence unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command == "soak-verify":
        try:
            _write(
                verify_soak(
                    read_records(args.evidence),
                    minimum_seconds=args.minimum_seconds,
                    max_gap_seconds=args.max_gap_seconds,
                ),
                as_json=args.json,
            )
        except (OSError, SoakEvidenceError, ValueError) as error:
            detail = str(error) if isinstance(error, SoakEvidenceError) else type(error).__name__
            print(f"soak evidence unavailable: {detail}", file=sys.stderr)
            return 1
        return 0
    control_plane = _control_plane_from_env()
    if control_plane is None:
        print("POLYARB_SUPABASE_DB_DSN is required", file=sys.stderr)
        return 2
    try:
        if args.command == "preflight":
            database = control_plane.deployment_preflight(expected_database=args.expected_database)
            object_client, bucket = _structure_object_client()
            object_client.head_bucket(Bucket=bucket)
            _write(
                {
                    "status": "ready-for-shadow-only",
                    "control_plane": database,
                    "r2": {"bucket": bucket, "reachable": True},
                },
                as_json=args.json,
            )
            return 0
        if args.command == "shadow-sync":
            sources = read_shadow_sources(args.db_path, limit=args.limit)
            projected = project_shadow_sources(
                sources,
                control_plane=control_plane,
                now=datetime.now(UTC),
            )
            _write(
                {
                    "status": "ok",
                    "projected_sources": projected,
                    "pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "quote-once":
            batch_worker, certifier = _transactional_quote_workers(
                control_plane, worker_id=args.worker_id
            )
            batch_result = asyncio.run(batch_worker.run_once())
            certifier_result = certifier.run_once()
            _write(
                {
                    "status": "ok",
                    "batch": {
                        "job_key": batch_result.job_key,
                        "outcome": batch_result.outcome,
                    },
                    "certifier": {
                        "job_key": certifier_result.job_key,
                        "outcome": certifier_result.outcome,
                    },
                },
                as_json=args.json,
            )
            return 0
        if args.command == "structure-once":
            worker = _transactional_structure_worker(control_plane, worker_id=args.worker_id)
            result = asyncio.run(worker.run_once())
            _write(
                {
                    "status": "ok",
                    "range": {"job_key": result.job_key, "outcome": result.outcome},
                    "pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "structure-source-once":
            _write(
                asyncio.run(
                    _run_one_structure_source_window(
                        control_plane,
                        window_key=args.window_key,
                        worker_id=args.worker_id,
                    )
                ),
                as_json=args.json,
            )
            return 0
        if args.command == "structure-shadow-once":
            identity, components = read_legacy_structure_bundle(
                args.db_path, publication_id=args.publication_id
            )
            artifact = StructureBundleArtifact.from_bytes(
                canonical_structure_bundle_bytes(identity=identity, components=components)
            )
            object_client, bucket = _structure_object_client()
            upload_structure_bundle_artifact(object_client, bucket=bucket, artifact=artifact)
            admitted = control_plane.enqueue_structure_generation(
                identity=identity,
                bundle=artifact,
                ranges=plan_structure_ranges(components, max_rows=args.range_max_rows),
                now=datetime.now(UTC),
            )
            _write(
                {
                    "status": "ok",
                    "source_identity": identity.header(),
                    "bundle_digest": artifact.sha256,
                    "admitted_job_count": len(admitted),
                    "pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "structure-shadow-publish":
            if not args.generation_key.startswith("structure:"):
                print("--generation-key must name a Structure generation", file=sys.stderr)
                return 2
            before = control_plane.structure_shadow_pointer()
            current = control_plane.publish_structure_shadow(
                generation_key=args.generation_key,
                now=datetime.now(UTC),
            )
            _write(
                {
                    "status": "ok",
                    "previous_generation_key": (
                        None if before is None else before["generation_key"]
                    ),
                    "current_generation_key": current,
                    "legacy_pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "tick-once":
            if (
                args.max_turns <= 0
                or args.structure_materializer_turns < 0
                or args.structure_range_turns < 0
            ):
                print(
                    "--max-turns must be positive and optional turn budgets non-negative",
                    file=sys.stderr,
                )
                return 2
            crash_after_r2_upload = _r2_upload_fault_callback(
                target_job_key=args.fault_crash_after_r2_upload_job_key,
                acknowledgement=(
                    args.fault_injection_ack
                    if args.fault_crash_after_r2_upload_job_key is not None
                    else None
                ),
            )
            retry_fault_before_receipt = _retry_fault_callback(
                target_job_key=args.fault_retry_job_key,
                attempts=args.fault_retry_attempts,
                acknowledgement=args.fault_injection_ack,
            )
            scheduler = _transactional_scheduler(
                control_plane,
                worker_id=args.worker_id,
                max_turns=args.max_turns,
                structure_materializer_turns=args.structure_materializer_turns,
                structure_range_turns=args.structure_range_turns,
                crash_after_r2_upload=crash_after_r2_upload,
                retry_fault_before_receipt=retry_fault_before_receipt,
                acceptance_run_id=args.acceptance_run_id,
            )
            _write(asyncio.run(scheduler.run_tick()), as_json=args.json)
            return 0
        if args.command == "serve":
            if (
                args.max_turns <= 0
                or args.pool_turns <= 0
                or args.structure_high_water <= 0
                or args.quote_high_water <= 0
                or args.structure_materializer_turns < 0
                or args.structure_range_turns < 0
                or args.interval_seconds <= 0
            ):
                print(
                    "--max-turns and --interval-seconds must be positive; "
                    "--pool-turns and high-water bounds must be positive; "
                    "optional turn budgets must be non-negative",
                    file=sys.stderr,
                )
                return 2
            crash_after_r2_upload = _r2_upload_fault_callback(
                target_job_key=args.fault_crash_after_r2_upload_job_key,
                acknowledgement=(
                    args.fault_injection_ack
                    if args.fault_crash_after_r2_upload_job_key is not None
                    else None
                ),
            )
            retry_fault_before_receipt = _retry_fault_callback(
                target_job_key=args.fault_retry_job_key,
                attempts=args.fault_retry_attempts,
                acknowledgement=args.fault_injection_ack,
            )
            if args.worker_role in {"all", "coordinator"}:
                scheduler: TransactionalControlPlaneScheduler | TransactionalWorkerLoop
                scheduler = _transactional_scheduler(
                    control_plane,
                    worker_id=args.worker_id,
                    max_turns=args.max_turns,
                    structure_materializer_turns=args.structure_materializer_turns,
                    structure_range_turns=args.structure_range_turns,
                    structure_high_water=args.structure_high_water,
                    quote_high_water=args.quote_high_water,
                    include_structure_range=args.worker_role == "all",
                    include_quote_batch=args.worker_role == "all",
                    crash_after_r2_upload=crash_after_r2_upload,
                    retry_fault_before_receipt=retry_fault_before_receipt,
                    acceptance_run_id=args.acceptance_run_id,
                )
            elif args.worker_role == "structure-range":
                if args.structure_high_water != 2_000 or args.quote_high_water != 512:
                    print(
                        "pool roles cannot configure source admission high-water bounds",
                        file=sys.stderr,
                    )
                    return 2
                scheduler = TransactionalWorkerLoop(
                    worker_name="structure-range",
                    worker=_transactional_structure_worker(
                        control_plane,
                        worker_id=f"{args.worker_id}:structure-range",
                        crash_after_r2_upload=crash_after_r2_upload,
                        retry_fault_before_receipt=retry_fault_before_receipt,
                        acceptance_run_id=args.acceptance_run_id,
                    ),
                    turns_per_tick=args.pool_turns,
                )
            else:
                if args.structure_high_water != 2_000 or args.quote_high_water != 512:
                    print(
                        "pool roles cannot configure source admission high-water bounds",
                        file=sys.stderr,
                    )
                    return 2
                quote_worker, _quote_certifier = _transactional_quote_workers(
                    control_plane,
                    worker_id=f"{args.worker_id}:quote-batch",
                    crash_after_r2_upload=crash_after_r2_upload,
                    retry_fault_before_receipt=retry_fault_before_receipt,
                    acceptance_run_id=args.acceptance_run_id,
                )
                scheduler = TransactionalWorkerLoop(
                    worker_name="quote-batch",
                    worker=quote_worker,
                    turns_per_tick=args.pool_turns,
                )
            result = asyncio.run(
                _run_scheduler_service(
                    scheduler,
                    interval_seconds=args.interval_seconds,
                    as_json=args.json,
                )
            )
            _write(result, as_json=args.json)
            return 0
        if args.command == "alert-serve":
            result = asyncio.run(
                _run_alert_service(
                    TransactionalAlertDeliveryWorker(
                        control_plane=control_plane,
                        worker_id=args.worker_id,
                        now=lambda: datetime.now(UTC),
                        acceptance_run_id=args.acceptance_run_id,
                    ),
                    interval_seconds=args.interval_seconds,
                    as_json=args.json,
                )
            )
            _write(result, as_json=args.json)
            return 0
        snapshot = control_plane.operational_snapshot(
            now=datetime.now(UTC), sample_limit=args.limit
        )
        _write({"status": "ok", **snapshot}, as_json=args.json)
        return 0
    except (OSError, RuntimeError, ValueError, psycopg.Error) as error:
        print(f"control-plane command unavailable: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
