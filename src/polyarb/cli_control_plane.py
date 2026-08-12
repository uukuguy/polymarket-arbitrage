"""Safe operator commands for the additive M1 transactional control plane."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.config import Settings
from polyarb.control_plane.postgres import PostgresControlPlane
from polyarb.control_plane.quote_worker import (
    TransactionalQuoteBatchWorker,
    TransactionalQuoteCertifier,
)
from polyarb.control_plane.rollout import render_rollout_artifacts
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler
from polyarb.control_plane.shadow import project_shadow_sources, read_shadow_sources
from polyarb.control_plane.shadow_parity import verify_shadow_parity
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    canonical_structure_bundle_bytes,
    upload_structure_bundle_artifact,
)
from polyarb.control_plane.structure_shadow import (
    plan_structure_ranges,
    read_legacy_structure_bundle,
)
from polyarb.control_plane.structure_worker import (
    TransactionalStructureCertifier,
    TransactionalStructureWorker,
)
from polyarb.storage.r2_sync import _build_client


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
    tick_once.add_argument("--json", action="store_true")
    serve = subcommands.add_parser(
        "serve",
        help="run bounded transactional ticks until SIGINT or SIGTERM",
    )
    serve.add_argument("--enable", action="store_true")
    serve.add_argument("--worker-id", default="control-plane-service")
    serve.add_argument("--max-turns", type=int, default=4)
    serve.add_argument("--interval-seconds", type=float, default=15.0)
    serve.add_argument("--json", action="store_true")
    render_rollout = subcommands.add_parser(
        "render-rollout",
        help="render local-only named control-plane rollout artifacts",
    )
    render_rollout.add_argument("--enable", action="store_true")
    render_rollout.add_argument("--api-app", required=True)
    render_rollout.add_argument("--worker-app", required=True)
    render_rollout.add_argument("--expected-database", required=True)
    render_rollout.add_argument("--output-dir", type=Path, required=True)
    render_rollout.add_argument("--json", action="store_true")
    verify_parity = subcommands.add_parser(
        "verify-shadow-parity",
        help="verify three local Structure/Quote shadow-run evidence records",
    )
    verify_parity.add_argument("--evidence", type=Path, required=True)
    verify_parity.add_argument("--json", action="store_true")
    return parser


def _control_plane_from_env() -> PostgresControlPlane | None:
    dsn = os.environ.get("POLYARB_SUPABASE_DB_DSN", "").strip()
    if not dsn:
        return None
    return PostgresControlPlane(lambda: psycopg.connect(dsn))


def _write(payload: dict[str, object], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}={value}")


def _transactional_quote_workers(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
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
) -> TransactionalStructureWorker:
    """Build an explicitly invoked worker; it never exports or changes pointers."""
    object_client, bucket = _structure_object_client()
    return TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=object_client,
        bucket=bucket,
        worker_id=worker_id,
        now=lambda: datetime.now(UTC),
    )


def _transactional_scheduler(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    max_turns: int,
) -> TransactionalControlPlaneScheduler:
    quote_worker, quote_certifier = _transactional_quote_workers(
        control_plane, worker_id=f"{worker_id}:quote"
    )
    object_client, bucket = _structure_object_client()
    return TransactionalControlPlaneScheduler(
        structure_worker=TransactionalStructureWorker(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:structure",
            now=lambda: datetime.now(UTC),
        ),
        structure_certifier=TransactionalStructureCertifier(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:structure-certifier",
            now=lambda: datetime.now(UTC),
        ),
        quote_worker=quote_worker,
        quote_certifier=quote_certifier,
        max_turns=max_turns,
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
    scheduler: TransactionalControlPlaneScheduler,
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

    return await scheduler.run_until_stopped(
        stop_event=stop_event,
        interval_seconds=interval_seconds,
        on_tick=emit_tick,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    requires_enable = {
        "quote-once",
        "structure-once",
        "structure-shadow-once",
        "structure-shadow-publish",
        "tick-once",
        "serve",
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
    control_plane = _control_plane_from_env()
    if control_plane is None:
        print("POLYARB_SUPABASE_DB_DSN is required", file=sys.stderr)
        return 2
    try:
        if args.command == "preflight":
            database = control_plane.deployment_preflight(
                expected_database=args.expected_database
            )
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
            if args.max_turns <= 0:
                print("--max-turns must be positive", file=sys.stderr)
                return 2
            scheduler = _transactional_scheduler(
                control_plane, worker_id=args.worker_id, max_turns=args.max_turns
            )
            _write(asyncio.run(scheduler.run_tick()), as_json=args.json)
            return 0
        if args.command == "serve":
            if args.max_turns <= 0 or args.interval_seconds <= 0:
                print("--max-turns and --interval-seconds must be positive", file=sys.stderr)
                return 2
            scheduler = _transactional_scheduler(
                control_plane, worker_id=args.worker_id, max_turns=args.max_turns
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
