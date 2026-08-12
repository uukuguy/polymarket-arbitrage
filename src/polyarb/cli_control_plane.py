"""Safe operator commands for the additive M1 transactional control plane."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
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
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler
from polyarb.control_plane.shadow import project_shadow_sources, read_shadow_sources
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


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    requires_enable = {
        "quote-once",
        "structure-once",
        "structure-shadow-once",
        "structure-shadow-publish",
        "tick-once",
    }
    if args.command in requires_enable and not args.enable:
        print(f"--enable is required for {args.command}", file=sys.stderr)
        return 2
    control_plane = _control_plane_from_env()
    if control_plane is None:
        print("POLYARB_SUPABASE_DB_DSN is required", file=sys.stderr)
        return 2
    try:
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
