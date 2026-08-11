"""Safe operator commands for the additive M1 transactional control plane."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from polyarb.control_plane.postgres import PostgresControlPlane
from polyarb.control_plane.shadow import project_shadow_sources, read_shadow_sources


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


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
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
