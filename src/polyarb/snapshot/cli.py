"""polyarb CLI: snapshot subcommand.

D-F1 (default): single-line stdout summary.
D-F2 (--verbose): DEBUG-level loguru output to stderr.
D-F3 (failure): stderr summary on is_valid=False, exit code 1.

Output convention (cron-grep friendly):
    stdout = ok line / summary line  (one per run)
    stderr = log noise + failure detail
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import typer
from loguru import logger

from polyarb.config import load_settings
from polyarb.snapshot.orchestrator import run_snapshot

app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.command()
def snapshot(
    full: bool = typer.Option(
        False,
        "--full",
        help="Fetch top-of-book for ALL markets (slower; ~1-2 hours).",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show progress + phase timings (DEBUG-level logs to stderr).",
    ),
    no_cache: bool = typer.Option(
        False,
        "--no-cache",
        help="Disable CLOB chunk cache (purges existing caches and forces full refetch).",
    ),
    config: Path | None = typer.Option(
        None,
        "--config",
        help="Path to YAML config (overrides config/snapshot.yaml).",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit one machine-readable result object on stdout.",
    ),
    low_priority: bool = typer.Option(
        False,
        "--low-priority",
        hidden=True,
        help="Lower this process priority for the in-app scheduler.",
    ),
) -> None:
    """Capture a one-shot Polymarket market snapshot."""
    if low_priority:
        os.nice(10)

    # Re-route loguru: default is INFO; --verbose drops to DEBUG.
    # Timestamp prefix lets the user (and post-mortem readers of /tmp logs)
    # measure phase durations and locate slow steps, surfaced after
    # LIVE-RUN-003/004 showed Gamma can be 10x slower than baseline without
    # any visible signal beyond "page N fetched" lines.
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )

    settings = load_settings(config)
    mode = "full" if full else "subset"

    result = asyncio.run(run_snapshot(settings, mode=mode, use_cache=not no_cache))

    # D-F1: single-line summary on stdout (cron / make can grep this).
    status = result.status.upper()  # "ok" → "OK", "degraded" → "DEGRADED", etc.
    summary = (
        f"{status} | {result.market_count} markets | mode={result.mode} | "
        f"{result.issue_count} issues | -> {result.parquet_path}"
    )
    if json_output:
        print(
            json.dumps(
                {
                    "is_valid": result.is_valid,
                    "issue_count": result.issue_count,
                    "market_count": result.market_count,
                    "mode": result.mode,
                    "snapshot_id": result.snapshot_id,
                    "status": result.status,
                },
                sort_keys=True,
            )
        )
    else:
        print(summary)

    # D-F3: stderr summary on FAILED + exit 1.
    if not result.is_valid:
        print("---", file=sys.stderr)
        print(f"VALIDATION FAILED: snapshot_id={result.snapshot_id}", file=sys.stderr)
        print("Issues by category:", file=sys.stderr)
        for cat, n in sorted(result.issue_categories.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {n}", file=sys.stderr)
        raise typer.Exit(code=1)
    raise typer.Exit(code=0)


@app.command(name="snapshots-purge")
def snapshots_purge_cmd(
    older_than_days: int = typer.Option(
        7, "--older-than-days", help="Delete snapshots older than N days."
    ),
    keep_last: int = typer.Option(5, "--keep-last", help="Always keep the last M snapshots."),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted without doing it."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Delete old snapshots (SQLite rows + Parquet files)."""
    from polyarb.storage.sqlite_store import SQLiteStore

    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )

    settings = load_settings()
    store = SQLiteStore(settings.db_path)
    store.init_schema()

    n, ids = store.purge_old_snapshots(
        older_than_days=older_than_days,
        keep_last=keep_last,
        parquet_root=settings.parquet_root,
        dry_run=dry_run,
    )

    if dry_run:
        print(f"DRY-RUN: would delete {len(ids)} snapshots (ids={ids})")
    else:
        print(f"OK | deleted {n} old snapshots (ids={ids})")
        print(f"  kept most recent {keep_last}, deleted everything older than {older_than_days}d")


if __name__ == "__main__":
    app()
