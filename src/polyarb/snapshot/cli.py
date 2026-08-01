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
import sqlite3
import sys
import time
from pathlib import Path

import typer
from loguru import logger

from polyarb.config import load_settings
from polyarb.perception.structure_publication import StructurePublicationCheckpoint
from polyarb.perception.structure_sync import (
    StructureSyncCheckpoint,
    run_structure_sync_until_published,
)
from polyarb.snapshot.orchestrator import run_snapshot

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _is_sqlite_writer_busy(error: sqlite3.OperationalError) -> bool:
    """Recognize only SQLite's BUSY/LOCKED primary result codes."""
    code = getattr(error, "sqlite_errorcode", None)
    return isinstance(code, int) and (code & 0xFF) in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }


def _generation_store(
    *,
    initialize: bool = True,
    writer_timeout_s: float | None = None,
):
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = load_settings()
    store = (
        SQLiteStore(settings.db_path)
        if writer_timeout_s is None
        else SQLiteStore(settings.db_path, writer_timeout_s=writer_timeout_s)
    )
    if initialize:
        store.init_structure_sync_schema()
    return settings, store


@app.command(name="structure-generation-status")
def structure_generation_status() -> None:
    """Read generation pointer, publication, comparison, and retention state."""
    settings, store = _generation_store(initialize=False)
    try:
        result = store.structure_generation_status(
            retain_generations=int(
                getattr(settings, "structure_generation_retention_floor", 2)
            ),
            pressure_probe_limit=int(
                getattr(settings, "structure_generation_pressure_fail_count", 8)
            ),
        )
    except (OSError, sqlite3.Error):
        print(
            json.dumps(
                {
                    "available": False,
                    "error": "structure-generation-status-unavailable",
                },
                sort_keys=True,
            )
        )
        raise typer.Exit(code=1) from None
    print(json.dumps(result, sort_keys=True))


@app.command(name="structure-generation-backfill")
def structure_generation_backfill(
    max_rows: int = typer.Option(500, "--max-rows", min=1, max=500),
    max_chunks: int = typer.Option(1, "--max-chunks", min=1, max=100),
    max_elapsed_seconds: float = typer.Option(
        30.0,
        "--max-elapsed-seconds",
        min=1.0,
        max=60.0,
    ),
) -> None:
    """Advance a bounded batch of legacy-to-generation backfill chunks."""
    store = None
    started_at: float | None = None
    chunks_attempted = 0
    chunks_succeeded = 0
    chunks_deferred = 0
    copied_rows = 0
    elapsed_seconds = 0.0
    final_progress: dict[str, object] | None = None
    stop_reason = "max-chunks"
    exit_code = 0
    try:
        _settings, store = _generation_store(
            initialize=False,
            writer_timeout_s=0.25,
        )
        store.init_structure_sync_schema()
        started_at = time.monotonic()
        for _chunk_index in range(max_chunks):
            elapsed_seconds = time.monotonic() - started_at
            if elapsed_seconds >= max_elapsed_seconds:
                stop_reason = "max-elapsed-seconds"
                break
            chunks_attempted += 1
            try:
                final_progress, chunk_exit_code = (
                    _advance_structure_generation_backfill_chunk(
                        store,
                        max_rows=max_rows,
                    )
                )
            except sqlite3.OperationalError as error:
                if not _is_sqlite_writer_busy(error):
                    raise
                chunks_deferred += 1
                final_progress = {
                    "complete": False,
                    "copied_rows": 0,
                    "defer_reason": "writer-busy",
                    "deferred": True,
                    "phase": "operator-admission",
                }
                stop_reason = "writer-busy"
                elapsed_seconds = time.monotonic() - started_at
                break

            chunk_copied_rows = int(final_progress["copied_rows"])
            copied_rows += chunk_copied_rows
            elapsed_seconds = time.monotonic() - started_at
            if chunk_exit_code != 0:
                exit_code = chunk_exit_code
                stop_reason = "blocked"
                break
            chunks_succeeded += 1
            phase_complete = bool(final_progress["complete"])
            bootstrap_complete = (
                final_progress.get("phase") == "event-market-bootstrap"
            )
            if phase_complete and not bootstrap_complete:
                stop_reason = "complete"
                break
            if elapsed_seconds >= max_elapsed_seconds:
                stop_reason = "max-elapsed-seconds"
                break
    except sqlite3.OperationalError as error:
        if not _is_sqlite_writer_busy(error):
            raise
        chunks_deferred = 1
        final_progress = {
            "complete": False,
            "copied_rows": 0,
            "defer_reason": "writer-busy",
            "deferred": True,
            "phase": "operator-admission",
        }
        stop_reason = "writer-busy"
        if started_at is not None:
            elapsed_seconds = time.monotonic() - started_at
    print(
        json.dumps(
            {
                "chunks_attempted": chunks_attempted,
                "chunks_deferred": chunks_deferred,
                "chunks_succeeded": chunks_succeeded,
                "copied_rows": copied_rows,
                "elapsed_seconds": round(elapsed_seconds, 6),
                "final_progress": final_progress,
                "stop_reason": stop_reason,
            },
            sort_keys=True,
        )
    )
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


def _advance_structure_generation_backfill_chunk(
    store,
    *,
    max_rows: int,
) -> tuple[dict[str, object], int]:
    """Advance one nonblocking chunk and preserve its failure truth."""
    latest = store.get_latest_structure_sync()
    if (
        latest is not None
        and latest["status"] == "complete"
        and store.get_structure_publication_progress(str(latest["id"])) is None
    ):
        migration = store.advance_structure_event_market_backfill(
            window_id=str(latest["id"]),
            max_events=max_rows,
            max_relationships=max_rows,
            now_ms=int(time.time() * 1_000),
        )
        if (
            int(migration["events_processed"]) > 0
            or int(migration["relationships_processed"]) > 0
            or not migration["completed"]
        ):
            rotated_to = None
            rotation_pending = False
            if migration["blocked"]:
                try:
                    successor = store.rotate_blocked_structure_sync_window(
                        window_id=str(latest["id"]),
                        rotated_at_ms=int(time.time() * 1_000),
                    )
                except sqlite3.OperationalError as error:
                    if not _is_sqlite_writer_busy(error):
                        raise
                    rotation_pending = True
                else:
                    rotated_to = successor["id"]
            payload: dict[str, object] = {
                "event_cursor": migration["event_cursor"],
                "member_offset": migration["member_offset"],
                "blocked": migration["blocked"],
                "blocked_reason": migration["blocked_reason"],
                "complete": migration["completed"],
                "copied_rows": migration["relationships_processed"],
                "defer_reason": None,
                "deferred": False,
                "events_processed": migration["events_processed"],
                "phase": "event-market-bootstrap",
                "rotated_to_window_id": rotated_to,
                "window_id": str(latest["id"]),
            }
            if rotation_pending:
                payload.update({"mutated": True, "rotation_pending": True})
            return payload, 1 if migration["blocked"] else 0
    result = store.backfill_current_structure_generation(max_rows=max_rows)
    return {
        "complete": result.complete,
        "copied_rows": result.copied_rows,
        "cursor": result.cursor,
        "defer_reason": None,
        "deferred": False,
        "snapshot_id": result.snapshot_id,
    }, 0


@app.command(name="structure-generation-compare")
def structure_generation_compare() -> None:
    """Read the immutable comparison receipt without changing read mode."""
    from polyarb.storage.sqlite_store import (
        StructureGenerationReadError,
        compare_current_structure_generation,
    )

    settings = load_settings()
    try:
        result = compare_current_structure_generation(settings.db_path)
    except StructureGenerationReadError as error:
        print(
            json.dumps(
                {"matches": False, "mismatch_reasons": [str(error)]},
                sort_keys=True,
            )
        )
        raise typer.Exit(code=1) from None
    payload = {
        "generation_market_count": result.generation_market_count,
        "generation_snapshot_id": result.generation_snapshot_id,
        "generation_source_truth_hash": result.generation_source_truth_hash,
        "generation_universe_hash": result.generation_universe_hash,
        "legacy_market_count": result.legacy_market_count,
        "legacy_snapshot_id": result.legacy_snapshot_id,
        "legacy_source_truth_hash": result.legacy_source_truth_hash,
        "legacy_universe_hash": result.legacy_universe_hash,
        "matches": result.matches,
        "mismatch_reasons": list(result.mismatch_reasons),
    }
    print(json.dumps(payload, sort_keys=True))
    if not result.matches:
        raise typer.Exit(code=1)


@app.command(name="structure-generation-cleanup")
def structure_generation_cleanup(
    max_rows: int = typer.Option(500, "--max-rows", min=1),
    retain_generations: int = typer.Option(2, "--retain-generations", min=2),
) -> None:
    """Advance one bounded evidence-aware old-generation cleanup phase."""
    import time

    _settings, store = _generation_store()
    result = store.cleanup_structure_generation_evidence(
        retain_generations=retain_generations,
        max_rows=max_rows,
        now_ms=int(time.time() * 1_000),
    )
    print(json.dumps(result, sort_keys=True))
    if result["blocked"]:
        raise typer.Exit(code=1)


@app.command(name="structure-sync")
def structure_sync(
    json_output: bool = typer.Option(False, "--json"),
    low_priority: bool = typer.Option(False, "--low-priority", hidden=True),
    max_pages: int | None = typer.Option(
        None,
        "--max-pages",
        min=1,
        hidden=True,
    ),
    max_elapsed_seconds: float | None = typer.Option(
        None,
        "--max-elapsed-seconds",
        min=1.0,
        hidden=True,
    ),
    max_publication_rows: int = typer.Option(
        500,
        "--max-publication-rows",
        min=1,
        max=500,
        hidden=True,
    ),
) -> None:
    """Resume bounded Gamma pages and publish one certified Structure revision."""
    if low_priority:
        os.nice(10)
    settings = load_settings()
    result = asyncio.run(
        run_structure_sync_until_published(
            settings,
            max_pages=max_pages,
            max_elapsed_s=max_elapsed_seconds,
            max_publication_rows=max_publication_rows,
        )
    )
    if isinstance(result, StructurePublicationCheckpoint):
        if json_output:
            print(
                json.dumps(
                    {
                        "checkpointed": True,
                        "stage": result.stage,
                        "component": result.component,
                        "rows_processed": result.rows_processed,
                        "cursor": result.cursor,
                        "publication_id": result.publication_id,
                        "chunks_processed": result.chunks_processed,
                        "elapsed_ms": result.elapsed_ms,
                    },
                    sort_keys=True,
                )
            )
        else:
            print(
                "CHECKPOINTED | "
                f"stage={result.stage} component={result.component} "
                f"rows={result.rows_processed} cursor={result.cursor} "
                f"publication_id={result.publication_id} "
                f"chunks={result.chunks_processed} elapsed_ms={result.elapsed_ms}"
            )
        return
    if isinstance(result, StructureSyncCheckpoint):
        if json_output:
            print(
                json.dumps(
                    {
                        "checkpointed": True,
                        "pages_processed": result.pages_processed,
                        "stage": result.stage,
                        "window_id": result.window_id,
                    },
                    sort_keys=True,
                )
            )
        else:
            print(
                "CHECKPOINTED | "
                f"stage={result.stage} pages={result.pages_processed} "
                f"window_id={result.window_id}"
            )
        return
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
        print(
            f"{result.status.upper()} | {result.market_count} markets | "
            f"snapshot_id={result.snapshot_id}"
        )
    raise typer.Exit(code=0 if result.is_valid else 1)


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
    product: str = typer.Option(
        "legacy_combined",
        "--product",
        help=(
            "Data product: structure (Gamma-only online truth), archive "
            "(research-only CLOB/Parquet), or legacy_combined."
        ),
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
    mode = "full" if full or product in ("structure", "archive") else "subset"

    result = asyncio.run(
        run_snapshot(settings, mode=mode, product=product, use_cache=not no_cache)
    )

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
