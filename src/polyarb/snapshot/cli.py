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
    config: Path | None = typer.Option(
        None,
        "--config",
        help="Path to YAML config (overrides config/snapshot.yaml).",
    ),
) -> None:
    """Capture a one-shot Polymarket market snapshot."""
    # Re-route loguru: default is INFO; --verbose drops to DEBUG.
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<level>{level:<7}</level> | {message}",
    )

    settings = load_settings(config)
    mode = "full" if full else "subset"

    result = asyncio.run(run_snapshot(settings, mode=mode))

    # D-F1: single-line summary on stdout (cron / make can grep this).
    status = "OK" if result.is_valid else "INVALID"
    summary = (
        f"{status} | {result.market_count} markets | mode={result.mode} | "
        f"{result.issue_count} issues | -> {result.parquet_path}"
    )
    print(summary)

    # D-F3: stderr summary when invalid + exit 1.
    if not result.is_valid:
        print("---", file=sys.stderr)
        print(f"VALIDATION FAILED: snapshot_id={result.snapshot_id}", file=sys.stderr)
        print("Issues by category:", file=sys.stderr)
        for cat, n in sorted(result.issue_categories.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {n}", file=sys.stderr)
        raise typer.Exit(code=1)
    raise typer.Exit(code=0)


if __name__ == "__main__":
    app()
