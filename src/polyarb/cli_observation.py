"""polyarb CLI: observation subcommands.

Naming: cli_observation.py (single file, NOT cli/ directory) — see PATTERNS §5.3.
This avoids any chance of `polyarb.cli` (single file) being shadowed by a
`polyarb/cli/` directory of the same name (SESSION 11 namespace shadow lesson).

Entry points (also exposed via Makefile):
    python -m polyarb.cli_observation scan --name <recipe>
    python -m polyarb.cli_observation list-recipes
    python -m polyarb.cli_observation scans-purge --older-than-days 30

Each builtin recipe also gets a dedicated Makefile target (scan-thick-but-slippery,
scan-near-end, scan-ghost-suspicious, scan-coin-flip, scan-neg-risk-incomplete,
scan-by-tag) for muscle-memory ergonomics.

Error handling:
    Scanner ValueError (validation failure) / sqlite3.OperationalError
    (e.g. DB missing) → friendly stderr + typer.Exit(1). Never let raw
    tracebacks reach users (T-01.1-14 mitigation).
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import typer
from loguru import logger

from polyarb.config import load_settings
from polyarb.observation.formatter import render_table, write_scan_parquet
from polyarb.observation.recipes import BUILTIN_RECIPES
from polyarb.observation.scanner import (
    list_all_recipes,
    run_recipe,
    run_recipe_grouped,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)

_DEFAULT_YAML = Path("config/scan_recipes.yaml")
_DEFAULT_SCANS_ROOT = Path("data/scans")


def _setup_logger(verbose: bool) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )


@app.command()
def scan(
    name: str = typer.Option(
        ..., "--name", help="Recipe name (builtin or from config/scan_recipes.yaml)"
    ),
    yaml_path: Path = typer.Option(
        _DEFAULT_YAML, "--yaml", help="User recipes yaml file path."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    no_parquet: bool = typer.Option(
        False, "--no-parquet", help="Skip parquet write (terminal output only)."
    ),
    scans_root: Path = typer.Option(
        _DEFAULT_SCANS_ROOT, "--scans-root", help="Where to drop parquet outputs."
    ),
) -> None:
    """Run a scan recipe (builtin or user-defined)."""
    _setup_logger(verbose)
    settings = load_settings()
    recipes = list_all_recipes(yaml_path if yaml_path.exists() else None)
    if name not in recipes:
        typer.echo(
            f"unknown recipe: {name!r}. Available: {sorted(recipes)}",
            err=True,
        )
        raise typer.Exit(1)
    recipe = recipes[name]
    try:
        if recipe.group_by:
            df = run_recipe_grouped(settings.db_path, recipe)
        else:
            df = run_recipe(settings.db_path, recipe)
    except (ValueError, sqlite3.OperationalError) as e:
        typer.echo(f"scan failed: {e}", err=True)
        raise typer.Exit(1) from e
    render_table(df, title=f"{name}: {recipe.description}")
    if not no_parquet:
        try:
            write_scan_parquet(df, name, scans_root)
        except OSError as e:
            # Non-fatal: terminal output already shown; warn but don't exit 1
            logger.warning(f"parquet write failed: {e}")
    typer.echo(f"OK | recipe={name} | rows={len(df)}")


@app.command(name="list-recipes")
def list_recipes_cmd(
    yaml_path: Path = typer.Option(_DEFAULT_YAML, "--yaml"),
) -> None:
    """List all recipes (builtin + user yaml)."""
    recipes = list_all_recipes(yaml_path if yaml_path.exists() else None)
    for n in sorted(recipes):
        r = recipes[n]
        source = "builtin" if n in BUILTIN_RECIPES else "user"
        typer.echo(f"  [{source}] {n}: {r.description}")


@app.command(name="scans-purge")
def scans_purge_cmd(
    older_than_days: int = typer.Option(
        30, "--older-than-days", help="Delete scan parquet files older than N days."
    ),
    scans_root: Path = typer.Option(
        _DEFAULT_SCANS_ROOT, "--scans-root", help="Scans directory to purge."
    ),
) -> None:
    """Open Question #5 housekeeping: prune old data/scans/ parquet files."""
    if not scans_root.exists():
        typer.echo(f"scans dir {scans_root} does not exist; nothing to purge")
        return
    cutoff = time.time() - older_than_days * 86400
    deleted = 0
    for f in scans_root.rglob("*.parquet"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        except OSError as e:
            logger.warning(f"could not unlink {f}: {e}")
    typer.echo(f"purged {deleted} parquet files older than {older_than_days} days")


if __name__ == "__main__":
    app()
