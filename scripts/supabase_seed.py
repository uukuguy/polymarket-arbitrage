#!/usr/bin/env python3
"""supabase_seed.py — Supabase mirror bootstrap + reconcile tool.

Phase 02 Plan 03 — D-02 / D-19 fail-soft mirror.

Two commands:
  reconcile: Compare local SQLite vs Supabase; push any missing snapshots.
  init_check: Verify Supabase connection + 3 expected tables exist.

Usage:
  uv run python scripts/supabase_seed.py reconcile
  uv run python scripts/supabase_seed.py init-check

Or via Makefile:
  make supabase-reconcile     # reconcile command
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import typer

# F-3: allow external paths for scripts (data/ may be elsewhere)
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Supabase mirror bootstrap + reconcile for Polymarket L1 daemon.",
)


@app.command()
def reconcile(
    config: Path = typer.Option(
        Path("config/snapshot.yaml"),
        "--config",
        "-c",
        help="Path to snapshot YAML config (default: config/snapshot.yaml).",
    ),
) -> None:
    """Compare local SQLite vs Supabase; push any missing snapshots.

    Uses POLYARB_SUPABASE_URL + POLYARB_SUPABASE_SERVICE_KEY from env/.env.
    Prints the list of snapshot_ids that were pushed (empty if already in sync).
    """
    from polyarb.config import load_settings
    from polyarb.storage.sqlite_store import SQLiteStore
    from polyarb.storage.supabase_mirror import SupabaseMirror

    settings = load_settings(config)

    if not settings.supabase_mirror_enabled:
        typer.echo(
            "ERROR: Supabase mirror not configured. "
            "Set POLYARB_SUPABASE_URL + POLYARB_SUPABASE_SERVICE_KEY env vars.",
            err=True,
        )
        raise typer.Exit(code=1)

    mirror = SupabaseMirror(
        settings.supabase_url,
        settings.supabase_service_key.get_secret_value(),
    )
    store = SQLiteStore(settings.db_path)

    typer.echo(f">> reconcile: comparing SQLite at {settings.db_path} vs Supabase mirror...")
    missing = mirror.reconcile(store)

    if not missing:
        typer.echo(">> reconcile: already in sync — no missing snapshots")
    else:
        typer.echo(f">> reconcile: pushed {len(missing)} snapshot(s): {missing}")


@app.command()
def init_check(
    config: Path = typer.Option(
        Path("config/snapshot.yaml"),
        "--config",
        "-c",
        help="Path to snapshot YAML config (default: config/snapshot.yaml).",
    ),
) -> None:
    """Verify Supabase connection + 3 expected tables exist.

    Checks that snapshots, markets_latest, and recipe_runs tables are accessible.
    Use this after running 'make supabase-migrate' to confirm migration landed.
    """
    from polyarb.config import load_settings
    from polyarb.storage.supabase_mirror import SupabaseMirror

    settings = load_settings(config)

    if not settings.supabase_mirror_enabled:
        typer.echo(
            "ERROR: Supabase mirror not configured. "
            "Set POLYARB_SUPABASE_URL + POLYARB_SUPABASE_SERVICE_KEY env vars.",
            err=True,
        )
        raise typer.Exit(code=1)

    mirror = SupabaseMirror(
        settings.supabase_url,
        settings.supabase_service_key.get_secret_value(),
    )

    typer.echo(f">> init_check: testing connection to {settings.supabase_url}")
    errors: list[str] = []
    for table_name in ("snapshots", "markets_latest", "recipe_runs"):
        try:
            result = mirror._client.table(table_name).select("*").limit(1).execute()
            typer.echo(f"  OK: table '{table_name}' accessible ({len(result.data)} rows sampled)")
        except Exception as e:
            errors.append(f"  FAIL: table '{table_name}' — {str(e)[:200]}")

    if errors:
        for err in errors:
            typer.echo(err, err=True)
        typer.echo(
            "\nFAIL: some tables not accessible. Did you run 'make supabase-migrate'?",
            err=True,
        )
        raise typer.Exit(code=1)
    else:
        typer.echo(">> init_check: all 3 tables accessible — Supabase mirror ready")


if __name__ == "__main__":
    app()
