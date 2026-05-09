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
from polyarb.observation.diff import (
    compare_snapshots,
    latest_snapshot_pair,
    resolve_snapshot_path,
)
from polyarb.observation.formatter import render_table, write_scan_parquet
from polyarb.observation.recipes import BUILTIN_RECIPES
from polyarb.observation.scanner import (
    list_all_recipes,
    run_recipe,
    run_recipe_grouped,
)
from polyarb.observation.show import show_market
from polyarb.observation.tracker import track_market
from polyarb.observation.watchlist import check_alerts, load_watchlist

app = typer.Typer(no_args_is_help=True, add_completion=False)

_DEFAULT_YAML = Path("config/scan_recipes.yaml")
_DEFAULT_SCANS_ROOT = Path("data/scans")
_DEFAULT_WATCHLIST = Path("watchlist.yaml")


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


@app.command(name="compare-snapshots")
def compare_snapshots_cmd(
    from_id: int | None = typer.Option(
        None, "--from", help="Snapshot ID (default: N-1)"
    ),
    to_id: int | None = typer.Option(None, "--to", help="Snapshot ID (default: N)"),
    limit: int = typer.Option(50, "--limit", help="Top N rows by drift magnitude"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Diff two snapshots — show appeared / vanished / drifted markets."""
    _setup_logger(verbose)
    settings = load_settings()
    try:
        if from_id is None or to_id is None:
            from_id, to_id = latest_snapshot_pair(settings.db_path)
            logger.info(f"defaulting to latest pair: from={from_id} to={to_id}")
        from_path = resolve_snapshot_path(from_id, settings.db_path)
        to_path = resolve_snapshot_path(to_id, settings.db_path)
    except ValueError as e:
        typer.echo(f"compare-snapshots failed: {e}", err=True)
        raise typer.Exit(1)
    df = compare_snapshots(from_path, to_path).head(limit)
    render_table(
        df,
        title=f"compare snapshots {from_id} → {to_id}",
        columns=(
            "slug",
            "category",
            "state",
            "mid_from",
            "mid_to",
            "mid_drift",
            "liq_from",
            "liq_to",
        ),
    )
    typer.echo(f"OK | from={from_id} to={to_id} | rows={len(df)}")


@app.command(name="track-market")
def track_market_cmd(
    slug: str = typer.Option(..., "--slug"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Track a single market across all snapshot parquets."""
    _setup_logger(verbose)
    settings = load_settings()
    df = track_market(slug, settings.parquet_root)
    if df.empty:
        typer.echo(f"no history for slug={slug}", err=True)
        raise typer.Exit(1)
    render_table(
        df,
        title=f"track-market: {slug}",
        columns=(
            "taken_at_ms",
            "snapshot_id",
            "mid_price",
            "best_bid_price",
            "best_ask_price",
            "spread",
            "liquidity_usd",
            "volume_usd",
        ),
    )
    typer.echo(f"OK | slug={slug} | snapshots={len(df)}")


@app.command(name="show-market")
def show_market_cmd(
    slug: str = typer.Option(..., "--slug"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Full detail for a single market — bilingual, time-dim, neg-risk siblings, 5-snapshot history."""
    from rich.console import Console

    _setup_logger(verbose)
    settings = load_settings()
    try:
        result = show_market(slug, settings.db_path, settings.parquet_root)
    except ValueError as e:
        typer.echo(f"show-market failed: {e}", err=True)
        raise typer.Exit(1)

    console = Console()
    m = result["market"]
    console.rule(f"[bold]{m['slug']}[/bold]")
    console.print(result["bilingual"])
    console.print(f"[cyan]时间维度:[/cyan] {result['time_dim']}")
    console.print(
        f"[cyan]流动性格:[/cyan] liquidity=${m.get('liquidity_usd', 0):,.0f}  "
        f"volume=${m.get('volume_usd', 0):,.0f}  "
        f"mid={m.get('mid_price')}  "
        f"bid={m.get('best_bid_price')}  "
        f"ask={m.get('best_ask_price')}  "
        f"spread={float(m.get('best_ask_price', 0) or 0) - float(m.get('best_bid_price', 0) or 0):.4f}"
    )
    console.print(f"[cyan]neg_risk:[/cyan] {m.get('neg_risk')}  market_id={m.get('neg_risk_market_id')}  event_id={m.get('event_id')}")

    if result["neg_risk_siblings"]:
        console.rule("[dim]Neg-risk 同组兄弟[/dim]")
        import pandas as pd
        render_table(
            pd.DataFrame(result["neg_risk_siblings"]),
            title="neg-risk siblings",
        )

    recent = result["recent_history"]
    if not recent.empty:
        console.rule("[dim]最近 5 次 snapshot 时序[/dim]")
        render_table(
            recent,
            title=f"recent history: {slug}",
            columns=(
                "taken_at_ms", "snapshot_id", "mid_price",
                "best_bid_price", "best_ask_price", "spread",
                "liquidity_usd", "volume_usd",
            ),
        )

    console.rule("[dim]Raw fields[/dim]")
    skipped = {"question", "question_zh"}
    for k, v in m.items():
        if k in skipped:
            continue
        console.print(f"  {k}: {v}")

    typer.echo(f"OK | slug={slug}")


@app.command(name="watchlist")
def watchlist_cmd(
    path: Path = typer.Option(_DEFAULT_WATCHLIST, "--path"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """List all markets in watchlist.yaml with current status."""
    _setup_logger(verbose)
    entries = load_watchlist(path)
    if not entries:
        typer.echo(
            "watchlist is empty. Copy watchlist.yaml.example → watchlist.yaml "
            "and add markets.", err=True
        )
        raise typer.Exit(1)

    settings = load_settings()
    import sqlite3
    import pandas as pd
    con = sqlite3.connect(f"file:{settings.db_path}?mode=ro", uri=True)
    try:
        slugs = [e.slug for e in entries]
        placeholders = ",".join(["?"] * len(slugs))
        rows = con.execute(
            f"""SELECT slug, question, mid_price, best_bid_price, best_ask_price,
                       liquidity_usd, volume_usd, neg_risk, end_time_ms
                FROM markets WHERE slug IN ({placeholders})""",
            slugs,
        ).fetchall()
    finally:
        con.close()

    slug_map = {r[0]: r for r in rows}
    rows_out = []
    for e in entries:
        r = slug_map.get(e.slug)
        if r is None:
            rows_out.append({
                "slug": e.slug, "alert": e.alert_when or "-",
                "question": "(not in DB)", "mid_price": None,
                "best_bid_price": None, "best_ask_price": None,
                "liquidity_usd": None, "volume_usd": None,
            })
        else:
            rows_out.append({
                "slug": r[0], "alert": e.alert_when or "-",
                "question": r[1], "mid_price": r[2],
                "best_bid_price": r[3], "best_ask_price": r[4],
                "liquidity_usd": r[5], "volume_usd": r[6],
            })

    render_table(
        pd.DataFrame(rows_out),
        title="watchlist",
        columns=(
            "slug", "alert", "mid_price", "best_bid_price",
            "best_ask_price", "liquidity_usd", "volume_usd",
        ),
    )
    typer.echo(f"OK | watchlist={len(entries)} entries")


@app.command(name="watchlist-alerts")
def watchlist_alerts_cmd(
    path: Path = typer.Option(_DEFAULT_WATCHLIST, "--path"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Check watchlist alert_when conditions — print triggered entries."""
    from rich.console import Console

    _setup_logger(verbose)
    entries = load_watchlist(path)
    if not entries:
        typer.echo("watchlist is empty", err=True)
        raise typer.Exit(1)

    settings = load_settings()
    triggered = check_alerts(entries, settings.db_path)

    if not triggered:
        typer.echo("OK | no alerts triggered")
        return

    console = Console()
    console.rule("[bold red]WATCHLIST ALERTS[/bold red]")
    for entry, row in triggered:
        console.print(f"[bold]{entry.slug}[/bold]")
        console.print(f"  reason: {entry.reason}")
        console.print(f"  condition: {entry.alert_when}")
        console.print(
            f"  mid={row.get('mid_price')}  bid={row.get('best_bid_price')}  "
            f"ask={row.get('best_ask_price')}  "
            f"liq=${row.get('liquidity_usd', 0):,.0f}"
        )
        console.print()

    typer.echo(f"ALERT | {len(triggered)} triggered")


if __name__ == "__main__":
    app()
