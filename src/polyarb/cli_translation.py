"""polyarb CLI: translation subcommands.

Naming: cli_translation.py (single file, NOT cli/ directory) — see PATTERNS §5.3.
This avoids any chance of `polyarb.cli` (single file) being shadowed by a
`polyarb/cli/` directory of the same name (SESSION 11 namespace shadow lesson).

Entry points (also exposed via Makefile):
    python -m polyarb.cli_translation translate-pending [--limit N] [--force-full] [--verbose]
    python -m polyarb.cli_translation translation-stats [--verbose]

Sample-first guard (Warning #8):
    First run with an empty cache and no --limit / --force-full prints a
    friendly stderr explanation and exits 1. This protects users from
    accidentally burning tokens on first run with bad config.
    Bypass via --force-full or `make translate-pending FORCE=1`.

ConfigError handling:
    A ConfigError from translate_pending (auth / model / api_key invalid)
    exits 1 with a stderr hint. This is the standalone-CLI side of the
    two-path semantics — orchestrator uses the other side (records
    translation_skipped_reason, snapshot stays valid).
"""

from __future__ import annotations

import asyncio
import sys

import typer
from loguru import logger
from pydantic import ValidationError

from polyarb.config import load_settings
from polyarb.translation.cache import TranslationCache
from polyarb.translation.config import TranslationConfig
from polyarb.translation.translator import (
    ConfigError,
    translate_pending,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _setup_logger(verbose: bool) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )


@app.command(name="translate-pending")
def translate_pending_cmd(
    limit: int | None = typer.Option(
        None, "--limit", help="Translate at most N questions (sample mode)."
    ),
    force_full: bool = typer.Option(
        False,
        "--force-full",
        help="Skip the first-run sample-first guard and translate everything.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Translate every market question that lacks a Chinese translation.

    First-run safety: if the cache is empty and no --limit / --force-full
    was given, abort with a friendly hint. Run `make translate-pending-sample`
    first to verify .env config on 50 questions.
    """
    _setup_logger(verbose)
    settings = load_settings()
    cache = TranslationCache(settings.db_path)
    cache.init_schema()  # idempotent — safe even when SQLiteStore already initialized.

    # ─── Sample-first guard (Warning #8) ─────────────────────────────────────
    # Triggers ONLY when the user did NOT pass --limit (so they're trying to
    # translate everything) AND the cache is empty (first run).
    if (
        limit is None
        and not force_full
        and cache.translated_count() == 0
    ):
        typer.echo(
            "first run detected (cache empty).\n"
            "  Run `make translate-pending-sample` (50 条 sample) first to verify .env config.\n"
            "  Then re-run with --force-full or `make translate-pending FORCE=1`.",
            err=True,
        )
        raise typer.Exit(1)

    # Load TranslationConfig — ValidationError → exit 1 with config hint.
    try:
        cfg = TranslationConfig()
    except ValidationError as e:
        typer.echo(
            f"TranslationConfig invalid (.env 配错？): {e}\n"
            "  Required: TRANSLATION_API_BASE / TRANSLATION_API_KEY / TRANSLATION_MODEL",
            err=True,
        )
        raise typer.Exit(1) from e

    # Run translate_pending — ConfigError surfaces as exit 1 with hint.
    try:
        summary = asyncio.run(
            translate_pending(cfg, settings.db_path, sample_limit=limit)
        )
    except ConfigError as e:
        typer.echo(
            f"translation config error: {e}\n"
            "  Check TRANSLATION_API_KEY / TRANSLATION_MODEL / TRANSLATION_API_BASE in .env",
            err=True,
        )
        raise typer.Exit(1) from e

    # Single-line stdout summary (cron / make can grep).
    typer.echo(
        f"OK | translated={summary.translated} "
        f"skipped={summary.skipped} "
        f"dead={summary.dead} "
        f"total_tokens={summary.total_tokens}"
    )


@app.command(name="translation-stats")
def translation_stats_cmd(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Show cumulative translation stats grouped by translator_model.

    Output format (TSV-ish, cron-grep friendly):
        model               n_questions   total_tokens   first_at      last_at
        deepseek-chat       12345         678901         1714...       1714...
    """
    _setup_logger(verbose)
    settings = load_settings()
    cache = TranslationCache(settings.db_path)
    cache.init_schema()  # idempotent
    rows = cache.stats()
    if not rows:
        typer.echo("(no translations yet)")
        return

    typer.echo(
        f"{'model':<28} {'n_questions':>12} {'total_tokens':>14} "
        f"{'first_at_ms':>16} {'last_at_ms':>16}"
    )
    for row in rows:
        typer.echo(
            f"{row['translator_model']:<28} "
            f"{row['n_questions']:>12} "
            f"{row['total_tokens']:>14} "
            f"{row['first_at_ms'] or 0:>16} "
            f"{row['last_at_ms'] or 0:>16}"
        )

    dead = cache.count_dead()
    if dead:
        typer.echo(f"  (warning: {dead} dead translations — manual reset required)")


if __name__ == "__main__":
    app()
