"""Output formatters for observation results.

Two output forms (CONTEXT.md `### 输出格式`):
- A: rich.Table to terminal (default)
- C: atomic parquet to data/scans/<recipe>/<timestamp>.parquet (always, unless empty)

Defense: rich.Table escapes ANSI by default. We additionally pre-strip ANSI
escape sequences from any cell value (T-01.1-13 mitigation) — even if rich
ever changed its default, an LLM-supplied question_zh containing
``\\x1b[31m`` will not render as colored output. Pre-stripping is the single
load-bearing line; the rest is plain text rendering.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger
from rich.console import Console
from rich.table import Table


# ANSI CSI / OSC sequences (T-01.1-13). Strip before rendering.
_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(\x07|\x1b\\)")


_DEFAULT_COLUMNS = (
    "slug",
    "question",
    "question_zh",
    "mid_price",
    "best_bid_price",
    "best_ask_price",
    "liquidity_usd",
    "volume_usd",
)


def _safe_str(value: object) -> str:
    """Stringify and strip ANSI escapes — no markup interpretation."""
    if value is None:
        return ""
    s = str(value)
    return _ANSI_RE.sub("", s)


def render_table(
    df: pd.DataFrame, title: str, columns: tuple[str, ...] | None = None
) -> None:
    """Render a DataFrame to terminal as a rich.Table.

    Empty DataFrames print a yellow "(no rows)" line and return.
    Long cells (e.g. question) wrap via overflow="fold".
    Markup is disabled on add_row so user content cannot smuggle [red]…[/red].
    """
    console = Console()
    if df.empty:
        console.print(f"[yellow]{title}: (no rows)[/yellow]")
        return
    cols = list(columns) if columns else [c for c in _DEFAULT_COLUMNS if c in df.columns]
    if not cols:
        cols = list(df.columns)
    table = Table(title=title, show_lines=False)
    for c in cols:
        table.add_column(c, overflow="fold", no_wrap=False)
    for _, row in df.iterrows():
        # Pass each cell through _safe_str + add_row(... , markup=False) so
        # neither ANSI nor rich-markup payloads can render.
        table.add_row(*(_safe_str(row.get(c)) for c in cols))
    console.print(table)


def write_scan_parquet(
    df: pd.DataFrame, recipe_name: str, scans_root: Path
) -> Path | None:
    """Atomic write to ``data/scans/<recipe>/<timestamp>.parquet``.

    Empty DataFrames are skipped — there's nothing to persist, but no error.

    Atomicity: write to .parquet.tmp first, then os.replace into final path.
    On any exception during the parquet write the .tmp file is removed before
    re-raise so the destination directory contains no partial files (analog:
    parquet_writer.write_parquet_atomic).
    """
    if df.empty:
        logger.info(f"scan {recipe_name}: 0 rows, skipping parquet write")
        return None
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    out_path = scans_root / recipe_name / f"{timestamp}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")
    try:
        df.to_parquet(tmp, compression="snappy", index=False)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, out_path)
    logger.info(f"scan {recipe_name}: wrote {len(df)} rows → {out_path}")
    return out_path
