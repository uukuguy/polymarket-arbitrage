"""Compare two snapshots — show drift / appeared / vanished markets.

Resolution: snapshot_id (int) → SELECT parquet_path FROM snapshots → DuckDB
The user gives integer snapshot IDs (from SQLite snapshots.id), never raw
paths (T-01.1-15 — defense against arbitrary file access).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd


def resolve_snapshot_path(snapshot_id: int, db_path: Path) -> Path:
    if not isinstance(snapshot_id, int) or snapshot_id < 1:
        raise ValueError(f"snapshot_id must be positive int, got {snapshot_id!r}")
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        row = con.execute(
            "SELECT parquet_path FROM snapshots WHERE id = ?", (snapshot_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"snapshot_id {snapshot_id} not found")
        return Path(row[0])
    finally:
        con.close()


def latest_snapshot_pair(db_path: Path) -> tuple[int, int]:
    """Return (second-newest, newest) snapshot IDs."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT id FROM snapshots WHERE market_count > 0 ORDER BY id DESC LIMIT 2"
        ).fetchall()
        if len(rows) < 2:
            raise ValueError("need at least 2 snapshots to compare; only "
                             f"{len(rows)} found")
        return (rows[1][0], rows[0][0])  # (older, newer)
    finally:
        con.close()


def compare_snapshots(from_path: Path, to_path: Path) -> pd.DataFrame:
    """Diff two specific snapshots — drift / appeared / vanished.

    Schema-invariant: each CTE reads with SELECT * so DuckDB returns only the
    columns that actually exist in that parquet. We add NULL AS known-missing
    columns so the CTE always has the same shape. The outer SELECT uses COALESCE
    to normalize NULL/empty category values and handle cross-parquet NULLs.
    """
    con = duckdb.connect()
    try:
        return con.execute(
            """
            WITH a AS (
                SELECT
                    *,
                    NULL::VARCHAR AS category,
                    slug AS _slug
                FROM read_parquet(?)
            ),
                 b AS (
                SELECT
                    *,
                    NULL::VARCHAR AS category,
                    slug AS _slug
                FROM read_parquet(?)
            )
            SELECT
                COALESCE(a.slug, b.slug) AS slug,
                COALESCE(a.question, b.question) AS question,
                NULLIF(COALESCE(NULLIF(NULLIF(a.category, ''), 'None'), NULLIF(NULLIF(b.category, ''), 'None')), '') AS category,
                a.mid_price AS mid_from,
                b.mid_price AS mid_to,
                (b.mid_price - a.mid_price) AS mid_drift,
                a.liquidity_usd AS liq_from,
                b.liquidity_usd AS liq_to,
                (b.liquidity_usd - a.liquidity_usd) AS liq_drift,
                CASE
                    WHEN a._slug IS NULL THEN 'appeared'
                    WHEN b._slug IS NULL THEN 'vanished'
                    ELSE 'persistent'
                END AS state
            FROM a FULL OUTER JOIN b USING (_slug)
            ORDER BY
                CASE WHEN a.mid_price IS NULL OR b.mid_price IS NULL THEN 1 ELSE 0 END,
                ABS(COALESCE(b.mid_price, a.mid_price) - COALESCE(a.mid_price, b.mid_price)) DESC
            """,
            [str(from_path), str(to_path)],
        ).fetchdf()
    finally:
        con.close()
