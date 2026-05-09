"""Track a single market across all snapshot parquet files via DuckDB.

Strategy (RESEARCH §3.1-3.6):
- in-memory duckdb (no spill needed at our scale ~ 100 parquet × 20k rows)
- glob across data/snapshots/**/*.parquet (POSIX-style, duckdb-native)
- union_by_name=true future-proofs against schema additions (e.g. Phase
  1.1 adds category/tags; older parquet auto-NULL filled)
- WHERE on slug pushes down via parquet zonemaps (RESEARCH §3.1)

Pitfalls (RESEARCH §3.6):
- fetchdf() materializes to pandas — fine at 20k rows, switch to .arrow()
  if scaling to 1M+
- union_by_name only aligns names not types; if a future phase changes a
  column's type, must rewrite history (RESEARCH §3.5 invariant)

Output column note (Warning #7 决议):
- taken_at_ms: snapshot_taken_at_ms (秒级；同秒可能撞)
- snapshot_id: tie-breaker for same-second snapshots (rare but real
  when re-running snapshot in close succession; ORDER BY (taken_at_ms,
  snapshot_id) is stable)
"""
from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import pandas as pd

_PARQUET_COUNT_WARN_THRESHOLD = 200  # RESEARCH A4


def track_market(slug: str, parquet_root: Path) -> pd.DataFrame:
    glob = str(parquet_root / "**" / "*.parquet")
    parquet_files = list(parquet_root.rglob("*.parquet"))
    if len(parquet_files) > _PARQUET_COUNT_WARN_THRESHOLD:
        warnings.warn(
            f"track_market: scanning {len(parquet_files)} parquet files "
            f"(>{_PARQUET_COUNT_WARN_THRESHOLD}); in-memory duckdb may slow"
        )
    con = duckdb.connect()  # in-memory
    try:
        return con.execute(
            """
            SELECT
                snapshot_taken_at_ms AS taken_at_ms,
                snapshot_id,
                slug,
                question,
                category,
                mid_price,
                best_bid_price,
                best_ask_price,
                (best_ask_price - best_bid_price) AS spread,
                liquidity_usd,
                volume_usd
            FROM read_parquet(?, union_by_name=true)
            WHERE slug = ?
            ORDER BY taken_at_ms, snapshot_id
            """,
            [glob, slug],  # 参数化（防 T-01.1-16）
        ).fetchdf()
    finally:
        con.close()
