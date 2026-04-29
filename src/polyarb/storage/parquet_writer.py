"""Atomic Parquet writer for snapshot archives.

Atomic write: tmp file in the same directory + os.replace (POSIX + Windows atomic
per docs.python.org/3/library/os.html#os.replace). Failed writes leave only the
.tmp sibling, which the orchestrator removes on rollback.

Path layout (D-C2):  {parquet_root}/YYYY/MM/DD/HH-MM-SS.parquet  (UTC)
Compression: snappy (DuckDB default-friendly, fast write).
Schema: explicit pa.Schema (NOT inferred — see Pitfall 3 for token_id).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from polyarb.storage.schemas import SNAPSHOT_SCHEMA


def compute_snapshot_path(parquet_root: Path, taken_at_ms: int) -> Path:
    """Build the deterministic UTC path for a snapshot taken at the given epoch ms.

    Does NOT mkdir — that's `write_parquet_atomic`'s job (so callers can compute
    paths cheaply without filesystem side effects).
    """
    dt = datetime.fromtimestamp(taken_at_ms / 1000, tz=timezone.utc)
    return (
        Path(parquet_root)
        / dt.strftime("%Y")
        / dt.strftime("%m")
        / dt.strftime("%d")
        / dt.strftime("%H-%M-%S.parquet")
    )


def write_parquet_atomic(rows: list[dict], out_path: Path) -> None:
    """Write rows as a Parquet file at out_path, atomically.

    Caller is responsible for shaping rows to match SNAPSHOT_SCHEMA (no schema
    coercion here — pyarrow will raise on mismatch and we let it propagate).

    On any exception during write_table, the .tmp file is removed before re-raise
    so the destination directory contains no partial files.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        pq.write_table(table, tmp, compression="snappy")
    except Exception:
        # Clean up the partial file before re-raising so callers see no leftovers.
        tmp.unlink(missing_ok=True)
        raise

    os.replace(tmp, out_path)
    logger.info(f"Parquet written: {out_path} ({len(rows)} rows)")
