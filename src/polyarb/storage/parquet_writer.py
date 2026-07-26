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
from collections.abc import Iterable
from datetime import UTC, datetime
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
    dt = datetime.fromtimestamp(taken_at_ms / 1000, tz=UTC)
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


# TODO(02-09 follow-up): once orchestrator and all callers migrate to
# write_parquet_streaming, remove write_parquet_atomic. Kept for backward compat
# with tests and one-off scripts that pass a fully-materialized list.
def write_parquet_streaming(
    rows: Iterable[dict],
    out_path: Path,
    *,
    batch_size: int = 500,
) -> int:
    """Write rows to a Parquet file at out_path, in chunks, atomically.

    Uses pyarrow.parquet.ParquetWriter under the hood — multiple write_table
    calls into one open file. Atomic via tmp file + os.replace, identical to
    write_parquet_atomic.

    Args:
        rows: Iterable of dicts shaped to match SNAPSHOT_SCHEMA. Consumed exactly
            once (single-pass — pass a list if you need to re-iterate).
        out_path: Final parquet location. Parent dirs auto-created.
        batch_size: Number of rows to buffer before flushing as one RecordBatch.
            500 is the production default (tradeoff: fewer = more RAM-friendly,
            more = better Parquet row-group density).

    Returns:
        Total row count written.

    Raises:
        Any pyarrow exception during write_table (schema mismatch, etc.). On
        failure the tmp file is removed and the destination is untouched.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")

    # Plan 02-09: pyarrow ParquetWriter IS a context manager (verified at
    # .venv/lib/.../pyarrow/parquet/core.py:1049-1052 — defines __enter__/__exit__).
    # Wrap the `with` in try/except so the atomic-write contract (delete .tmp on
    # failure, os.replace on success) is honored on both normal and exception path.
    total = 0
    batch: list[dict] = []
    try:
        with pq.ParquetWriter(tmp, SNAPSHOT_SCHEMA, compression="snappy") as writer:
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    table = pa.Table.from_pylist(batch, schema=SNAPSHOT_SCHEMA)
                    writer.write_table(table)
                    total += len(batch)
                    batch.clear()  # release row references so dicts become GC-eligible
            if batch:
                table = pa.Table.from_pylist(batch, schema=SNAPSHOT_SCHEMA)
                writer.write_table(table)
                total += len(batch)
                batch.clear()
        # `with` block exited normally → writer.close() ran → file finalized.
    except Exception:
        # Drop any partial .tmp before re-raising so the destination dir is clean.
        tmp.unlink(missing_ok=True)
        raise

    os.replace(tmp, out_path)
    logger.info(f"Parquet streaming written: {out_path} ({total} rows, batch_size={batch_size})")
    return total
