"""Unit tests for polyarb.storage.parquet_writer.

Verifies:
- compute_snapshot_path produces YYYY/MM/DD/HH-MM-SS.parquet (UTC)
- write_parquet_atomic creates the file with explicit SNAPSHOT_SCHEMA
- token_id strings round-trip exactly (Pitfall 3 — pa.string() not int64)
- Atomic write: failure leaves no partial file and no .tmp leftovers
- Parent dirs are auto-created
- Snappy compression actually used
"""

from __future__ import annotations

# Belt-and-suspenders for F-3 path validator (see test_sqlite_store.py).
import os

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from polyarb.storage.parquet_writer import (
    compute_snapshot_path,
    write_parquet_atomic,
    write_parquet_streaming,
)


def make_market_row(
    market_id: str,
    *,
    snapshot_taken_at_ms: int = 1_714_435_200_000,
    snapshot_id: int = 1,
    yes_token_id: str = "1" * 70,
    no_token_id: str = "2" * 70,
) -> dict:
    """Build a row matching SNAPSHOT_SCHEMA exactly (parquet uses bool, not int)."""
    return dict(
        market_id=market_id,
        condition_id=f"c-{market_id}",
        slug=None,
        question=None,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        mid_price=0.5,
        liquidity_usd=1000.0,
        volume_usd=100.0,
        best_bid_price=0.49,
        best_bid_size=100.0,
        best_ask_price=0.51,
        best_ask_size=100.0,
        end_time_ms=2_000_000_000_000,
        active=True,
        closed=False,
        neg_risk=False,
        neg_risk_market_id=None,
        fetched_at_ms=1_714_435_200_000,
        snapshot_taken_at_ms=snapshot_taken_at_ms,
        snapshot_id=snapshot_id,
        incomplete=False,
    )


# ---------- 1 + 2. compute_snapshot_path -------------------------------------


def test_compute_snapshot_path_format(tmp_path: Path) -> None:
    """1714435200000 ms = 2024-04-30T00:00:00Z (Polymarket production-friendly date)."""
    p = compute_snapshot_path(tmp_path, 1_714_435_200_000)
    expected = tmp_path / "2024" / "04" / "30" / "00-00-00.parquet"
    assert p == expected, f"Got {p}"
    assert p.suffix == ".parquet"
    # Caller (write_parquet_atomic) is responsible for mkdir — compute should not create dirs.
    assert not p.parent.exists(), "compute_snapshot_path must NOT create directories"


def test_compute_snapshot_path_uses_utc(tmp_path: Path) -> None:
    """A UTC-noon timestamp must produce UTC components regardless of local TZ."""
    # 2024-06-15T12:34:56Z
    dt_utc = datetime(2024, 6, 15, 12, 34, 56, tzinfo=UTC)
    epoch_ms = int(dt_utc.timestamp() * 1000)
    p = compute_snapshot_path(tmp_path, epoch_ms)
    assert p == tmp_path / "2024" / "06" / "15" / "12-34-56.parquet"


# ---------- 3. round-trip ----------------------------------------------------


def test_write_parquet_creates_file(tmp_path: Path) -> None:
    out = tmp_path / "out.parquet"
    rows = [make_market_row("a"), make_market_row("b"), make_market_row("c")]
    write_parquet_atomic(rows, out)
    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 3
    assert set(table.column("market_id").to_pylist()) == {"a", "b", "c"}


# ---------- 4. uint256 token_id (Pitfall 3) ----------------------------------


def test_write_parquet_token_id_preserved_as_string(tmp_path: Path) -> None:
    """A 70-char numeric token_id must survive the round-trip as the exact string.

    If the schema were inferred (or used pa.int64), this string would become
    scientific-notation float or overflow silently.
    """
    big = "1" * 70
    out = tmp_path / "x.parquet"
    write_parquet_atomic([make_market_row("a", yes_token_id=big)], out)
    rows_back = pq.read_table(out).to_pylist()
    assert rows_back[0]["yes_token_id"] == big
    assert isinstance(rows_back[0]["yes_token_id"], str)


# ---------- 5. atomic on failure ---------------------------------------------


def test_write_parquet_atomic_no_partial_file_on_failure(tmp_path: Path) -> None:
    """Schema mismatch (int where string expected) raises and leaves no leftovers."""
    bad_rows = [
        # condition_id MUST be a string per SNAPSHOT_SCHEMA — 12345 is int.
        dict(make_market_row("a"), condition_id=12345),
    ]
    out = tmp_path / "broken.parquet"
    with pytest.raises(Exception):
        write_parquet_atomic(bad_rows, out)
    assert not out.exists(), "Final file must not exist on failure"
    # No .tmp leftovers in the parent dir either.
    leftovers = [p for p in out.parent.iterdir() if p.suffix == ".tmp"]
    assert leftovers == [], f"Stale .tmp files: {leftovers}"


# ---------- 6. parent dirs ----------------------------------------------------


def test_write_parquet_creates_parent_dirs(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b" / "c" / "file.parquet"
    assert not (tmp_path / "a").exists()
    write_parquet_atomic([make_market_row("a")], out)
    assert out.exists()
    assert (tmp_path / "a" / "b" / "c").is_dir()


# ---------- 7. snappy compression --------------------------------------------


def test_write_parquet_uses_snappy_compression(tmp_path: Path) -> None:
    out = tmp_path / "snap.parquet"
    write_parquet_atomic([make_market_row("a")], out)
    metadata = pq.ParquetFile(out).metadata
    compression = metadata.row_group(0).column(0).compression
    assert compression == "SNAPPY", f"Expected SNAPPY, got {compression}"


# ---------- Plan 02-09: streaming writer ------------------------------------


def test_write_parquet_streaming_basic(tmp_path: Path) -> None:
    """100 rows / batch_size=30 — file exists, row count matches, schema matches."""
    out = tmp_path / "stream.parquet"
    rows = [make_market_row(f"m{i}") for i in range(100)]
    total = write_parquet_streaming(iter(rows), out, batch_size=30)
    assert total == 100
    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 100
    # Schema must match SNAPSHOT_SCHEMA (field names + order).
    from polyarb.storage.schemas import SNAPSHOT_SCHEMA

    assert table.schema.names == SNAPSHOT_SCHEMA.names
    # Row content survives — pick a sentinel field.
    assert sorted(table.column("market_id").to_pylist()) == sorted(f"m{i}" for i in range(100))


def test_write_parquet_streaming_byte_equivalent_to_atomic(tmp_path: Path) -> None:
    """Streaming and atomic paths produce identical row content for identical input.

    Row-group layout (and thus file bytes) may differ — we compare via
    pq.read_table(...).to_pylist() which is the durable contract callers see.
    """
    rows = [make_market_row(f"m{i}") for i in range(100)]
    p_atomic = tmp_path / "atomic.parquet"
    p_stream = tmp_path / "stream.parquet"

    write_parquet_atomic(rows, p_atomic)
    # Use a fresh generator so we exercise the streaming-single-pass contract.
    write_parquet_streaming((r for r in rows), p_stream, batch_size=37)

    rows_atomic = pq.read_table(p_atomic).to_pylist()
    rows_stream = pq.read_table(p_stream).to_pylist()
    assert rows_atomic == rows_stream


def test_write_parquet_streaming_atomic_on_error(tmp_path: Path) -> None:
    """Generator yields 250 valid rows then raises — no .parquet, no .tmp leftover."""
    out = tmp_path / "broken.parquet"

    class BoomError(RuntimeError):
        pass

    def _explode_after_250():
        for i in range(250):
            yield make_market_row(f"m{i}")
        raise BoomError("synthetic mid-stream failure")

    with pytest.raises(BoomError):
        write_parquet_streaming(_explode_after_250(), out, batch_size=100)

    # No final file at the destination
    assert not out.exists(), "Final file must not exist after error"
    # No .tmp leftover in the parent dir
    leftovers = [p for p in out.parent.iterdir() if p.suffix == ".tmp"]
    assert leftovers == [], f"Stale .tmp files: {leftovers}"


def test_write_parquet_streaming_empty(tmp_path: Path) -> None:
    """Empty iterator → valid empty parquet file with SNAPSHOT_SCHEMA."""
    out = tmp_path / "empty.parquet"
    total = write_parquet_streaming(iter([]), out, batch_size=500)
    assert total == 0
    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 0
    from polyarb.storage.schemas import SNAPSHOT_SCHEMA

    # Schema names must still match — a writer that punted the empty case would
    # likely produce a schemaless or 0-column file.
    assert table.schema.names == SNAPSHOT_SCHEMA.names
