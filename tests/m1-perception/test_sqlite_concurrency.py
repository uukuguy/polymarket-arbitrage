"""Chaos: SQLite WAL mode concurrent reader + writer → no crash, eventual consistency.

Scenario: A writer thread does 5 INSERTs into a temp table while a reader thread
does 100 SELECTs concurrently. WAL mode allows readers and writers to proceed
without blocking each other (readers see old snapshot, not blocked by writer).

Expected:
  - No exceptions in either thread
  - Reader sees increasing row count over time (eventual consistency)
  - Final SELECT count == 5 after writer finishes

This mirrors RESEARCH §11 row "SQLite WAL lock contention → reader waits + eventually succeeds".
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_wal_db(db_path: Path) -> None:
    """Initialize a SQLite DB with WAL journal mode + a test table."""
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE IF NOT EXISTS chaos_test (id INTEGER PRIMARY KEY, value TEXT)")
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# Test: concurrent reader + writer, WAL mode
# ---------------------------------------------------------------------------


def test_wal_concurrent_reader_writer_no_crash(tmp_path: Path) -> None:
    """Writer inserts 5 rows; reader SELECTs 100 times concurrently.

    WAL mode must allow both threads to proceed without OperationalError
    ("database is locked") or other SQLite errors.
    """
    db_path = tmp_path / "wal_test.db"
    _setup_wal_db(db_path)

    writer_errors: list[Exception] = []
    reader_errors: list[Exception] = []
    reader_counts: list[int] = []

    def writer_thread() -> None:
        """Insert 5 rows with a small delay between each."""
        try:
            con = sqlite3.connect(str(db_path), timeout=10.0)
            con.execute("PRAGMA journal_mode=WAL")
            for i in range(5):
                con.execute("INSERT INTO chaos_test (value) VALUES (?)", (f"row-{i}",))
                con.commit()
                time.sleep(0.005)  # 5ms between writes
            con.close()
        except Exception as exc:
            writer_errors.append(exc)

    def reader_thread() -> None:
        """SELECT count 100 times with a tiny delay between each."""
        try:
            con = sqlite3.connect(str(db_path), timeout=10.0)
            con.execute("PRAGMA journal_mode=WAL")
            for _ in range(100):
                count = con.execute("SELECT COUNT(*) FROM chaos_test").fetchone()[0]
                reader_counts.append(count)
                time.sleep(0.001)  # 1ms between reads
            con.close()
        except Exception as exc:
            reader_errors.append(exc)

    wt = threading.Thread(target=writer_thread, name="chaos-writer")
    rt = threading.Thread(target=reader_thread, name="chaos-reader")

    # Start both concurrently
    wt.start()
    rt.start()
    wt.join(timeout=10.0)
    rt.join(timeout=10.0)

    # No exceptions in either thread
    assert not writer_errors, f"Writer thread raised: {writer_errors}"
    assert not reader_errors, f"Reader thread raised: {reader_errors}"

    # Reader saw some counts (should have at least a few reads recorded)
    assert len(reader_counts) >= 10, (
        f"Reader should have completed >= 10 reads, got {len(reader_counts)}"
    )

    # Final count in DB should be 5
    con = sqlite3.connect(str(db_path))
    final_count = con.execute("SELECT COUNT(*) FROM chaos_test").fetchone()[0]
    con.close()
    assert final_count == 5, f"Expected 5 rows after writer finishes, got {final_count}"


def test_wal_reader_sees_increasing_count(tmp_path: Path) -> None:
    """Reader counts must be non-decreasing over time (eventual consistency).

    WAL allows readers to see a consistent snapshot. As the writer commits,
    subsequent reader calls MUST see counts >= previous reads.
    In WAL mode a BEGIN transaction sees a snapshot, but each new connection
    (or new SELECT without open txn) should see the latest committed state.
    """
    db_path = tmp_path / "wal_monotonic.db"
    _setup_wal_db(db_path)

    reader_counts: list[int] = []
    reader_errors: list[Exception] = []

    def writer() -> None:
        con = sqlite3.connect(str(db_path), timeout=10.0)
        con.execute("PRAGMA journal_mode=WAL")
        for i in range(5):
            con.execute("INSERT INTO chaos_test (value) VALUES (?)", (f"v{i}",))
            con.commit()
            time.sleep(0.01)
        con.close()

    def reader() -> None:
        try:
            for _ in range(30):
                # Each query opens a fresh connection to see latest committed state
                con = sqlite3.connect(str(db_path), timeout=10.0)
                con.execute("PRAGMA journal_mode=WAL")
                count = con.execute("SELECT COUNT(*) FROM chaos_test").fetchone()[0]
                reader_counts.append(count)
                con.close()
                time.sleep(0.002)
        except Exception as exc:
            reader_errors.append(exc)

    wt = threading.Thread(target=writer)
    rt = threading.Thread(target=reader)
    wt.start()
    rt.start()
    wt.join(timeout=10.0)
    rt.join(timeout=10.0)

    assert not reader_errors, f"Reader raised: {reader_errors}"
    assert reader_counts, "Reader must have recorded some counts"

    # Monotonicity check: counts must be non-decreasing
    for i in range(1, len(reader_counts)):
        assert reader_counts[i] >= reader_counts[i - 1], (
            f"Reader saw count decrease: {reader_counts[i - 1]} → {reader_counts[i]} "
            f"at index {i}. WAL mode should provide monotonic reads."
        )


def test_sqlite_wal_mode_enabled(tmp_path: Path) -> None:
    """Verify that WAL journal mode is actually enabled on the DB file."""
    db_path = tmp_path / "journal_check.db"
    con = sqlite3.connect(str(db_path))
    mode = con.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    con.close()

    assert mode.upper() == "WAL", (
        f"Expected journal_mode=WAL, got {mode!r}. "
        "SQLiteStore must enable WAL on init for concurrent access."
    )


def test_polyarb_store_uses_wal_mode(tmp_path: Path) -> None:
    """SQLiteStore.init_schema must enable WAL journal mode.

    This is the production path — daemon opens the DB via SQLiteStore.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    db_path = tmp_path / "store_wal.db"
    store = SQLiteStore(db_path)
    store.init_schema()

    con = sqlite3.connect(str(db_path))
    mode = con.execute("PRAGMA journal_mode").fetchone()[0]
    con.close()

    # SQLiteStore sets WAL mode for concurrent access (reader/writer coexistence)
    assert mode.upper() == "WAL", (
        f"SQLiteStore must enable WAL journal mode, got {mode!r}. "
        "Check SQLiteStore.init_schema() → PRAGMA journal_mode=WAL."
    )
