"""Print read-only SQLite page allocation for production storage diagnosis."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def main() -> int:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "/data/state.db")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        page_count = connection.execute("PRAGMA page_count").fetchone()[0]
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        freelist_count = connection.execute("PRAGMA freelist_count").fetchone()[0]
        print(
            "PRAGMA",
            {
                "page_count": page_count,
                "page_size": page_size,
                "freelist_count": freelist_count,
                "allocated_bytes": page_count * page_size,
                "free_bytes": freelist_count * page_size,
            },
        )
        # Keep this production diagnostic bounded. Counting every table scanned the
        # multi-gigabyte event_tags table and routinely exceeded Fly SSH timeouts.
        existing_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        for table in ("snapshots", "markets", "scheduler_state"):
            if table not in existing_tables:
                continue
            quoted = table.replace('"', '""')
            rows = connection.execute(
                f'SELECT count(*) FROM "{quoted}"'  # noqa: S608 - schema-owned name
            ).fetchone()[0]
            print("ROWS", table, rows, flush=True)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
