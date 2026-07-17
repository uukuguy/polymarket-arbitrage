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
        for row in connection.execute(
            "SELECT name, sum(pgsize) AS bytes, count(*) AS pages "
            "FROM dbstat GROUP BY name ORDER BY bytes DESC"
        ):
            print(row)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
