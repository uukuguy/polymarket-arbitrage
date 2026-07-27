#!/usr/bin/env python3
"""Print the newest locally persisted L1 scheduler snapshot attempt as JSON.

Read-only diagnostic for the volume-local SQLite database.  It deliberately
does not initialize schema or contact Fly: no history is a valid ``null``
result rather than a reason to create state while investigating an incident.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from polyarb.config import load_settings
from polyarb.storage.sqlite_store import SQLiteStore


def main() -> int:
    # This command never opens a control endpoint or starts a daemon.  Permit
    # a local read even when the unrelated write/control HMAC is unavailable.
    os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
    settings = load_settings()
    # The daemon normally takes its path from snapshot.yaml.  Preserve that
    # default, while allowing an explicit operator/test environment override
    # to select a different read-only database without editing the YAML file.
    configured_db_path = os.environ.get("POLYARB_DB_PATH")
    db_path = Path(configured_db_path).resolve() if configured_db_path else settings.db_path
    latest = SQLiteStore(db_path).get_latest_snapshot_attempt()
    print(json.dumps({"latest": latest}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
