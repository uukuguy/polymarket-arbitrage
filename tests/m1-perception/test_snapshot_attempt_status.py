"""Black-box contract tests for the local snapshot-attempt diagnostic."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
SCRIPT = PROJECT_ROOT / "scripts" / "snapshot_attempt_status.py"


def _run(db_path: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["POLYARB_DB_PATH"] = str(db_path)
    environment["POLYARB_ALLOW_EXTERNAL_PATHS"] = "1"
    return subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=PROJECT_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def test_snapshot_attempt_status_reports_latest_failed_attempt(tmp_path: Path) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="failed",
        finished_at_ms=2_000,
        snapshot_id=None,
        failure_kind="snapshot-subprocess-signal-sigkill-possible-oom",
        last_stage="gamma-markets",
        elapsed_ms=245_012,
    )

    result = _run(db_path)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "latest": {
            "failure_kind": "snapshot-subprocess-signal-sigkill-possible-oom",
            "elapsed_ms": 245_012,
            "chunks_processed": None,
            "finished_at_ms": 2_000,
            "id": 1,
            "last_stage": "gamma-markets",
            "outcome": "failed",
            "snapshot_id": None,
            "stderr_bytes": None,
            "stderr_sha256": None,
            "stderr_tail": None,
            "started_at_ms": 1_000,
        }
    }


def test_snapshot_attempt_status_is_successful_when_history_is_absent(tmp_path: Path) -> None:
    result = _run(tmp_path / "absent.db")

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"latest": None}
