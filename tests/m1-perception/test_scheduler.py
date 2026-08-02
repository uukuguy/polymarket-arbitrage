"""Tests for SnapshotScheduler N-failure-pause state machine.

Covers D-13 / T-02-04 (Phase 02 baseline) + D-02 (Phase 03.1-04 raises
FAILURE_THRESHOLD 3 → 5 to absorb DNS jitter via tenacity retry):

- scheduler.state == PAUSED after FAILURE_THRESHOLD consecutive FAILED results
- DEGRADED is NOT failure (D-12 amendment); N×DEGRADED must NOT pause
- Paused scheduler skips tick (no run_snapshot called)
- Failure counter resets on OK/DEGRADED success
- Counter persists across restart (restored from DB)
- 4 failures keep RUNNING; the 5th transitions to PAUSED (explicit guard)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polyarb.daemon.scheduler import SchedulerState, SnapshotScheduler
from polyarb.validator.category import SnapshotStatus

# ---------------------------------------------------------------------------
# scheduler_interval_s configurability (Inj 2 P0 fix, 2026-05-20)
# ---------------------------------------------------------------------------


def test_scheduler_interval_default_3600() -> None:
    """Default Settings.scheduler_interval_s == 3600 (preserves Plan 02 behavior)."""
    from polyarb.config import Settings

    s = Settings(_env_file=None)
    assert s.scheduler_interval_s == 3600


def test_scheduler_interval_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """POLYARB_SCHEDULER_INTERVAL_S env var overrides default.

    Inj 2 P0 fix: pre-2026-05-20 the value was read via getattr fallback on a
    field that Settings did not declare, so the env var was silently ignored
    and prod was stuck at 3600s (1h) — chaos injection could not verify
    the 3-failure-pause path within a reasonable window.
    """
    from polyarb.config import Settings

    monkeypatch.setenv("POLYARB_SCHEDULER_INTERVAL_S", "60")
    s = Settings(_env_file=None)
    assert s.scheduler_interval_s == 60


def test_structure_sync_enablement_reads_production_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.config import Settings

    monkeypatch.setenv("POLYARB_STRUCTURE_SYNC_ENABLED", "true")
    settings = Settings(_env_file=None)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=MagicMock())

    assert scheduler.structure_sync_enabled is True


def test_structure_drift_scheduler_defaults_are_bounded_and_off() -> None:
    from polyarb.config import Settings

    settings = Settings(_env_file=None)
    assert settings.structure_generation_drift_compare_enabled is False
    assert settings.structure_generation_drift_max_rows == 500
    assert settings.structure_generation_drift_max_chunks_per_tick == 100
    assert settings.structure_generation_drift_slice_s == 45.0


def test_structure_drift_scheduler_enablement_reads_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.config import Settings

    monkeypatch.setenv("POLYARB_STRUCTURE_GENERATION_DRIFT_COMPARE_ENABLED", "true")
    assert Settings(_env_file=None).structure_generation_drift_compare_enabled is True


def test_structure_drift_attempt_lifecycle_recovery_and_retention(
    tmp_path: Path,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "attempts.db")
    store.init_schema()
    identity = {
        "legacy_snapshot_id": 845,
        "generation_snapshot_id": 848,
        "publication_id": "publication-848",
        "window_id": "window-97b",
    }
    orphan_id = store.begin_structure_drift_attempt(
        identity=identity,
        progress_id="progress-orphan",
        started_at_ms=1_000,
    )
    assert store.recover_orphaned_structure_drift_attempts(recovered_at_ms=2_000) == 1
    assert (
        store.get_latest_structure_drift_attempt()["failure_kind"]
        == "parent-restarted-orphan"
    )

    attempt_id = store.begin_structure_drift_attempt(
        identity=identity,
        progress_id="progress-current",
        started_at_ms=3_000,
    )
    store.finish_structure_drift_attempt(
        attempt_id=attempt_id,
        outcome="checkpointed",
        finished_at_ms=3_100,
        last_phase="legacy-members",
        chunks_processed=2,
        rows_processed=1_000,
        elapsed_ms=100,
        failure_kind=None,
        stderr=b"structure-drift stage=legacy-members chunks=2 rows=1000",
    )
    latest = store.get_latest_structure_drift_attempt()
    assert latest is not None
    assert latest["id"] == attempt_id
    assert latest["identity"] == identity
    assert latest["progress_id"] == "progress-current"
    assert latest["outcome"] == "checkpointed"
    assert latest["stderr_bytes"] == 55
    assert (
        latest["stderr_sha256"]
        == hashlib.sha256(
            b"structure-drift stage=legacy-members chunks=2 rows=1000"
        ).hexdigest()
    )
    assert latest["stderr_safe_marker"] == (
        "structure-drift stage=legacy-members chunks=2 rows=1000"
    )
    with pytest.raises(ValueError, match="already-terminal"):
        store.finish_structure_drift_attempt(
            attempt_id=attempt_id,
            outcome="failed",
            finished_at_ms=3_200,
            last_phase=None,
            chunks_processed=0,
            rows_processed=0,
            elapsed_ms=200,
            failure_kind="invalid-json",
            stderr=b"unsafe secret",
        )
    assert orphan_id != attempt_id
    for index in range(105):
        retained_id = store.begin_structure_drift_attempt(
            identity=identity,
            progress_id=None,
            started_at_ms=4_000 + index,
        )
        store.finish_structure_drift_attempt(
            attempt_id=retained_id,
            outcome="checkpointed",
            finished_at_ms=5_000 + index,
            last_phase="source-events",
            chunks_processed=1,
            rows_processed=1,
            elapsed_ms=1,
            failure_kind=None,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts"
        ).fetchone() == (100,)


def test_structure_drift_attempt_schema_migrates_legacy_database(
    tmp_path: Path,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as con:
        con.execute(
            "CREATE TABLE structure_drift_attempts("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,started_at_ms INTEGER NOT NULL,"
            "outcome TEXT NOT NULL)"
        )
        con.execute(
            "INSERT INTO structure_drift_attempts(started_at_ms,outcome) VALUES(1,'running')"
        )
    store = SQLiteStore(db_path)
    store.init_schema()
    columns = {
        row[1]
        for row in sqlite3.connect(db_path).execute(
            "PRAGMA table_info(structure_drift_attempts)"
        )
    }
    assert {"identity_json", "progress_id", "stderr_safe_marker"} <= columns
    assert store.recover_orphaned_structure_drift_attempts(recovered_at_ms=10) == 1


def test_structure_drift_attempt_rejects_fresh_owner_and_recovers_stale(
    tmp_path: Path,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "owner.db")
    store.init_schema()
    first = store.begin_structure_drift_attempt(
        identity={"generation_snapshot_id": 848}, progress_id=None, started_at_ms=1_000
    )
    with pytest.raises(ValueError, match="owner-running"):
        store.begin_structure_drift_attempt(
            identity={"generation_snapshot_id": 848},
            progress_id=None,
            started_at_ms=1_050,
            stale_before_ms=900,
        )
    second = store.begin_structure_drift_attempt(
        identity={"generation_snapshot_id": 848},
        progress_id=None,
        started_at_ms=2_000,
        stale_before_ms=1_500,
    )
    assert second != first


@pytest.mark.parametrize(
    "marker",
    ("raw secret", "structure-drift stage=none chunks=0 rows=0秘密"),
)
def test_structure_drift_attempt_rejects_untrusted_explicit_marker(
    tmp_path: Path, marker: str,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "marker.db")
    store.init_schema()
    attempt_id = store.begin_structure_drift_attempt(
        identity={}, progress_id=None, started_at_ms=1
    )
    with pytest.raises(ValueError, match="stderr"):
        store.finish_structure_drift_attempt(
            attempt_id=attempt_id,
            outcome="failed",
            finished_at_ms=2,
            last_phase=None,
            chunks_processed=0,
            rows_processed=0,
            elapsed_ms=1,
            failure_kind="invalid-json",
            stderr_safe_marker=marker,
        )


def test_recovery_retry_delay_is_exponential_and_bounded() -> None:
    from polyarb.daemon.scheduler import recovery_retry_delay_s

    assert [recovery_retry_delay_s(counter) for counter in (1, 2, 3, 4, 5, 20)] == [
        5.0,
        10.0,
        20.0,
        40.0,
        60.0,
        60.0,
    ]


# ---------------------------------------------------------------------------
# Helper result types
# ---------------------------------------------------------------------------


class _FakeResult:
    """Minimal snapshot result stub."""

    def __init__(
        self,
        status: SnapshotStatus,
        *,
        snapshot_id: int = 1,
        last_stage: str | None = None,
        elapsed_ms: int | None = None,
    ) -> None:
        self.status = status
        self.snapshot_id = snapshot_id
        self.last_stage = last_stage
        self.elapsed_ms = elapsed_ms


class _FakeProcess:
    def __init__(
        self,
        payload: object,
        *,
        returncode: int,
        block: bool = False,
        stderr: bytes = b"bounded stderr",
    ) -> None:
        self.stdout = (
            payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        )
        self.returncode = returncode
        self.block = block
        self.stderr = stderr
        self.terminated = False
        self.killed = False
        self._reaped = asyncio.Event()

    async def communicate(self):
        if self.block and not self.killed:
            await self._reaped.wait()
        return self.stdout, self.stderr

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True
        self._reaped.set()


def _seed_snapshot(store: Any, snapshot_id: int) -> None:
    """Model the child-owned snapshot row required by a successful attempt."""
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "is_valid,parquet_path) VALUES (?,?,?,'subset',0,1,'fixture.parquet')",
            (snapshot_id, 1, 2),
        )


def test_snapshot_attempt_lifecycle_is_append_only(
    daemon_settings_for_test: Any,
) -> None:
    """A terminal scheduler attempt keeps its original OOM classification."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="failed",
        finished_at_ms=2_000,
        snapshot_id=None,
        failure_kind="snapshot-subprocess-signal-sigkill-possible-oom",
        chunks_processed=9,
    )

    assert store.get_latest_snapshot_attempt() == {
        "id": attempt_id,
        "started_at_ms": 1_000,
        "finished_at_ms": 2_000,
        "outcome": "failed",
        "snapshot_id": None,
        "failure_kind": "snapshot-subprocess-signal-sigkill-possible-oom",
        "last_stage": None,
        "elapsed_ms": None,
        "chunks_processed": 9,
        "stderr_bytes": None,
        "stderr_sha256": None,
        "stderr_tail": None,
    }


def test_snapshot_attempt_diagnostic_columns_migrate_legacy_rows(
    tmp_path: Path,
) -> None:
    """Fresh and pre-diagnostic attempt tables expose nullable diagnostic columns."""
    from polyarb.storage.sqlite_store import SQLiteStore

    fresh_db = tmp_path / "fresh.db"
    SQLiteStore(fresh_db).init_schema()
    with sqlite3.connect(fresh_db) as con:
        fresh_columns = {
            row[1] for row in con.execute("PRAGMA table_info(snapshot_attempts)")
        }
    diagnostic_columns = {
        "elapsed_ms",
        "last_stage",
        "chunks_processed",
        "stderr_bytes",
        "stderr_sha256",
        "stderr_tail",
    }
    assert diagnostic_columns <= fresh_columns

    legacy_db = tmp_path / "legacy.db"
    with sqlite3.connect(legacy_db) as con:
        con.execute(
            "CREATE TABLE snapshot_attempts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, started_at_ms INTEGER NOT NULL, "
            "finished_at_ms INTEGER, outcome TEXT NOT NULL, snapshot_id INTEGER, "
            "failure_kind TEXT)"
        )
        con.execute(
            "INSERT INTO snapshot_attempts(started_at_ms, outcome) VALUES (?, ?)",
            (1_000, "running"),
        )

    legacy_store = SQLiteStore(legacy_db)
    legacy_store.init_schema()
    legacy_store.init_schema()

    with sqlite3.connect(legacy_db) as con:
        legacy_columns = {
            row[1] for row in con.execute("PRAGMA table_info(snapshot_attempts)")
        }
        historical_diagnostics = con.execute(
            "SELECT last_stage,elapsed_ms,chunks_processed,stderr_bytes,stderr_sha256,"
            "stderr_tail "
            "FROM snapshot_attempts WHERE id = 1"
        ).fetchone()
    assert diagnostic_columns <= legacy_columns
    assert historical_diagnostics == (None, None, None, None, None, None)
    legacy_store.finish_snapshot_attempt(
        attempt_id=1,
        outcome="failed",
        finished_at_ms=2_000,
        snapshot_id=None,
        failure_kind="snapshot-subprocess-structure-child-error",
        stderr_bytes=350,
        stderr_sha256="0" * 64,
        stderr_tail=None,
    )
    assert legacy_store.get_latest_snapshot_attempt()["outcome"] == "failed"


def test_snapshot_attempt_terminal_row_cannot_be_rewritten(
    daemon_settings_for_test: Any,
) -> None:
    """A later writer cannot turn recorded success into a fabricated failure."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_snapshot(store, 746)

    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="succeeded",
        finished_at_ms=2_000,
        snapshot_id=746,
        failure_kind=None,
    )

    with pytest.raises(ValueError, match="not running"):
        store.finish_snapshot_attempt(
            attempt_id=attempt_id,
            outcome="failed",
            finished_at_ms=3_000,
            snapshot_id=None,
            failure_kind="late-rewrite",
        )


@pytest.mark.parametrize(
    "diagnostics",
    (
        {"stderr_bytes": True, "stderr_sha256": "0" * 64},
        {"stderr_bytes": -1, "stderr_sha256": "0" * 64},
        {"stderr_bytes": 100_000_001, "stderr_sha256": "0" * 64},
        {"stderr_bytes": 1, "stderr_sha256": "G" * 64},
        {"stderr_bytes": 1, "stderr_sha256": "0" * 64, "stderr_tail": "secret"},
        {
            "stderr_bytes": 128,
            "stderr_sha256": "0" * 64,
            "stderr_tail": (
                "structure-sync-failure failure_kind=sqlite-busy "
                "membership_kind=group-truth key_sha256=" + "a" * 64
            ),
        },
        {
            "stderr_bytes": 1,
            "stderr_sha256": "0" * 64,
            "stderr_tail": "snapshot-stage stage=persist state=start elapsed_ms="
            + "1" * 300,
        },
        {"stderr_bytes": 1},
    ),
)
def test_snapshot_attempt_rejects_untrusted_stderr_diagnostics(
    daemon_settings_for_test: Any,
    diagnostics: dict[str, object],
) -> None:
    """The final SQLite boundary cannot be bypassed with secret-bearing text."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)

    with pytest.raises(ValueError, match="snapshot attempt stderr"):
        store.finish_snapshot_attempt(
            attempt_id=attempt_id,
            outcome="failed",
            finished_at_ms=2_000,
            snapshot_id=None,
            failure_kind="test-failure",
            **diagnostics,
        )


@pytest.mark.asyncio
async def test_scheduler_persists_sigkill_attempt_failure(
    daemon_settings_for_test: Any,
) -> None:
    """A kernel-killed child leaves durable operational evidence behind."""
    from polyarb.daemon.scheduler import SnapshotSubprocessError
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    child_stderr = (
        b"sensitive arbitrary output\n"
        b"structure-publication-progress stage=certifying "
        b"component=memberships chunks=7 rows=3500\n"
    )
    scheduler._run_snapshot = AsyncMock(
        side_effect=SnapshotSubprocessError(
            "timeout",
            last_stage="persist",
            elapsed_ms=245_012,
            chunks_processed=7,
            stderr=child_stderr,
        )
    )

    await scheduler._tick()

    assert store.get_latest_snapshot_attempt() == {
        "id": 1,
        "started_at_ms": pytest.approx(int(time.time() * 1000), abs=2_000),
        "finished_at_ms": pytest.approx(int(time.time() * 1000), abs=2_000),
        "outcome": "failed",
        "snapshot_id": None,
        "failure_kind": "snapshot-subprocess-timeout",
        "last_stage": "persist",
        "elapsed_ms": 245_012,
        "chunks_processed": 7,
        "stderr_bytes": len(child_stderr),
        "stderr_sha256": hashlib.sha256(child_stderr).hexdigest(),
        "stderr_tail": (
            "structure-publication-progress stage=certifying "
            "component=memberships chunks=7 rows=3500"
        ),
    }


@pytest.mark.asyncio
async def test_scheduler_discards_oversized_tail_but_finishes_attempt(
    daemon_settings_for_test: Any,
) -> None:
    from polyarb.daemon.scheduler import SnapshotSubprocessError
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    child_stderr = b"snapshot-stage stage=persist state=start elapsed_ms=" + b"1" * 300
    scheduler._run_snapshot = AsyncMock(
        side_effect=SnapshotSubprocessError(
            "structure-child-error",
            stderr=child_stderr,
        )
    )

    await scheduler._tick()

    attempt = store.get_latest_snapshot_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "failed"
    assert attempt["stderr_bytes"] == len(child_stderr)
    assert attempt["stderr_sha256"] == hashlib.sha256(child_stderr).hexdigest()
    assert attempt["stderr_tail"] is None


@pytest.mark.asyncio
async def test_scheduler_persists_successful_attempt_diagnostics(
    daemon_settings_for_test: Any,
) -> None:
    """A terminal successful row retains parent-observed diagnostics."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_snapshot(store, 1)
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(
            SnapshotStatus.OK,
            last_stage="persist",
            elapsed_ms=1_234,
        )
    )

    await scheduler._tick()

    latest_attempt = store.get_latest_snapshot_attempt()
    assert latest_attempt is not None
    assert latest_attempt["outcome"] == "succeeded"
    assert latest_attempt["last_stage"] == "persist"
    assert latest_attempt["elapsed_ms"] == 1_234


@pytest.mark.asyncio
async def test_scheduler_preserves_result_diagnostics_when_result_is_rejected(
    daemon_settings_for_test: Any,
) -> None:
    """A parent validation error retains the child facts it was handed."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(
            SnapshotStatus.OK,
            snapshot_id=0,
            last_stage="persist",
            elapsed_ms=1_234,
        )
    )

    await scheduler._tick()

    latest_attempt = store.get_latest_snapshot_attempt()
    assert latest_attempt is not None
    assert latest_attempt["failure_kind"] == "snapshot-subprocess-missing-snapshot-id"
    assert latest_attempt["last_stage"] == "persist"
    assert latest_attempt["elapsed_ms"] == 1_234


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "is_valid", "returncode"),
    (
        ("ok", True, 0),
        ("degraded", True, 0),
        ("failed", False, 1),
    ),
)
async def test_snapshot_pipeline_runs_in_isolated_subprocess(
    status: str,
    is_valid: bool,
    returncode: int,
) -> None:
    from polyarb.daemon.scheduler import run_snapshot_in_subprocess

    process = _FakeProcess(
        {
            "status": status,
            "is_valid": is_valid,
            "snapshot_id": 746,
            "market_count": 81959,
            "issue_count": 3,
        },
        returncode=returncode,
    )
    calls = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert result.status == SnapshotStatus(status)
    assert result.snapshot_id == 746
    assert result.market_count == 81959
    assert result.issue_count == 3
    args, kwargs = calls[0]
    assert args[1:] == (
        "-m",
        "polyarb.snapshot",
        "structure-sync",
        "--json",
        "--low-priority",
        "--max-pages",
        "40",
        "--max-elapsed-seconds",
        "45.0",
    )
    assert kwargs["stdout"] == asyncio.subprocess.PIPE
    assert kwargs["stderr"] == asyncio.subprocess.PIPE


@pytest.mark.asyncio
async def test_structure_drift_child_parser_accepts_bounded_slice() -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    process = _FakeProcess(
        {
            "checkpointed": True,
            "chunks_processed": 100,
            "defer_reason": None,
            "deferred": False,
            "elapsed_ms": 44_999,
            "kind": "structure-drift",
            "phase": "legacy-members",
            "ready": False,
            "rows_processed": 50_000,
            "stop_reason": "max-chunks",
        },
        returncode=0,
    )
    calls = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return process

    result = await run_structure_drift_in_subprocess(
        db_path="/tmp/drift.db",
        max_rows=500,
        max_chunks=100,
        max_elapsed_s=45.0,
        spawn=spawn,
    )

    assert result.rows_processed == 50_000
    assert result.chunks_processed == 100
    assert calls[0][0][1:] == (
        "-m",
        "polyarb.snapshot",
        "structure-generation-drift-advance",
        "--db-path",
        "/tmp/drift.db",
        "--max-rows",
        "500",
        "--max-chunks",
        "100",
        "--max-elapsed-seconds",
        "45.0",
    )


@pytest.mark.asyncio
async def test_structure_drift_child_cancel_terminates_then_kills() -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    process = _FakeProcess({}, returncode=0, block=True)

    async def spawn(*_args, **_kwargs):
        return process

    task = asyncio.create_task(
        run_structure_drift_in_subprocess(
            db_path="/tmp/drift.db",
            max_rows=500,
            max_chunks=100,
            max_elapsed_s=45.0,
            spawn=spawn,
            terminate_timeout_s=0.001,
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.terminated is True
    assert process.killed is True


@pytest.mark.asyncio
async def test_structure_drift_child_sigkill_is_possible_oom() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_structure_drift_in_subprocess,
    )

    process = _FakeProcess({}, returncode=-signal.SIGKILL)

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(
        SnapshotSubprocessError,
        match="structure-drift-signal-sigkill-possible-oom",
    ):
        await run_structure_drift_in_subprocess(
            db_path="/tmp/drift.db",
            max_rows=500,
            max_chunks=100,
            max_elapsed_s=45.0,
            spawn=spawn,
        )


@pytest.mark.asyncio
async def test_structure_drift_child_timeout_reaps_before_failure() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_structure_drift_in_subprocess,
    )

    process = _FakeProcess({}, returncode=0, block=True)

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="structure-drift-timeout"):
        await run_structure_drift_in_subprocess(
            db_path="/tmp/drift.db",
            max_rows=500,
            max_chunks=100,
            max_elapsed_s=45.0,
            spawn=spawn,
            timeout_s=0.001,
            terminate_timeout_s=0.001,
        )
    assert process.terminated is True
    assert process.killed is True


@pytest.mark.asyncio
async def test_structure_drift_child_invalid_json_is_bounded_failure() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_structure_drift_in_subprocess,
    )

    process = _FakeProcess(b"not-json", returncode=1, stderr=b"unsafe secret")

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="structure-drift-invalid-json"):
        await run_structure_drift_in_subprocess(
            db_path="/tmp/drift.db",
            max_rows=500,
            max_chunks=100,
            max_elapsed_s=45.0,
            spawn=spawn,
        )


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_cooperative_checkpoint() -> None:
    from polyarb.daemon.scheduler import (
        IsolatedStructureCheckpoint,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {
            "checkpointed": True,
            "window_id": "window-1",
            "stage": "markets",
            "pages_processed": 80,
        },
        returncode=0,
    )

    async def spawn(*_args, **_kwargs):
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert isinstance(result, IsolatedStructureCheckpoint)
    assert result.window_id == "window-1"
    assert result.stage == "markets"
    assert result.pages_processed == 80


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_publication_checkpoint() -> None:
    """A durable publication chunk must not be reclassified as child failure."""
    from polyarb.daemon.scheduler import (
        IsolatedStructurePublicationCheckpoint,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {
            "checkpointed": True,
            "stage": "normalizing",
            "component": "events",
            "rows_processed": 500,
            "cursor": "events|event-500",
            "publication_id": "publication-1",
            "chunks_processed": 11,
            "elapsed_ms": 44_000,
        },
        returncode=0,
    )

    async def spawn(*_args, **_kwargs):
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert isinstance(result, IsolatedStructurePublicationCheckpoint)
    assert result.stage == "normalizing"
    assert result.component == "events"
    assert result.rows_processed == 500
    assert result.cursor == "events|event-500"
    assert result.publication_id == "publication-1"
    assert result.chunks_processed == 11


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_exact_contract_supersession_checkpoint() -> (
    None
):
    from polyarb.daemon.scheduler import (
        IsolatedStructurePublicationCheckpoint,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {
            "checkpointed": True,
            "stage": "superseded",
            "component": None,
            "rows_processed": 0,
            "cursor": None,
            "publication_id": "a" * 32,
            "chunks_processed": 1,
            "elapsed_ms": 10,
        },
        returncode=0,
        stderr=(
            b"structure-publication-superseded publication_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
        ),
    )

    async def spawn(*_args, **_kwargs):
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert isinstance(result, IsolatedStructurePublicationCheckpoint)
    assert result.stage == "superseded"
    assert result.rows_processed == 0
    assert result.cursor is None


def test_shared_publication_stage_vocabulary_includes_supersession() -> None:
    from polyarb.perception.structure_contract import (
        STRUCTURE_PUBLICATION_CHECKPOINT_STAGES,
    )

    assert "superseded" in STRUCTURE_PUBLICATION_CHECKPOINT_STAGES


@pytest.mark.asyncio
async def test_actual_child_pipe_carries_contract_supersession_to_parent() -> None:
    from polyarb.daemon.scheduler import (
        IsolatedStructurePublicationCheckpoint,
        run_snapshot_in_subprocess,
    )

    script = """
import json, sys
publication_id = "a" * 32
print(json.dumps({
    "checkpointed": True,
    "stage": "superseded",
    "component": None,
    "rows_processed": 0,
    "cursor": None,
    "publication_id": publication_id,
    "chunks_processed": 1,
    "elapsed_ms": 10,
}))
print(
    "structure-publication-superseded publication_id=" + publication_id,
    file=sys.stderr,
)
"""

    async def spawn(*_args, **kwargs):
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            script,
            stdout=kwargs["stdout"],
            stderr=kwargs["stderr"],
        )

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert isinstance(result, IsolatedStructurePublicationCheckpoint)
    assert result.stage == "superseded"
    assert result.publication_id == "a" * 32


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows_processed", "cursor"),
    ((1, None), (0, "unexpected")),
)
async def test_snapshot_subprocess_rejects_nonzero_supersession_work(
    rows_processed: int,
    cursor: str | None,
) -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {
            "checkpointed": True,
            "stage": "superseded",
            "component": None,
            "rows_processed": rows_processed,
            "cursor": cursor,
            "publication_id": "a" * 32,
        },
        returncode=0,
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(
        SnapshotSubprocessError, match="snapshot-subprocess-invalid-json"
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_timeout_recovers_last_committed_publication_chunk() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        b"",
        returncode=-signal.SIGKILL,
        stderr=(
            b"snapshot-stage stage=persist state=start elapsed_ms=0\n"
            b"structure-publication-progress stage=normalizing component=events "
            b"chunks=7 rows=3500\n"
        ),
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError) as captured:
        await run_snapshot_in_subprocess(spawn=spawn)

    assert captured.value.last_stage == "persist"
    assert captured.value.chunks_processed == 7


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_every_shared_publication_component() -> None:
    from polyarb.daemon.scheduler import (
        IsolatedStructurePublicationCheckpoint,
        run_snapshot_in_subprocess,
    )
    from polyarb.perception.structure_contract import (
        STRUCTURE_PUBLICATION_CHECKPOINT_COMPONENTS,
    )

    for component in STRUCTURE_PUBLICATION_CHECKPOINT_COMPONENTS:
        process = _FakeProcess(
            {
                "checkpointed": True,
                "stage": "certifying",
                "component": component,
                "rows_processed": 1,
                "cursor": "cursor",
                "publication_id": "publication-1",
            },
            returncode=0,
        )

        async def spawn(*_args, **_kwargs):
            return process

        result = await run_snapshot_in_subprocess(spawn=spawn)
        assert isinstance(result, IsolatedStructurePublicationCheckpoint)
        assert result.component == component

    ready = _FakeProcess(
        {
            "checkpointed": True,
            "stage": "ready",
            "component": None,
            "rows_processed": 0,
            "cursor": None,
            "publication_id": "publication-1",
        },
        returncode=0,
    )

    async def spawn_ready(*_args, **_kwargs):
        return ready

    result = await run_snapshot_in_subprocess(spawn=spawn_ready)
    assert isinstance(result, IsolatedStructurePublicationCheckpoint)
    assert result.stage == "ready"
    assert result.component is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stage", "component"),
    [
        ("ready", "events"),
        ("normalizing", "source_events"),
        ("normalizing", "legacy-universe"),
    ],
)
async def test_snapshot_subprocess_rejects_invalid_publication_checkpoint_pair(
    stage: str,
    component: str,
) -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {
            "checkpointed": True,
            "stage": stage,
            "component": component,
            "rows_processed": 1,
            "cursor": "cursor",
            "publication_id": "publication-1",
        },
        returncode=0,
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(
        SnapshotSubprocessError, match="snapshot-subprocess-invalid-json"
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


def test_snapshot_stage_parser_keeps_only_final_allowlisted_marker() -> None:
    """Arbitrary child stderr never becomes a scheduler diagnostic."""
    from polyarb.daemon.scheduler import _parse_last_snapshot_stage

    stderr = b"\n".join(
        (
            b"network error: upstream sent a secret-looking message",
            b"snapshot-stage stage=gamma-events state=complete elapsed_ms=12",
            b"snapshot-stage stage=not-allowed state=start elapsed_ms=13",
            b"snapshot-stage stage=gamma-markets state=unexpected elapsed_ms=14",
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=-1",
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=15",
        )
    )

    assert _parse_last_snapshot_stage(stderr) == "gamma-markets"


def test_snapshot_stderr_tail_discards_oversized_allowlisted_marker() -> None:
    from polyarb.daemon.scheduler import _safe_stderr_tail

    stderr = b"snapshot-stage stage=persist state=start elapsed_ms=" + b"1" * 300

    assert _safe_stderr_tail(stderr) is None


@pytest.mark.asyncio
async def test_snapshot_subprocess_result_has_parent_elapsed_and_final_stage() -> None:
    """The parent returns bounded diagnostics, never the child's stderr payload."""
    from polyarb.daemon.scheduler import run_snapshot_in_subprocess

    process = _FakeProcess(
        {
            "status": "ok",
            "is_valid": True,
            "snapshot_id": 746,
            "market_count": 81959,
            "issue_count": 3,
        },
        returncode=0,
        stderr=(
            b"snapshot-stage stage=gamma-events state=complete elapsed_ms=91\n"
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=999999"
        ),
    )

    async def spawn(*_args, **_kwargs):
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert result.last_stage == "gamma-markets"
    assert 0 <= result.elapsed_ms < 999999
    assert not hasattr(result, "stderr")


@pytest.mark.asyncio
async def test_snapshot_waits_for_shared_producer_slot(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module

    producer_lock = asyncio.Lock()
    await producer_lock.acquire()
    calls = 0
    observed_timeout_s = None

    async def run_snapshot(*, timeout_s: float):
        nonlocal calls, observed_timeout_s
        calls += 1
        observed_timeout_s = timeout_s
        return SimpleNamespace(status=SnapshotStatus.OK)

    monkeypatch.setattr(
        scheduler_module,
        "run_snapshot_in_subprocess",
        run_snapshot,
    )
    store = MagicMock()
    store.get_latest_structure_sync.return_value = {"status": "complete"}
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=producer_lock,
    )
    scheduler._effective_timeout_s = 240
    running = asyncio.create_task(scheduler._run_snapshot())
    await asyncio.sleep(0)
    assert calls == 0

    producer_lock.release()
    await running

    assert calls == 1
    assert observed_timeout_s == 75


@pytest.mark.asyncio
async def test_snapshot_attempt_starts_after_shared_producer_slot_is_acquired(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    """Queue wait is not a running child and cannot fabricate a timeout."""
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_snapshot(store, 10)
    lock_wait_started = asyncio.Event()

    class ObservedLock(asyncio.Lock):
        async def acquire(self) -> bool:
            lock_wait_started.set()
            return await super().acquire()

    producer_lock = ObservedLock()
    await producer_lock.acquire()
    lock_wait_started.clear()
    child_started = asyncio.Event()
    finish_child = asyncio.Event()
    wall_s = 1_000.0

    async def run_snapshot(*, timeout_s: float):
        assert timeout_s > 0
        child_started.set()
        await finish_child.wait()
        return _FakeResult(
            SnapshotStatus.OK,
            snapshot_id=10,
            last_stage="persist",
            elapsed_ms=2_000,
        )

    monkeypatch.setattr(scheduler_module, "run_snapshot_in_subprocess", run_snapshot)
    monkeypatch.setattr(scheduler_module.time, "time", lambda: wall_s)
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=producer_lock,
    )

    tick = asyncio.create_task(scheduler._tick())
    await lock_wait_started.wait()
    wall_s = 1_181.0
    await asyncio.sleep(0)

    queued_attempt = store.get_latest_snapshot_attempt()

    producer_lock.release()
    await child_started.wait()
    running = store.get_latest_snapshot_attempt()

    wall_s = 1_183.0
    finish_child.set()
    await tick

    terminal = store.get_latest_snapshot_attempt()
    assert queued_attempt is None
    assert running is not None
    assert running["outcome"] == "running"
    assert running["started_at_ms"] == 1_181_000
    assert terminal is not None
    assert terminal["outcome"] == "succeeded"
    assert terminal["finished_at_ms"] - terminal["started_at_ms"] == 2_000
    assert terminal["elapsed_ms"] == 2_000


@pytest.mark.asyncio
async def test_incomplete_structure_slice_has_shorter_producer_slot_budget(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    """An upstream retry cannot let an incomplete slice starve Quote for 180s."""
    from polyarb.daemon import scheduler as scheduler_module

    observed_timeout_s = None

    async def run_snapshot(*, timeout_s: float):
        nonlocal observed_timeout_s
        observed_timeout_s = timeout_s
        return SimpleNamespace(status=SnapshotStatus.OK)

    monkeypatch.setattr(
        scheduler_module,
        "run_snapshot_in_subprocess",
        run_snapshot,
    )
    store = MagicMock()
    store.get_latest_structure_sync.return_value = {"status": "events_complete"}
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
    )
    scheduler._effective_timeout_s = 240

    await scheduler._run_snapshot()

    assert observed_timeout_s == 75


@pytest.mark.asyncio
async def test_ready_structure_publication_uses_pointer_switch_hard_budget(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module

    observed_timeout_s = None

    async def run_snapshot(*, timeout_s: float):
        nonlocal observed_timeout_s
        observed_timeout_s = timeout_s
        return SimpleNamespace(status=SnapshotStatus.OK)

    monkeypatch.setattr(scheduler_module, "run_snapshot_in_subprocess", run_snapshot)
    store = MagicMock()
    store.get_latest_structure_publication.return_value = SimpleNamespace(
        status="ready"
    )
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._effective_timeout_s = 240

    await scheduler._run_snapshot()

    assert observed_timeout_s == 15


def test_structure_defer_receipts_are_restart_visible_and_bounded(
    daemon_settings_for_test,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    for index in range(105):
        receipt_id = store.record_structure_defer(
            reason="quote-pipeline-active",
            queued_at_ms=1_000,
            observed_at_ms=1_000 + index,
        )

    restarted = SQLiteStore(daemon_settings_for_test.db_path)
    latest = restarted.get_latest_structure_defer()
    with sqlite3.connect(store.db_path) as con:
        retained_count = con.execute(
            "SELECT COUNT(*) FROM structure_defer_receipts"
        ).fetchone()[0]

    assert latest == {
        "id": receipt_id,
        "reason": "quote-pipeline-active",
        "queued_at_ms": 1_000,
        "observed_at_ms": 1_104,
    }
    assert retained_count == 100


@pytest.mark.asyncio
async def test_structure_rechecks_quote_priority_after_lock_acquisition(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    """A Quote transition between initial check and lock acquisition still wins."""
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructureCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    active_checks = iter((False, True, False, False, False))
    quote_runtime = MagicMock()
    quote_runtime.pipeline_active.side_effect = lambda: next(active_checks)
    quote_runtime.pipeline_due.return_value = False
    child_calls = 0

    async def run_snapshot(*, timeout_s: float):
        nonlocal child_calls
        child_calls += 1
        assert timeout_s == 75
        return IsolatedStructureCheckpoint(
            window_id="window-1",
            stage="markets",
            pages_processed=1,
            elapsed_ms=10,
        )

    sleep = AsyncMock()
    monkeypatch.setattr(scheduler_module, "run_snapshot_in_subprocess", run_snapshot)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", sleep)
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        quote_worker_runtime=quote_runtime,
    )

    await scheduler._tick()

    receipt = store.get_latest_structure_defer()
    attempts = store.get_snapshot_attempts(limit=10)
    assert receipt is not None
    assert receipt["reason"] == "quote-pipeline-active"
    assert receipt["observed_at_ms"] >= receipt["queued_at_ms"]
    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "cancelled"
    assert attempts[0]["failure_kind"] == "structure-checkpoint"
    assert child_calls == 1
    assert scheduler._failure_counter == 0
    assert getattr(scheduler, "_active_attempt_id", None) is None
    assert scheduler._admitted_timeout_s is None
    sleep.assert_awaited_once_with(5.0)


@pytest.mark.asyncio
async def test_pending_structure_drift_slice_precedes_snapshot_child(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructureDriftCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": "generation-members",
            "reason": "structure-drift-incomplete",
        }
    )
    child = AsyncMock(
        return_value=IsolatedStructureDriftCheckpoint(
            phase="legacy-members",
            rows_processed=50_000,
            chunks_processed=100,
            ready=False,
            deferred=False,
            defer_reason=None,
            stop_reason="max-chunks",
            elapsed_ms=4_000,
        )
    )
    monkeypatch.setattr(scheduler_module, "run_structure_drift_in_subprocess", child)
    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=producer_lock,
    )
    scheduler._run_snapshot = AsyncMock()

    assert await scheduler._tick_once(queued_at_ms=1_000) is True

    scheduler._run_snapshot.assert_not_awaited()
    child.assert_awaited_once_with(
        db_path=settings.db_path,
        max_rows=500,
        max_chunks=100,
        max_elapsed_s=45.0,
        timeout_s=75.0,
        terminate_timeout_s=15.0,
    )
    assert producer_lock.locked() is False
    assert scheduler._failure_counter == 0
    assert store.get_latest_structure_drift_attempt()["outcome"] == "checkpointed"


@pytest.mark.asyncio
async def test_structure_drift_rechecks_quote_after_shared_lock(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": "source-events",
            "reason": "structure-drift-incomplete",
        }
    )
    active = iter((False, True))
    runtime = MagicMock()
    runtime.pipeline_active.side_effect = lambda: next(active)
    runtime.pipeline_due.return_value = False
    child = AsyncMock()
    monkeypatch.setattr(scheduler_module, "run_structure_drift_in_subprocess", child)
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        quote_worker_runtime=runtime,
    )

    assert await scheduler._tick_once(queued_at_ms=1_000) is False

    child.assert_not_awaited()
    receipt = store.get_latest_structure_defer()
    assert receipt is not None
    assert receipt["reason"] == "structure-drift:quote-pipeline-active"
    assert scheduler._failure_counter == 0


@pytest.mark.asyncio
async def test_structure_drift_quote_due_wins_next_slice_admission(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructureDriftCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": "generation-members",
            "reason": "structure-drift-incomplete",
        }
    )
    runtime = MagicMock()
    runtime.pipeline_active.return_value = False
    runtime.pipeline_due.side_effect = (False, False, False, True)
    child = AsyncMock(
        return_value=IsolatedStructureDriftCheckpoint(
            phase="generation-members",
            rows_processed=50_000,
            chunks_processed=100,
            ready=False,
            deferred=False,
            defer_reason=None,
            stop_reason="max-chunks",
            elapsed_ms=40_000,
        )
    )
    monkeypatch.setattr(scheduler_module, "run_structure_drift_in_subprocess", child)
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        quote_worker_runtime=runtime,
    )

    assert await scheduler._tick_once(queued_at_ms=1_000) is True
    assert await scheduler._tick_once(queued_at_ms=2_000) is False
    child.assert_awaited_once()
    receipt = store.get_latest_structure_defer()
    assert receipt is not None
    assert receipt["reason"] == "structure-drift:quote-pipeline-due"


@pytest.mark.asyncio
async def test_request_now_reaches_same_structure_drift_child_path(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructureDriftCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": "legacy-members",
            "reason": "structure-drift-incomplete",
        }
    )
    stop_event = asyncio.Event()

    async def child(**_kwargs):
        stop_event.set()
        return IsolatedStructureDriftCheckpoint(
            phase="fresh-group-truth",
            rows_processed=500,
            chunks_processed=1,
            ready=False,
            deferred=False,
            defer_reason=None,
            stop_reason="max-chunks",
            elapsed_ms=10,
        )

    child_mock = AsyncMock(side_effect=child)
    monkeypatch.setattr(
        scheduler_module, "run_structure_drift_in_subprocess", child_mock
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )
    assert scheduler.request_now() is True

    await scheduler.run(stop_event)

    child_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_structure_drift_cancellation_releases_shared_producer_lock(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": "source-markets",
            "reason": "structure-drift-incomplete",
        }
    )
    monkeypatch.setattr(
        scheduler_module,
        "run_structure_drift_in_subprocess",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=producer_lock,
    )

    with pytest.raises(asyncio.CancelledError):
        await scheduler._tick_once(queued_at_ms=1_000)

    assert producer_lock.locked() is False
    assert scheduler._failure_counter == 0
    attempt = store.get_latest_structure_drift_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "cancelled"
    assert attempt["failure_kind"] == "scheduler-cancelled"


@pytest.mark.asyncio
async def test_structure_drift_terminal_write_failure_is_isolated_and_blocks_respawn(
    daemon_settings_for_test, monkeypatch
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructureDriftCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": None,
            "reason": "structure-drift-progress-missing",
        }
    )
    child = AsyncMock(
        return_value=IsolatedStructureDriftCheckpoint(
            phase="source-events", rows_processed=1, chunks_processed=1,
            ready=False, deferred=False, defer_reason=None,
            stop_reason="max-chunks", elapsed_ms=1,
        )
    )
    monkeypatch.setattr(scheduler_module, "run_structure_drift_in_subprocess", child)
    original_finish = store.finish_structure_drift_attempt
    store.finish_structure_drift_attempt = MagicMock(
        side_effect=sqlite3.OperationalError("database is locked")
    )
    lock = asyncio.Lock()
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store, producer_lock=lock)

    assert await scheduler._tick_once(queued_at_ms=1_000) is True
    assert await scheduler._tick_once(queued_at_ms=2_000) is True
    assert child.await_count == 1
    assert scheduler._failure_counter == 0
    assert lock.locked() is False
    store.finish_structure_drift_attempt = original_finish


@pytest.mark.asyncio
async def test_structure_drift_cancel_survives_terminal_write_failure(
    daemon_settings_for_test, monkeypatch
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": None,
            "reason": "structure-drift-progress-missing",
        }
    )
    monkeypatch.setattr(
        scheduler_module, "run_structure_drift_in_subprocess",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    store.finish_structure_drift_attempt = MagicMock(
        side_effect=sqlite3.OperationalError("database is locked")
    )
    lock = asyncio.Lock()
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store, producer_lock=lock)

    with pytest.raises(asyncio.CancelledError):
        await scheduler._tick_once(queued_at_ms=1_000)
    assert scheduler._failure_counter == 0
    assert lock.locked() is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_kind",
    (
        "structure-drift-timeout",
        "structure-drift-signal-sigkill-possible-oom",
        "structure-drift-invalid-json",
    ),
)
async def test_structure_drift_parent_terminalizes_child_failures(
    daemon_settings_for_test,
    monkeypatch,
    failure_kind: str,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import SnapshotSubprocessError
    from polyarb.storage.sqlite_store import SQLiteStore

    settings = daemon_settings_for_test.model_copy(
        update={"structure_generation_drift_compare_enabled": True}
    )
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    store.structure_generation_drift_status = MagicMock(
        return_value={
            "authorized": False,
            "phase": "source-events",
            "reason": "structure-drift-incomplete",
            "progress_id": "progress-1",
        }
    )
    monkeypatch.setattr(
        scheduler_module,
        "run_structure_drift_in_subprocess",
        AsyncMock(
            side_effect=SnapshotSubprocessError(
                failure_kind,
                last_stage="structure-drift",
                elapsed_ms=75_000,
                stderr=b"unsafe secret",
            )
        ),
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )

    assert await scheduler._tick_once(queued_at_ms=1_000) is True

    attempt = store.get_latest_structure_drift_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "failed"
    assert attempt["failure_kind"] == failure_kind
    assert attempt["stderr_safe_marker"] is None
    assert scheduler._failure_counter == 0


@pytest.mark.asyncio
async def test_structure_child_failure_closes_attempt_and_releases_slot(
    daemon_settings_for_test,
) -> None:
    from polyarb.daemon.scheduler import SnapshotSubprocessError
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=producer_lock,
    )
    scheduler._run_snapshot = AsyncMock(
        side_effect=SnapshotSubprocessError(
            "timeout",
            last_stage="gamma-markets",
            elapsed_ms=75_001,
        )
    )

    await scheduler._tick()

    attempt = store.get_latest_snapshot_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "failed"
    assert attempt["last_stage"] == "gamma-markets"
    assert attempt["elapsed_ms"] == 75_001
    assert producer_lock.locked() is False


@pytest.mark.asyncio
async def test_publication_lookup_precedes_attempt_and_rechecks_quote_after_lookup(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    """Slow publication-state I/O is queue time, never attempt runtime."""
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructureCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    lookup_entered = threading.Event()
    release_lookup = threading.Event()
    lookup_calls = 0

    def blocking_publication_lookup():
        nonlocal lookup_calls
        lookup_calls += 1
        if lookup_calls == 1:
            lookup_entered.set()
            assert release_lookup.wait(timeout=2)
        return SimpleNamespace(status="writing")

    store.get_latest_structure_publication = blocking_publication_lookup  # type: ignore[method-assign]

    class Runtime:
        active = False

        def pipeline_active(self) -> bool:
            return self.active

        def pipeline_due(self, _interval_s: float) -> bool:
            return False

    runtime = Runtime()
    child_started = asyncio.Event()
    wall_s = 1_000.0

    async def run_snapshot():
        child_started.set()
        running = store.get_latest_snapshot_attempt()
        assert running is not None
        assert running["started_at_ms"] == int(wall_s * 1_000)
        return IsolatedStructureCheckpoint(
            window_id="window-lookup",
            stage="markets",
            pages_processed=1,
            elapsed_ms=5,
        )

    async def defer_sleep(delay_s: float) -> None:
        assert delay_s == 5.0
        assert producer_lock.locked() is False
        assert scheduler._tick_lock.locked() is False
        runtime.active = False

    monkeypatch.setattr(scheduler_module.time, "time", lambda: wall_s)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", defer_sleep)
    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=producer_lock,
        quote_worker_runtime=runtime,
    )
    scheduler._run_snapshot = run_snapshot  # type: ignore[method-assign]

    tick = asyncio.create_task(scheduler._tick())
    assert await asyncio.to_thread(lookup_entered.wait, 1)
    assert store.get_latest_snapshot_attempt() is None

    runtime.active = True
    wall_s = 1_100.0
    release_lookup.set()
    await asyncio.wait_for(child_started.wait(), timeout=1)
    await tick

    receipt = store.get_latest_structure_defer()
    attempt = store.get_latest_snapshot_attempt()
    assert receipt is not None
    assert receipt["reason"] == "quote-pipeline-active"
    assert attempt is not None
    assert attempt["outcome"] == "cancelled"
    assert attempt["started_at_ms"] == 1_100_000
    assert lookup_calls == 2


@pytest.mark.asyncio
async def test_concurrent_ticks_serialize_attempt_ownership(
    daemon_settings_for_test,
) -> None:
    from polyarb.daemon.scheduler import IsolatedStructureCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    first_child_started = asyncio.Event()
    release_first_child = asyncio.Event()
    child_calls = 0

    async def run_snapshot():
        nonlocal child_calls
        child_calls += 1
        if child_calls == 1:
            first_child_started.set()
            await release_first_child.wait()
        return IsolatedStructureCheckpoint(
            window_id=f"window-{child_calls}",
            stage="markets",
            pages_processed=1,
            elapsed_ms=5,
        )

    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=producer_lock,
    )
    scheduler._run_snapshot = run_snapshot  # type: ignore[method-assign]

    first = asyncio.create_task(scheduler._tick())
    await first_child_started.wait()
    second = asyncio.create_task(scheduler._tick())
    await asyncio.sleep(0)

    assert len(store.get_snapshot_attempts(limit=10)) == 1
    release_first_child.set()
    await asyncio.gather(first, second)

    attempts = store.get_snapshot_attempts(limit=10)
    assert len(attempts) == 2
    assert {attempt["outcome"] for attempt in attempts} == {"cancelled"}
    assert {attempt["id"] for attempt in attempts} == {1, 2}
    assert producer_lock.locked() is False
    assert getattr(scheduler, "_active_attempt_id", None) is None
    assert scheduler._admitted_timeout_s is None


@pytest.mark.asyncio
async def test_cancellation_after_admission_closes_local_attempt_ownership(
    daemon_settings_for_test,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    child_started = asyncio.Event()

    async def run_snapshot():
        child_started.set()
        await asyncio.Event().wait()

    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        producer_lock=producer_lock,
    )
    scheduler._run_snapshot = run_snapshot  # type: ignore[method-assign]
    tick = asyncio.create_task(scheduler._tick())
    await child_started.wait()

    tick.cancel()
    with pytest.raises(asyncio.CancelledError):
        await tick

    attempt = store.get_latest_snapshot_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "cancelled"
    assert attempt["failure_kind"] == "scheduler-cancelled"
    assert producer_lock.locked() is False
    assert getattr(scheduler, "_active_attempt_id", None) is None
    assert scheduler._admitted_timeout_s is None


async def test_snapshot_timeout_reaps_before_reading_bounded_stage_diagnostics() -> (
    None
):
    """Timeout data arrives only from the child communicate result after reaping."""
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {},
        returncode=0,
        block=True,
        stderr=(
            b"arbitrary child error\nsnapshot-stage stage=gamma-markets state=start elapsed_ms=17"
        ),
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(
        SnapshotSubprocessError, match="snapshot-subprocess-timeout"
    ) as raised:
        await run_snapshot_in_subprocess(
            spawn=spawn,
            timeout_s=0.01,
            terminate_timeout_s=0.01,
        )

    error = raised.value
    assert process.terminated is True
    assert process.killed is True
    assert error.last_stage == "gamma-markets"
    assert error.elapsed_ms >= 0
    assert not hasattr(error, "stderr")


@pytest.mark.asyncio
async def test_snapshot_subprocess_rejects_mismatched_exit_contract() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(
            {
                "status": "ok",
                "is_valid": True,
                "snapshot_id": 746,
                "market_count": 81959,
                "issue_count": 3,
            },
            returncode=1,
        )

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-invalid-json",
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_subprocess_classifies_sqlite_writer_contention() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(
            b"",
            returncode=1,
            stderr=b"OperationalError: database is locked",
        )

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-sqlite-busy",
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_bounded_child_failure_contract() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    stderr = (
        b"arbitrary secret-bearing diagnostic\n"
        b"structure-publication-progress stage=certifying component=memberships "
        b"chunks=1 rows=500\n"
        b"structure-sync-failure failure_kind=membership-invalid\n"
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(
            {"failed": True, "failure_kind": "membership-invalid"},
            returncode=1,
            stderr=stderr,
        )

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-membership-invalid",
    ) as raised:
        await run_snapshot_in_subprocess(spawn=spawn)

    error = raised.value
    assert error.stderr_bytes == len(stderr)
    assert len(error.stderr_sha256) == 64
    assert error.stderr_tail == "structure-sync-failure failure_kind=membership-invalid"
    assert "secret" not in error.stderr_tail
    assert error.chunks_processed == 1


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_allowlisted_membership_evidence_marker() -> (
    None
):
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    fingerprint = "a" * 64
    stderr = (
        "structure-sync-failure failure_kind=membership-invalid "
        f"membership_kind=active-market-missing key_sha256={fingerprint}\n"
    ).encode()

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(
            {"failed": True, "failure_kind": "membership-invalid"},
            returncode=1,
            stderr=stderr,
        )

    with pytest.raises(SnapshotSubprocessError) as raised:
        await run_snapshot_in_subprocess(spawn=spawn)
    assert raised.value.stderr_tail == stderr.decode().strip()


def test_safe_tail_rejects_membership_evidence_on_non_membership_failure() -> None:
    from polyarb.daemon.scheduler import _safe_stderr_tail

    forged = (
        b"structure-sync-failure failure_kind=sqlite-busy "
        b"membership_kind=group-truth key_sha256=" + b"a" * 64
    )
    assert _safe_stderr_tail(forged) is None


@pytest.mark.asyncio
async def test_snapshot_subprocess_classifies_sigkill_as_possible_oom() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(b"", returncode=-9)

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-signal-sigkill-possible-oom",
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_subprocess_cancellation_terminates_then_kills() -> None:
    from polyarb.daemon.scheduler import run_snapshot_in_subprocess

    process = _FakeProcess({}, returncode=0, block=True)

    async def spawn(*_args, **_kwargs):
        return process

    task = asyncio.create_task(
        run_snapshot_in_subprocess(
            spawn=spawn,
            terminate_timeout_s=0.01,
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated is True
    assert process.killed is True


@pytest.mark.asyncio
async def test_snapshot_subprocess_timeout_terminates_then_kills() -> None:
    """A CPU-bound child cannot hold the 5-minute production cadence forever."""
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess({}, returncode=0, block=True)

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="snapshot-subprocess-timeout"):
        await run_snapshot_in_subprocess(
            spawn=spawn,
            timeout_s=0.01,
            terminate_timeout_s=0.01,
        )

    assert process.terminated is True
    assert process.killed is True


@pytest.mark.asyncio
async def test_snapshot_timeout_keeps_one_reap_task_until_child_exit() -> None:
    """Timeout cleanup must not abandon a child after cancelling its pipe reader."""
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    class Process:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.calls = 0
            self.released = asyncio.Event()
            self.terminated = False
            self.killed = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.calls += 1
            if self.calls != 1:
                raise AssertionError("communicate must not be re-entered after timeout")
            await self.released.wait()
            return b"", b""

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9
            self.released.set()

    process = Process()

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="snapshot-subprocess-timeout"):
        await run_snapshot_in_subprocess(
            spawn=spawn,
            timeout_s=0.01,
            terminate_timeout_s=0.01,
        )

    assert process.terminated is True
    assert process.killed is True
    assert process.calls == 1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_after_failure_threshold(
    daemon_settings_for_test: Any,
    tmp_path: Path,
) -> None:
    """FAILURE_THRESHOLD consecutive FAILED results → RECOVERING.

    Phase 03.1-04 D-02: threshold is 5. Loop drives the class attribute so the
    invariant ("after N failures recovery is visible") survives future tuning.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    # Always raise → counts as FAILED
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    for _ in range(SnapshotScheduler.FAILURE_THRESHOLD):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.RECOVERING
    assert scheduler._failure_counter >= SnapshotScheduler.FAILURE_THRESHOLD


@pytest.mark.asyncio
async def test_recovery_after_5_failures_not_3(
    daemon_settings_for_test: Any,
) -> None:
    """Four failures keep RUNNING; the fifth starts recovery.

    Pinned to literal 5 (not the class attribute) because this test's purpose
    is to catch silent threshold regressions back to 3. If someone bumps
    FAILURE_THRESHOLD to 3 (or 7), this test should fail loudly.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    # 4 failures: counter goes 1, 2, 3, 4 — state must remain RUNNING
    for tick in range(4):
        await scheduler._tick()
        assert scheduler.state == SchedulerState.RUNNING, (
            f"After {tick + 1} failures, expected RUNNING; got {scheduler.state}. "
            "Threshold may have regressed below 5."
        )

    # 5th failure: counter = 5 → RECOVERING
    await scheduler._tick()
    assert scheduler.state == SchedulerState.RECOVERING, (
        f"After 5 failures, expected RECOVERING; got {scheduler.state}. "
        "Threshold may have increased above 5."
    )
    assert scheduler._failure_counter == 5


@pytest.mark.asyncio
async def test_no_pause_after_degraded_then_ok(
    daemon_settings_for_test: Any,
) -> None:
    """DEGRADED then OK → counter resets, state stays RUNNING."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    # Tick 1: DEGRADED
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(SnapshotStatus.DEGRADED)
    )
    await scheduler._tick()
    assert scheduler._failure_counter == 0  # DEGRADED does not count as failure
    assert scheduler.state == SchedulerState.RUNNING

    # Tick 2: OK
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    await scheduler._tick()
    assert scheduler._failure_counter == 0
    assert scheduler.state == SchedulerState.RUNNING


@pytest.mark.asyncio
async def test_legacy_paused_state_is_migrated_to_recovering(
    daemon_settings_for_test: Any,
) -> None:
    """An in-memory legacy PAUSED state cannot disable the producer forever."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler.state = SchedulerState.PAUSED

    run_mock = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    scheduler._run_snapshot = run_mock

    await scheduler._tick()

    run_mock.assert_called_once()
    assert scheduler.state == SchedulerState.RUNNING


@pytest.mark.asyncio
async def test_degraded_does_not_count_as_failure(
    daemon_settings_for_test: Any,
) -> None:
    """Per D-12 amendment, DEGRADED is success-with-warnings.

    3 consecutive DEGRADED must NOT pause the scheduler.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(SnapshotStatus.DEGRADED)
    )

    for _ in range(5):  # More than FAILURE_THRESHOLD
        await scheduler._tick()

    # Counter should stay at 0; state should stay RUNNING
    assert scheduler._failure_counter == 0
    assert scheduler.state == SchedulerState.RUNNING


@pytest.mark.asyncio
async def test_structure_checkpoint_releases_slot_without_failure_or_alert(
    daemon_settings_for_test: Any,
) -> None:
    """A cooperative page slice is progress, not a degraded incident."""
    from polyarb.daemon.scheduler import IsolatedStructureCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=IsolatedStructureCheckpoint(
            window_id="window-1",
            stage="markets",
            pages_processed=80,
            elapsed_ms=40_000,
        )
    )

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as heartbeat:
        await scheduler._tick()

    attempt = store.get_latest_snapshot_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "cancelled"
    assert attempt["failure_kind"] == "structure-checkpoint"
    assert scheduler._failure_counter == 0
    assert scheduler.state == SchedulerState.RUNNING
    assert scheduler._checkpoint_pending is True
    heartbeat.assert_not_called()


@pytest.mark.asyncio
async def test_contract_supersession_checkpoint_preserves_existing_failure_counter(
    daemon_settings_for_test: Any,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module
    from polyarb.daemon.scheduler import IsolatedStructurePublicationCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    store.upsert_scheduler_state(state="RECOVERING", failure_counter=193)
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=IsolatedStructurePublicationCheckpoint(
            stage="superseded",
            component=None,
            rows_processed=0,
            cursor=None,
            publication_id="a" * 32,
            elapsed_ms=10,
        )
    )

    with patch.object(scheduler_module.logger, "warning") as warning:
        await scheduler._tick()

    attempt = store.get_latest_snapshot_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "cancelled"
    assert attempt["failure_kind"] == "structure-contract-superseded"
    assert scheduler._failure_counter == 193
    assert store.get_scheduler_state()["failure_counter"] == 193
    warning.assert_called_once()
    assert "publication contract superseded" in warning.call_args.args[0].lower()

    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(SnapshotStatus.OK, snapshot_id=847)
    )
    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    assert scheduler._failure_counter == 0
    assert store.get_scheduler_state()["failure_counter"] == 0


@pytest.mark.asyncio
async def test_successful_tick_calls_heartbeat_ok(
    daemon_settings_for_test: Any,
) -> None:
    """Plan 02-05 fix-up: successful snapshot tick must ping Better Stack heartbeat.

    Wired via `alerts.send_heartbeat_ok(self._settings)` from _tick() success branch.
    Test patches the module attribute so we can assert the call without HTTP traffic.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as hb_mock:
        await scheduler._tick()
        hb_mock.assert_called_once_with(daemon_settings_for_test)


@pytest.mark.asyncio
async def test_successful_structure_publish_wakes_quote_worker(
    daemon_settings_for_test: Any,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    request_quote = MagicMock(return_value=True)
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        on_snapshot_published=request_quote,
    )
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    request_quote.assert_called_once_with()


@pytest.mark.asyncio
async def test_successful_tick_purges_expired_snapshots_on_attached_store(
    daemon_settings_for_test: Any,
) -> None:
    """Retention must run in the app process that owns the mounted volume."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    purge = MagicMock(return_value=(0, []))
    store.purge_old_snapshots = purge  # type: ignore[method-assign]
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    purge.assert_called_once_with(
        older_than_days=7,
        keep_last=5,
        max_snapshots_per_run=10,
        parquet_root=daemon_settings_for_test.parquet_root,
    )


@pytest.mark.asyncio
async def test_success_persists_recovery_before_slow_retention(
    daemon_settings_for_test: Any,
) -> None:
    """Health must see recovery before bounded cleanup starts."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    store.upsert_scheduler_state(state="RECOVERING", failure_counter=7)
    observed_counters: list[int] = []

    def purge(**_kwargs):
        state = store.get_scheduler_state()
        assert state is not None
        observed_counters.append(int(state["failure_counter"]))
        return (0, [])

    store.purge_old_snapshots = purge  # type: ignore[method-assign]
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    assert observed_counters == [0]


@pytest.mark.asyncio
async def test_successful_tick_purges_failed_then_old_published_structure_window(
    daemon_settings_for_test: Any,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    purge_failed = MagicMock(return_value=(0, []))
    purge_published = MagicMock(return_value=(0, []))
    store.purge_failed_structure_sync_windows = purge_failed  # type: ignore[method-assign]
    store.purge_published_structure_sync_windows = purge_published  # type: ignore[method-assign]
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    purge_failed.assert_called_once_with(max_windows_per_run=1)
    purge_published.assert_called_once_with(keep_last=1, max_windows_per_run=1)


@pytest.mark.asyncio
async def test_degraded_tick_also_calls_heartbeat_ok(
    daemon_settings_for_test: Any,
) -> None:
    """DEGRADED is success-with-warnings (D-12) — heartbeat OK still fires.

    Better Stack monitor should stay green for DEGRADED ticks; the degradation
    is signalled through Sentry breadcrumbs + supabase mirror age check, not
    through silencing the heartbeat.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(SnapshotStatus.DEGRADED)
    )

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as hb_mock:
        await scheduler._tick()
        hb_mock.assert_called_once_with(daemon_settings_for_test)


@pytest.mark.asyncio
async def test_failed_tick_does_not_call_heartbeat_ok(
    daemon_settings_for_test: Any,
) -> None:
    """FAILED tick must NOT ping heartbeat — that's how Better Stack notices the outage.

    If heartbeat OK fires on failure, the monitor stays green and 75-min grace
    never triggers, defeating the external-watcher safety net.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as hb_mock:
        await scheduler._tick()
        hb_mock.assert_not_called()


@pytest.mark.asyncio
async def test_counter_persists_across_restart(
    daemon_settings_for_test: Any,
) -> None:
    """Counter=N-1 from prior shutdown → one more failure starts recovery at N.

    Phase 03.1-04 D-02: threshold is 5. We drive both halves from the class
    attribute so the persistence invariant survives future tuning.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    threshold = SnapshotScheduler.FAILURE_THRESHOLD
    pre_shutdown_counter = threshold - 1  # one short of trigger

    # First instance: run threshold-1 failures, then "shut down"
    scheduler1 = SnapshotScheduler(
        settings=daemon_settings_for_test, sqlite_store=store
    )
    scheduler1._run_snapshot = AsyncMock(side_effect=RuntimeError("failed"))

    for _ in range(pre_shutdown_counter):
        await scheduler1._tick()

    assert scheduler1._failure_counter == pre_shutdown_counter
    assert scheduler1.state == SchedulerState.RUNNING

    # Second instance reads from DB → restores counter
    scheduler2 = SnapshotScheduler(
        settings=daemon_settings_for_test, sqlite_store=store
    )

    assert scheduler2._failure_counter == pre_shutdown_counter, (
        f"Expected restored counter={pre_shutdown_counter}, "
        f"got {scheduler2._failure_counter}. Scheduler must persist counter to SQLite."
    )

    # One more failure → RECOVERING
    scheduler2._run_snapshot = AsyncMock(side_effect=RuntimeError("failed again"))
    await scheduler2._tick()

    assert scheduler2.state == SchedulerState.RECOVERING


@pytest.mark.asyncio
async def test_failure_threshold_enters_recovering_and_a_later_success_self_heals(
    daemon_settings_for_test: Any,
) -> None:
    """A repeated failure must not disable the only Structure producer forever."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        side_effect=[
            RuntimeError("upstream unavailable")
            for _ in range(SnapshotScheduler.FAILURE_THRESHOLD)
        ]
        + [_FakeResult(SnapshotStatus.OK, snapshot_id=99)]
    )

    for _ in range(SnapshotScheduler.FAILURE_THRESHOLD):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.RECOVERING
    assert scheduler._failure_counter == SnapshotScheduler.FAILURE_THRESHOLD

    await scheduler._tick()

    assert scheduler.state == SchedulerState.RUNNING
    assert scheduler._failure_counter == 0
    assert (
        scheduler._run_snapshot.await_count == SnapshotScheduler.FAILURE_THRESHOLD + 1
    )
