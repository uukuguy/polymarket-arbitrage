from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from polyarb.perception.structure_publication import StructurePublicationCheckpoint
from polyarb.perception.structure_sync import StructureSyncCheckpoint
from polyarb.snapshot.cli import app
from polyarb.storage.sqlite_store import SQLiteStore, StructureMembershipInvalidError


def test_structure_drift_internal_child_caps_slice_at_100_chunks(
    monkeypatch,
    tmp_path,
) -> None:
    import polyarb.storage.sqlite_store as store_module
    from polyarb.storage.sqlite_store import StructureCertificationChunk

    class FakeStore:
        calls = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def advance_current_structure_drift_chunk(self, **_kwargs):
            self.calls += 1
            return StructureCertificationChunk(
                "generation-members", "cursor", 500, False
            )

    fake = FakeStore()
    monkeypatch.setattr(store_module, "SQLiteStore", lambda *_a, **_k: fake)
    result = CliRunner().invoke(
        app,
        [
            "structure-generation-drift-advance",
            "--db-path",
            str(tmp_path / "state.db"),
            "--max-rows",
            "500",
            "--max-chunks",
            "100",
            "--max-elapsed-seconds",
            "45",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload["chunks_processed"] == 100
    assert payload["rows_processed"] == 50_000
    assert payload["stop_reason"] == "max-chunks"
    assert fake.calls == 100


def test_structure_drift_internal_child_keeps_partial_commit_at_post_deadline(
    monkeypatch,
    tmp_path,
) -> None:
    import polyarb.storage.sqlite_store as store_module
    from polyarb.snapshot import cli as cli_module
    from polyarb.storage.sqlite_store import StructureCertificationChunk

    class FakeStore:
        calls = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def advance_current_structure_drift_chunk(self, **_kwargs):
            self.calls += 1
            return StructureCertificationChunk("source-events", "cursor", 500, False)

    fake = FakeStore()
    clock = iter((0.0, 0.0, 46.0, 46.0))
    monkeypatch.setattr(store_module, "SQLiteStore", lambda *_a, **_k: fake)
    monkeypatch.setattr(cli_module.time, "monotonic", lambda: next(clock))
    result = CliRunner().invoke(
        app,
        [
            "structure-generation-drift-advance",
            "--db-path",
            str(tmp_path / "state.db"),
            "--max-chunks",
            "100",
            "--max-elapsed-seconds",
            "45",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload["chunks_processed"] == 1
    assert payload["rows_processed"] == 500
    assert payload["stop_reason"] == "max-elapsed-seconds"
    assert fake.calls == 1


def test_event_member_internal_child_keeps_partial_commit_at_45_second_deadline(
    monkeypatch,
    tmp_path,
) -> None:
    import polyarb.storage.sqlite_store as store_module
    from polyarb.snapshot import cli as cli_module

    class FakeStore:
        calls = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def advance_structure_event_member_staging_chunk(self, **_kwargs):
            self.calls += 1
            return {"rows_written": 500, "sealed": False}

    fake = FakeStore()
    clock = iter((0.0, 0.0, 46.0, 46.0))
    monkeypatch.setattr(store_module, "SQLiteStore", lambda *_a, **_k: fake)
    monkeypatch.setattr(cli_module.time, "monotonic", lambda: next(clock))
    result = CliRunner().invoke(
        app,
        [
            "structure-event-members-advance",
            "--db-path", str(tmp_path / "state.db"),
            "--window-id", "window-1",
            "--max-chunks", "100",
            "--max-elapsed-seconds", "45",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload["chunks_processed"] == 1
    assert payload["rows_processed"] == 500
    assert payload["sealed"] is False
    assert payload["stop_reason"] == "max-elapsed-seconds"
    assert fake.calls == 1


def test_event_member_internal_child_caps_production_slice_at_50000_rows(
    monkeypatch,
    tmp_path,
) -> None:
    import polyarb.storage.sqlite_store as store_module

    class FakeStore:
        calls = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def advance_structure_event_member_staging_chunk(self, **_kwargs):
            self.calls += 1
            return {"rows_written": 500, "sealed": False}

    fake = FakeStore()
    monkeypatch.setattr(store_module, "SQLiteStore", lambda *_a, **_k: fake)
    result = CliRunner().invoke(
        app,
        [
            "structure-event-members-advance",
            "--db-path", str(tmp_path / "state.db"),
            "--window-id", "window-1",
            "--max-rows", "500",
            "--max-chunks", "100",
            "--max-elapsed-seconds", "45",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout.splitlines()[-1])
    assert payload["chunks_processed"] == 100
    assert payload["rows_processed"] == 50_000
    assert payload["stop_reason"] == "max-chunks"
    assert fake.calls == 100


def test_structure_drift_internal_child_fails_fast_on_oversized_source_event(
    monkeypatch,
    tmp_path,
) -> None:
    import polyarb.storage.sqlite_store as store_module

    class FakeStore:
        calls = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def advance_current_structure_drift_chunk(self, **_kwargs):
            self.calls += 1
            raise ValueError("structure-drift-source-event-workload-oversized")

    fake = FakeStore()
    monkeypatch.setattr(store_module, "SQLiteStore", lambda *_a, **_k: fake)
    started = time.monotonic()
    result = CliRunner().invoke(
        app,
        [
            "structure-generation-drift-advance",
            "--db-path",
            str(tmp_path / "state.db"),
        ],
    )

    assert time.monotonic() - started < 1.0
    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "failed": True,
        "failure_kind": "source-event-workload-oversized",
    }
    assert "structure-drift stage=" not in result.output
    assert fake.calls == 1


def test_structure_sync_cli_returns_certified_snapshot_json(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    result_object = SimpleNamespace(
        is_valid=True,
        issue_categories={},
        issue_count=0,
        market_count=81959,
        mode="full",
        parquet_path=None,
        snapshot_id=800,
        status="ok",
    )
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(return_value=result_object),
    ):
        result = CliRunner().invoke(app, ["structure-sync", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout)["snapshot_id"] == 800


def test_structure_sync_cli_returns_bounded_failure_json(monkeypatch) -> None:
    """Exceptions must not become an unbounded Rich traceback and empty stdout."""
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(side_effect=ValueError("membership-invalid")),
    ):
        result = CliRunner().invoke(app, ["structure-sync", "--json"])

    assert result.exit_code == 1
    lines = result.stdout.splitlines()
    assert lines[0] == "structure-sync-failure failure_kind=membership-invalid"
    assert json.loads(lines[-1]) == {
        "failed": True,
        "failure_kind": "membership-invalid",
    }
    assert len(result.stdout.encode()) <= 128
    assert "membership-invalid" in result.output


def test_structure_sync_cli_emits_bounded_membership_failure_evidence(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    error = StructureMembershipInvalidError(
        "active-market-missing", ("event-secret", "market-secret")
    )
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(side_effect=error),
    ):
        result = CliRunner().invoke(app, ["structure-sync", "--json"])

    marker = result.stdout.splitlines()[0]
    assert marker.startswith(
        "structure-sync-failure failure_kind=membership-invalid "
        "membership_kind=active-market-missing key_sha256="
    )
    assert len(marker.rsplit("=", 1)[1]) == 64
    assert "event-secret" not in result.stdout
    assert "market-secret" not in result.stdout
    assert json.loads(result.stdout.splitlines()[-1]) == {
        "failed": True,
        "failure_kind": "membership-invalid",
    }


def test_structure_sync_cli_redacts_unexpected_exception(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    secret = "credential=must-not-leak"
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(side_effect=RuntimeError(secret)),
    ):
        result = CliRunner().invoke(app, ["structure-sync", "--json"])

    assert result.exit_code == 1
    lines = result.stdout.splitlines()
    assert lines[0] == "structure-sync-failure failure_kind=structure-child-error"
    assert json.loads(lines[-1]) == {
        "failed": True,
        "failure_kind": "structure-child-error",
    }
    assert secret not in result.stdout
    assert secret not in result.output


def test_structure_sync_failure_uses_separate_os_pipe_contract() -> None:
    """The real process boundary keeps terminal JSON and failure marker separate."""
    script = """
from unittest.mock import AsyncMock, patch
from polyarb.snapshot.cli import app
with patch(
    'polyarb.snapshot.cli.run_structure_sync_until_published',
    new=AsyncMock(side_effect=ValueError('source-truth-invalid')),
):
    app(['structure-sync', '--json'], standalone_mode=True)
"""
    environment = os.environ.copy()
    environment["POLYARB_ALLOW_EMPTY_SECRET"] = "1"
    environment["POLYARB_ALLOW_EXTERNAL_PATHS"] = "1"

    result = subprocess.run(
        [sys.executable, "-c", script],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "failed": True,
        "failure_kind": "source-truth-invalid",
    }
    assert result.stderr == (
        "structure-sync-failure failure_kind=source-truth-invalid\n"
    )


def test_structure_sync_cli_returns_cooperative_checkpoint_json(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    checkpoint = StructureSyncCheckpoint(
        window_id="window-1",
        stage="markets",
        pages_processed=80,
    )
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(return_value=checkpoint),
    ) as run:
        result = CliRunner().invoke(
            app,
            [
                "structure-sync",
                "--json",
                "--max-pages",
                "80",
                "--max-elapsed-seconds",
                "45",
            ],
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "checkpointed": True,
        "pages_processed": 80,
        "stage": "markets",
        "window_id": "window-1",
    }
    assert run.await_args.kwargs["max_pages"] == 80
    assert run.await_args.kwargs["max_elapsed_s"] == 45.0


def test_structure_sync_cli_reports_publication_checkpoint_and_row_budget(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    checkpoint = StructurePublicationCheckpoint(
        stage="normalizing",
        component="memberships",
        rows_processed=17,
        cursor="event-17",
        publication_id="publication-1",
        chunks_processed=4,
        elapsed_ms=12_345,
    )
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(return_value=checkpoint),
    ) as run:
        result = CliRunner().invoke(
            app,
            [
                "structure-sync",
                "--json",
                "--max-publication-rows",
                "17",
                "--max-elapsed-seconds",
                "45",
            ],
        )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "checkpointed": True,
        "stage": "normalizing",
        "component": "memberships",
        "rows_processed": 17,
        "cursor": "event-17",
        "publication_id": "publication-1",
        "chunks_processed": 4,
        "elapsed_ms": 12_345,
    }
    assert run.await_args.kwargs["max_publication_rows"] == 17


def test_structure_sync_cli_emits_controlled_supersession_checkpoint(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    checkpoint = StructurePublicationCheckpoint(
        stage="superseded",
        component=None,
        rows_processed=0,
        cursor=None,
        publication_id="a" * 32,
        chunks_processed=1,
        elapsed_ms=12,
    )
    with patch(
        "polyarb.snapshot.cli.run_structure_sync_until_published",
        new=AsyncMock(return_value=checkpoint),
    ):
        result = CliRunner().invoke(app, ["structure-sync", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout.splitlines()[-1]) == {
        "checkpointed": True,
        "stage": "superseded",
        "component": None,
        "rows_processed": 0,
        "cursor": None,
        "publication_id": "a" * 32,
        "chunks_processed": 1,
        "elapsed_ms": 12,
    }
    assert result.stdout.splitlines()[0] == (
        "structure-publication-superseded publication_id=" + "a" * 32
    )


def test_structure_sync_cli_rejects_publication_chunks_above_500(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

    result = CliRunner().invoke(
        app,
        ["structure-sync", "--max-publication-rows", "501"],
    )

    assert result.exit_code == 2


def test_generation_backfill_cli_prioritizes_bounded_event_market_bootstrap(
    monkeypatch, tmp_path
) -> None:
    from polyarb.snapshot import cli as cli_module

    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[
            {"id": f"event-{index}", "markets": [{"id": f"market-{index}"}]}
            for index in range(3)
        ],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": f"market-{index}"} for index in range(3)],
        finished_at_ms=300,
    )
    with sqlite3.connect(db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )
        con.execute("DROP TRIGGER trg_structure_event_market_delete_guard")
        con.execute(
            "DELETE FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        )
    store.init_structure_sync_schema()
    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=db_path),
    )

    with sqlite3.connect(db_path, isolation_level=None) as locker:
        locker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        deferred = CliRunner().invoke(
            app, ["structure-generation-backfill", "--max-rows", "2"]
        )
        elapsed = time.monotonic() - started
        locker.execute("ROLLBACK")

    assert elapsed < 1.0
    assert deferred.exit_code == 0, deferred.output
    deferred_payload = json.loads(deferred.stdout)
    assert deferred_payload["chunks_attempted"] == 0
    assert deferred_payload["chunks_deferred"] == 1
    assert deferred_payload["chunks_succeeded"] == 0
    assert deferred_payload["copied_rows"] == 0
    assert deferred_payload["stop_reason"] == "writer-busy"
    assert deferred_payload["final_progress"] == {
        "complete": False,
        "copied_rows": 0,
        "defer_reason": "writer-busy",
        "deferred": True,
        "phase": "operator-admission",
    }
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (0,)
        assert con.execute("SELECT COUNT(*) FROM snapshot_attempts").fetchone() == (0,)
        assert con.execute(
            "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
            (window["id"],),
        ).fetchone() == ("complete", None)

    result = CliRunner().invoke(
        app,
        [
            "structure-generation-backfill",
            "--max-rows",
            "2",
            "--max-chunks",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 2
    assert payload["chunks_deferred"] == 0
    assert payload["chunks_succeeded"] == 2
    assert payload["copied_rows"] == 3
    assert payload["stop_reason"] == "max-chunks"
    assert payload["final_progress"] == {
        "blocked": False,
        "blocked_reason": None,
        "complete": True,
        "copied_rows": 1,
        "defer_reason": None,
        "deferred": False,
        "event_cursor": "event-2",
        "events_processed": 1,
        "member_offset": 0,
        "phase": "event-market-bootstrap",
        "rotated_to_window_id": None,
        "window_id": window["id"],
    }

    generation_batches = []
    for _attempt in range(5):
        continued = CliRunner().invoke(
            app,
            [
                "structure-generation-backfill",
                "--max-rows",
                "500",
                "--max-chunks",
                "10",
            ],
        )
        assert continued.exit_code == 0, continued.output
        generation_batches.append(json.loads(continued.stdout))
        if generation_batches[-1]["stop_reason"] == "complete":
            break

    assert generation_batches[-1]["stop_reason"] == "complete"
    replay = CliRunner().invoke(
        app,
        ["structure-generation-backfill", "--max-chunks", "10"],
    )
    assert replay.exit_code == 0, replay.output
    replay_payload = json.loads(replay.stdout)
    assert replay_payload["chunks_attempted"] == 1
    assert replay_payload["chunks_succeeded"] == 1
    assert replay_payload["copied_rows"] == 0
    assert replay_payload["final_progress"]["complete"] is True
    assert replay_payload["stop_reason"] == "complete"


def test_generation_backfill_cli_rejects_unsafe_batch_bounds() -> None:
    runner = CliRunner()

    assert (
        runner.invoke(
            app, ["structure-generation-backfill", "--max-rows", "501"]
        ).exit_code
        == 2
    )
    assert (
        runner.invoke(
            app, ["structure-generation-backfill", "--max-chunks", "101"]
        ).exit_code
        == 2
    )
    assert (
        runner.invoke(
            app,
            ["structure-generation-backfill", "--max-elapsed-seconds", "61"],
        ).exit_code
        == 2
    )


def test_generation_backfill_cli_defers_writer_busy_from_later_phase(
    monkeypatch,
) -> None:
    from polyarb.snapshot import cli as cli_module

    class BusyStore:
        def init_structure_sync_schema(self):
            return None

        def get_latest_structure_sync(self):
            return None

        def backfill_current_structure_generation(self, *, max_rows):
            error = sqlite3.OperationalError("database is locked")
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY | (1 << 8)
            raise error

    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), BusyStore()),
    )

    result = CliRunner().invoke(
        app, ["structure-generation-backfill", "--max-rows", "500"]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 1
    assert payload["chunks_deferred"] == 1
    assert payload["chunks_succeeded"] == 0
    assert payload["copied_rows"] == 0
    assert payload["stop_reason"] == "writer-busy"
    assert payload["final_progress"] == {
        "complete": False,
        "copied_rows": 0,
        "defer_reason": "writer-busy",
        "deferred": True,
        "phase": "operator-admission",
    }


def test_generation_backfill_cli_does_not_swallow_non_lock_operational_error(
    monkeypatch,
) -> None:
    from polyarb.snapshot import cli as cli_module

    class BrokenStore:
        def init_structure_sync_schema(self):
            raise sqlite3.OperationalError("no such table: busy_queue")

    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), BrokenStore()),
    )

    result = CliRunner().invoke(app, ["structure-generation-backfill"])

    assert result.exit_code == 1
    assert isinstance(result.exception, sqlite3.OperationalError)


def test_generation_backfill_cli_never_defers_after_blocked_progress_commit(
    monkeypatch,
) -> None:
    from polyarb.snapshot import cli as cli_module

    class RotationBusyStore:
        def init_structure_sync_schema(self):
            return None

        def get_latest_structure_sync(self):
            return {"id": "window-1", "status": "complete"}

        def get_structure_publication_progress(self, _window_id):
            return None

        def advance_structure_event_market_backfill(self, **_kwargs):
            return {
                "event_cursor": "broken-event",
                "member_offset": 0,
                "blocked": True,
                "blocked_reason": "invalid-event-json:broken-event",
                "completed": False,
                "relationships_processed": 0,
                "events_processed": 0,
            }

        def rotate_blocked_structure_sync_window(self, **_kwargs):
            error = sqlite3.OperationalError("database is locked")
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise error

    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), RotationBusyStore()),
    )

    result = CliRunner().invoke(app, ["structure-generation-backfill"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 1
    assert payload["chunks_deferred"] == 0
    assert payload["chunks_succeeded"] == 0
    assert payload["copied_rows"] == 0
    assert payload["stop_reason"] == "blocked"
    assert payload["final_progress"] == {
        "blocked": True,
        "blocked_reason": "invalid-event-json:broken-event",
        "complete": False,
        "copied_rows": 0,
        "defer_reason": None,
        "deferred": False,
        "event_cursor": "broken-event",
        "events_processed": 0,
        "member_offset": 0,
        "mutated": True,
        "phase": "event-market-bootstrap",
        "rotated_to_window_id": None,
        "rotation_pending": True,
        "window_id": "window-1",
    }


def test_generation_backfill_cli_batches_one_hundred_chunks_with_one_store_init(
    monkeypatch,
) -> None:
    from polyarb.snapshot import cli as cli_module

    class BatchStore:
        def __init__(self) -> None:
            self.init_calls = 0
            self.backfill_calls: list[int] = []

        def init_structure_sync_schema(self) -> None:
            self.init_calls += 1

        def get_latest_structure_sync(self):
            return None

        def backfill_current_structure_generation(self, *, max_rows):
            self.backfill_calls.append(max_rows)
            return SimpleNamespace(
                complete=False,
                copied_rows=max_rows,
                cursor=f"cursor-{len(self.backfill_calls)}",
                snapshot_id=42,
            )

    store = BatchStore()
    generation_store_calls = 0

    def generation_store(**_kwargs):
        nonlocal generation_store_calls
        generation_store_calls += 1
        return SimpleNamespace(), store

    monkeypatch.setattr(cli_module, "_generation_store", generation_store)

    result = CliRunner().invoke(
        app,
        [
            "structure-generation-backfill",
            "--max-rows",
            "500",
            "--max-chunks",
            "100",
            "--max-elapsed-seconds",
            "60",
        ],
    )

    assert result.exit_code == 0, result.output
    assert generation_store_calls == 1
    assert store.init_calls == 1
    assert store.backfill_calls == [500] * 100
    payload = json.loads(result.stdout)
    assert payload == {
        "chunks_attempted": 100,
        "chunks_deferred": 0,
        "chunks_succeeded": 100,
        "copied_rows": 50_000,
        "elapsed_seconds": payload["elapsed_seconds"],
        "final_progress": {
            "complete": False,
            "copied_rows": 500,
            "cursor": "cursor-100",
            "defer_reason": None,
            "deferred": False,
            "snapshot_id": 42,
        },
        "stop_reason": "max-chunks",
    }
    assert 0 <= payload["elapsed_seconds"] <= 60
    assert len(result.stdout) < 1_024


def test_generation_backfill_cli_yields_to_quote_writer_between_chunks(
    monkeypatch, tmp_path
) -> None:
    from polyarb.snapshot import cli as cli_module

    db_path = tmp_path / "writer-boundary.db"
    with sqlite3.connect(db_path) as con:
        con.execute("CREATE TABLE progress (chunk INTEGER NOT NULL)")

    class QuoteInterleavingStore:
        def __init__(self) -> None:
            self.calls = 0
            self.quote_writer = sqlite3.connect(
                db_path,
                isolation_level=None,
                timeout=0.25,
            )

        def init_structure_sync_schema(self) -> None:
            return None

        def get_latest_structure_sync(self):
            return None

        def backfill_current_structure_generation(self, *, max_rows):
            self.calls += 1
            with sqlite3.connect(db_path, timeout=0.25) as con:
                con.execute("INSERT INTO progress(chunk) VALUES (?)", (self.calls,))
            if self.calls == 1:
                self.quote_writer.execute("BEGIN IMMEDIATE")
            return SimpleNamespace(
                complete=False,
                copied_rows=max_rows,
                cursor=f"cursor-{self.calls}",
                snapshot_id=42,
            )

    store = QuoteInterleavingStore()
    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), store),
    )
    try:
        result = CliRunner().invoke(
            app,
            ["structure-generation-backfill", "--max-chunks", "100"],
        )
    finally:
        store.quote_writer.execute("ROLLBACK")
        store.quote_writer.close()

    assert result.exit_code == 0, result.output
    assert store.calls == 2
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 2
    assert payload["chunks_succeeded"] == 1
    assert payload["chunks_deferred"] == 1
    assert payload["copied_rows"] == 500
    assert payload["stop_reason"] == "writer-busy"
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT chunk FROM progress").fetchall() == [(1,)]


def test_generation_backfill_cli_stops_immediately_after_writer_defer(
    monkeypatch,
) -> None:
    from polyarb.snapshot import cli as cli_module

    class DeferredStore:
        def __init__(self) -> None:
            self.calls = 0

        def init_structure_sync_schema(self) -> None:
            return None

        def get_latest_structure_sync(self):
            return None

        def backfill_current_structure_generation(self, *, max_rows):
            self.calls += 1
            if self.calls == 2:
                error = sqlite3.OperationalError("database is locked")
                error.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise error
            return SimpleNamespace(
                complete=False,
                copied_rows=max_rows,
                cursor="cursor-1",
                snapshot_id=42,
            )

    store = DeferredStore()
    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), store),
    )

    result = CliRunner().invoke(
        app,
        ["structure-generation-backfill", "--max-chunks", "10"],
    )

    assert result.exit_code == 0, result.output
    assert store.calls == 2
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 2
    assert payload["chunks_succeeded"] == 1
    assert payload["chunks_deferred"] == 1
    assert payload["copied_rows"] == 500
    assert payload["stop_reason"] == "writer-busy"
    assert payload["final_progress"] == {
        "complete": False,
        "copied_rows": 0,
        "defer_reason": "writer-busy",
        "deferred": True,
        "phase": "operator-admission",
    }


def test_generation_backfill_cli_stops_at_elapsed_deadline(monkeypatch) -> None:
    from polyarb.snapshot import cli as cli_module

    class TimedStore:
        def __init__(self) -> None:
            self.calls = 0

        def init_structure_sync_schema(self) -> None:
            return None

        def get_latest_structure_sync(self):
            return None

        def backfill_current_structure_generation(self, *, max_rows):
            self.calls += 1
            return SimpleNamespace(
                complete=False,
                copied_rows=max_rows,
                cursor="cursor-1",
                snapshot_id=42,
            )

    store = TimedStore()
    ticks = iter((100.0, 100.0, 110.0, 131.0))
    monkeypatch.setattr(cli_module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), store),
    )

    result = CliRunner().invoke(
        app,
        [
            "structure-generation-backfill",
            "--max-chunks",
            "10",
            "--max-elapsed-seconds",
            "30",
        ],
    )

    assert result.exit_code == 0, result.output
    assert store.calls == 1
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 1
    assert payload["chunks_succeeded"] == 1
    assert payload["copied_rows"] == 500
    assert payload["stop_reason"] == "max-elapsed-seconds"
    assert payload["elapsed_seconds"] == 31.0


def test_generation_backfill_cli_checks_deadline_after_each_chunk(monkeypatch) -> None:
    from polyarb.snapshot import cli as cli_module

    class InitStore:
        def init_structure_sync_schema(self) -> None:
            return None

    store = InitStore()
    ticks = iter((100.0, 100.0, 131.0))
    monkeypatch.setattr(cli_module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(
        cli_module,
        "_generation_store",
        lambda **_kwargs: (SimpleNamespace(), store),
    )
    monkeypatch.setattr(
        cli_module,
        "_advance_structure_generation_backfill_chunk",
        lambda *_args, **_kwargs: (
            {
                "complete": False,
                "copied_rows": 500,
                "cursor": "cursor-1",
                "defer_reason": None,
                "deferred": False,
                "snapshot_id": 42,
            },
            0,
        ),
    )

    result = CliRunner().invoke(
        app,
        [
            "structure-generation-backfill",
            "--max-chunks",
            "100",
            "--max-elapsed-seconds",
            "30",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["chunks_attempted"] == 1
    assert payload["chunks_succeeded"] == 1
    assert payload["copied_rows"] == 500
    assert payload["stop_reason"] == "max-elapsed-seconds"
    assert payload["elapsed_seconds"] == 31.0


def test_snapshot_cli_json_contract(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    result_object = SimpleNamespace(
        is_valid=True,
        issue_categories={},
        issue_count=3,
        market_count=81959,
        mode="subset",
        parquet_path="/data/snapshots/fixture.parquet",
        snapshot_id=746,
        status="degraded",
    )

    with patch(
        "polyarb.snapshot.cli.run_snapshot",
        new=AsyncMock(return_value=result_object),
    ):
        result = CliRunner().invoke(app, ["snapshot", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "is_valid": True,
        "issue_count": 3,
        "market_count": 81959,
        "mode": "subset",
        "snapshot_id": 746,
        "status": "degraded",
    }


def test_snapshot_cli_can_lower_child_process_priority(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    result_object = SimpleNamespace(
        is_valid=True,
        issue_categories={},
        issue_count=0,
        market_count=1,
        mode="subset",
        parquet_path="/data/snapshots/fixture.parquet",
        snapshot_id=747,
        status="ok",
    )

    with (
        patch("polyarb.snapshot.cli.os.nice") as nice,
        patch(
            "polyarb.snapshot.cli.run_snapshot",
            new=AsyncMock(return_value=result_object),
        ),
    ):
        result = CliRunner().invoke(
            app,
            ["snapshot", "--json", "--low-priority"],
        )

    assert result.exit_code == 0
    nice.assert_called_once_with(10)


def test_snapshot_cli_structure_product_forces_full_gamma(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    result_object = SimpleNamespace(
        is_valid=True,
        issue_categories={},
        issue_count=0,
        market_count=1,
        mode="full",
        parquet_path=None,
        snapshot_id=748,
        status="ok",
    )
    run_snapshot = AsyncMock(return_value=result_object)

    with patch("polyarb.snapshot.cli.run_snapshot", new=run_snapshot):
        result = CliRunner().invoke(
            app, ["snapshot", "--product", "structure", "--json"]
        )

    assert result.exit_code == 0
    assert run_snapshot.await_args.kwargs["mode"] == "full"
    assert run_snapshot.await_args.kwargs["product"] == "structure"


def test_snapshot_cli_archive_product_forces_full_collection(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    result_object = SimpleNamespace(
        is_valid=True,
        issue_categories={},
        issue_count=0,
        market_count=1,
        mode="full",
        parquet_path=None,
        snapshot_id=749,
        status="ok",
    )
    run_snapshot = AsyncMock(return_value=result_object)

    with patch("polyarb.snapshot.cli.run_snapshot", new=run_snapshot):
        result = CliRunner().invoke(app, ["snapshot", "--product", "archive", "--json"])

    assert result.exit_code == 0
    assert run_snapshot.await_args.kwargs["mode"] == "full"
    assert run_snapshot.await_args.kwargs["product"] == "archive"
