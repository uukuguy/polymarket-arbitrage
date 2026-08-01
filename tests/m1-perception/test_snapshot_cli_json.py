from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from polyarb.perception.structure_publication import StructurePublicationCheckpoint
from polyarb.perception.structure_sync import StructureSyncCheckpoint
from polyarb.snapshot.cli import app
from polyarb.storage.sqlite_store import SQLiteStore


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
    }
    assert run.await_args.kwargs["max_publication_rows"] == 17


def test_generation_backfill_cli_prioritizes_bounded_event_market_bootstrap(
    monkeypatch, tmp_path
) -> None:
    from polyarb.snapshot import cli as cli_module

    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[
            {"id": f"event-{index}", "markets": [{"id": f"market-{index}"}]}
            for index in range(3)
        ], finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
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

    result = CliRunner().invoke(
        app, ["structure-generation-backfill", "--max-rows", "2"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "blocked": False,
        "blocked_reason": None,
        "complete": False,
        "copied_rows": 2,
        "event_cursor": "event-1",
        "events_processed": 2,
        "member_offset": 0,
        "phase": "event-market-bootstrap",
        "rotated_to_window_id": None,
        "window_id": window["id"],
    }


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
        result = CliRunner().invoke(app, ["snapshot", "--product", "structure", "--json"])

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
