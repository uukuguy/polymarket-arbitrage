from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from polyarb.snapshot.cli import app


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
