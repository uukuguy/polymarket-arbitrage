from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from polyarb.snapshot.cli import app


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
