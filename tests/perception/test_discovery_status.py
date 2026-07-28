from __future__ import annotations

import json
from pathlib import Path

from polyarb.cli_discovery import main
from polyarb.perception.store import OpportunityPerceptionStore


def test_status_is_read_only_and_low_coverage_is_success(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "state.db"
    OpportunityPerceptionStore(db_path).init_schema()

    assert main(["--db-path", str(db_path), "--now-ms", "10000"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["coverage"]["15"]["raw_fraction"] == "0"
    assert payload["queue_depth_by_class"] == {
        "explore": 0,
        "high": 0,
        "normal": 0,
    }


def test_status_rejects_missing_or_invalid_state_without_creating_db(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "missing.db"

    assert main(["--db-path", str(db_path)]) == 2
    assert not db_path.exists()
    assert str(db_path) not in capsys.readouterr().err
