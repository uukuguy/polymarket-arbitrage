from __future__ import annotations

import json
import sqlite3
import threading
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


def test_status_rejects_semantically_corrupt_cursor_state(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "state.db"
    OpportunityPerceptionStore(db_path).init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'impossible',1,20,10,0,0,0)"
        )

    assert main(["--db-path", str(db_path)]) == 2
    assert str(db_path) not in capsys.readouterr().err


def test_status_uses_one_read_snapshot_during_concurrent_commit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'c-2',0,10,20,1,1,0)"
        )
    original = OpportunityPerceptionStore._coverage_windows_in_snapshot
    writer_done = threading.Event()

    def hooked(con, now_ms):
        def write() -> None:
            with sqlite3.connect(db_path) as writer:
                writer.execute(
                    "UPDATE neg_risk_discovery_state SET groups_seen=99 WHERE id=1"
                )
            writer_done.set()

        thread = threading.Thread(target=write)
        thread.start()
        assert writer_done.wait(timeout=2)
        thread.join(timeout=2)
        return original(con, now_ms)

    monkeypatch.setattr(
        OpportunityPerceptionStore,
        "_coverage_windows_in_snapshot",
        staticmethod(hooked),
    )

    status = store.discovery_status(now_ms=100)

    assert status.groups_seen == 1
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT groups_seen FROM neg_risk_discovery_state"
        ).fetchone()[0] == 99
