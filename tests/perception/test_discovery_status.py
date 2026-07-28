from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest

from polyarb.cli_discovery import main
from polyarb.perception.store import DiscoveryAdmissionProof, OpportunityPerceptionStore


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
    assert payload["load_control"]["probe_every_cycles"] == 10
    assert payload["admission_control"] is None
    assert payload["candidate_start_control"] == {
        "attempt_start_count": 0,
        "deadline_breach_count": 0,
        "ready": True,
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
    proof = DiscoveryAdmissionProof(
        effective_capacity=2,
        candidate_max_wait_ms=60_000,
        selection_budget_ms=6_000,
        poll_interval_ms=1_000,
        group_timeout_ms=10_000,
        terminal_write_budget_ms=5_000,
        high_burst_groups=1,
        reserved_non_high_slots=2,
    )
    store.configure_discovery_admission(proof, now_ms=0)
    store.publish_discovery_batch(
        requested_cursor=None,
        next_cursor="c-2",
        completed=False,
        started_at_ms=10,
        finished_at_ms=20,
        page_event_count=1,
        candidates=(),
        admission_proof=proof,
    )
    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA journal_mode=WAL")
    original = OpportunityPerceptionStore._coverage_windows_in_snapshot
    writer_done = threading.Event()

    def hooked(con, now_ms, **kwargs):
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
        return original(con, now_ms, **kwargs)

    monkeypatch.setattr(
        OpportunityPerceptionStore,
        "_coverage_windows_in_snapshot",
        staticmethod(hooked),
    )

    status = store.discovery_status(now_ms=100)

    assert status.groups_seen == 0
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT groups_seen FROM neg_risk_discovery_state"
        ).fetchone()[0] == 99


def test_status_rejects_broken_historical_cursor_receipt_chain(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "state.db"
    OpportunityPerceptionStore(db_path).init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_discovery_batches("
            "sweep_id,batch_sequence,requested_cursor,next_cursor,completed,"
            "started_at_ms,finished_at_ms,page_event_count,groups_seen,promoted_count"
            ") VALUES (1,1,'c-1','c-2',0,10,20,0,0,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_discovery_batches("
            "sweep_id,batch_sequence,requested_cursor,next_cursor,completed,"
            "started_at_ms,finished_at_ms,page_event_count,groups_seen,promoted_count"
            ") VALUES (1,2,'broken','c-3',0,30,40,0,0,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'c-3',0,30,40,0,0,0)"
        )

    assert main(["--db-path", str(db_path)]) == 2
    captured = capsys.readouterr()
    assert str(db_path) not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize(
    "first_receipt_update",
    [
        "UPDATE neg_risk_discovery_batches SET sweep_id=7 WHERE id=1",
        "UPDATE neg_risk_discovery_batches SET groups_seen=1 WHERE id=1",
        "UPDATE neg_risk_discovery_batches SET promoted_count=1 WHERE id=1",
        "UPDATE neg_risk_discovery_batches SET started_at_ms=-1 WHERE id=1",
    ],
)
def test_status_rejects_corrupt_non_latest_receipt_and_first_sweep(
    tmp_path: Path,
    capsys,
    first_receipt_update: str,
) -> None:
    db_path = tmp_path / "state.db"
    OpportunityPerceptionStore(db_path).init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_discovery_batches("
            "sweep_id,batch_sequence,requested_cursor,next_cursor,completed,"
            "started_at_ms,finished_at_ms,page_event_count,groups_seen,promoted_count"
            ") VALUES (1,1,'c-1','c-2',0,10,20,0,0,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_discovery_batches("
            "sweep_id,batch_sequence,requested_cursor,next_cursor,completed,"
            "started_at_ms,finished_at_ms,page_event_count,groups_seen,promoted_count"
            ") VALUES (1,2,'c-2','c-3',0,30,40,0,0,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'c-3',0,30,40,0,0,0)"
        )
        con.execute(first_receipt_update)

    assert main(["--db-path", str(db_path)]) == 2
    captured = capsys.readouterr()
    assert str(db_path) not in captured.err
    assert "Traceback" not in captured.err
